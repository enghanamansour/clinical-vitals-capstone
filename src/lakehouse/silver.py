"""Silver layer - one clean, current row per reading, keyed on the business key.

Bronze is append-only and may hold duplicates and late corrections (the same
``reading_id`` re-sent with new values). Silver resolves those:

1. Read the Bronze rows newer than what Silver already has (incremental).
2. Collapse duplicates in that slice, keeping the latest by ``ingested_at``.
3. **MERGE** into the Silver Delta table on ``reading_id``:
   matched -> UPDATE (a correction overwrites in place, no new row);
   not matched -> INSERT.

Schema enforcement carries over from Bronze: the Silver table is created with
``SILVER_SCHEMA`` and the merge source is built to that exact schema.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from config.settings import settings
from src.lakehouse.bronze import read_bronze

SILVER_SCHEMA = pa.schema(
    [
        pa.field("reading_id", pa.string(), nullable=False),
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("recorded_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("recorded_hour", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("heart_rate", pa.int32(), nullable=False),
        pa.field("resp_rate", pa.int32(), nullable=False),
        pa.field("systolic_bp", pa.int32(), nullable=False),
        pa.field("spo2", pa.int32(), nullable=False),
        pa.field("temperature_c", pa.float64(), nullable=False),
        pa.field("consciousness", pa.string(), nullable=False),
        pa.field("on_supplemental_o2", pa.bool_(), nullable=False),
        pa.field("source_device", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

_SILVER_COLS = [f.name for f in SILVER_SCHEMA]


def _uri() -> str:
    return str(settings.silver_path)


def silver_exists() -> bool:
    try:
        DeltaTable(_uri())
        return True
    except Exception:
        return False


def _shape_source(bronze_df: pd.DataFrame) -> pa.Table:
    df = bronze_df.copy()
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    # keep the most recently ingested version of each reading in this slice
    df = (
        df.sort_values("ingested_at")
        .drop_duplicates(subset="reading_id", keep="last")
        .reset_index(drop=True)
    )
    df["recorded_hour"] = df["recorded_at"].dt.floor("h")
    df["updated_at"] = datetime.now(timezone.utc)
    return pa.Table.from_pandas(df[_SILVER_COLS], schema=SILVER_SCHEMA, preserve_index=False)


def build_silver(full_refresh: bool = False) -> dict:
    """Upsert Bronze into Silver. Returns merge metrics."""
    bronze_df = read_bronze().to_pandas()

    if silver_exists() and not full_refresh:
        hwm = DeltaTable(_uri()).to_pyarrow_table().column("ingested_at")
        if len(hwm):
            watermark = pd.to_datetime(pd.Series(hwm.to_pylist()), utc=True).max()
            bronze_df["ingested_at"] = pd.to_datetime(bronze_df["ingested_at"], utc=True)
            # strictly greater: rows already merged (ingested_at == watermark) are skipped.
            bronze_df = bronze_df[bronze_df["ingested_at"] > watermark]

    if bronze_df.empty:
        return {"source_rows": 0, "num_target_rows_updated": 0, "num_target_rows_inserted": 0}

    source = _shape_source(bronze_df)

    if not silver_exists():
        Path(_uri()).parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(_uri(), source, mode="overwrite" if full_refresh else "append",
                        schema=SILVER_SCHEMA)
        return {
            "source_rows": source.num_rows,
            "num_target_rows_updated": 0,
            "num_target_rows_inserted": source.num_rows,
        }

    metrics = (
        DeltaTable(_uri())
        .merge(
            source=source,
            predicate="target.reading_id = source.reading_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    metrics["source_rows"] = source.num_rows
    return metrics


def read_silver() -> pa.Table:
    return DeltaTable(_uri()).to_pyarrow_table()
