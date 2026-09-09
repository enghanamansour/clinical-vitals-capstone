"""Gold layer - NEWS2 early-warning aggregate per patient per hour.

This is a genuine aggregate, not a filtered copy of Silver: every reading in a
(patient_id, window_start) bucket is scored with NEWS2, then the bucket is
reduced to one row of summary statistics. Row count is far smaller than Silver
(one row per patient-hour, not per reading).

Upsert keyed on (patient_id, window_start) so re-running after more readings
arrive for an open hour refreshes that bucket in place.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from config.settings import settings
from src.lakehouse.news2 import risk_band, score_reading
from src.lakehouse.silver import read_silver

GOLD_SCHEMA = pa.schema(
    [
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("window_start", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("readings_in_window", pa.int32(), nullable=False),
        pa.field("mean_news2", pa.float64(), nullable=False),
        pa.field("max_news2", pa.int32(), nullable=False),
        pa.field("max_single_parameter_score", pa.int32(), nullable=False),
        pa.field("worst_risk_band", pa.string(), nullable=False),
        pa.field("pct_readings_on_oxygen", pa.float64(), nullable=False),
        pa.field("mean_heart_rate", pa.float64(), nullable=False),
        pa.field("min_spo2", pa.int32(), nullable=False),
        pa.field("max_resp_rate", pa.int32(), nullable=False),
        pa.field("min_systolic_bp", pa.int32(), nullable=False),
        pa.field("max_temperature_c", pa.float64(), nullable=False),
        pa.field("first_reading_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_reading_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("computed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

_GOLD_COLS = [f.name for f in GOLD_SCHEMA]


def _uri() -> str:
    return str(settings.gold_path)


def gold_exists() -> bool:
    try:
        DeltaTable(_uri())
        return True
    except Exception:
        return False


def _aggregate(silver_df: pd.DataFrame) -> pd.DataFrame:
    df = silver_df.copy()
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)
    df["window_start"] = pd.to_datetime(df["recorded_hour"], utc=True)

    scored = df.apply(
        lambda r: score_reading(
            {
                "resp_rate": r["resp_rate"],
                "spo2": r["spo2"],
                "on_supplemental_o2": r["on_supplemental_o2"],
                "systolic_bp": r["systolic_bp"],
                "heart_rate": r["heart_rate"],
                "consciousness": r["consciousness"],
                "temperature_c": r["temperature_c"],
            }
        ),
        axis=1,
    )
    df["news2_total"] = [s.total for s in scored]
    df["news2_max_param"] = [s.max_parameter_score for s in scored]

    rows = []
    for (patient_id, window_start), g in df.groupby(["patient_id", "window_start"]):
        max_news2 = int(g["news2_total"].max())
        max_param = int(g["news2_max_param"].max())
        rows.append(
            {
                "patient_id": patient_id,
                "window_start": window_start,
                "readings_in_window": int(len(g)),
                "mean_news2": round(float(g["news2_total"].mean()), 3),
                "max_news2": max_news2,
                "max_single_parameter_score": max_param,
                "worst_risk_band": risk_band(max_news2, max_param),
                "pct_readings_on_oxygen": round(float(g["on_supplemental_o2"].mean()), 3),
                "mean_heart_rate": round(float(g["heart_rate"].mean()), 2),
                "min_spo2": int(g["spo2"].min()),
                "max_resp_rate": int(g["resp_rate"].max()),
                "min_systolic_bp": int(g["systolic_bp"].min()),
                "max_temperature_c": round(float(g["temperature_c"].max()), 2),
                "first_reading_at": g["recorded_at"].min(),
                "last_reading_at": g["recorded_at"].max(),
                "computed_at": datetime.now(timezone.utc),
            }
        )
    return pd.DataFrame(rows, columns=_GOLD_COLS)


def build_gold() -> dict:
    """Recompute every patient-hour bucket from Silver and upsert into Gold."""
    silver_df = read_silver().to_pandas()
    if silver_df.empty:
        return {"gold_rows": 0, "silver_rows": 0}

    agg = _aggregate(silver_df)
    source = pa.Table.from_pandas(agg, schema=GOLD_SCHEMA, preserve_index=False)

    if not gold_exists():
        Path(_uri()).parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(_uri(), source, mode="overwrite", schema=GOLD_SCHEMA)
        metrics = {"num_target_rows_inserted": source.num_rows, "num_target_rows_updated": 0}
    else:
        metrics = (
            DeltaTable(_uri())
            .merge(
                source=source,
                predicate=(
                    "target.patient_id = source.patient_id "
                    "AND target.window_start = source.window_start"
                ),
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    metrics["gold_rows"] = source.num_rows
    metrics["silver_rows"] = len(silver_df)
    return metrics


def read_gold() -> pa.Table:
    return DeltaTable(_uri()).to_pyarrow_table()
