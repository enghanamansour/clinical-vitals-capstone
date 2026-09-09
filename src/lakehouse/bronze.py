"""Bronze layer - the validated landing zone for vital-sign readings.

Bronze is append-only. Every row here has already passed the Pydantic data
contract at the ingestion boundary, so Bronze holds the raw-but-valid history,
including duplicates and late corrections (those are resolved in Silver).

Schema enforcement: the Delta table is created with ``BRONZE_SCHEMA`` and writes
use the default (non-permissive) mode, so a write whose columns or types do not
match is **refused** by delta-rs rather than silently evolving the table. That
refusal is demonstrated in ``tests/test_bronze_schema.py``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, TableNotFoundError

from config.settings import settings

BRONZE_SCHEMA = pa.schema(
    [
        pa.field("reading_id", pa.string(), nullable=False),
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("recorded_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("heart_rate", pa.int32(), nullable=False),
        pa.field("resp_rate", pa.int32(), nullable=False),
        pa.field("systolic_bp", pa.int32(), nullable=False),
        pa.field("spo2", pa.int32(), nullable=False),
        pa.field("temperature_c", pa.float64(), nullable=False),
        pa.field("consciousness", pa.string(), nullable=False),
        pa.field("on_supplemental_o2", pa.bool_(), nullable=False),
        pa.field("source_device", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
    ]
)


def _table_uri() -> str:
    return str(settings.bronze_path)


def bronze_exists() -> bool:
    try:
        DeltaTable(_table_uri())
        return True
    except TableNotFoundError:
        return False


def append_bronze(rows: list[dict]) -> int:
    """Append rows to the Bronze Delta table, creating it on first use.

    Raises ``DeltaError`` (or a pyarrow error) if the rows do not conform to
    ``BRONZE_SCHEMA`` - this is the schema-enforcement guarantee.
    """
    if not rows:
        return 0

    table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
    Path(_table_uri()).parent.mkdir(parents=True, exist_ok=True)

    if bronze_exists():
        # schema_mode is left at its default: a mismatching schema is an error.
        write_deltalake(_table_uri(), table, mode="append")
    else:
        write_deltalake(_table_uri(), table, mode="append", schema=BRONZE_SCHEMA)
    return table.num_rows


def read_bronze() -> "pa.Table":
    return DeltaTable(_table_uri()).to_pyarrow_table()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "BRONZE_SCHEMA",
    "append_bronze",
    "bronze_exists",
    "read_bronze",
    "utcnow",
    "DeltaError",
]
