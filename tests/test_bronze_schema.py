"""Schema-enforcement proof for the Bronze Delta table.

A conforming batch is written and read back; a batch with a wrong column type and
a batch with an extra column are both refused. Uses a temp table path so it does
not touch the real lakehouse.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from src.lakehouse.bronze import BRONZE_SCHEMA


def _row(**over) -> dict:
    base = dict(
        reading_id="r1",
        patient_id="P100001",
        recorded_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        heart_rate=80,
        resp_rate=16,
        systolic_bp=120,
        spo2=98,
        temperature_c=36.9,
        consciousness="A",
        on_supplemental_o2=False,
        source_device="dev",
        ingested_at=datetime.now(timezone.utc),
        kafka_partition=0,
        kafka_offset=1,
    )
    base.update(over)
    return base


def test_conforming_write_round_trips(tmp_path):
    uri = str(tmp_path / "bronze")
    table = pa.Table.from_pylist([_row()], schema=BRONZE_SCHEMA)
    write_deltalake(uri, table, mode="append", schema=BRONZE_SCHEMA)

    back = DeltaTable(uri).to_pyarrow_table()
    assert back.num_rows == 1
    assert back.schema.field("heart_rate").type == pa.int32()


def test_wrong_type_is_refused(tmp_path):
    uri = str(tmp_path / "bronze")
    write_deltalake(uri, pa.Table.from_pylist([_row()], schema=BRONZE_SCHEMA),
                    mode="append", schema=BRONZE_SCHEMA)

    # heart_rate as a string instead of int32
    bad_schema = pa.schema(
        [f if f.name != "heart_rate" else pa.field("heart_rate", pa.string(), nullable=False)
         for f in BRONZE_SCHEMA]
    )
    bad = pa.Table.from_pylist([_row(heart_rate="fast")], schema=bad_schema)
    with pytest.raises(Exception):
        write_deltalake(uri, bad, mode="append")


def test_extra_column_is_refused(tmp_path):
    uri = str(tmp_path / "bronze")
    write_deltalake(uri, pa.Table.from_pylist([_row()], schema=BRONZE_SCHEMA),
                    mode="append", schema=BRONZE_SCHEMA)

    wide_schema = pa.schema(list(BRONZE_SCHEMA) + [pa.field("triage_note", pa.string())])
    wide = pa.Table.from_pylist([{**_row(), "triage_note": "unexpected"}], schema=wide_schema)
    with pytest.raises(Exception):
        write_deltalake(uri, wide, mode="append")
