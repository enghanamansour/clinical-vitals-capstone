"""Silver MERGE tests - a re-sent reading updates in place (no duplicate row),
and a genuinely new reading is inserted. The table paths are redirected to a
temp dir by monkeypatching the modules' URI helpers.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import src.lakehouse.bronze as bronze_mod
import src.lakehouse.silver as silver_mod


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze_mod, "_table_uri", lambda: str(tmp_path / "bronze"))
    monkeypatch.setattr(silver_mod, "_uri", lambda: str(tmp_path / "silver"))
    return bronze_mod, silver_mod


def _bronze_row(reading_id: str, hr: int, ingested_at: datetime, **over) -> dict:
    base = dict(
        reading_id=reading_id,
        patient_id="P100001",
        recorded_at=datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc),
        heart_rate=hr,
        resp_rate=16,
        systolic_bp=120,
        spo2=98,
        temperature_c=36.9,
        consciousness="A",
        on_supplemental_o2=False,
        source_device="dev",
        ingested_at=ingested_at,
        kafka_partition=0,
        kafka_offset=1,
    )
    base.update(over)
    return base


def test_correction_updates_in_place(lake):
    bronze, silver = lake
    t0 = datetime(2026, 1, 1, 12, 31, tzinfo=timezone.utc)

    bronze.append_bronze([_bronze_row("R1", 80, t0), _bronze_row("R2", 90, t0)])
    silver.build_silver()
    s1 = silver.read_silver().to_pandas().set_index("reading_id")
    assert len(s1) == 2
    assert s1.loc["R1", "heart_rate"] == 80

    # correction: R1 re-sent with hr 125, ingested later
    bronze.append_bronze([_bronze_row("R1", 125, t0 + timedelta(minutes=5))])
    metrics = silver.build_silver()

    s2 = silver.read_silver().to_pandas().set_index("reading_id")
    assert len(s2) == 2, "correction must not add a row"
    assert s2.loc["R1", "heart_rate"] == 125, "correction overwrites in place"
    assert s2.loc["R2", "heart_rate"] == 90, "untouched reading unchanged"
    assert metrics["num_target_rows_updated"] == 1
    assert metrics["num_target_rows_inserted"] == 0


def test_new_reading_is_inserted(lake):
    bronze, silver = lake
    t0 = datetime(2026, 1, 1, 12, 31, tzinfo=timezone.utc)
    bronze.append_bronze([_bronze_row("R1", 80, t0)])
    silver.build_silver()

    bronze.append_bronze([_bronze_row("R3", 77, t0 + timedelta(minutes=10))])
    metrics = silver.build_silver()

    assert metrics["num_target_rows_inserted"] == 1
    assert len(silver.read_silver()) == 2
