"""Gold aggregate tests - Gold is a real reduction of Silver (far fewer rows,
one per patient-hour), NEWS2 stats are correct, and a re-run after new readings
refreshes the bucket in place.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import src.lakehouse.bronze as bronze_mod
import src.lakehouse.gold as gold_mod
import src.lakehouse.silver as silver_mod


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze_mod, "_table_uri", lambda: str(tmp_path / "bronze"))
    monkeypatch.setattr(silver_mod, "_uri", lambda: str(tmp_path / "silver"))
    monkeypatch.setattr(gold_mod, "_uri", lambda: str(tmp_path / "gold"))
    return bronze_mod, silver_mod, gold_mod


def _row(reading_id, patient_id, recorded_at, ingested_at, **over):
    base = dict(
        reading_id=reading_id, patient_id=patient_id, recorded_at=recorded_at,
        heart_rate=72, resp_rate=16, systolic_bp=120, spo2=98, temperature_c=36.8,
        consciousness="A", on_supplemental_o2=False, source_device="dev",
        ingested_at=ingested_at, kafka_partition=0, kafka_offset=1,
    )
    base.update(over)
    return base


def test_gold_is_a_reduction_of_silver(lake):
    bronze, silver, gold = lake
    base = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    rows = []
    # 2 patients x 2 hours x 15 readings = 60 Silver rows -> 4 Gold rows
    for p in ("P100001", "P100002"):
        for hour in (0, 1):
            for i in range(15):
                ts = base + timedelta(hours=hour, minutes=i * 3)
                rows.append(_row(f"{p}-{hour}-{i}", p, ts, ts))
    bronze.append_bronze(rows)
    silver.build_silver()

    metrics = gold.build_gold()
    g = gold.read_gold().to_pandas()

    assert metrics["silver_rows"] == 60
    assert len(g) == 4
    assert set(g["readings_in_window"]) == {15}
    assert (g["worst_risk_band"] == "low").all()  # all-healthy readings


def test_gold_reflects_a_deteriorating_window(lake):
    bronze, silver, gold = lake
    base = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    rows = [_row("a", "P100003", base + timedelta(minutes=1), base + timedelta(minutes=1))]
    # one severely abnormal reading in the same hour
    rows.append(_row(
        "b", "P100003", base + timedelta(minutes=30), base + timedelta(minutes=30),
        resp_rate=30, spo2=88, on_supplemental_o2=True, systolic_bp=85,
        heart_rate=140, consciousness="P", temperature_c=39.5,
    ))
    bronze.append_bronze(rows)
    silver.build_silver()
    gold.build_gold()

    g = gold.read_gold().to_pandas().iloc[0]
    assert g["readings_in_window"] == 2
    assert g["max_news2"] >= 15
    assert g["worst_risk_band"] == "high"
    assert g["min_spo2"] == 88
    assert g["pct_readings_on_oxygen"] == 0.5


def test_gold_rerun_refreshes_bucket_in_place(lake):
    bronze, silver, gold = lake
    base = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    bronze.append_bronze([_row("x1", "P100004", base + timedelta(minutes=2),
                                base + timedelta(minutes=2))])
    silver.build_silver()
    gold.build_gold()
    assert gold.read_gold().to_pandas().iloc[0]["readings_in_window"] == 1

    # a later reading lands in the same hour
    bronze.append_bronze([_row("x2", "P100004", base + timedelta(minutes=40),
                                base + timedelta(minutes=40), heart_rate=95)])
    silver.build_silver()
    gold.build_gold()

    g = gold.read_gold().to_pandas()
    assert len(g) == 1, "same patient-hour stays one row"
    assert g.iloc[0]["readings_in_window"] == 2
