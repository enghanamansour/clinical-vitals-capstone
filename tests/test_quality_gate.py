"""Quality-gate tests - a clean Silver batch passes; a batch that violates a
range, uniqueness, the patient-id pattern, or freshness raises QualityGateError
(which is what halts the DAG before Gold).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from src.quality.expectations import QualityGateError, run_silver_quality_gate


def _silver_df(n: int = 30) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    return pd.DataFrame(
        {
            "reading_id": [f"r{i}" for i in range(n)],
            "patient_id": ["P100001"] * n,
            "recorded_at": [now - timedelta(minutes=i) for i in range(n)],
            "recorded_hour": [(now - timedelta(minutes=i)).replace(minute=0, second=0, microsecond=0)
                              for i in range(n)],
            "heart_rate": [70 + (i % 10) for i in range(n)],
            "resp_rate": [16] * n,
            "systolic_bp": [120] * n,
            "spo2": [97] * n,
            "temperature_c": [36.8] * n,
            "consciousness": ["A"] * n,
            "on_supplemental_o2": [False] * n,
            "source_device": ["dev"] * n,
            "ingested_at": [now] * n,
            "updated_at": [now] * n,
        }
    )


def test_clean_batch_passes():
    result = run_silver_quality_gate(_silver_df())
    assert result.success is True
    assert result.statistics["unsuccessful_expectations"] == 0


def test_out_of_range_heart_rate_fails_the_gate():
    df = _silver_df()
    df.loc[3, "heart_rate"] = 999
    with pytest.raises(QualityGateError) as ei:
        run_silver_quality_gate(df)
    assert any(f["expectation"] == "expect_column_values_to_be_between"
               and f["column"] == "heart_rate" for f in ei.value.failures)


def test_duplicate_business_key_fails_the_gate():
    df = _silver_df()
    df.loc[5, "reading_id"] = df.loc[4, "reading_id"]
    with pytest.raises(QualityGateError) as ei:
        run_silver_quality_gate(df)
    assert any(f["expectation"] == "expect_column_values_to_be_unique" for f in ei.value.failures)


def test_bad_patient_id_fails_the_gate():
    df = _silver_df()
    df.loc[2, "patient_id"] = "patient-2"
    with pytest.raises(QualityGateError):
        run_silver_quality_gate(df)


def test_stale_batch_fails_freshness():
    df = _silver_df()
    df["recorded_at"] = datetime.now(timezone.utc) - timedelta(days=3)
    with pytest.raises(QualityGateError) as ei:
        run_silver_quality_gate(df, max_age_minutes=60)
    assert any(f["expectation"] == "expect_column_max_to_be_between" for f in ei.value.failures)


def test_raise_on_failure_false_returns_result():
    df = _silver_df()
    df.loc[1, "spo2"] = 5
    result = run_silver_quality_gate(df, raise_on_failure=False)
    assert result.success is False
