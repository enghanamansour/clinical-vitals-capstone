"""Great Expectations quality gate for the Silver table.

``run_silver_quality_gate`` validates Silver against a fixed expectation suite
and, on any failure, raises ``QualityGateError``. In the Airflow DAG this task
sits between ``build_silver`` and ``build_gold``; because the exception
propagates, a failed gate halts the pipeline before Gold (and the RAG refresh)
run.

Each run's full validation result is written to
``<gx_root>/validations/silver_<timestamp>.json`` as evidence; the expectation
suite itself is written once to ``<gx_root>/expectations/silver_quality.json``.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("TQDM_DISABLE", "1")  # silence GX metric progress bars

import great_expectations as gx  # noqa: E402
import pandas as pd  # noqa: E402

from config.settings import settings  # noqa: E402

SUITE_NAME = "silver_quality"

# Age of the newest reading allowed before the batch is considered stale.
DEFAULT_MAX_AGE_MINUTES = 24 * 60


class QualityGateError(RuntimeError):
    """Raised when one or more Silver expectations fail."""

    def __init__(self, failures: list[dict]):
        self.failures = failures
        lines = [
            f"  - {f['expectation']}({f.get('column', '')}): "
            f"{f['unexpected_count']} unexpected"
            for f in failures
        ]
        super().__init__(
            f"Silver quality gate failed: {len(failures)} expectation(s) not met\n"
            + "\n".join(lines)
        )


def _apply_expectations(validator, max_age_minutes: int) -> None:
    validator.expect_table_row_count_to_be_between(min_value=1)

    validator.expect_column_values_to_not_be_null("reading_id")
    validator.expect_column_values_to_be_unique("reading_id")

    validator.expect_column_values_to_not_be_null("patient_id")
    validator.expect_column_values_to_match_regex("patient_id", r"^P\d{6}$")

    validator.expect_column_values_to_not_be_null("recorded_at")
    validator.expect_column_values_to_not_be_null("on_supplemental_o2")

    validator.expect_column_values_to_be_between("heart_rate", 20, 250)
    validator.expect_column_values_to_be_between("resp_rate", 4, 60)
    validator.expect_column_values_to_be_between("systolic_bp", 50, 260)
    validator.expect_column_values_to_be_between("spo2", 50, 100)
    validator.expect_column_values_to_be_between("temperature_c", 30.0, 45.0)

    validator.expect_column_values_to_be_in_set("consciousness", ["A", "V", "P", "U"])

    # freshness: the newest reading must not be older than max_age_minutes
    validator.expect_column_max_to_be_between("age_minutes", min_value=0,
                                             max_value=max_age_minutes)


def _with_age_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    recorded = pd.to_datetime(df["recorded_at"], utc=True)
    now = pd.Timestamp.now(tz="UTC")
    df["age_minutes"] = (now - recorded).dt.total_seconds() / 60.0
    return df


def _collect_failures(result) -> list[dict]:
    failures = []
    for r in result.results:
        if r.success:
            continue
        cfg = r.expectation_config
        failures.append(
            {
                "expectation": cfg.expectation_type,
                "column": cfg.kwargs.get("column"),
                "unexpected_count": r.result.get("unexpected_count", 0),
                "partial_unexpected": r.result.get("partial_unexpected_list", []),
            }
        )
    return failures


def _persist(result, failures: list[dict]) -> Path:
    out_dir = Path(settings.gx_root) / "validations"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"silver_{ts}.json"
    path.write_text(
        json.dumps(
            {
                "evaluated_at": ts,
                "success": bool(result.success),
                "statistics": result.statistics,
                "failures": failures,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return path


def run_silver_quality_gate(
    df: pd.DataFrame | None = None,
    *,
    max_age_minutes: int = DEFAULT_MAX_AGE_MINUTES,
    raise_on_failure: bool = True,
):
    """Validate Silver. Returns the GX result; raises QualityGateError on failure."""
    if df is None:
        from src.lakehouse.silver import read_silver

        df = read_silver().to_pandas()

    df = _with_age_column(df)

    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_pandas("vitals_silver")
    asset = datasource.add_dataframe_asset("silver")
    batch_request = asset.build_batch_request(dataframe=df)
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name=SUITE_NAME,
    )
    _apply_expectations(validator, max_age_minutes)
    result = validator.validate()

    failures = _collect_failures(result)
    _persist(result, failures)
    _write_suite_doc(validator)

    if not result.success and raise_on_failure:
        raise QualityGateError(failures)
    return result


def _write_suite_doc(validator) -> None:
    out_dir = Path(settings.gx_root) / "expectations"
    out_dir.mkdir(parents=True, exist_ok=True)
    suite = validator.get_expectation_suite(discard_failed_expectations=False)
    (out_dir / f"{SUITE_NAME}.json").write_text(
        json.dumps(suite.to_json_dict(), indent=2, default=str), encoding="utf-8"
    )


if __name__ == "__main__":
    import sys

    try:
        res = run_silver_quality_gate()
        print(f"quality gate PASSED - {res.statistics['successful_expectations']}"
              f"/{res.statistics['evaluated_expectations']} expectations met")
    except QualityGateError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
