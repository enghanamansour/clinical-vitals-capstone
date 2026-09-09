"""Contract tests - the happy path passes, and every injected fault is rejected
with a reason. This is the Stage-1 evidence that schema validation actually bites.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.contracts.vitals import validate_record
from src.generator.synth_vitals import generate


def _good_record() -> dict:
    return {
        "reading_id": "11111111-1111-1111-1111-111111111111",
        "patient_id": "P100001",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "heart_rate": 78,
        "resp_rate": 16,
        "systolic_bp": 120,
        "spo2": 98,
        "temperature_c": 36.8,
        "consciousness": "A",
        "on_supplemental_o2": False,
        "source_device": "GE-CARESCAPE-B450",
    }


def test_valid_record_passes():
    reading, reasons = validate_record(_good_record())
    assert reading is not None
    assert reasons == []
    assert reading.patient_id == "P100001"


@pytest.mark.parametrize(
    "mutate, bad_field",
    [
        (lambda r: r.pop("heart_rate"), "heart_rate"),
        (lambda r: r.update(heart_rate=400), "heart_rate"),
        (lambda r: r.update(heart_rate="fast"), "heart_rate"),
        (lambda r: r.update(spo2=3), "spo2"),
        (lambda r: r.update(temperature_c=55.0), "temperature_c"),
        (lambda r: r.update(consciousness="Z"), "consciousness"),
        (lambda r: r.update(patient_id="12345"), "patient_id"),
        (lambda r: r.update(reading_id=""), "reading_id"),
        (lambda r: r.update(diagnosis="sepsis"), "diagnosis"),
        (lambda r: r.update(
            recorded_at=(datetime.now(timezone.utc) + timedelta(days=2)).isoformat()),
         "recorded_at"),
    ],
)
def test_malformed_record_is_rejected_with_reason(mutate, bad_field):
    rec = _good_record()
    mutate(rec)
    reading, reasons = validate_record(rec)
    assert reading is None
    assert reasons, "a rejected record must carry at least one reason"
    assert any(bad_field in r for r in reasons), f"{bad_field} not named in {reasons}"


def test_non_object_payload_is_rejected():
    reading, reasons = validate_record(["not", "an", "object"])
    assert reading is None
    assert reasons


def test_generator_bad_rate_is_respected():
    records = generate(rows=400, bad_rate=0.25, seed=7)
    malformed = [r for r in records if "_corruption" in r]
    # every generated malformed record must actually fail the contract
    for r in malformed:
        clean = {k: v for k, v in r.items() if k != "_corruption"}
        reading, reasons = validate_record(clean)
        assert reading is None, f"generator said bad but contract accepted: {r}"
        assert reasons
    assert 0.15 < len(malformed) / len(records) < 0.35
