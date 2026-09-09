"""Routing tests - valid messages become Bronze rows, invalid ones become
dead-letter payloads that carry the rejection reason and the source offset.
No Kafka broker needed: route_message is pure.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.ingestion.consumer import route_message


def _good() -> dict:
    return {
        "reading_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "patient_id": "P100002",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "heart_rate": 82,
        "resp_rate": 15,
        "systolic_bp": 118,
        "spo2": 97,
        "temperature_c": 37.0,
        "consciousness": "A",
        "on_supplemental_o2": False,
        "source_device": "Masimo-Rad-97",
    }


def test_valid_message_routes_to_bronze_with_kafka_metadata():
    dest, row = route_message(_good(), topic="vitals.raw", partition=2, offset=41)
    assert dest == "bronze"
    assert row["reading_id"] == "aaaaaaaa-0000-0000-0000-000000000001"
    assert row["kafka_partition"] == 2
    assert row["kafka_offset"] == 41
    assert row["consciousness"] == "A"  # enum flattened to its value
    assert "ingested_at" in row


def test_invalid_message_routes_to_deadletter_with_reason():
    bad = _good()
    bad["spo2"] = 250  # out of range
    dest, payload = route_message(bad, topic="vitals.raw", partition=0, offset=7)
    assert dest == "deadletter"
    assert payload["raw"]["spo2"] == 250
    assert any("spo2" in e for e in payload["errors"])
    assert payload["source"] == {"topic": "vitals.raw", "partition": 0, "offset": 7}
    assert payload["rejected_at"]


def test_unknown_field_is_rejected():
    bad = _good()
    bad["notes"] = "patient stable"
    dest, payload = route_message(bad, topic="vitals.raw", partition=1, offset=3)
    assert dest == "deadletter"
    assert any("notes" in e for e in payload["errors"])


def test_non_object_message_is_rejected():
    dest, payload = route_message("not-json-object", topic="vitals.raw", partition=0, offset=0)
    assert dest == "deadletter"
    assert payload["errors"]
