"""Pydantic data contract enforced at the ingestion boundary.

Every record pulled off the raw Kafka topic is validated against
``VitalSignReading`` before anything is written to the lakehouse. A record that
fails validation is routed to the dead-letter topic together with the list of
rejection reasons produced by :func:`validate_record` - it never reaches Bronze.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

# Tolerance for clock skew between the sending device and the ingestion host.
_FUTURE_SKEW = timedelta(minutes=5)


class Consciousness(str, Enum):
    """ACVPU consciousness scale (the 'C' in NEWS2)."""

    ALERT = "A"
    VOICE = "V"
    PAIN = "P"
    UNRESPONSIVE = "U"


class VitalSignReading(BaseModel):
    """Contract for a single incoming vital-sign measurement event.

    ``reading_id`` is the business key: a late correction re-sends the same
    ``reading_id`` with updated values, which the Silver-layer MERGE upserts.
    """

    # extra="forbid" -> unknown fields are a contract violation, not silently kept.
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    reading_id: str = Field(..., min_length=1, description="UUID string; business key")
    patient_id: str = Field(..., pattern=r"^P\d{6}$", description="P followed by 6 digits")
    recorded_at: datetime = Field(..., description="ISO-8601; must not be in the future")

    heart_rate: int = Field(..., ge=20, le=250, description="beats/min")
    resp_rate: int = Field(..., ge=4, le=60, description="breaths/min")
    systolic_bp: int = Field(..., ge=50, le=260, description="mmHg")
    spo2: int = Field(..., ge=50, le=100, description="oxygen saturation %")
    temperature_c: float = Field(..., ge=30.0, le=45.0, description="degrees Celsius")

    consciousness: Consciousness
    on_supplemental_o2: bool
    source_device: str = Field(..., min_length=1, max_length=100)

    @field_validator("reading_id", "source_device")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be blank")
        return v

    @field_validator("recorded_at")
    @classmethod
    def _not_in_future(cls, v: datetime) -> datetime:
        v_utc = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if v_utc > now + _FUTURE_SKEW:
            raise ValueError(f"recorded_at {v_utc.isoformat()} is in the future")
        return v_utc


def validate_record(raw: Any) -> tuple[VitalSignReading | None, list[str]]:
    """Validate one raw record against the contract.

    Returns ``(reading, [])`` when valid, or ``(None, reasons)`` when not, where
    ``reasons`` is a flat list of human-readable strings such as
    ``"heart_rate: Input should be less than or equal to 250"``.
    """
    if not isinstance(raw, dict):
        return None, [f"record: expected a JSON object, got {type(raw).__name__}"]
    try:
        return VitalSignReading.model_validate(raw), []
    except ValidationError as exc:
        reasons = [
            f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
            for err in exc.errors()
        ]
        return None, reasons


def to_bronze_row(reading: VitalSignReading, *, ingested_at: datetime | None = None) -> dict[str, Any]:
    """Flatten a validated reading into the dict shape written to Bronze."""
    row = reading.model_dump()
    row["consciousness"] = reading.consciousness.value
    row["recorded_at"] = reading.recorded_at
    row["ingested_at"] = ingested_at or datetime.now(timezone.utc)
    return row
