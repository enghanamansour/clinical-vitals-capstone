"""NEWS2 early-warning score (Royal College of Physicians, 2017).

Pure functions, no IO - this module is the clinical logic behind the Gold
aggregate and is unit-tested against the published worked examples.

The aggregate NEWS2 score is the sum of seven parameter sub-scores. A sub-score
of 3 in any single parameter raises the clinical concern even when the total is
low, so it is carried through to the risk band.
"""
from __future__ import annotations

from dataclasses import dataclass, field


def _resp_rate_score(rr: float) -> int:
    if rr <= 8:
        return 3
    if rr <= 11:
        return 1
    if rr <= 20:
        return 0
    if rr <= 24:
        return 2
    return 3


def _spo2_score(spo2: float) -> int:
    if spo2 >= 96:
        return 0
    if spo2 >= 94:
        return 1
    if spo2 >= 92:
        return 2
    return 3


def _oxygen_score(on_supplemental_o2: bool) -> int:
    return 2 if on_supplemental_o2 else 0


def _systolic_bp_score(sbp: float) -> int:
    if sbp <= 90:
        return 3
    if sbp <= 100:
        return 2
    if sbp <= 110:
        return 1
    if sbp <= 219:
        return 0
    return 3


def _pulse_score(pulse: float) -> int:
    if pulse <= 40:
        return 3
    if pulse <= 50:
        return 1
    if pulse <= 90:
        return 0
    if pulse <= 110:
        return 1
    if pulse <= 130:
        return 2
    return 3


def _consciousness_score(consciousness: str) -> int:
    # ACVPU: Alert -> 0; anything else (Confusion/Voice/Pain/Unresponsive) -> 3
    return 0 if consciousness == "A" else 3


def _temperature_score(temp_c: float) -> int:
    if temp_c <= 35.0:
        return 3
    if temp_c <= 36.0:
        return 1
    if temp_c <= 38.0:
        return 0
    if temp_c <= 39.0:
        return 1
    return 2


@dataclass(frozen=True)
class News2Result:
    subscores: dict[str, int] = field(default_factory=dict)
    total: int = 0
    max_parameter_score: int = 0
    risk_band: str = "low"


def risk_band(total: int, max_parameter_score: int) -> str:
    """Map an aggregate score to a clinical-response band.

    high      : total >= 7  -> emergency response
    medium    : total 5-6   -> urgent response
    low-medium: any single parameter == 3 -> urgent ward review
    low       : total 0-4 with no single parameter == 3
    """
    if total >= 7:
        return "high"
    if total >= 5:
        return "medium"
    if max_parameter_score == 3:
        return "low-medium"
    return "low"


def score_reading(reading: dict) -> News2Result:
    """Score one reading. Expects the Silver/Bronze field names."""
    subscores = {
        "resp_rate": _resp_rate_score(reading["resp_rate"]),
        "spo2": _spo2_score(reading["spo2"]),
        "oxygen": _oxygen_score(bool(reading["on_supplemental_o2"])),
        "systolic_bp": _systolic_bp_score(reading["systolic_bp"]),
        "pulse": _pulse_score(reading["heart_rate"]),
        "consciousness": _consciousness_score(reading["consciousness"]),
        "temperature": _temperature_score(reading["temperature_c"]),
    }
    total = sum(subscores.values())
    max_param = max(subscores.values())
    return News2Result(
        subscores=subscores,
        total=total,
        max_parameter_score=max_param,
        risk_band=risk_band(total, max_param),
    )
