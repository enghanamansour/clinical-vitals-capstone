"""NEWS2 scoring tests - boundary values for each parameter plus the RCP
worked examples.
"""
from __future__ import annotations

import pytest

from src.lakehouse.news2 import (
    _pulse_score,
    _resp_rate_score,
    _spo2_score,
    _systolic_bp_score,
    _temperature_score,
    risk_band,
    score_reading,
)


@pytest.mark.parametrize(
    "rr, expected",
    [(8, 3), (9, 1), (11, 1), (12, 0), (20, 0), (21, 2), (24, 2), (25, 3)],
)
def test_resp_rate_bands(rr, expected):
    assert _resp_rate_score(rr) == expected


@pytest.mark.parametrize("spo2, expected", [(96, 0), (95, 1), (94, 1), (93, 2), (92, 2), (91, 3)])
def test_spo2_bands(spo2, expected):
    assert _spo2_score(spo2) == expected


@pytest.mark.parametrize(
    "sbp, expected",
    [(90, 3), (91, 2), (100, 2), (101, 1), (110, 1), (111, 0), (219, 0), (220, 3)],
)
def test_systolic_bp_bands(sbp, expected):
    assert _systolic_bp_score(sbp) == expected


@pytest.mark.parametrize(
    "pulse, expected",
    [(40, 3), (41, 1), (50, 1), (51, 0), (90, 0), (91, 1), (110, 1), (111, 2), (130, 2), (131, 3)],
)
def test_pulse_bands(pulse, expected):
    assert _pulse_score(pulse) == expected


@pytest.mark.parametrize(
    "temp, expected",
    [(35.0, 3), (35.1, 1), (36.0, 1), (36.1, 0), (38.0, 0), (38.1, 1), (39.0, 1), (39.1, 2)],
)
def test_temperature_bands(temp, expected):
    assert _temperature_score(temp) == expected


def test_healthy_reading_scores_zero():
    r = dict(resp_rate=16, spo2=98, on_supplemental_o2=False, systolic_bp=120,
             heart_rate=72, consciousness="A", temperature_c=36.8)
    res = score_reading(r)
    assert res.total == 0
    assert res.risk_band == "low"


def test_deteriorating_reading_scores_high():
    # RR 28 (3) + SpO2 91 (3) + on O2 (2) + SBP 88 (3) + HR 132 (3) + V (3) + T 39.2 (2)
    r = dict(resp_rate=28, spo2=91, on_supplemental_o2=True, systolic_bp=88,
             heart_rate=132, consciousness="V", temperature_c=39.2)
    res = score_reading(r)
    assert res.total == 19
    assert res.max_parameter_score == 3
    assert res.risk_band == "high"


def test_single_param_three_forces_low_medium():
    # total only 3, but it is all from one parameter (SpO2 91)
    r = dict(resp_rate=16, spo2=91, on_supplemental_o2=False, systolic_bp=120,
             heart_rate=72, consciousness="A", temperature_c=36.8)
    res = score_reading(r)
    assert res.total == 3
    assert res.risk_band == "low-medium"


@pytest.mark.parametrize(
    "total, max_param, band",
    [(0, 0, "low"), (4, 2, "low"), (3, 3, "low-medium"), (5, 2, "medium"),
     (6, 3, "medium"), (7, 3, "high"), (12, 3, "high")],
)
def test_risk_band_mapping(total, max_param, band):
    assert risk_band(total, max_param) == band
