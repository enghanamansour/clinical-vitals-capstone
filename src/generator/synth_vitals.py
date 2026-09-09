"""Synthetic vital-sign generator.

Produces physiologically plausible readings for a fixed cohort of patients and,
on demand, deliberately malformed records so the ingestion failure path can be
demonstrated (rubric: "prove the failure paths, not just the happy path").

Examples
--------
Happy-path batch::

    python -m src.generator.synth_vitals --rows 5000 --out data/raw/vitals.jsonl

Batch with ~15% malformed records and a fixed seed (reproducible)::

    python -m src.generator.synth_vitals --rows 300 --bad-rate 0.15 --seed 7 \
        --out data/raw/vitals_mixed.jsonl

Each output line is one JSON object. Malformed lines carry an extra
``"_corruption"`` key naming the injected fault so the consumer's rejection
reasons can be checked against what was actually broken. The consumer ignores
that key (it is stripped before validation in the ingestion stage).
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.contracts.vitals import Consciousness

# --- cohort -----------------------------------------------------------------

_NUM_PATIENTS = 20
_DEVICES = ["Philips-IntelliVue-MX40", "GE-CARESCAPE-B450", "Masimo-Rad-97", "Mindray-uMEC12"]


def _patient_ids(n: int) -> list[str]:
    return [f"P{100000 + i:06d}" for i in range(n)]


def _stable_baseline(patient_id: str) -> dict[str, float]:
    """Deterministic per-patient baseline so a patient looks like themselves."""
    r = random.Random(patient_id)
    return {
        "heart_rate": r.uniform(62, 88),
        "resp_rate": r.uniform(12, 18),
        "systolic_bp": r.uniform(108, 132),
        "spo2": r.uniform(95, 99),
        "temperature_c": r.uniform(36.3, 37.1),
        # ~1 patient in 5 is quietly deteriorating over the batch window
        "deteriorating": r.random() < 0.2,
    }


def _plausible_reading(rng: random.Random, patient_id: str, recorded_at: datetime,
                       baseline: dict[str, float], progress: float) -> dict:
    """One physiologically plausible reading.

    ``progress`` in [0, 1] is how far through the batch window we are; a
    deteriorating patient drifts toward abnormal values as progress -> 1.
    """
    drift = progress if baseline["deteriorating"] else 0.0

    hr = baseline["heart_rate"] + rng.gauss(0, 3) + drift * 45
    rr = baseline["resp_rate"] + rng.gauss(0, 1.2) + drift * 12
    sbp = baseline["systolic_bp"] + rng.gauss(0, 5) - drift * 35
    spo2 = baseline["spo2"] + rng.gauss(0, 0.8) - drift * 10
    temp = baseline["temperature_c"] + rng.gauss(0, 0.15) + drift * 1.6
    on_o2 = drift > 0.5 or rng.random() < 0.05
    consc = Consciousness.ALERT
    if drift > 0.8 and rng.random() < 0.4:
        consc = rng.choice([Consciousness.VOICE, Consciousness.PAIN])

    return {
        "reading_id": str(uuid.uuid4()),
        "patient_id": patient_id,
        "recorded_at": recorded_at.isoformat(),
        "heart_rate": int(round(max(20, min(250, hr)))),
        "resp_rate": int(round(max(4, min(60, rr)))),
        "systolic_bp": int(round(max(50, min(260, sbp)))),
        "spo2": int(round(max(50, min(100, spo2)))),
        "temperature_c": round(max(30.0, min(45.0, temp)), 1),
        "consciousness": consc.value,
        "on_supplemental_o2": bool(on_o2),
        "source_device": rng.choice(_DEVICES),
    }


# --- corruption ------------------------------------------------------------

def _corrupt(rng: random.Random, rec: dict) -> dict:
    """Return a copy of ``rec`` with exactly one injected fault."""
    rec = dict(rec)
    kind = rng.choice([
        "missing_required",
        "hr_out_of_range",
        "spo2_out_of_range",
        "temp_out_of_range",
        "bad_enum",
        "bad_patient_id",
        "future_timestamp",
        "unknown_field",
        "empty_reading_id",
        "wrong_type",
        "structurally_broken",
    ])

    if kind == "missing_required":
        del rec[rng.choice(["heart_rate", "resp_rate", "spo2", "recorded_at"])]
    elif kind == "hr_out_of_range":
        rec["heart_rate"] = rng.choice([-5, 0, 400, 999])
    elif kind == "spo2_out_of_range":
        rec["spo2"] = rng.choice([3, 12, 105, 250])
    elif kind == "temp_out_of_range":
        rec["temperature_c"] = rng.choice([12.0, 55.0, 100.0])
    elif kind == "bad_enum":
        rec["consciousness"] = rng.choice(["Z", "awake", "", "a"])
    elif kind == "bad_patient_id":
        rec["patient_id"] = rng.choice(["12345", "PXYZ", "P12", "patient-1"])
    elif kind == "future_timestamp":
        rec["recorded_at"] = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    elif kind == "unknown_field":
        rec["diagnosis"] = "sepsis"  # extra="forbid" -> rejected
    elif kind == "empty_reading_id":
        rec["reading_id"] = ""
    elif kind == "wrong_type":
        rec["heart_rate"] = "fast"
    elif kind == "structurally_broken":
        return {"payload": [rec.get("reading_id")], "_corruption": kind}

    rec["_corruption"] = kind
    return rec


# --- driver --------------------------------------------------------------

def generate(rows: int, bad_rate: float, seed: int) -> list[dict]:
    rng = random.Random(seed)
    patients = _patient_ids(_NUM_PATIENTS)
    baselines = {p: _stable_baseline(p) for p in patients}
    window_start = datetime.now(timezone.utc) - timedelta(hours=6)

    out: list[dict] = []
    for i in range(rows):
        progress = i / max(rows - 1, 1)
        recorded_at = window_start + timedelta(seconds=int(progress * 6 * 3600) + rng.randint(0, 60))
        patient_id = rng.choice(patients)
        rec = _plausible_reading(rng, patient_id, recorded_at, baselines[patient_id], progress)
        if rng.random() < bad_rate:
            rec = _corrupt(rng, rec)
        out.append(rec)
    return out


def _summarise(records: list[dict]) -> str:
    bad = [r for r in records if "_corruption" in r]
    by_kind: dict[str, int] = {}
    for r in bad:
        by_kind[r["_corruption"]] = by_kind.get(r["_corruption"], 0) + 1
    lines = [f"total={len(records)}  valid={len(records) - len(bad)}  malformed={len(bad)}"]
    for k, v in sorted(by_kind.items()):
        lines.append(f"  malformed[{k}] = {v}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic vital-sign readings")
    ap.add_argument("--rows", type=int, default=2000)
    ap.add_argument("--bad-rate", type=float, default=0.0, help="fraction of malformed records (0..1)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=Path("data/raw/vitals.jsonl"))
    args = ap.parse_args()

    records = generate(args.rows, args.bad_rate, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")

    print(f"wrote {len(records)} records -> {args.out}")
    print(_summarise(records))


if __name__ == "__main__":
    main()
