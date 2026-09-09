# Clinical Vitals Lakehouse & Early-Warning RAG

An end-to-end data engineering pipeline that ingests streaming patient vital-sign
readings, curates them through a Delta Lakehouse, computes a clinical early-warning
score, gates the whole flow on data-quality checks, and answers clinician questions
from grounded clinical-guideline documents.

> **Scope & disclaimer.** This is an educational data-engineering capstone.
> It is a **clinical decision-support / early-warning demonstration — it is not a
> diagnostic tool** and must not be used for real patient care. All data is
> **synthetically generated**; no real patient records are used.

---

## Problem it solves

Hospitals stream large volumes of vital-sign telemetry (heart rate, SpO2, blood
pressure, respiratory rate, temperature, consciousness level). Raw telemetry is
noisy, arrives out of order, and is occasionally malformed. Clinical teams need:

1. A **trustworthy curated store** of vitals with quality guarantees.
2. An **early-warning signal** (NEWS2 aggregate) computed per patient over time.
3. A way to **ask "what does the guideline say about this situation?"** and get an
   answer grounded in real clinical-guideline text, with citations.

This project builds that pipeline with production-style tooling.

---

## Architecture (high level)

```
              synthetic generator
                     |
                     v
     Kafka topic: vitals.raw ──► consumer ──► Pydantic data contract
                     |                              |
             (valid) |                              | (malformed)
                     v                              v
              Delta BRONZE                 Kafka topic: vitals.deadletter
                     |                     (record + rejection reason)
                     v
              Delta SILVER  ──  MERGE (upsert) on business key reading_id
                     |
                     v
         Great Expectations quality gate  ──►  FAIL halts the pipeline
                     |
                     v
              Delta GOLD  ──  NEWS2 early-warning aggregate per patient/window
                     |
                     v
          RAG index refresh (guideline corpus)
                     |
                     v
   Hybrid retrieval (dense + BM25 → RRF) → cross-encoder rerank → grounded answer

  OpenLineage START / COMPLETE / FAIL events are emitted for every stage.
  Apache Airflow orchestrates all stages with the quality gate as a hard dependency.
```

See [docs/architecture.md](docs/architecture.md) for the detailed design.

---

## Tech stack

| Concern        | Library / tool |
|----------------|----------------|
| Ingestion      | `kafka-python` (Kafka broker via Docker) |
| Data contract  | `pydantic` v2 |
| Lakehouse      | `deltalake` (delta-rs) + `pyarrow` |
| Quality gate   | `great-expectations` |
| Lineage        | `openlineage-python` |
| RAG            | `sentence-transformers`, `rank-bm25`, `qdrant-client` |
| Orchestration  | `apache-airflow` (via Docker) |

---

## Prerequisites

- Python 3.11+
- Docker Desktop (for the Kafka broker and Airflow only)
- ~2 GB free disk (embedding + cross-encoder models are downloaded on first run)

## Setup

```powershell
git clone <your-repo-url>
cd clinical-vitals-capstone

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

copy .env.example .env   # then edit values if needed
```

## How to run

> Filled in stage by stage as the pipeline is built. See [docs/ROADMAP.md](docs/ROADMAP.md).

```powershell
# Stage 1 - generate synthetic vitals, then validate against the data contract
python -m src.generator.synth_vitals --rows 300 --bad-rate 0.15 --seed 7 `
    --out data/raw/vitals_mixed.jsonl
python -m pytest -q            # 13 contract tests

# (more stages added as they are implemented)
```

## Expected output

**Stage 1** - the generator reports the valid / malformed split, and `pytest`
shows the contract rejecting every injected fault:

```
wrote 300 records -> data\raw\vitals_mixed.jsonl
total=300  valid=260  malformed=40
  malformed[bad_enum] = 5
  malformed[missing_required] = 4
  ...
13 passed
```

Example rejection reasons produced by the contract:

| Injected fault      | Reason recorded |
|---------------------|-----------------|
| `hr_out_of_range`   | `heart_rate: Input should be greater than or equal to 20` |
| `bad_enum`          | `consciousness: Input should be 'A', 'V', 'P' or 'U'` |
| `unknown_field`     | `diagnosis: Extra inputs are not permitted` |
| `future_timestamp`  | `recorded_at: Value error, ... is in the future` |
| `bad_patient_id`    | `patient_id: String should match pattern '^P\d{6}$'` |

Later stages capture their output under `notebooks/`.

---

## Training program attribution

This project was completed under the **Modern Data Engineering for AI Systems**
program at **SDAIA Academy** (delivered via Learning Space) — a 5-day capstone.

- Trainer: Mohammed Albeladi
- Cohort / session dates: _<fill in your cohort dates>_

SDAIA Academy on GitHub: https://github.com/SDAIAAcademy

---

## License

Educational use.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
