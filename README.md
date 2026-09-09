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
# --- Stage 1 - synthetic data + data contract -------------------------------
python -m src.generator.synth_vitals --rows 300 --bad-rate 0.15 --seed 7 `
    --out data/raw/vitals_mixed.jsonl
python -m pytest -q                       # contract + routing + schema tests

# --- Stage 2 - Kafka ingestion --------------------------------------------
docker compose -f docker/docker-compose.yml up -d      # Kafka + Kafka UI + Qdrant
python -m src.ingestion.admin                          # create topics
python -m src.ingestion.producer --input data/raw/vitals_mixed.jsonl
python -m src.ingestion.consumer --max-messages 300 --idle-timeout 10
# valid rows -> Delta Bronze at ./lakehouse/bronze/vitals
# malformed  -> Kafka topic vitals.deadletter (with reasons), browse at :8080

# --- Stage 3 - Delta Lakehouse: Silver (MERGE) + Gold (NEWS2) --------------
python -c "from src.lakehouse.silver import build_silver; print(build_silver())"
python -c "from src.lakehouse.gold import build_gold; print(build_gold())"
# Silver: one current row per reading_id (upsert). Gold: NEWS2 early-warning
# aggregate per patient per hour at ./lakehouse/gold/news2_scores

# --- Stage 4 - Great Expectations quality gate on Silver ------------------
python -m src.quality.expectations
# PASS -> prints "14/14 expectations met"; FAIL -> exits 1 (this is what the
# Airflow DAG uses to halt the pipeline before build_gold)

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

**Stage 3** - a 1200-record run (`seed 11`, 12% bad-rate):

```
consumed=1200  ->  bronze=1074   deadletter=126
silver merge   ->  source_rows=1074  inserted=1074  updated=0   (total 1074)
gold           ->  139 rows   (7.7x reduction from Silver)
                   worst_risk_band: {'low': 135, 'high': 4}
```

A follow-up correction batch re-sending 5 `reading_id`s with changed vitals:

```
merge metrics: num_target_rows_updated=5  num_target_rows_inserted=0
silver rows    before=1074  after=1074      (upsert in place, not appended)
```

**Stage 4** - the gate on the live Silver table, then on a deliberately
corrupted batch:

```
quality gate PASSED - 14/14 expectations met

Silver quality gate failed: 3 expectation(s) not met
  - expect_column_values_to_be_unique(reading_id): 2 unexpected
  - expect_column_values_to_be_between(heart_rate): 1 unexpected
  - expect_column_values_to_be_between(spo2): 1 unexpected
# -> QualityGateError raised -> build_gold and the RAG refresh never run
```

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
