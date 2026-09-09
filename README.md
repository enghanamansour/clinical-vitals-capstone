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
| RAG            | `fastembed` (ONNX), `rank-bm25`, `qdrant-client` |
| Orchestration  | `apache-airflow` (via Docker) |

> Embeddings and the cross-encoder run through **fastembed / ONNX Runtime**
> rather than `sentence-transformers` + PyTorch: on this Windows machine Smart
> App Control blocks PyTorch's unsigned native DLLs, while ONNX Runtime's are
> signed and load normally. Same models, no functional difference for the
> pipeline.

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

### 1. Infrastructure

```powershell
docker compose -f docker/docker-compose.yml up -d        # Kafka + Kafka UI + Qdrant
```

### 2. Generate a batch and run the whole pipeline

```powershell
python -m src.generator.synth_vitals --rows 1000 --bad-rate 0.1 --seed 21 `
    --out data/raw/vitals_run.jsonl

python -m src.pipeline --input data/raw/vitals_run.jsonl
```

`src/pipeline.py` runs every stage in order -
`ingest -> build_silver -> quality_gate -> build_gold -> rag_index` - wrapping
each in an OpenLineage span (START / COMPLETE / FAIL to
`./lineage_events.jsonl`). A failing quality gate stops the run before Gold.

Expected tail:

```
pipeline run <uuid>
  ingest:    {'consumed': 1000, 'valid': 894, 'rejected': 106}
  silver:    {'source_rows': 894, 'num_target_rows_inserted': 894, ...}
  gold:      {'gold_rows': 139, 'silver_rows': 894, ...}
  rag_index: 22
```

### 3. Same pipeline under Airflow

```powershell
docker compose -f docker/docker-compose.airflow.yml up -d --build   # UI :8081 admin/admin
docker compose -f docker/docker-compose.airflow.yml exec airflow-scheduler `
  airflow dags trigger capstone_pipeline

# failure path: poison Bronze so the quality gate fails and downstream is skipped
docker compose -f docker/docker-compose.airflow.yml exec airflow-scheduler `
  airflow dags trigger capstone_pipeline --conf '{\"poison\": true}'
```

### 4. Query the RAG pipeline

```powershell
python -m src.rag.cli build                              # chunk -> embed -> Qdrant
python -m src.rag.cli ask "When should sepsis screening be started?"
python -m src.rag.cli explain --patient P100007          # a Gold NEWS2 row -> cited answer
```

### 5. Tests

```powershell
python -m pytest -q            # 98 pass; RAG/Gold integration tests skip without Qdrant
```

## Expected output & evidence

Captured output for every rubric deliverable is in
[docs/RESULTS.md](docs/RESULTS.md). The executed notebooks in
[`notebooks/`](notebooks/) hold the same runs with their output cells saved:

| Notebook | Covers |
|----------|--------|
| `01_ingestion_and_contract.ipynb` | Pydantic contract, Kafka produce/consume, Bronze, dead-letter |
| `02_lakehouse_silver_gold.ipynb`  | Silver MERGE upsert, correction demo, Gold NEWS2 aggregate, schema enforcement |
| `03_quality_gate_and_lineage.ipynb` | Great Expectations gate (pass + fail), OpenLineage START/COMPLETE/FAIL |
| `04_rag_pipeline.ipynb` | chunk -> embed -> Qdrant, hybrid + RRF, cross-encoder rerank, citations, refusal |

Airflow screenshots go in [docs/images/](docs/images/).

---

## Repository layout

```
clinical-vitals-capstone/
├── config/settings.py          # typed .env configuration
├── corpus/                     # 7 clinical-guideline docs for the RAG stage
├── dags/capstone_pipeline.py   # Airflow DAG (deliverable 4)
├── docker/                     # docker-compose for Kafka/Qdrant + Airflow image
├── docs/                       # architecture, roadmap, RESULTS.md, screenshots
├── notebooks/                  # executed evidence notebooks
├── src/
│   ├── contracts/vitals.py     # Pydantic data contract (deliverable 1)
│   ├── generator/synth_vitals.py
│   ├── ingestion/              # admin, producer, consumer, serde (deliverable 1)
│   ├── lakehouse/              # bronze, silver (MERGE), gold (NEWS2), news2 (deliverable 2)
│   ├── quality/expectations.py # Great Expectations gate (deliverable 5)
│   ├── lineage/emit.py         # OpenLineage spans (deliverable 5)
│   ├── rag/                    # chunk, embed, index, search, rerank, answer, cli (deliverable 3)
│   └── pipeline.py             # stages wired with lineage; the Airflow tasks call these
└── tests/                      # 98 tests
```

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
