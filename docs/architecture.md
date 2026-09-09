# Architecture & Design

## 1. Domain

Streaming **patient vital-sign readings** flow into the pipeline. Each reading is a
single measurement event for one patient at one point in time:

| Field                 | Type      | Notes / valid range |
|-----------------------|-----------|---------------------|
| `reading_id`          | str (UUID)| **business key** for MERGE / upsert |
| `patient_id`          | str       | `P` + 6 digits |
| `recorded_at`         | datetime  | ISO-8601, not in the future |
| `heart_rate`          | int       | 20–250 bpm |
| `resp_rate`           | int       | 4–60 /min |
| `systolic_bp`         | int       | 50–260 mmHg |
| `spo2`                | int       | 50–100 % |
| `temperature_c`       | float     | 30.0–45.0 °C |
| `consciousness`       | enum      | `A`, `V`, `P`, `U` (ACVPU scale) |
| `on_supplemental_o2`  | bool      | — |
| `source_device`       | str       | free text |
| `ingested_at`         | datetime  | set by the consumer |

A **late correction** re-sends the same `reading_id` with updated values — this is
what the Silver MERGE handles.

## 2. Stages

### Stage 1 — Ingestion (rubric #1, 20 pts)
- `src/generator/synth_vitals.py` produces physiologically plausible readings and,
  on demand, **deliberately malformed** ones (out-of-range numbers, missing keys,
  bad enums, future timestamps).
- `src/ingestion/producer.py` publishes raw JSON to Kafka topic `vitals.raw`.
- `src/ingestion/consumer.py` reads `vitals.raw`, validates each record against the
  **Pydantic data contract** `src/contracts/vitals.py`.
  - Valid  -> written to Delta **Bronze**.
  - Invalid -> published to `vitals.deadletter` with `{ "raw": ..., "errors": [...],
    "rejected_at": ... }`. Nothing malformed reaches Bronze.

### Stage 2 — Delta Lakehouse (rubric #2, 25 pts)
- **Bronze** (`deltalake`): append-only landing of validated records, plus ingest
  metadata. Schema is fixed; a write with the wrong schema is refused
  (`schema_mode` NOT set to overwrite) — this is the schema-enforcement proof.
- **Silver**: deduplicated, type-normalised. Built with a real
  `DeltaTable.merge(...)` **keyed on `reading_id`**:
  `WHEN MATCHED UPDATE` (late corrections) / `WHEN NOT MATCHED INSERT`.
- **Gold**: `news2_scores` — a **genuine aggregate**. For each patient and each
  1-hour tumbling window it computes the **NEWS2** early-warning score (sum of the
  six sub-scores + O2 supplement flag), the max sub-score, and a risk band
  (`low` / `low-medium` / `medium` / `high`). Row count is far smaller than Silver;
  it is not a copy.

### Stage 3 — Quality gate (rubric #5, part of 15 pts)
- `src/quality/expectations.py` runs a **Great Expectations** checkpoint against the
  Silver table **before Gold is allowed to build**:
  - business key not null / unique
  - each vital within its valid range
  - `recorded_at` not null and not in the future
  - freshness: newest row within N minutes
- In Airflow the Gold task has `trigger_rule=all_success`; the GE task raises on
  failure, so **Gold and everything downstream never runs** on a bad batch.

### Stage 4 — RAG (rubric #3, 25 pts)
Corpus: `corpus/` holds open clinical-guideline text (e.g. NEWS2 escalation
guidance, sepsis screening, hypoxaemia management) as Markdown, each with a
front-matter `source` citation.

1. **Chunk** — `src/rag/chunk.py`, ~130-word windows with 30-word overlap,
   headings kept.
2. **Embed** — `src/rag/embed.py`, `fastembed` (ONNX Runtime) with
   `BAAI/bge-small-en-v1.5` (384-dim). fastembed is used instead of
   `sentence-transformers` + PyTorch because Windows Smart App Control blocks
   PyTorch's unsigned native DLLs; ONNX Runtime's are signed.
3. **Index** — `src/rag/index.py`, vectors + raw text into **Qdrant**; BM25 index
   built in-process over the same chunks.
4. **Hybrid search** — `src/rag/search.py`: dense top-k from Qdrant + BM25 top-k,
   fused with **Reciprocal Rank Fusion** (`k=60`).
5. **Rerank** — `src/rag/rerank.py`: **cross-encoder** `ms-marco-MiniLM-L-6-v2`
   re-scores the fused candidates; keep top-n.
6. **Answer** — `src/rag/answer.py`: builds a grounded answer from the top chunks
   and returns **inline citations** (`[source, chunk_id]`). Refuses to answer when
   retrieval score is below a floor.

The pipeline links to Stage 2: a high-risk NEWS2 row's condition (e.g. "SpO2 92% on
oxygen") is turned into a retrieval query so the answer cites the matching
escalation guideline.

### Stage 5 — Orchestration (rubric #4, 15 pts)
`dags/capstone_pipeline.py` — one Airflow DAG:

```
generate >> produce >> consume >> bronze >> silver >> quality_gate >> gold >> rag_refresh
                                                          |
                                                     (on fail) -> pipeline stops
```

Task dependencies are explicit; `quality_gate` failure halts the run before `gold`.

### Stage 6 — Lineage (rubric #5, part of 15 pts)
`src/lineage/emit.py` wraps each stage: emits OpenLineage **START** on entry,
**COMPLETE** on success, **FAIL** (with the exception message) on error, with
input/output dataset facets (Kafka topic, Delta path).

## 3. Repository layout

```
clinical-vitals-capstone/
├── README.md
├── requirements.txt
├── .env.example
├── docker/                  # docker-compose for Kafka + Qdrant + Airflow
├── config/settings.py       # loads .env into typed settings
├── src/
│   ├── contracts/vitals.py
│   ├── generator/synth_vitals.py
│   ├── ingestion/{producer,consumer}.py
│   ├── lakehouse/{bronze,silver,gold}.py
│   ├── quality/expectations.py
│   ├── lineage/emit.py
│   └── rag/{chunk,embed,index,search,rerank,answer}.py
├── dags/capstone_pipeline.py
├── corpus/                  # clinical-guideline source docs (committed)
├── notebooks/               # executed, output captured - the evidence
└── tests/
```

## 4. Key configuration / environment variables

All via `.env` (see `.env.example`): Kafka bootstrap + topic names, Delta table
paths, GX root, OpenLineage transport, Qdrant URL + collection, embedding and
reranker model names, corpus directory.
