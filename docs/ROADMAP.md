# Build Roadmap

Each stage is one meaningful commit (incremental history, not a bulk upload).
Check off as we go.

- [x] **Stage 0 — Scaffold**: repo structure, README, .gitignore, requirements,
      .env.example, architecture doc. Install Docker Desktop.
- [x] **Stage 1 — Data contract + generator**: `src/contracts/vitals.py` (Pydantic
      v2, `extra="forbid"`, range + pattern + not-in-future checks),
      `src/generator/synth_vitals.py` (plausible cohort + 11 injected fault types),
      `tests/test_contract.py` (13 tests). Runs on Windows, no Docker. Evidence:
      `pytest` green; every corruption type rejected with a named reason.
- [x] **Stage 2 — Kafka ingestion**: `docker/docker-compose.yml` (Kafka KRaft +
      Kafka UI + Qdrant), `src/ingestion/admin.py` (explicit topic creation),
      `src/ingestion/producer.py` (JSONL -> `vitals.raw`, keyed by patient),
      `src/ingestion/consumer.py` (bounded batch: contract check -> Bronze /
      dead-letter), `src/lakehouse/bronze.py` (typed Delta schema + enforced
      append). Tests: `test_ingestion.py` (routing), `test_bronze_schema.py`
      (wrong type / extra column refused by delta-rs). Evidence: run log showing
      malformed records in `vitals.deadletter` with reasons + source offset,
      Bronze row count == valid count only.
- [x] **Stage 3 — Delta Lakehouse**: `src/lakehouse/news2.py` (RCP NEWS2 scoring,
      pure), `src/lakehouse/silver.py` (incremental watermark + `DeltaTable.merge`
      on `reading_id`, matched->update / not-matched->insert),
      `src/lakehouse/gold.py` (NEWS2 aggregate per patient-hour, upsert on
      `(patient_id, window_start)`). Tests: `test_news2.py` (band boundaries + RCP
      examples), `test_silver_merge.py` (correction updates in place),
      `test_gold_aggregate.py` (Gold is a reduction, deterioration reflected).
      Evidence: 1200 in -> 1074 Silver -> **139 Gold (7.7x reduction)**; a 5-row
      correction batch -> `num_target_rows_updated=5, inserted=0`, Silver row
      count unchanged; delta-rs refuses wrong-type / extra-column writes.
- [ ] **Stage 4 — Quality gate**: `src/quality/expectations.py` (Great
      Expectations checkpoint on Silver). Evidence: a bad batch fails the
      checkpoint and Gold does not build.
- [ ] **Stage 5 — RAG**: `corpus/` docs + `src/rag/*`. Hybrid (dense + BM25) →
      RRF → cross-encoder rerank → grounded answer with citations. Evidence:
      notebook with a question, the retrieved chunks, RRF + rerank scores, and the
      cited answer.
- [ ] **Stage 6 — Lineage**: `src/lineage/emit.py`. Evidence: `lineage_events.jsonl`
      with START/COMPLETE per stage and a FAIL event from a forced failure.
- [ ] **Stage 7 — Airflow DAG**: `dags/capstone_pipeline.py` + Airflow service in
      docker-compose. Evidence: green DAG run screenshot; a run where the quality
      gate fails and downstream tasks are skipped.
- [ ] **Stage 8 — Evidence pass & docs**: execute all notebooks with output,
      finalise README run/output sections, screenshots, failure-path proofs.
