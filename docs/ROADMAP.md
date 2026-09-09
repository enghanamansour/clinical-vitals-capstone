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
- [ ] **Stage 2 — Kafka ingestion**: `docker/docker-compose.yml` (Kafka + Kafka UI
      + Qdrant), `src/ingestion/producer.py`, `src/ingestion/consumer.py`,
      dead-letter routing. Evidence: log of malformed records landing in
      `vitals.deadletter` with reasons; Bronze contains only valid rows.
- [ ] **Stage 3 — Delta Lakehouse**: `src/lakehouse/{bronze,silver,gold}.py`.
      Silver `DeltaTable.merge` on `reading_id`; Gold NEWS2 aggregate. Evidence:
      re-sent `reading_id` updates in place (not duplicated); a wrong-schema write
      is refused; Gold row count << Silver.
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
