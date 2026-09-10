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
      Evidence: 1000 in -> 894 Silver -> **126 Gold (7.1x reduction)**; a 5-row
      correction batch -> `num_target_rows_updated=5, inserted=0`, Silver row
      count unchanged; delta-rs refuses wrong-type / extra-column writes.
- [x] **Stage 4 — Quality gate**: `src/quality/expectations.py` - 14-expectation
      Silver suite (null/unique business key, patient-id regex, per-vital ranges,
      ACVPU set, freshness via `age_minutes`). `run_silver_quality_gate` raises
      `QualityGateError` on any failure; each run's result is persisted under
      `gx/validations/`. Tests: `test_quality_gate.py` (clean passes; range /
      uniqueness / pattern / freshness each raise). Evidence: live Silver -> 14/14
      met; corrupted batch -> raises listing 3 failed expectations, so `build_gold`
      is skipped.
- [x] **Stage 5 — RAG**: 7 guideline docs in `corpus/`; `src/rag/` -
      `chunk.py` (heading-aware, word-window + overlap), `embed.py` (fastembed
      ONNX `bge-small-en-v1.5` - torch is blocked by Smart App Control),
      `index.py` (Qdrant collection), `search.py` (dense + BM25 fused with
      Reciprocal Rank Fusion), `rerank.py` (fastembed cross-encoder
      `ms-marco-MiniLM-L-6-v2`), `answer.py` (extractive, every sentence cited;
      refuses below a rerank floor), `cli.py` (`build` / `ask` / `explain`).
      `explain` turns a Gold NEWS2 row into the query, linking Stage 3 -> 5.
      Tests: chunking, RRF math, hybrid retrieval hits the right doc, answer is
      grounded + cited, off-topic question refused, Gold-window link.
      Evidence: "sepsis screening" -> cross-encoder promotes `sepsis-screening`
      from RRF rank ~5 to rank 1 (score +2.1); gift-shop question -> refused.
- [x] **Stage 6 — Lineage**: `src/lineage/emit.py` - `Lineage.stage()` context
      emits OpenLineage START on entry, COMPLETE on success, FAIL (with an
      `ErrorMessageRunFacet`) on exception; all stages share one parent run;
      input/output `Dataset`s per stage (Kafka topics, Delta paths, Qdrant
      collection). `src/pipeline.py` wraps every stage and is what the Airflow
      DAG calls. Tests: `test_lineage.py` (START->COMPLETE, START->FAIL + facet
      + re-raise, shared parent). Evidence: a full run writes 10 events (5 stages
      x START/COMPLETE); a poisoned Silver -> `quality_gate` emits START then FAIL
      and `build_gold` / `rag_index` emit nothing.
- [x] **Stage 7 — Airflow DAG**: `dags/capstone_pipeline.py` (TaskFlow) -
      `ingest >> build_silver >> quality_gate >> build_gold >> rag_index`, each
      task calling the matching `src.pipeline.stage_*`; all tasks derive one
      OpenLineage parent run from the Airflow `run_id`. `docker/airflow/Dockerfile`
      + `docker/docker-compose.airflow.yml` (LocalExecutor, Postgres, attaches to
      the base stack network). `poison` param injects a bad Bronze row to force
      the gate to fail. Tests: `test_dag.py` (static chain check always; full
      DagBag parse + downstream edges when Airflow is importable). Evidence: a
      normal run - all 5 tasks success; a `poison=true` run - `quality_gate`
      failed, `build_gold` + `rag_index` `upstream_failed` (never ran).
- [x] **Stage 8 — Evidence pass & docs**: 4 executed notebooks under
      `notebooks/` (contract+ingestion, lakehouse, quality+lineage, RAG), all
      code cells carrying saved output; `docs/RESULTS.md` consolidating captured
      output per rubric deliverable; README rewritten with an end-to-end run
      guide, repository layout, and evidence pointers; `docs/images/` with a
      note on which Airflow screenshots to attach.

---

All five rubric deliverables are implemented against the real libraries
(kafka-python, deltalake, great-expectations, openlineage-python, apache-airflow,
Qdrant) with executed evidence and proven failure paths. Remaining owner tasks:
add the Airflow screenshots to `docs/images/`, fill the cohort dates in the
README, and push to GitHub.
