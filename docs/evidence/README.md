# Evidence from real runs

Captured output committed so the pipeline can be assessed **without running
anything**. Generated on 2026-09-09 against the Dockerised Kafka + Qdrant +
Airflow stack.

| File | What it shows |
|------|---------------|
| `pipeline_run.log` | full `python -m src.pipeline` run: topic creation, per-record dead-letter routing with reasons, and the stage results (`ingest 1000 -> 894 valid / 106 rejected`, `silver 894`, `gold 126`, `rag_index 22`). |
| `pipeline_failure_run.log` | a poisoned Bronze row makes `run_silver_quality_gate` raise `QualityGateError`; `build_gold` and `rag_index` are not executed. |
| `lineage_events.jsonl` | raw OpenLineage events from the normal run - START/COMPLETE for all five stages, one shared `parent` run facet. |
| `lineage_failure.jsonl` | raw OpenLineage events from the failure path - `build_silver` START/COMPLETE, then `quality_gate` START/**FAIL** with the `errorMessage` facet. |
| `lineage_summary.txt` | both event streams rendered one line per event. |
| `pytest.log` | `94 passed, 2 skipped` (the 2 skips need Airflow / a built Gold table). |
| `airflow_task_states.txt` | `states-for-dag-run` for both Airflow runs: `normal_run_1` all success; `poison_run_1` -> `quality_gate` failed, `build_gold` + `rag_index` `upstream_failed`. |
| `airflow_quality_gate_failure.log` | the real Airflow task log for `quality_gate` @ `poison_run_1`: the `QualityGateError` traceback and `Marking task as FAILED`. |
| `airflow_ingest_success.log` | the real Airflow task log for `ingest` @ `normal_run_1`: Kafka consumer-group coordination, dead-letter routing, `Marking task as SUCCESS`. |
| `deadletter_sample.jsonl` | first 25 of 212 rejected records verbatim - each with `raw`, `errors`, `rejected_at`, `source` offset. |
| `gold_news2_sample.txt` | the Gold table: 126 rows from 894 Silver rows (7.1x), and patient `P100007` deteriorating across four hourly windows (mean NEWS2 5.0 -> 9.7 -> 11.0 -> 15.0, SpO2 93 -> 89). |

Screenshots of the Airflow Graph view for both runs are in
[`../images/`](../images/).
