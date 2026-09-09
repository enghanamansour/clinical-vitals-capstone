# Screenshots

Add the following from the Airflow UI (http://localhost:8081, admin / admin),
DAG `capstone_pipeline`, **Graph** view:

| File | What it should show |
|------|---------------------|
| `airflow_dag_success.png` | `normal_run_1` - all five tasks green (success) |
| `airflow_dag_halted.png`  | `poison_run_1` - `quality_gate` red (failed), `build_gold` and `rag_index` in `upstream_failed` (never ran) |
| `airflow_dag_graph.png`   | the DAG structure: ingest -> build_silver -> quality_gate -> build_gold -> rag_index |

Optional: the Kafka UI (http://localhost:8080) showing the `vitals.deadletter`
topic with rejected records.
