"""Airflow DAG that wires the whole capstone pipeline together.

    ingest -> build_silver -> quality_gate -> build_gold -> rag_index

Task dependencies are linear and use the default ``all_success`` trigger rule,
so if ``quality_gate`` fails, ``build_gold`` and ``rag_index`` are marked
``upstream_failed`` and never run - a failed quality gate halts the pipeline
before any downstream stage.

Every task calls the matching ``src.pipeline.stage_*`` function, which emits the
OpenLineage START / COMPLETE / FAIL events. All tasks in one DAG run derive the
same OpenLineage parent run id from the Airflow ``run_id``.

Params
------
- ``input_path``: JSONL batch the ingest stage publishes (default points at a
  file mounted from the project).
- ``poison``: when true, ingest also writes one out-of-range row straight into
  Bronze so the quality gate fails - use it to demonstrate the halt.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from airflow.decorators import dag, task

_NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")


def _lineage(context):
    from src.lineage.emit import Lineage

    return Lineage(run_id=str(uuid.uuid5(_NS, context["run_id"])))


def _inject_poison() -> None:
    from datetime import timezone

    from src.lakehouse.bronze import append_bronze

    now = datetime.now(timezone.utc)
    append_bronze([dict(
        reading_id=f"POISON-{now.timestamp()}", patient_id="P100001",
        recorded_at=now, heart_rate=999, resp_rate=16, systolic_bp=120, spo2=97,
        temperature_c=36.8, consciousness="A", on_supplemental_o2=False,
        source_device="poison", ingested_at=now, kafka_partition=0, kafka_offset=-1,
    )])


@dag(
    dag_id="capstone_pipeline",
    description="Vitals ingestion -> Delta lakehouse -> quality gate -> NEWS2 gold -> RAG index",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["capstone", "vitals"],
    params={
        "input_path": "/opt/project/data/raw/vitals_run.jsonl",
        "poison": False,
    },
)
def capstone_pipeline():
    @task
    def ingest(**context):
        from src.pipeline import stage_ingest

        if context["params"]["poison"]:
            _inject_poison()
        return stage_ingest(_lineage(context), context["params"]["input_path"])

    @task
    def build_silver(**context):
        from src.pipeline import stage_build_silver

        return stage_build_silver(_lineage(context))

    @task
    def quality_gate(**context):
        from src.pipeline import stage_quality_gate

        # raises QualityGateError on failure -> this task fails ->
        # build_gold and rag_index are skipped (upstream_failed)
        stage_quality_gate(_lineage(context))

    @task
    def build_gold(**context):
        from src.pipeline import stage_build_gold

        return stage_build_gold(_lineage(context))

    @task
    def rag_index(**context):
        from src.pipeline import stage_rag_index

        return stage_rag_index(_lineage(context))

    i = ingest()
    s = build_silver()
    q = quality_gate()
    g = build_gold()
    r = rag_index()

    i >> s >> q >> g >> r


capstone_pipeline()
