"""Pipeline stages wired with OpenLineage events.

Each ``stage_*`` function runs one unit of work inside a ``Lineage.stage(...)``
context, so every stage emits START then COMPLETE (or FAIL). ``run_pipeline``
runs them in order under a single parent run; a failing quality gate raises and
the later stages never run. The Airflow DAG (Stage 7) calls these same
functions, one per task.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.ingestion.admin import ensure_topics
from src.ingestion.consumer import run as consume
from src.ingestion.producer import publish_file
from src.lakehouse.gold import build_gold
from src.lakehouse.silver import build_silver
from src.lineage.emit import Lineage
from src.quality.expectations import run_silver_quality_gate
from src.rag.index import build_index


def stage_ingest(lin: Lineage, input_path: str, max_messages: int = 100_000,
                 idle_timeout: float = 15.0) -> dict:
    with lin.stage("ingest",
                   inputs=["kafka.raw"],
                   outputs=["delta.bronze", "kafka.deadletter"]):
        ensure_topics()
        publish_file(Path(input_path))
        return consume(max_messages=max_messages, idle_timeout=idle_timeout)


def stage_build_silver(lin: Lineage) -> dict:
    with lin.stage("build_silver", inputs=["delta.bronze"], outputs=["delta.silver"]):
        return build_silver()


def stage_quality_gate(lin: Lineage) -> None:
    with lin.stage("quality_gate", inputs=["delta.silver"]):
        run_silver_quality_gate()  # raises QualityGateError -> stage emits FAIL


def stage_build_gold(lin: Lineage) -> dict:
    with lin.stage("build_gold", inputs=["delta.silver"], outputs=["delta.gold"]):
        return build_gold()


def stage_rag_index(lin: Lineage) -> int:
    with lin.stage("rag_index", inputs=["delta.gold"], outputs=["qdrant.index"]):
        return build_index(recreate=True)


def run_pipeline(input_path: str, run_id: str | None = None) -> dict:
    lin = Lineage(run_id=run_id)
    print(f"pipeline run {lin.parent_run_id}")
    results = {"parent_run_id": lin.parent_run_id}
    results["ingest"] = stage_ingest(lin, input_path)
    results["silver"] = stage_build_silver(lin)
    stage_quality_gate(lin)          # halts here on failure
    results["gold"] = stage_build_gold(lin)
    results["rag_index"] = stage_rag_index(lin)
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the full pipeline with lineage events")
    ap.add_argument("--input", default="data/raw/vitals_run.jsonl")
    ap.add_argument("--run-id", default=os.getenv("PIPELINE_RUN_ID"))
    args = ap.parse_args()

    out = run_pipeline(args.input, run_id=args.run_id)
    for key, val in out.items():
        print(f"  {key}: {val}")


if __name__ == "__main__":
    main()
