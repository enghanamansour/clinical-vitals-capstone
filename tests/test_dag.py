"""DAG structure tests.

The full parse test needs Airflow (only installed in the Airflow image), so it
skips locally. The static test always runs: it reads the DAG source and checks
the five stages are wired in a single linear chain ending
ingest -> ... -> quality_gate -> build_gold -> rag_index.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parent.parent / "dags" / "capstone_pipeline.py"

EXPECTED_TASKS = {"ingest", "build_silver", "quality_gate", "build_gold", "rag_index"}


def test_dag_defines_the_five_stages_statically():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    task_fns = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            (isinstance(d, ast.Name) and d.id == "task")
            or (isinstance(d, ast.Attribute) and d.attr == "task")
            for d in node.decorator_list
        )
    }
    assert task_fns == EXPECTED_TASKS


def test_dag_chains_gate_before_gold_statically():
    src = DAG_FILE.read_text(encoding="utf-8")
    assert "i >> s >> q >> g >> r" in src, "stages must form one linear chain"
    # quality gate task must not swallow the failure
    assert "stage_quality_gate(_lineage(context))" in src


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("airflow") is None,
    reason="Airflow only installed in the Airflow image",
)
def test_dag_parses_with_airflow():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert not dagbag.import_errors, dagbag.import_errors
    dag = dagbag.get_dag("capstone_pipeline")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == EXPECTED_TASKS

    downstream = {t.task_id: set(t.downstream_task_ids) for t in dag.tasks}
    assert downstream["ingest"] == {"build_silver"}
    assert downstream["build_silver"] == {"quality_gate"}
    assert downstream["quality_gate"] == {"build_gold"}
    assert downstream["build_gold"] == {"rag_index"}
    assert downstream["rag_index"] == set()

    gate = dag.get_task("quality_gate")
    gold = dag.get_task("build_gold")
    assert gate.trigger_rule == "all_success"
    assert gold.trigger_rule == "all_success"
