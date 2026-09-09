"""Lineage tests - a stage emits START then COMPLETE on success and START then
FAIL (with an error facet) on exception, all sharing one parent run.
"""
from __future__ import annotations

import json

import pytest

from src.lineage.emit import Lineage


@pytest.fixture
def lineage(tmp_path):
    path = tmp_path / "events.jsonl"
    lin = Lineage(run_id="00000000-0000-0000-0000-0000000000aa", event_file_path=path)
    return lin, path


def _events(path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_successful_stage_emits_start_then_complete(lineage):
    lin, path = lineage
    with lin.stage("build_silver", inputs=["delta.bronze"], outputs=["delta.silver"]) as run_id:
        assert run_id

    ev = _events(path)
    assert [e["eventType"] for e in ev] == ["START", "COMPLETE"]
    assert ev[0]["job"]["name"] == "build_silver"
    assert ev[0]["run"]["runId"] == ev[1]["run"]["runId"]
    assert ev[0]["inputs"][0]["name"].replace("\\", "/").endswith("bronze/vitals")
    assert ev[1]["outputs"][0]["name"].replace("\\", "/").endswith("silver/vitals")
    assert ev[0]["producer"]


def test_failing_stage_emits_fail_with_error_facet_and_reraises(lineage):
    lin, path = lineage
    with pytest.raises(ValueError, match="boom"):
        with lin.stage("quality_gate", inputs=["delta.silver"]):
            raise ValueError("boom")

    ev = _events(path)
    assert [e["eventType"] for e in ev] == ["START", "FAIL"]
    facet = ev[1]["run"]["facets"]["errorMessage"]
    assert "boom" in facet["message"]
    assert facet["programmingLanguage"] == "python"
    assert "ValueError" in facet["stackTrace"]


def test_all_stages_share_the_parent_run(lineage):
    lin, path = lineage
    with lin.stage("build_silver", inputs=["delta.bronze"], outputs=["delta.silver"]):
        pass
    with lin.stage("build_gold", inputs=["delta.silver"], outputs=["delta.gold"]):
        pass

    parents = {
        e["run"]["facets"]["parent"]["run"]["runId"] for e in _events(path)
    }
    assert parents == {"00000000-0000-0000-0000-0000000000aa"}
    jobs = [e["job"]["name"] for e in _events(path)]
    assert jobs == ["build_silver", "build_silver", "build_gold", "build_gold"]
