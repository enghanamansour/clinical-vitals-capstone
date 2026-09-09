"""OpenLineage event emission for pipeline stages.

Every stage is wrapped in ``Lineage.stage(...)``, which emits an OpenLineage
**START** event on entry and either **COMPLETE** on success or **FAIL** (with an
``ErrorMessageRunFacet``) if the body raises. All stage runs in one pipeline
execution share a parent run so they group together in a lineage backend.

By default events are written as JSON lines to ``settings.openlineage_file_path``
(no Marquez server required); set ``OPENLINEAGE_TRANSPORT=http`` and
``OPENLINEAGE_URL`` to send them to a real backend instead.
"""
from __future__ import annotations

import os
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import error_message_run, parent_run
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.uuid import generate_new_uuid

from config.settings import settings

PRODUCER = "https://github.com/whdrimal/clinical-vitals-capstone"
_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
PIPELINE_JOB = "clinical_vitals_pipeline"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dataset_registry() -> dict[str, Dataset]:
    ns = settings.openlineage_namespace
    kafka = f"kafka://{settings.kafka_bootstrap_servers}"
    return {
        "kafka.raw": Dataset(namespace=kafka, name=settings.kafka_topic_raw),
        "kafka.deadletter": Dataset(namespace=kafka, name=settings.kafka_topic_deadletter),
        "delta.bronze": Dataset(namespace="file", name=str(settings.bronze_path)),
        "delta.silver": Dataset(namespace="file", name=str(settings.silver_path)),
        "delta.gold": Dataset(namespace="file", name=str(settings.gold_path)),
        "qdrant.index": Dataset(
            namespace=f"qdrant://{settings.qdrant_url}", name=settings.qdrant_collection
        ),
    }


class Lineage:
    def __init__(self, run_id: str | None = None,
                 event_file_path: str | Path | None = None):
        self.namespace = settings.openlineage_namespace
        self.parent_run_id = run_id or os.getenv("PIPELINE_RUN_ID") or str(generate_new_uuid())
        self._event_file_path = Path(event_file_path or settings.openlineage_file_path)
        self._registry = _dataset_registry()
        self._client = self._build_client()

    def _build_client(self) -> OpenLineageClient:
        if settings.openlineage_transport == "http":
            return OpenLineageClient(url=settings.openlineage_url)
        self._event_file_path.parent.mkdir(parents=True, exist_ok=True)
        return OpenLineageClient(
            transport=FileTransport(
                FileConfig(log_file_path=str(self._event_file_path), append=True)
            )
        )

    def datasets(self, keys) -> list[Dataset]:
        return [self._registry[k] for k in keys]

    def _parent_facet(self) -> dict:
        return {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=self.parent_run_id),
                job=parent_run.Job(namespace=self.namespace, name=PIPELINE_JOB),
                producer=PRODUCER,
            )
        }

    def _emit(self, state: RunState, job: str, run_id: str,
              inputs: list[Dataset], outputs: list[Dataset],
              run_facets: dict | None = None) -> None:
        event = RunEvent(
            eventTime=_now(),
            eventType=state,
            producer=PRODUCER,
            run=Run(runId=run_id, facets={**self._parent_facet(), **(run_facets or {})}),
            job=Job(namespace=self.namespace, name=job),
            inputs=inputs,
            outputs=outputs,
        )
        # schemaURL is set by the client library on serialisation
        self._client.emit(event)

    @contextmanager
    def stage(self, job: str, *, inputs=(), outputs=()):
        run_id = str(generate_new_uuid())
        ins = self.datasets(inputs)
        outs = self.datasets(outputs)
        self._emit(RunState.START, job, run_id, ins, outs)
        try:
            yield run_id
        except BaseException as exc:  # noqa: BLE001 - emit FAIL for any failure
            facet = {
                "errorMessage": error_message_run.ErrorMessageRunFacet(
                    message=f"{type(exc).__name__}: {exc}",
                    programmingLanguage="python",
                    stackTrace=traceback.format_exc(),
                    producer=PRODUCER,
                )
            }
            self._emit(RunState.FAIL, job, run_id, ins, outs, run_facets=facet)
            raise
        else:
            self._emit(RunState.COMPLETE, job, run_id, ins, outs)
