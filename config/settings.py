"""Typed access to configuration loaded from the environment / .env file.

Import `settings` from here; do not read os.environ directly elsewhere.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_ROOT = Path(__file__).resolve().parent.parent


def _path(env_key: str, default: str) -> Path:
    raw = os.getenv(env_key, default)
    p = Path(raw)
    return p if p.is_absolute() else (_ROOT / p).resolve()


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "vitals.raw")
    kafka_topic_deadletter: str = os.getenv("KAFKA_TOPIC_DEADLETTER", "vitals.deadletter")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "vitals-ingest")

    # Lakehouse
    bronze_path: Path = _path("BRONZE_PATH", "./lakehouse/bronze/vitals")
    silver_path: Path = _path("SILVER_PATH", "./lakehouse/silver/vitals")
    gold_path: Path = _path("GOLD_PATH", "./lakehouse/gold/news2_scores")

    # Great Expectations
    gx_root: Path = _path("GX_ROOT", "./gx")

    # OpenLineage
    openlineage_namespace: str = os.getenv("OPENLINEAGE_NAMESPACE", "clinical-vitals-capstone")
    openlineage_transport: str = os.getenv("OPENLINEAGE_TRANSPORT", "file")
    openlineage_file_path: Path = _path("OPENLINEAGE_FILE_PATH", "./lineage_events.jsonl")
    openlineage_url: str = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")

    # RAG
    qdrant_url: str = os.getenv("QDRANT_URL", "http://localhost:6333")
    qdrant_collection: str = os.getenv("QDRANT_COLLECTION", "clinical_guidelines")
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    reranker_model: str = os.getenv("RERANKER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")
    corpus_dir: Path = _path("CORPUS_DIR", "./corpus")


settings = Settings()
