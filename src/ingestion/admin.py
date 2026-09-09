"""Create the Kafka topics the pipeline needs.

Auto-topic-creation is disabled on the broker, so every stage that talks to Kafka
can rely on the topics existing with the right partition count. Idempotent:
topics that already exist are left alone.

    python -m src.ingestion.admin
"""
from __future__ import annotations

import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config.settings import settings

TOPICS = [
    NewTopic(name=settings.kafka_topic_raw, num_partitions=3, replication_factor=1),
    NewTopic(name=settings.kafka_topic_deadletter, num_partitions=1, replication_factor=1),
]


def ensure_topics() -> list[str]:
    """Create any missing topics. Returns the full list of topic names."""
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="capstone-admin",
    )
    try:
        for topic in TOPICS:
            try:
                admin.create_topics([topic])
                print(f"created topic {topic.name} (partitions={topic.num_partitions})")
            except TopicAlreadyExistsError:
                print(f"topic {topic.name} already exists")
        return [t.name for t in TOPICS]
    finally:
        admin.close()


if __name__ == "__main__":
    try:
        ensure_topics()
    except Exception as exc:  # noqa: BLE001 - CLI entry point
        print(f"failed to create topics: {exc}", file=sys.stderr)
        sys.exit(1)
