"""Consume raw vitals, enforce the data contract, route the results.

For every message on ``vitals.raw``:

* **valid**   -> a Bronze row (buffered, then written to the Bronze Delta table)
* **invalid** -> published to ``vitals.deadletter`` as
  ``{"raw": <original>, "errors": [...], "rejected_at": <iso>,
     "source": {"topic","partition","offset"}}``

Nothing malformed is ever written to Bronze. The consumer runs as a bounded
batch: it stops after ``--max-messages`` records or after ``--idle-timeout``
seconds with no new message, so it slots into an Airflow task later.

    python -m src.ingestion.consumer --max-messages 1000 --idle-timeout 10
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from config.settings import settings
from src.contracts.vitals import to_bronze_row, validate_record
from src.ingestion.serde import JsonDeserializer, JsonSerializer
from src.lakehouse.bronze import append_bronze

Route = tuple[str, dict]  # ("bronze", row) | ("deadletter", payload)


def route_message(raw: Any, *, topic: str, partition: int, offset: int,
                  ingested_at: datetime | None = None) -> Route:
    """Pure routing decision for one message. No IO - unit tested directly."""
    reading, reasons = validate_record(raw)
    if reading is not None:
        row = to_bronze_row(reading, ingested_at=ingested_at)
        row["kafka_partition"] = int(partition)
        row["kafka_offset"] = int(offset)
        return "bronze", row
    return "deadletter", {
        "raw": raw,
        "errors": reasons,
        "rejected_at": (ingested_at or datetime.now(timezone.utc)).isoformat(),
        "source": {"topic": topic, "partition": int(partition), "offset": int(offset)},
    }


def run(max_messages: int, idle_timeout: float, bronze_batch: int = 500) -> dict:
    consumer = KafkaConsumer(
        settings.kafka_topic_raw,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=JsonDeserializer(),
        consumer_timeout_ms=int(idle_timeout * 1000),
    )
    dlq = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=JsonSerializer(),
        acks="all",
    )

    pending: list[dict] = []
    stats = {"consumed": 0, "valid": 0, "rejected": 0}

    def flush_bronze() -> None:
        if pending:
            append_bronze(pending)
            pending.clear()

    try:
        for msg in consumer:
            stats["consumed"] += 1
            dest, payload = route_message(
                msg.value, topic=msg.topic, partition=msg.partition, offset=msg.offset
            )
            if dest == "bronze":
                pending.append(payload)
                stats["valid"] += 1
                if len(pending) >= bronze_batch:
                    flush_bronze()
            else:
                dlq.send(settings.kafka_topic_deadletter, value=payload)
                stats["rejected"] += 1
                reason = payload["errors"][0] if payload["errors"] else "unknown"
                print(f"  DLQ p{msg.partition}@{msg.offset}: {reason}")

            if stats["consumed"] >= max_messages:
                break

        flush_bronze()
        dlq.flush()
        consumer.commit()
    finally:
        consumer.close()
        dlq.close()

    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate raw vitals and route to Bronze / DLQ")
    ap.add_argument("--max-messages", type=int, default=100_000)
    ap.add_argument("--idle-timeout", type=float, default=10.0,
                    help="stop after this many seconds with no new message")
    args = ap.parse_args()

    stats = run(args.max_messages, args.idle_timeout)
    print(
        f"consumed={stats['consumed']}  ->  bronze={stats['valid']}  "
        f"deadletter={stats['rejected']}"
    )


if __name__ == "__main__":
    main()
