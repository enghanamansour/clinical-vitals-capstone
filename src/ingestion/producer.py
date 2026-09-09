"""Publish raw vital-sign records to the ingestion topic.

Reads a JSONL file (one JSON object per line, as written by
``src.generator.synth_vitals``) and produces each line to ``vitals.raw`` keyed by
``patient_id`` so all readings for a patient land on the same partition and keep
their order. The ``_corruption`` marker (if present) is stripped before sending -
the broker only ever sees what a real device would send.

    python -m src.ingestion.producer --input data/raw/vitals_mixed.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from kafka import KafkaProducer

from config.settings import settings
from src.ingestion.serde import JsonSerializer, StringSerializer


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=JsonSerializer(),
        key_serializer=StringSerializer(),
        acks="all",
        linger_ms=20,
    )


def publish_file(input_path: Path) -> int:
    producer = _make_producer()
    sent = 0
    try:
        with input_path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                record.pop("_corruption", None)
                key = record.get("patient_id") if isinstance(record, dict) else None
                producer.send(settings.kafka_topic_raw, key=key, value=record)
                sent += 1
        producer.flush()
    finally:
        producer.close()
    return sent


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish raw vitals JSONL to Kafka")
    ap.add_argument("--input", type=Path, default=Path("data/raw/vitals_mixed.jsonl"))
    args = ap.parse_args()

    count = publish_file(args.input)
    print(f"published {count} records to '{settings.kafka_topic_raw}' "
          f"on {settings.kafka_bootstrap_servers}")


if __name__ == "__main__":
    main()
