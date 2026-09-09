"""JSON serializers for Kafka.

kafka-python 3.x warns unless serializers implement its ``Serializer`` /
``Deserializer`` interface, so we provide small concrete classes instead of bare
lambdas.
"""
from __future__ import annotations

import json
from typing import Any

from kafka.serializer import Deserializer, Serializer


class JsonSerializer(Serializer):
    def serialize(self, topic: str, value: Any) -> bytes | None:
        if value is None:
            return None
        return json.dumps(value, separators=(",", ":")).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic: str, data: bytes | None) -> Any:
        if data is None:
            return None
        return json.loads(data.decode("utf-8"))


class StringSerializer(Serializer):
    def serialize(self, topic: str, value: Any) -> bytes | None:
        if value is None:
            return None
        return str(value).encode("utf-8")
