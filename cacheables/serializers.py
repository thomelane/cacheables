import json
import pickle
from typing import Any
from abc import ABC, abstractmethod


class Serializer(ABC):
    metadata: dict

    @abstractmethod
    def serialize(self, value: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, value: bytes) -> Any:
        pass


class JsonSerializer(Serializer):
    metadata = { "extension": "json" }

    def serialize(self, value: Any) -> bytes:
        return json.dumps(value, indent=4).encode("utf-8")

    def deserialize(self, value: bytes) -> Any:
        return json.loads(value.decode("utf-8"))


class PickleSerializer(Serializer):
    metadata = { "extension": "pickle" }

    def serialize(self, value: Any) -> bytes:
        return pickle.dumps(value)

    def deserialize(self, value: bytes) -> Any:
        return pickle.loads(value)
