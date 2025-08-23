import pickle
from typing import Any

from .base import BaseSerializer


class PickleSerializer(BaseSerializer):
    metadata = {"extension": "pickle"}

    def serialize(self, value: Any) -> bytes:
        return pickle.dumps(value)

    def deserialize(self, value: bytes) -> Any:
        return pickle.loads(value)
