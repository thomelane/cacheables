import json
from typing import Any

from .base import BaseSerializer


class JsonSerializer(BaseSerializer):
    metadata = {"extension": "json"}

    def serialize(self, value: Any) -> bytes:
        return json.dumps(value, indent=4).encode("utf-8")

    def deserialize(self, value: bytes) -> Any:
        return json.loads(value.decode("utf-8"))
