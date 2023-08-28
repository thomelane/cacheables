from typing import Any
from abc import ABC, abstractmethod


class BaseSerializer(ABC):
    metadata: dict = {}

    @abstractmethod
    def serialize(self, value: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, value: bytes) -> Any:
        pass
