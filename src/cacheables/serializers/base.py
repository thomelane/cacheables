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


def check_serializer(serializer: BaseSerializer, data: Any) -> None:
    serialized_data = serializer.serialize(data)
    assert isinstance(serialized_data, bytes)
    deserialized_data = serializer.deserialize(serialized_data)
    assert deserialized_data == data
