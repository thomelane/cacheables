from abc import ABC, abstractmethod
from typing import List, Optional
import datetime

from ..keys import FunctionKey, InputKey


class BaseCache(ABC):
    # input methods

    @abstractmethod
    def list(self, function_key: FunctionKey) -> List[InputKey]:
        pass

    @abstractmethod
    def evict(self, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def clear(self, function_key: FunctionKey) -> None:
        pass

    @abstractmethod
    def adopt(
        self, from_function_key: FunctionKey, to_function_key: FunctionKey
    ) -> None:
        """
        Assumes the to_function is using the same cache as the from_function.
        e.g. both using DiskCache with the same base_path, etc.
        """
        pass

    # output methods

    @abstractmethod
    def exists(self, input_key: InputKey) -> bool:
        pass

    @abstractmethod
    def read_output(self, metadata: dict, input_key: InputKey) -> bytes:
        pass

    @abstractmethod
    def load_metadata(self, input_key: InputKey) -> dict:
        pass

    @abstractmethod
    def write_output(
        self, output_bytes: bytes, metadata: dict, input_key: InputKey
    ) -> None:
        pass

    @abstractmethod
    def dump_metadata(self, metadata: dict, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def get_output_path(self, input_key: InputKey) -> str:
        pass

    @abstractmethod
    def update_last_accessed(self, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def get_last_accessed(self, input_key: InputKey) -> Optional[datetime.datetime]:
        pass

    def read(self, input_key: InputKey) -> bytes:
        self.update_last_accessed(input_key)
        metadata = self.load_metadata(input_key)
        output_bytes = self.read_output(metadata, input_key)
        return output_bytes

    def write(self, output_bytes: bytes, metadata: dict, input_key: InputKey) -> None:
        self.evict(input_key)
        self.write_output(output_bytes, metadata, input_key)
        self.dump_metadata(metadata, input_key)
        self.update_last_accessed(input_key)
