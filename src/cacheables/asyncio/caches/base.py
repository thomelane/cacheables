import datetime
from abc import ABC, abstractmethod
from typing import List, Optional

from cacheables.keys import FunctionKey, InputKey


class BaseAsyncCache(ABC):
    # Input methods
    @abstractmethod
    async def list(self, function_key: FunctionKey) -> List[InputKey]:
        pass

    @abstractmethod
    async def evict(self, input_key: InputKey) -> None:
        pass

    @abstractmethod
    async def clear(self, function_key: FunctionKey) -> None:
        pass

    @abstractmethod
    async def adopt(
        self, from_function_key: FunctionKey, to_function_key: FunctionKey
    ) -> None:
        """
        Adopt caches from one function key to another.
        """
        pass

    # Output methods
    @abstractmethod
    async def exists(self, input_key: InputKey) -> bool:
        pass

    @abstractmethod
    async def read_output(self, metadata: dict, input_key: InputKey) -> bytes:
        pass

    @abstractmethod
    async def load_metadata(self, input_key: InputKey) -> dict:
        pass

    @abstractmethod
    async def write_output(
        self, output_bytes: bytes, metadata: dict, input_key: InputKey
    ) -> None:
        pass

    @abstractmethod
    async def dump_metadata(self, metadata: dict, input_key: InputKey) -> None:
        pass

    @abstractmethod
    async def get_output_path(self, input_key: InputKey) -> str:
        pass

    @abstractmethod
    async def update_last_accessed(self, input_key: InputKey) -> None:
        pass

    @abstractmethod
    async def get_last_accessed(
        self, input_key: InputKey
    ) -> Optional[datetime.datetime]:
        pass

    # Synchronous-style convenience methods, now asynchronous
    async def read(self, input_key: InputKey) -> bytes:
        await self.update_last_accessed(input_key)
        metadata = await self.load_metadata(input_key)
        output_bytes = await self.read_output(metadata, input_key)
        return output_bytes

    async def write(
        self, output_bytes: bytes, metadata: dict, input_key: InputKey
    ) -> None:
        await self.evict(input_key)
        await self.write_output(output_bytes, metadata, input_key)
        await self.dump_metadata(metadata, input_key)
        await self.update_last_accessed(input_key)
