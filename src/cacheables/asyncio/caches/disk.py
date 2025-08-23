import asyncio
import datetime
import hashlib
import json
import os
import shutil
from functools import wraps
from inspect import signature
from pathlib import Path
from typing import List, Optional

import aiofiles
import aiofiles.os
from filelock import AsyncFileLock

from cacheables.exceptions import InputKeyNotFoundError, ReadException, WriteException
from cacheables.keys import FunctionKey, InputKey

from .base import BaseAsyncCache


def async_acquire_lock(func):
    @wraps(func)
    async def wrapper(self: "AsyncDiskCache", *args, **kwargs):
        sig = signature(func)
        bound_args = sig.bind(self, *args, **kwargs)
        input_key = bound_args.arguments.get("input_key", None)
        if input_key is None:
            raise ValueError(f"input_key argument not found for method {func.__name__}")
        lock_path = self._construct_lock_path(input_key)
        await asyncio.to_thread(lock_path.parent.mkdir, parents=True, exist_ok=True)
        async with AsyncFileLock(str(lock_path)):
            return await func(self, *args, **kwargs)

    return wrapper


class AsyncDiskCache(BaseAsyncCache):
    def __init__(self, base_path: Optional[str] = None):
        self._base_path = base_path or os.path.join(os.getcwd(), ".cacheables")
        self._base_path = Path(self._base_path).resolve()

    def _construct_functions_path(self) -> Path:
        return self._base_path / "functions"

    def _construct_function_path(self, function_key: FunctionKey) -> Path:
        functions_path = self._construct_functions_path()
        return functions_path / function_key.function_id

    def _construct_inputs_path(self, function_key: FunctionKey) -> Path:
        function_path = self._construct_function_path(function_key)
        return function_path / "inputs"

    def _construct_input_path(self, input_key: InputKey) -> Path:
        inputs_path = self._construct_inputs_path(FunctionKey(input_key.function_id))
        return inputs_path / input_key.input_id

    def _construct_metadata_path(self, input_key: InputKey) -> Path:
        input_path = self._construct_input_path(input_key)
        return input_path / "metadata.json"

    def _construct_output_path(self, input_key: InputKey, metadata: dict) -> Path:
        input_path = self._construct_input_path(input_key)
        extension = metadata["serializer"].get("extension", "bin")
        filename = f"{metadata['output_id']}.{extension}"
        return input_path / filename

    async def get_output_path(self, input_key: InputKey) -> str:
        if not await self.exists(input_key):
            raise InputKeyNotFoundError(f"{input_key} not found in cache")
        metadata = await self.load_metadata(input_key)
        output_path = self._construct_output_path(input_key, metadata)
        return str(output_path)

    def _construct_lock_directory(self) -> Path:
        return self._base_path / "locks"

    def _construct_lock_path(self, input_key: InputKey) -> Path:
        lock_name = hashlib.md5(
            str(self._construct_input_path(input_key)).encode()
        ).hexdigest()
        return self._construct_lock_directory() / f"{lock_name}.lock"

    async def exists(self, input_key: InputKey) -> bool:
        output_path = self._construct_input_path(input_key)
        exists = await aiofiles.os.path.exists(str(output_path))
        if exists:
            return await aiofiles.os.path.isdir(str(output_path))
        return False

    async def list(self, function_key: FunctionKey) -> List[InputKey]:
        inputs_path = self._construct_inputs_path(function_key)
        try:
            names = await aiofiles.os.listdir(str(inputs_path))
        except Exception:
            names = []
        result = []
        for name in names:
            full_path = inputs_path / name
            if await aiofiles.os.path.isdir(str(full_path)):
                result.append(
                    InputKey(function_id=function_key.function_id, input_id=name)
                )
        return result

    async def evict(self, input_key: InputKey) -> None:
        input_path = self._construct_input_path(input_key)
        await asyncio.to_thread(shutil.rmtree, input_path, ignore_errors=True)

    async def clear(self, function_key: FunctionKey) -> None:
        function_path = self._construct_function_path(function_key)
        await asyncio.to_thread(shutil.rmtree, function_path, ignore_errors=True)

    async def adopt(
        self, from_function_key: FunctionKey, to_function_key: FunctionKey
    ) -> None:
        from_path = self._construct_function_path(from_function_key)
        to_path = self._construct_function_path(to_function_key)
        await asyncio.to_thread(shutil.copytree, from_path, to_path, dirs_exist_ok=True)
        await asyncio.to_thread(shutil.rmtree, from_path, ignore_errors=True)

    @async_acquire_lock
    async def dump_metadata(self, metadata: dict, input_key: InputKey) -> None:
        metadata_path = self._construct_metadata_path(input_key)
        await aiofiles.os.makedirs(str(metadata_path.parent), exist_ok=True)
        async with aiofiles.open(str(metadata_path), "w", encoding="utf-8") as f:
            await f.write(json.dumps(metadata, indent=4))

    @async_acquire_lock
    async def load_metadata(self, input_key: InputKey) -> dict:
        metadata_path = self._construct_metadata_path(input_key)
        async with aiofiles.open(str(metadata_path), "r", encoding="utf-8") as f:
            contents = await f.read()
        return json.loads(contents)

    @async_acquire_lock
    async def read_output(self, metadata: dict, input_key: InputKey) -> bytes:
        try:
            output_path = self._construct_output_path(input_key, metadata)
            async with aiofiles.open(output_path, "rb") as file:
                output_bytes = await file.read()
            return output_bytes
        except Exception as error:
            raise ReadException(str(error)) from error

    @async_acquire_lock
    async def write_output(
        self, output_bytes: bytes, metadata: dict, input_key: InputKey
    ) -> None:
        try:

            def _mkdir():
                output_path = self._construct_output_path(input_key, metadata)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                return output_path

            output_path = await asyncio.to_thread(_mkdir)
            async with aiofiles.open(output_path, "wb") as file:
                await file.write(output_bytes)
        except Exception as error:
            raise WriteException(str(error)) from error

    async def update_last_accessed(self, input_key: InputKey) -> None:
        metadata = await self.load_metadata(input_key)
        metadata["last_accessed"] = datetime.datetime.now(datetime.UTC).isoformat()
        await self.dump_metadata(metadata, input_key)

    async def get_last_accessed(
        self, input_key: InputKey
    ) -> Optional[datetime.datetime]:
        metadata = await self.load_metadata(input_key)
        if "last_accessed" in metadata:
            iso_string = metadata["last_accessed"]
            return datetime.datetime.fromisoformat(iso_string)
        return None
