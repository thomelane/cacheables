import asyncio
import os

import pytest

from cacheables.asyncio.caches.disk import AsyncDiskCache
from cacheables.exceptions import InputKeyNotFoundError, ReadException
from cacheables.keys import FunctionKey, InputKey


@pytest.mark.asyncio
async def test_write_and_read(tmp_path):
    # Create the async disk cache with the temporary path
    cache = AsyncDiskCache(base_path=tmp_path)
    input_key = InputKey(function_id="test_function", input_id="test_key")
    data = b"hello async cache!"
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    await cache.write(data, metadata, input_key)
    read_data = await cache.read(input_key)
    assert read_data == data


@pytest.mark.asyncio
async def test_exists_method(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    input_key = InputKey(function_id="existence_function", input_id="existence_key")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    exists_before = await cache.exists(input_key)
    assert exists_before is False
    await cache.write(b"some data", metadata, input_key)
    exists_after = await cache.exists(input_key)
    assert exists_after is True


@pytest.mark.asyncio
async def test_read_nonexistent(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    input_key = InputKey(function_id="nonexistent_function", input_id="nonexistent_key")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    with pytest.raises(ReadException):
        await cache.read_output(metadata, input_key)


@pytest.mark.asyncio
async def test_concurrent_writes(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    input_key = InputKey(function_id="concurrent_function", input_id="concurrent_key")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    data1 = b"first"
    data2 = b"second"
    await asyncio.gather(
        cache.write(data1, metadata, input_key),
        cache.write(data2, metadata, input_key),
    )
    read_data = await cache.read_output(metadata, input_key)
    assert read_data in (data1, data2)


@pytest.mark.asyncio
async def test_update_and_get_last_accessed(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    input_key = InputKey(function_id="test_func", input_id="last_access_key")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "dummy"}
    await cache.dump_metadata(metadata, input_key)
    # Capture last_accessed before update (likely missing or empty)
    before = await cache.get_last_accessed(input_key)
    await cache.update_last_accessed(input_key)
    after = await cache.get_last_accessed(input_key)
    # Ensure that last_accessed is updated (non-empty and changed)
    assert after is not None
    if before is not None:
        assert after != before


@pytest.mark.asyncio
async def test_clear_method(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    # Create a function key (for clear, we need the function-level path)
    func_key = FunctionKey(function_id="clear_test")
    input_key = InputKey(function_id="clear_test", input_id="key1")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    await cache.write(b"some data", metadata, input_key)
    # Verify list returns at least one input
    inputs = await cache.list(func_key)
    assert len(inputs) >= 1
    # Clear the function's cache and re-check
    await cache.clear(func_key)
    inputs_after = await cache.list(func_key)
    assert len(inputs_after) == 0


@pytest.mark.asyncio
async def test_adopt_method(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    # Write data under one function key
    from_func = FunctionKey(function_id="from_func")
    to_func = FunctionKey(function_id="to_func")
    input_key_from = InputKey(function_id=from_func.function_id, input_id="key_adopt")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    data = b"adoption test"
    await cache.write(data, metadata, input_key_from)
    # Call adopt to copy from "from_func" to "to_func" and delete from "from_func"
    await cache.adopt(from_func, to_func)
    # Create an input_key for to_func with the same id
    input_key_to = InputKey(function_id=to_func.function_id, input_id="key_adopt")
    # Read the adopted data
    adopted_data = await cache.read_output(metadata, input_key_to)
    assert adopted_data == data


@pytest.mark.asyncio
async def test_get_output_path(tmp_path):
    cache = AsyncDiskCache(base_path=tmp_path)
    input_key = InputKey(function_id="path_test", input_id="key_path")
    metadata = {"serializer": {"extension": "bin"}, "output_id": "output"}
    # Write some output so that metadata is available to get the path using
    # the full write method
    await cache.write(b"output data", metadata, input_key)
    path = await cache.get_output_path(input_key)
    # Verify that the path exists as a file
    assert os.path.isfile(path)
    # Test non-existent path
    bad_key = InputKey(function_id="bad_path", input_id="nonexistent")
    with pytest.raises(InputKeyNotFoundError):
        await cache.get_output_path(bad_key)
