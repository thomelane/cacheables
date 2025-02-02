import asyncio
import pytest

from cacheables.asyncio.decorator import async_cacheable
from cacheables.asyncio.caches.disk import AsyncDiskCache


@pytest.mark.asyncio
async def test_async_cacheable_function(tmp_path):
    # Create an async function decorated with async_cacheable.
    @async_cacheable(cache=AsyncDiskCache(base_path=tmp_path))
    async def add(a: int, b: int) -> int:
        await asyncio.sleep(0.05)
        return a + b

    # Call the decorated async function.
    result1 = await add(1, 2)
    result2 = await add(1, 2)  # Test caching by running it twice

    # Basic assertions
    assert result1 == 3
    assert result2 == 3
