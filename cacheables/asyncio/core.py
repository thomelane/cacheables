from functools import update_wrapper


class AsyncCacheableFunction:
    def __init__(
        self, fn, function_id: str = None, cache=None
    ):
        self._fn = fn
        self._function_id = function_id or f"{fn.__module__}:{fn.__qualname__}"
        self._cache = cache
        update_wrapper(self, fn)  # preserves signature and docstring

    async def __call__(self, *args, **kwargs):
        # For now, simply call the underlying async function bypassing cache logic.
        result = await self._fn(*args, **kwargs)
        return result
