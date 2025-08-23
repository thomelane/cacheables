from typing import Callable, Optional

from .caches.disk import AsyncDiskCache
from .core import AsyncCacheableFunction


def async_cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    *,
    function_id: Optional[str] = None,
    cache: Optional[AsyncDiskCache] = None,
) -> Callable[[Callable], AsyncCacheableFunction]:
    def decorator(fn: Callable) -> AsyncCacheableFunction:
        return AsyncCacheableFunction(
            fn=fn,
            function_id=function_id,
            cache=cache,
        )

    # when cacheable is used as @cacheable without parentheses,
    # _fn is the function to be decorated.
    if _fn is not None:
        return decorator(_fn)
    return decorator
