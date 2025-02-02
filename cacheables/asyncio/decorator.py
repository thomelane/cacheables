from typing import Callable, Optional
from .core import AsyncCacheableFunction
from .caches.disk import AsyncDiskCache

def async_cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    *,  # what's this for? ai?
    function_id: Optional[str] = None,
    cache: Optional[AsyncDiskCache] = None,
) -> Callable[[Callable], AsyncCacheableFunction]:  # what does this mean? ai?
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
