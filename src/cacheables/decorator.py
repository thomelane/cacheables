from typing import Callable, Optional

from .caches import BaseCache
from .core import CacheableFunction
from .serializers import BaseSerializer


def cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    function_id: Optional[str] = None,
    cache: Optional[BaseCache] = None,
    serializer: Optional[BaseSerializer] = None,
    key_builder: Optional[Callable] = None,
) -> Callable[[Callable], CacheableFunction]:
    def decorator(fn: Callable) -> CacheableFunction:
        return CacheableFunction(
            fn=fn,
            function_id=function_id,
            cache=cache,
            serializer=serializer,
            key_builder=key_builder,
        )

    # when cacheable is used as @cacheable without parentheses,
    # _fn is the function to be decorated.
    if _fn is not None:
        return decorator(_fn)

    return decorator
