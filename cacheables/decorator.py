from typing import Callable, Optional

from .core import CacheableFunction
from .caches import Cache
from .serializers import Serializer


def cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    function_id: Optional[str] = None,
    cache: Optional[Cache] = None,
    serializer: Optional[Serializer] = None,
    exclude_args_fn: Optional[Callable] = None
) -> Callable[[Callable], CacheableFunction]:
    def decorator(fn: Callable) -> CacheableFunction:
        return CacheableFunction(
            fn=fn,
            function_id=function_id,
            cache=cache,
            serializer=serializer,
            exclude_args_fn=exclude_args_fn
        )

    # when cacheable is used as @cacheable without parentheses,
    # _fn is the function to be decorated.
    if _fn is not None:
        return decorator(_fn)

    return decorator
