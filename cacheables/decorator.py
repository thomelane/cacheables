from typing import Callable, Optional

from .core import CacheableFunction
from .backends import Backend


def cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    function_id: Optional[str] = None,
    backend: Optional[Backend] = None,
    exclude_args_fn: Optional[Callable] = None
) -> Callable[[Callable], CacheableFunction]:
    def decorator(fn: Callable) -> CacheableFunction:
        return CacheableFunction(
            fn=fn,
            function_id=function_id,
            backend=backend,
            exclude_args_fn=exclude_args_fn
        )

    # when cacheable is used as @cacheable without parentheses,
    # _fn is the function to be decorated.
    if _fn is not None:
        return decorator(_fn)

    return decorator
