from typing import Callable, Optional

from .core import CacheableFunction
from .serializers import Serializer


def cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    base_path: Optional[str] = None,
    name: Optional[str] = None,
    metadata: Optional[dict] = None,
    version_id_fn: Optional[Callable] = None,
    input_id_fn: Optional[Callable] = None,
    serializer: Optional[Serializer] = None
) -> Callable[[Callable], CacheableFunction]:
    def decorator(fn: Callable) -> CacheableFunction:
        return CacheableFunction(
            fn=fn,
            base_path=base_path,
            name=name,
            metadata=metadata,
            version_id_fn=version_id_fn,
            input_id_fn=input_id_fn,
            serializer=serializer
        )

    # when cacheable is used as @cacheable without parentheses,
    # _fn is the function to be decorated.
    if _fn is not None:
        return decorator(_fn)

    return decorator
