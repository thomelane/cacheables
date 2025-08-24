import hashlib
import inspect
import pickle
from dataclasses import dataclass
from functools import lru_cache, wraps
from typing import Any, Callable


def safe_lru_cache(maxsize=128, typed=False):
    """
    A safe version of lru_cache that falls back to executing the wrapped
    function if the arguments are unhashable. Original lru_cache would raise a
    TypeError in this case.
    """

    def decorator(func):
        @lru_cache(maxsize=maxsize, typed=typed)
        def cached_func(*args, **kwargs):
            return func(*args, **kwargs)

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return cached_func(*args, **kwargs)
            except TypeError as e:
                if "unhashable type" in str(e):
                    return func(*args, **kwargs)
                raise

        # expose cache control methods (used in testing)
        wrapper.cache_clear = cached_func.cache_clear
        wrapper.cache_info = cached_func.cache_info
        return wrapper

    return decorator


@dataclass
class FunctionKey:
    function_id: str


@dataclass
class InputKey:
    function_id: str
    input_id: str

    @property
    def function_key(self) -> FunctionKey:
        return FunctionKey(function_id=self.function_id)


@safe_lru_cache()  # small cache for argument hashes to avoid frequent recomputation
def _hash_argument(arg: Any) -> str:
    arg_bytes = pickle.dumps(arg)
    return hashlib.md5(arg_bytes).hexdigest()


def _default_exclude_args(arg: str) -> bool:
    return arg.startswith("_")


def _get_arguments(fn: Callable, args: tuple, kwargs: dict) -> dict:
    signature = inspect.signature(fn)
    bound_arguments = signature.bind(*args, **kwargs)
    bound_arguments.apply_defaults()
    arguments = bound_arguments.arguments
    return arguments


def _filter_arguments(arguments: dict, exclude_args_fn: Callable[[str], bool]) -> dict:
    return {key: value for key, value in arguments.items() if not exclude_args_fn(key)}


def _hash_arguments(arguments: dict) -> str:
    hashed_values = {key: _hash_argument(value) for key, value in arguments.items()}
    arguments = tuple(sorted(hashed_values.items()))
    str_to_hash = pickle.dumps(arguments)
    argument_hash = hashlib.md5(str_to_hash).hexdigest()[:16]
    return argument_hash


def create_key_builder(
    exclude_args_fn: Callable[[str], bool] = None,
) -> Callable[[Callable, tuple, dict], str]:
    """Create a key builder function that generates input_id from function arguments.

    Args:
        exclude_args_fn: Function to determine which arguments to exclude (defaults to excluding args starting with "_")

    Returns:
        A key builder function with signature (fn, args, kwargs) -> str (input_id)
    """
    if exclude_args_fn is None:
        exclude_args_fn = _default_exclude_args

    def key_builder(fn: Callable, args: tuple, kwargs: dict) -> str:
        """Generate input_id from function arguments."""
        arguments = _get_arguments(fn, args, kwargs)
        arguments = _filter_arguments(arguments, exclude_args_fn)
        input_id = _hash_arguments(arguments)
        return input_id

    return key_builder


default_key_builder = create_key_builder()
