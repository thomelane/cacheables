import hashlib
import inspect
import pickle
from dataclasses import dataclass
from typing import Any, Callable


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


def default_key_builder(fn: Callable, args: tuple, kwargs: dict) -> InputKey:
    function_id = f"{fn.__module__}:{fn.__qualname__}"
    arguments = _get_arguments(fn, args, kwargs)
    arguments = _filter_arguments(arguments, _default_exclude_args)
    input_id = _hash_arguments(arguments)
    return InputKey(function_id=function_id, input_id=input_id)
