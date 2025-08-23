import contextlib
import functools
import hashlib
import pickle

import warnings
from typing import Callable, Optional, Any
import inspect
from functools import lru_cache, wraps

from loguru import logger

from .exceptions import (
    LoadException,
    DumpException,
    InputKeyNotFoundError,
    CacheNotEnabledError,
)
from .caches import BaseCache, DiskCache
from .keys import FunctionKey, InputKey
from .metadata import create_metadata
from .controllers import CacheController
from .serializers import BaseSerializer, PickleSerializer


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

        return wrapper

    return decorator


class CacheableFunction:
    def __init__(
        self,
        fn: Callable,
        function_id: Optional[str] = None,
        cache: Optional[BaseCache] = None,
        serializer: Optional[BaseSerializer] = None,
        exclude_args_fn: Optional[Callable] = None,
    ):
        self._fn = fn
        self._function_id = function_id or self.get_function_id()
        self._cache = cache or DiskCache()
        self._controller = CacheController()
        self._serializer = serializer or PickleSerializer()
        self._exclude_args_fn = exclude_args_fn or (lambda arg: arg.startswith("_"))
        self._logger = logger.bind(function_id=self._function_id)
        functools.update_wrapper(self, fn)  # preserves signature and docstring

    def get_function_id(self) -> str:
        return f"{self._fn.__module__}:{self._fn.__qualname__}"

    def _get_function_key(self) -> FunctionKey:
        return FunctionKey(function_id=self._function_id)

    @safe_lru_cache()  # LRU cache for argument hashes
    def _hash_argument(self, arg: Any) -> str:
        arg_bytes = pickle.dumps(arg)
        return hashlib.md5(arg_bytes).hexdigest()

    def get_input_id(self, *args, **kwargs) -> str:
        signature = inspect.signature(self._fn)
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        arguments = bound_arguments.arguments
        # remove excluded arguments and hash the rest
        arguments = {
            key: self._hash_argument(value)
            for key, value in arguments.items()
            if not self._exclude_args_fn(key)
        }
        # sort arguments to ensure consistent ordering
        arguments = tuple(sorted(arguments.items()))
        str_to_hash = pickle.dumps(arguments)
        input_id = hashlib.md5(str_to_hash).hexdigest()[:16]
        return input_id

    def _get_input_key_from_args(self, *args, **kwargs) -> InputKey:
        return InputKey(
            function_id=self._function_id,
            input_id=self.get_input_id(*args, **kwargs),
        )

    def _get_input_key_from_input_id(self, input_id: str) -> InputKey:
        return InputKey(
            function_id=self._function_id,
            input_id=input_id,
        )

    def get_output_id(self, output: Any) -> str:
        output_bytes = self._serializer.serialize(output)
        return self._get_output_id_from_bytes(output_bytes)

    def _get_output_id_from_bytes(self, output_bytes: bytes) -> str:
        return hashlib.md5(output_bytes).hexdigest()[:16]

    def __call__(self, *args, **kwargs):
        read = self._controller.is_read_enabled()
        write = self._controller.is_write_enabled()

        if not (read or write):
            self._logger.debug("executing function without cache")
            return self._fn(*args, **kwargs)

        try:
            input_key = self._get_input_key_from_args(*args, **kwargs)
        except Exception as error:
            warning_msg = f"failed to construct input key: {error}"
            self._logger.warning(warning_msg)
            warnings.warn(warning_msg)
            self._logger.debug("executing function without cache")
            output = self._fn(*args, **kwargs)
            return output

        if read:
            if self._cache.exists(input_key):
                try:
                    output = self._load(input_key)
                    if not self._controller.is_passing_filter(output):
                        self._logger.debug("read output doesn't pass through filter")
                    else:
                        return output
                except LoadException as error:
                    warning_msg = f"failed to load output from cache: {error}"
                    self._logger.warning(warning_msg)
                    warnings.warn(warning_msg)
            else:
                self._logger.debug("output not found in cache")

        self._logger.debug("executing function")
        output = self._fn(*args, **kwargs)

        if write:
            try:
                self._dump(output, input_key)
            except DumpException as error:
                message_msg = f"failed to dump output to cache: {error}"
                self._logger.warning(message_msg)
                warnings.warn(message_msg)

        return output

    def _load(self, input_key: InputKey) -> Any:
        self._logger.info("loading output from cache")
        try:
            if not self._controller.is_read_enabled():
                raise CacheNotEnabledError("Cache reads are not enabled.")
            if not self._cache.exists(input_key):
                raise InputKeyNotFoundError(f"{input_key} not found in cache")
            output_bytes = self._cache.read(input_key)
            output = self._serializer.deserialize(output_bytes)
            return output
        except Exception as error:
            raise LoadException(error) from error

    def load_output(self, input_id: str) -> Any:
        input_key = self._get_input_key_from_input_id(input_id)
        return self._load(input_key)

    def load_metadata(self, input_id: str) -> dict:
        input_key = self._get_input_key_from_input_id(input_id)
        if not self._cache.exists(input_key):
            raise InputKeyNotFoundError(f"{input_key} not found in cache")
        return self._cache.load_metadata(input_key)

    def _dump(self, output: Any, input_key: InputKey) -> None:
        self._logger.info("dumping output to cache")
        try:
            if not self._controller.is_write_enabled():
                raise CacheNotEnabledError("Cache writes are not enabled.")
            output_bytes = self._serializer.serialize(output)
            output_id = self._get_output_id_from_bytes(output_bytes)
            metadata = create_metadata(
                input_key.input_id, output_id, self._serializer.metadata
            )
            self._cache.write(output_bytes, metadata, input_key)
        except Exception as error:
            raise DumpException(error) from error

    def dump_output(self, output: Any, input_id: str) -> None:
        input_key = self._get_input_key_from_input_id(input_id)
        return self._dump(output, input_key)

    def get_output_path(self, input_id: str) -> str:
        input_key = self._get_input_key_from_input_id(input_id)
        return self._cache.get_output_path(input_key)

    def enable_cache(
        self, read: bool = True, write: bool = True, filter: Optional[Callable] = None
    ) -> contextlib.AbstractContextManager[None]:
        return self._controller.enable(read=read, write=write, filter=filter)

    def disable_cache(self) -> contextlib.AbstractContextManager[None]:
        return self._controller.disable()

    def clear_cache(self) -> None:
        function_key = self._get_function_key()
        self._cache.clear(function_key)

    def adopt_cache(self, function_id: str) -> None:
        from_function_key = FunctionKey(function_id=function_id)
        to_function_key = self._get_function_key()
        self._cache.adopt(from_function_key, to_function_key)
