import contextlib
import functools
import hashlib
import os
import pickle
import sys
import warnings
from typing import Callable, Optional, Any
import inspect

from loguru import logger

from .exceptions import (
    ReadException,
    WriteException,
    MissingOutputException
)
from .caches import Cache, DiskCache
from .keys import FunctionKey, InputKey
from .metadata import create_metadata
from .controller import CacheController


logger.disable(__name__)


def enable_debug_logging():
    logger.enable(__name__)
    logger.remove()
    logger.add(
        sink=sys.stderr,
        level="DEBUG",
        format=" | ".join(
            [
                "<green>{time:HH:mm:ss.SSS}</green>",
                "<level>{level: <8}</level>",
                "<level>fn: {extra[function_id]}</level>",
                "<level>{message}</level>",
            ]
        ),
    )


class CacheableFunction:
    def __init__(
        self,
        fn: Callable,
        function_id: Optional[str] = None,
        cache: Optional[Cache] = None,
        exclude_args_fn: Optional[Callable] = None,
    ):
        self._fn = fn
        self._function_id = function_id or self.get_function_id()
        self._cache = cache or DiskCache()
        self._controller = CacheController()
        self._exclude_args_fn = exclude_args_fn or (lambda arg: arg.startswith("_"))
        self._logger = logger.bind(function_id=self._function_id)
        functools.update_wrapper(self, fn)  # preserves signature and docstring

    def get_function_id(self) -> str:
        return f"{self._fn.__module__}:{self._fn.__qualname__}"
    
    def _get_function_key(self) -> FunctionKey:
        return FunctionKey(
            function_id=self._function_id
        )

    def get_input_id(self, *args, **kwargs) -> str:
        signature = inspect.signature(self._fn)
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        arguments = bound_arguments.arguments
        # remove excluded arguments
        arguments = {
            key: value
            for key, value in arguments.items()
            if not self._exclude_args_fn(key)
        }
        # sort arguments to ensure consistent ordering
        arguments = tuple(sorted(arguments.items()))
        str_to_hash = pickle.dumps(arguments)
        input_id = hashlib.md5(str_to_hash).hexdigest()[:16]
        return input_id

    def _get_input_key(self, *args, **kwargs) -> InputKey:
        return InputKey(
            function_id=self._function_id,
            input_id=self.get_input_id(*args, **kwargs),
        )

    def __call__(self, *args, **kwargs):
        read = self._controller.is_read_enabled()
        write = self._controller.is_write_enabled()

        if not (read or write):
            self._logger.debug("executing function without cache")
            return self._fn(*args, **kwargs)

        try:
            input_key = self._get_input_key(*args, **kwargs)
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
                    self._logger.info("reading output from cache")
                    return self._cache.read_output(input_key)
                except ReadException as error:
                    warning_msg = f"failed to read output from cache: {error}"
                    self._logger.warning(warning_msg)
                    warnings.warn(warning_msg)
            else:
                self._logger.debug("output not found in cache")

        self._logger.debug("executing function")
        output = self._fn(*args, **kwargs)

        if write:
            try:
                self._logger.info("writing output to cache")
                metadata = create_metadata(input_key)
                self._cache.write_output(output, metadata, input_key)
            except WriteException as error:
                message_msg = f"failed to write output to cache: {error}"
                self._logger.warning(message_msg)
                warnings.warn(message_msg)

        return output

    # cache-property
    def read(self, input_id: str):
        # should abide by DISABLE_CACHEABLE
        input_key = InputKey(
            function_id=self._function_id,
            input_id=input_id,
        )
        if not self._cache.exists(input_key):
            raise MissingOutputException("output not found in cache")
        return self._cache.read_output(input_key)

    # cache-property
    def write(self, output: Any, input_id: str):
        # should abide by DISABLE_CACHEABLE
        input_key = InputKey(
            function_id=self._function_id,
            input_id=input_id,
        )
        metadata = create_metadata(input_key)
        return self._cache.write_output(output, metadata, input_key)
    
    def enable_cache(self, read: bool = True, write: bool = True) -> contextlib.AbstractContextManager[None]:
        return self._controller.enable(read=read, write=write)

    def disable_cache(self) -> contextlib.AbstractContextManager[None]:
        return self._controller.disable()
