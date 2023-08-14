import contextlib
import functools
import hashlib
import os
import pickle
import sys
import warnings
from typing import Callable, Optional
import inspect

from loguru import logger

from .exceptions import (
    LoadException,
    DumpException,
    MissingOutputException
)
from .backends import Backend, DiskBackend
from .keys import FunctionKey, VersionKey, InputKey


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
    _instances = set()  # used to enable/disable cache on all cacheable functions

    def __init__(
        self,
        fn: Callable,
        backend: Optional[Backend] = None,
        exclude_args_fn: Optional[Callable] = None,
    ):
        self._fn = fn
        self._backend = backend or DiskBackend()
        self._exclude_args_fn = exclude_args_fn or (lambda arg: arg.startswith("_"))
        self._read: Optional[bool] = None  # None acts as an overridable False
        self._write: Optional[bool] = None  # None acts as an overridable False
        self._logger = logger.bind(function_id=self._get_function_id())
        functools.update_wrapper(self, fn)  # preserves signature and docstring
        self.__class__._instances.add(self)

    def _get_function_id(self) -> str:
        return self._fn.__name__
    
    def get_function_key(self) -> FunctionKey:
        return FunctionKey(
            function_id=self._get_function_id()
        )
    
    def _get_version_id(self) -> str:
        signature = inspect.signature(self._fn)
        str_to_hash = pickle.dumps(signature)
        version_id = hashlib.md5(str_to_hash).hexdigest()
        return version_id
    
    def get_version_key(self) -> VersionKey:
        return VersionKey(
            function_id=self._get_function_id(),
            version_id=self._get_version_id(),
        )

    def _get_input_id(self, *args, **kwargs) -> str:
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
        input_id = hashlib.md5(str_to_hash).hexdigest()
        return input_id

    def get_input_key(self, *args, **kwargs) -> InputKey:
        return InputKey(
            function_id=self._get_function_id(),
            version_id=self._get_version_id(),
            input_id=self._get_input_id(*args, **kwargs),
        )
    
    def list_version_keys(self) -> list:
        function_key = self.get_function_key()
        return self._backend.list_version_keys(function_key)

    def __call__(self, *args, **kwargs):
        if os.getenv("DISABLE_CACHEABLE", "false").lower() == "true":
            warning_msg = (
                "executing cacheable functions without cache "
                "(because DISABLE_CACHEABLE=true)"
            )
            warnings.warn(warning_msg)
            self._logger.debug(warning_msg.replace("functions", "function"))
            return self._fn(*args, **kwargs)

        try:
            input_key = self.get_input_key(*args, **kwargs)
        except Exception as error:
            warning_msg = f"failed to construct input key: {error}"
            self._logger.warning(warning_msg)
            warnings.warn(warning_msg)
            self._logger.debug("executing function without cache")
            output = self._fn(*args, **kwargs)
            return output

        if self._read:
            if self._backend.output_exists(input_key):
                try:
                    self._logger.info("reading output from cache")
                    return self._backend.read_output(input_key)
                except LoadException as error:
                    warning_msg = f"failed to read output from cache: {error}"
                    self._logger.warning(warning_msg)
                    warnings.warn(warning_msg)
            else:
                self._logger.debug("output not found in cache")

        self._logger.debug("executing function")
        output = self._fn(*args, **kwargs)

        if self._write:
            try:
                self._logger.info("writing output to cache")
                self._backend.write_output(output, input_key)
            except DumpException as error:
                message_msg = f"failed to write output to cache: {error}"
                self._logger.warning(message_msg)
                warnings.warn(message_msg)

        return output

    @contextlib.contextmanager
    def enable_cache(self, read: bool = True, write: bool = True):
        previous_read, previous_write = self._read, self._write
        # most restrictive settings will be used
        self._read = (read and previous_read) if (previous_read is not None) else read
        self._write = (
            (write and previous_write) if (previous_write is not None) else write
        )
        try:
            yield
        finally:
            self._read, self._write = previous_read, previous_write

    @contextlib.contextmanager
    def disable_cache(self):
        with self.enable_cache(read=False, write=False):
            yield

    def load_from_key(self, input_key: InputKey):
        if not self._backend.output_exists(input_key):
            raise MissingOutputException("output not found in cache")
        return self._backend.read_output(input_key)
    
    def load_from_args(self, *args, **kwargs):
        input_key = self.get_input_key(*args, **kwargs)
        return self.load_from_key(input_key)


# should be a classmethod, but `enable_cache` is an instance method
# and this gives a clean interface for enabling cache on all cacheable functions
@contextlib.contextmanager
def enable_cache(read: bool = True, write: bool = True):
    with contextlib.ExitStack() as stack:
        for (
            instance
        ) in CacheableFunction._instances:  # pylint: disable=protected-access
            stack.enter_context(instance.enable_cache(read=read, write=write))
        yield


# should be a classmethod, but `disable_cache` is an instance method
# and this gives a clean interface for disabling cache on all cacheable functions
@contextlib.contextmanager
def disable_cache():
    with enable_cache(read=False, write=False):
        yield
