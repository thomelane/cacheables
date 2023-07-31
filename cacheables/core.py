import contextlib
import functools
import hashlib
import json
import os
import pickle
import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any, Callable, Optional

from loguru import logger

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
                "<level>fn: {extra[fn_name]}</level>",
                "<level>{message}</level>",
            ]
        ),
    )


def compute_input_id(*args, **kwargs) -> str:
    input_str = pickle.dumps((args, kwargs))
    return hashlib.md5(input_str).hexdigest()


def compute_version_id(name: str, metadata: dict) -> str:
    """
    Compute the version id by hashing the metadata dictionary.
    The metadata dictionary must be able to be serialized as a string.
    """
    str_to_hash = name + str(
        sorted(metadata.items())
    )  # Sort the items for consistent results.
    return hashlib.md5(str_to_hash.encode("utf-8")).hexdigest()


def pickle_dump(obj: Any, path: Path):
    path = path / "output.pkl"
    with open(path, "wb") as file:
        pickle.dump(obj, file)


def pickle_load(path: Path) -> Any:
    path = path / "output.pkl"
    with open(path, "rb") as file:
        return pickle.load(file)


def json_dump(obj: Any, path: Path):
    path = path / "output.json"
    with open(path, "w", encoding="utf-8") as file:
        json.dump(obj, file, indent=4)


def json_load(path: Path) -> Any:
    path = path / "output.json"
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


class DumpException(Exception):
    pass


class LoadException(Exception):
    pass


class MissingResultException(Exception):
    pass


class VersionIdException(Exception):
    pass


class InputIdException(Exception):
    pass


class CacheableFunction:
    _instances = set()  # used to enable/disable cache on all cacheable functions

    def __init__(
        self,
        fn: Callable,
        base_path: Optional[str] = None,
        name: Optional[str] = None,
        metadata: Optional[dict] = None,
        version_id_fn: Optional[Callable] = None,
        input_id_fn: Optional[Callable] = None,
        dump_fn: Optional[Callable[[Any, Path], None]] = None,
        load_fn: Optional[Callable[[Path], Any]] = None,
    ):
        self._fn = fn
        self._base_path = base_path or os.getcwd()
        self._name = name or fn.__name__
        self._metadata = metadata or {}
        self._version_id_fn = version_id_fn or compute_version_id
        self._input_id_fn = input_id_fn or compute_input_id
        self._dump_fn = dump_fn or pickle_dump
        self._load_fn = load_fn or pickle_load
        self._read: Optional[bool] = None  # None acts as an overridable False
        self._write: Optional[bool] = None  # None acts as an overridable False
        self._logger = logger.bind(fn_name=self._name)
        functools.update_wrapper(self, fn)  # preserves signature and docstring
        self.__class__._instances.add(self)

    def _get_input_id(self, *args, **kwargs):
        try:
            input_id = self._input_id_fn(*args, **kwargs)
            if (not isinstance(input_id, str)) or (len(input_id) == 0):
                raise ValueError(
                    f"input_id_fn must return a non-empty string, got {input_id}"
                )
            return input_id
        except Exception as error:
            raise InputIdException(str(error)) from error

    def _get_version_id(self):
        try:
            version_id = self._version_id_fn(self._name, self._metadata)
            if (not isinstance(version_id, str)) or (len(version_id) == 0):
                raise ValueError(
                    f"version_id_fn must return a non-empty string, got {version_id}"
                )
            return version_id
        except Exception as error:
            raise VersionIdException(str(error)) from error

    def get_path_from_inputs(self, *args, **kwargs) -> Path:
        input_id = self._get_input_id(*args, **kwargs)
        version_id = self._get_version_id()
        path = Path(
            self._base_path,
            "functions",
            self._name,
            "versions",
            version_id,
            "inputs",
            input_id,
            "outputs",
        )
        return path

    def get_path_from_ids(self, input_id: str, version_id: Optional[str]) -> Path:
        version_id = version_id or self._get_version_id()
        path = Path(
            self._base_path,
            "functions",
            self._name,
            "versions",
            version_id,
            "inputs",
            input_id,
            "outputs",
        )
        return path

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
            path = self.get_path_from_inputs(*args, **kwargs)
        except (VersionIdException, InputIdException) as error:
            warning_msg = f"failed to construct cache path: {error}"
            self._logger.warning(warning_msg)
            warnings.warn(warning_msg)
            self._logger.debug("executing function")
            result = self._fn(*args, **kwargs)
            return result

        if self._read:
            if os.path.exists(path):
                try:
                    self._logger.info("reading result from cache")
                    result = self._read_from_cache(path)
                    return result
                except LoadException as error:
                    warning_msg = f"failed to read result from cache: {error}"
                    self._logger.warning(warning_msg)
                    warnings.warn(warning_msg)
            else:
                self._logger.debug("result not found in cache")

        self._logger.debug("executing function")
        result = self._fn(*args, **kwargs)

        if self._write:
            try:
                self._logger.info("writing result to cache")
                self._write_to_cache(result, path)
            except DumpException as error:
                message_msg = f"failed to write result to cache: {error}"
                self._logger.warning(message_msg)
                warnings.warn(message_msg)

        return result

    @contextlib.contextmanager
    def enable_cache(self, read: bool = True, write: bool = True):
        _read, _write = self._read, self._write
        # most restrictive settings will be used
        self._read = (read and _read) if (_read is not None) else read
        self._write = (write and _write) if (_write is not None) else write
        try:
            yield
        finally:
            self._read, self._write = _read, _write

    @contextlib.contextmanager
    def disable_cache(self):
        with self.enable_cache(read=False, write=False):
            yield

    def _read_from_cache(self, path):
        try:
            return self._load_fn(path)
        except Exception as error:
            raise LoadException(str(error)) from error

    def _write_to_cache(self, result, path):
        """
        A close to "atomic" write to the cache. We dump the result to a temporary folder
        first, instead of directly to the cache path. Avoids the issue of partially
        written files (e.g. if the process is killed in the middle of a dump), and
        then appearing as if we have a valid result in the cache.

        Still have a small window where there could be a partially written files: after
        the files are dumped to the temporary folder, but before they are moved to the
        final cache path.
        """
        try:
            with tempfile.TemporaryDirectory() as tmp_path:
                tmp_path = Path(tmp_path)
                self._dump_fn(result, tmp_path)
                assert tmp_path.exists(), f"{tmp_path} (for {path}) does not exist."
                assert tmp_path.is_dir(), f"{tmp_path} (for {path}) is not a folder."
                assert any(
                    tmp_path.iterdir()
                ), f"{tmp_path} (for {path}) is an empty folder."
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.exists() and path.is_dir():
                    shutil.rmtree(path)
                shutil.copytree(tmp_path, path)
        except Exception as error:
            raise DumpException(str(error)) from error

    def load_from_inputs(self, *args, **kwargs):
        path = self.get_path_from_inputs(*args, **kwargs)
        if not path.exists():
            raise MissingResultException("result not found in cache")
        return self._read_from_cache(path)

    def load_from_ids(self, input_id: str, version_id: Optional[str]):
        version_id = version_id or self._get_version_id()
        path = Path(
            self._base_path,
            "functions",
            self._name,
            "versions",
            version_id,
            "inputs",
            input_id,
            "outputs",
        )
        if not path.exists():
            raise MissingResultException("result not found in cache")
        return self._read_from_cache(path)

    def get_ids_from_inputs(self, *args, **kwargs) -> dict:
        return {
            "input_id": self._get_input_id(*args, **kwargs),
            "version_id": self._get_version_id(),
        }


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


def cacheable(
    _fn: Optional[Callable] = None,  # enable @cacheable() and @cacheable usage
    base_path: Optional[str] = None,
    name: Optional[str] = None,
    metadata: Optional[dict] = None,
    version_id_fn: Optional[Callable] = None,
    input_id_fn: Optional[Callable] = None,
    dump_fn: Optional[Callable[[Any, Path], None]] = None,
    load_fn: Optional[Callable[[Path], Any]] = None,
) -> Callable[[Callable], CacheableFunction]:
    def decorator(fn: Callable) -> CacheableFunction:
        return CacheableFunction(
            fn=fn,
            base_path=base_path,
            name=name,
            metadata=metadata,
            version_id_fn=version_id_fn,
            input_id_fn=input_id_fn,
            dump_fn=dump_fn,
            load_fn=load_fn,
        )

    # when cacheable is used as @cacheable without parentheses,
    # _fn is the function to be decorated.
    if _fn is not None:
        return decorator(_fn)

    return decorator
