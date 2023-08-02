import contextlib
import datetime
import functools
import hashlib
import json
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Callable, Optional

from loguru import logger

from .serializers import Serializer, PickleSerializer
from .exceptions import (
    InputIdException,
    VersionIdException,
    LoadException,
    DumpException,
    MissingResultException
)


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
        serializer: Optional[Serializer] = None,
    ):
        self._fn = fn
        self._base_path = base_path or os.getcwd() + "/.cacheables"
        self._name = name or fn.__name__
        self._metadata = metadata or {}
        self._version_id_fn = version_id_fn or compute_version_id
        self._input_id_fn = input_id_fn or compute_input_id
        self._serializer = serializer or PickleSerializer()
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

    def _construct_function_path(
        self,
        name: Optional[str] = None,
        base_path: Optional[str] = None,
    ) -> Path:
        name = name or self._name
        base_path = base_path or self._base_path
        path = Path(base_path, "functions", name)
        return path

    def _construct_version_path(
        self,
        version_id: Optional[str] = None,
        name: Optional[str] = None,
        base_path: Optional[str] = None,
    ) -> Path:
        version_id = version_id or self._get_version_id()
        path = Path(
            self._construct_function_path(name, base_path), "versions", version_id
        )
        return path

    def _construct_input_path(
        self,
        input_id: str,
        version_id: Optional[str] = None,
        name: Optional[str] = None,
        base_path: Optional[str] = None,
    ) -> Path:
        path = Path(
            self._construct_version_path(version_id, name, base_path),
            "inputs",
            input_id,
        )
        return path

    def _construct_output_path(
        self,
        input_id: str,
        version_id: Optional[str] = None,
        name: Optional[str] = None,
        base_path: Optional[str] = None,
    ) -> Path:
        path = Path(
            self._construct_input_path(input_id, version_id, name, base_path), "outputs"
        )
        return path

    def get_path_from_inputs(self, *args, **kwargs) -> Path:
        input_id = self._get_input_id(*args, **kwargs)
        path = self._construct_output_path(input_id=input_id)
        return path

    def get_path_from_ids(
        self, input_id: str, version_id: Optional[str] = None
    ) -> Path:
        path = self._construct_output_path(input_id=input_id, version_id=version_id)
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

    def _read_from_cache(self, path):
        try:
            return self._serializer.load(path)
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
                self._serializer.dump(result, tmp_path)
                assert tmp_path.exists(), f"{tmp_path} (for {path}) does not exist."
                assert tmp_path.is_dir(), f"{tmp_path} (for {path}) is not a folder."
                assert any(
                    tmp_path.iterdir()
                ), f"{tmp_path} (for {path}) is an empty folder."
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.exists() and path.is_dir():
                    shutil.rmtree(path)
                shutil.copytree(tmp_path, path)
                self._dump_version_metadata()
        except Exception as error:
            raise DumpException(str(error)) from error

    def load_from_inputs(self, *args, **kwargs):
        path = self.get_path_from_inputs(*args, **kwargs)
        if not path.exists():
            raise MissingResultException("result not found in cache")
        return self._read_from_cache(path)

    def load_from_ids(self, input_id: str, version_id: Optional[str] = None):
        path = self._construct_output_path(input_id=input_id, version_id=version_id)
        if not path.exists():
            raise MissingResultException("result not found in cache")
        return self._read_from_cache(path)

    def get_ids_from_inputs(self, *args, **kwargs) -> dict:
        return {
            "input_id": self._get_input_id(*args, **kwargs),
            "version_id": self._get_version_id(),
        }

    def _dump_version_metadata(self) -> None:
        """
        Save metadata.json for the current version (if it doesn't already exist)
        Save it at `<base_path>/<name>/versions/<version_id>/metadata.json`
        """
        metadata_path = Path(self._construct_version_path(), "metadata.json")
        if not metadata_path.exists():
            output_metadata = {
                "created_at": datetime.datetime.utcnow().isoformat() + "Z",
                "version_id": self._get_version_id(),
                "metadata": self._metadata,
            }
            try:
                commit_hash = (
                    subprocess.check_output(["git", "rev-parse", "HEAD"])
                    .decode("utf-8")
                    .strip()
                )
                # check if there are any uncommitted changes (clean=False if there are)
                clean = (
                    subprocess.check_output(["git", "status", "--porcelain"])
                    .decode("utf-8")
                    .strip()
                    == ""
                )
                output_metadata["git"] = {"commit_hash": commit_hash, "clean": clean}
            except subprocess.CalledProcessError as error:
                self._logger.warning(f"failed to get git metadata: {error}")
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(output_metadata, f)

    @staticmethod
    def _get_directory_statistics(directory: Path) -> dict:
        """
        Get statistics for a given directory: number of subdirectories and total bytes.
        """
        if directory.exists():
            return {
                "count": len(list(directory.glob("*/"))),
                "total_bytes": sum(
                    f.stat().st_size for f in directory.glob("**/*") if f.is_file()
                ),
            }
        else:
            return {"count": 0, "total_bytes": 0}

    @staticmethod
    def _load_version_metadata(metadata_path: Path) -> dict:
        """
        Read version metadata from a JSON file.
        """
        return (
            json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata_path.exists()
            else {}
        )

    def get_versions(self) -> list:
        """
        Loop through all versions in cache and return the metadata.json for each
        i.e. `<base_path>/<name>/versions/*/metadata.json`
        """
        versions_path = Path(self._construct_function_path(), "versions").resolve(
            strict=False
        )
        versions = [
            {
                **self._load_version_metadata(version_dir / "metadata.json"),
                "statistics": self._get_directory_statistics(version_dir / "inputs"),
            }
            for version_dir in versions_path.glob("*/")
        ]
        versions.sort(key=lambda x: x["created_at"])
        return versions


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
