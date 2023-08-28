from typing import List, Optional, Union
from pathlib import Path
import os
import json
import shutil
import datetime

from .base import BaseCache
from ..keys import FunctionKey, InputKey
from ..exceptions import ReadException, WriteException, InputKeyNotFoundError


class DiskCache(BaseCache):
    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        super().__init__()
        self._base_path = (
            base_path
            or os.getenv("CACHEABLES_DISK_CACHE_BASE_PATH")
            or os.getcwd() + "/.cacheables"
        )
        self._base_path = Path(self._base_path).expanduser().resolve()

    # path construction methods

    def _construct_functions_path(self) -> Path:
        base_path = Path(self._base_path)
        return base_path / "functions"

    def _construct_function_path(self, function_key: FunctionKey) -> Path:
        functions_path = self._construct_functions_path()
        return functions_path / function_key.function_id

    def _construct_inputs_path(self, function_key: FunctionKey) -> Path:
        function_path = self._construct_function_path(function_key)
        return function_path / "inputs"

    def _construct_input_path(self, input_key: InputKey) -> Path:
        inputs_path = self._construct_inputs_path(input_key.function_key)
        return inputs_path / input_key.input_id

    def _construct_metadata_path(self, input_key: InputKey) -> Path:
        input_path = self._construct_input_path(input_key)
        return input_path / "metadata.json"

    def _construct_output_path(self, input_key: InputKey, metadata: dict) -> Path:
        input_path = self._construct_input_path(input_key)
        extension = metadata["serializer"].get("extension", "bin")
        filename = f"{metadata['output_id']}.{extension}"
        return input_path / filename

    def get_output_path(self, input_key: InputKey) -> str:
        if not self.exists(input_key):
            raise InputKeyNotFoundError(f"{input_key} not found in cache")
        metadata = self.load_metadata(input_key)
        output_path = self._construct_output_path(input_key, metadata)
        return str(output_path)

    # input methods

    def exists(self, input_key: InputKey) -> bool:
        output_path = self._construct_input_path(input_key)
        return output_path.exists() and output_path.is_dir()

    def list(self, function_key: FunctionKey) -> List[InputKey]:
        inputs_path = self._construct_inputs_path(function_key)
        return [
            InputKey(function_id=function_key.function_id, input_id=folder.name)
            for folder in inputs_path.glob("*/")
        ]

    def evict(self, input_key: InputKey) -> None:
        input_path = self._construct_input_path(input_key)
        shutil.rmtree(input_path, ignore_errors=True)

    def clear(self, function_key: FunctionKey) -> None:
        function_path = self._construct_function_path(function_key)
        shutil.rmtree(function_path, ignore_errors=True)

    def adopt(
        self, from_function_key: FunctionKey, to_function_key: FunctionKey
    ) -> None:
        from_path = self._construct_function_path(from_function_key)
        to_path = self._construct_function_path(to_function_key)
        shutil.copytree(from_path, to_path, dirs_exist_ok=True)
        shutil.rmtree(from_path, ignore_errors=True)

    # metadata methods

    def dump_metadata(self, metadata: dict, input_key: InputKey) -> None:
        metadata_path = self._construct_metadata_path(input_key)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)

    def load_metadata(self, input_key: InputKey) -> dict:
        metadata_path = self._construct_metadata_path(input_key)
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # output methods

    def read_output(self, metadata: dict, input_key: InputKey) -> bytes:
        try:
            output_path = self._construct_output_path(input_key, metadata)
            with open(output_path, "rb") as file:
                output_bytes = file.read()
            return output_bytes
        except Exception as error:
            raise ReadException(str(error)) from error

    def write_output(
        self, output_bytes: bytes, metadata: dict, input_key: InputKey
    ) -> None:
        try:
            output_path = self._construct_output_path(input_key, metadata)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "wb") as file:
                file.write(output_bytes)
        except Exception as error:
            raise WriteException(str(error)) from error

    # last accessed

    def update_last_accessed(self, input_key: InputKey) -> None:
        metadata = self.load_metadata(input_key)
        metadata["last_accessed"] = datetime.datetime.utcnow().isoformat() + "Z"
        self.dump_metadata(metadata, input_key)

    def get_last_accessed(self, input_key: InputKey) -> Optional[datetime.datetime]:
        metadata = self.load_metadata(input_key)
        if "last_accessed" in metadata:
            return datetime.datetime.fromisoformat(metadata["last_accessed"])
        return None
