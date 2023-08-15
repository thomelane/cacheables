# <base_path>/functions/<function_key>/versions/<version_key>/metadata.json
# <base_path>/functions/<function_key>/versions/<version_key>/inputs/<input_key>/metadata.json
# <base_path>/functions/<function_key>/versions/<version_key>/inputs/<input_key>/outputs/<arbritrary-files-can-go-here>


from typing import List, Optional, Union, Any
from pathlib import Path
import os
import json
import tempfile
import shutil
import datetime

from .backend import Backend
from ..serializers import Serializer, PickleSerializer
from ..keys import FunctionKey, InputKey
from ..exceptions import (
    ReadException,
    WriteException
)


class DiskBackend(Backend):

    def __init__(
        self,
        base_path: Optional[Union[str, Path]] = None,
        serializer: Optional[Serializer] = None,
    ):
        self._base_path = base_path or os.getcwd() + "/.cacheables"
        self._serializer = serializer or PickleSerializer()

    # path construction methods

    def _construct_functions_path(self) -> Path:
        base_path = Path(self._base_path)
        return base_path / 'functions'

    def _construct_function_path(self, function_key: FunctionKey) -> Path:
        functions_path = self._construct_functions_path()
        return functions_path / function_key.function_id
    
    def _construct_inputs_path(self, function_key: FunctionKey) -> Path:
        function_path = self._construct_function_path(function_key)
        return function_path / 'inputs'
    
    def _construct_input_path(self, input_key: InputKey) -> Path:
        inputs_path = self._construct_inputs_path(input_key.function_key)
        return inputs_path / input_key.input_id
    
    def _construct_input_metadata_path(self, input_key: InputKey) -> Path:
        input_path = self._construct_input_path(input_key)
        return input_path / 'metadata.json'
    
    def _construct_output_path(self, input_key: InputKey) -> Path:
        input_path = self._construct_input_path(input_key)
        return input_path / f"output.{self._serializer.extension}"

    # input methods

    def list(self, function_key: FunctionKey) -> List[InputKey]:
        inputs_path = self._construct_inputs_path(function_key)
        return [
            InputKey(
                function_id=function_key.function_id,
                input_id=folder.name
            ) for folder in inputs_path.glob("*/")
        ]

    def delete(self, input_key: InputKey) -> None:
        input_path = self._construct_input_path(input_key)
        shutil.rmtree(input_path, ignore_errors=True)

    def write_metadata(self, metadata: dict, input_key: InputKey) -> None:
        input_metadata_path = self._construct_input_metadata_path(input_key)
        input_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(input_metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f)

    def read_metadata(self, input_key: InputKey) -> dict:
        input_metadata_path = self._construct_input_metadata_path(input_key)
        with open(input_metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    # output methods

    def exists(self, input_key: InputKey) -> bool:
        output_path = self._construct_output_path(input_key)
        return output_path.exists() and output_path.is_file()
    
    def read(self, input_key: InputKey) -> Any:
        try:
            metadata_path = self._construct_input_metadata_path(input_key)
            with open(metadata_path, "r", encoding="utf-8") as file:
                metadata = json.load(file)
            metadata["last_accessed_at"] = datetime.datetime.utcnow().isoformat() + "Z"
            with open(metadata_path, "w", encoding="utf-8") as file:
                json.dump(metadata, file, indent=4)
            output_path = self._construct_output_path(input_key)
            with open(output_path, "rb") as file:
                output_bytes = file.read()
            output = self._serializer.deserialize(output_bytes)
            return output
        except Exception as error:
            raise ReadException(str(error)) from error
        
    def write(self, output: Any, metadata: dict, input_key: InputKey) -> None:
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
            path = self._construct_input_path(input_key)
            relative_metadata_path = self._construct_input_metadata_path(input_key).relative_to(path)
            relative_output_path = self._construct_output_path(input_key).relative_to(path)
            with tempfile.TemporaryDirectory() as tmp_path:
                tmp_path = Path(tmp_path)
                # write metadata to temporary folder
                metadata_path = tmp_path / relative_metadata_path
                with open(metadata_path, "w", encoding="utf-8") as file:
                    json.dump(metadata, file, indent=4)
                # write output to temporary folder
                output_path = tmp_path / relative_output_path
                output_bytes = self._serializer.serialize(output)
                with open(output_path, "wb") as file:
                    file.write(output_bytes)
                # move from temporary folder to real path
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.exists() and path.is_dir():
                    shutil.rmtree(path)
                shutil.copytree(tmp_path, path)
                # tempfile handles cleanup of temporary folder
        except Exception as error:
            raise WriteException(str(error)) from error
