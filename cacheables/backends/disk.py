# <base_path>/functions/<function_key>/versions/<version_key>/metadata.json
# <base_path>/functions/<function_key>/versions/<version_key>/inputs/<input_key>/metadata.json
# <base_path>/functions/<function_key>/versions/<version_key>/inputs/<input_key>/outputs/<arbritrary-files-can-go-here>


from typing import List, Generator, Optional, Union, Any
from pathlib import Path
import os
import json
import tempfile
import shutil
import contextlib

from .backend import Backend
from ..serializers import Serializer, PickleSerializer
from ..keys import FunctionKey, VersionKey, InputKey
from ..exceptions import (
    LoadException,
    DumpException
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
    
    def _construct_versions_path(self, function_key: FunctionKey) -> Path:
        function_path = self._construct_function_path(function_key)
        return function_path / 'versions'
    
    def _construct_version_path(self, version_key: VersionKey) -> Path:
        versions_path = self._construct_versions_path(version_key.function_key)
        return versions_path / version_key.version_id
    
    def _construct_version_metadata_path(self, version_key: VersionKey) -> Path:
        version_path = self._construct_version_path(version_key)
        return version_path / 'metadata.json'
    
    def _construct_inputs_path(self, version_key: VersionKey) -> Path:
        version_path = self._construct_version_path(version_key)
        return version_path / 'inputs'
    
    def _construct_input_path(self, input_key: InputKey) -> Path:
        inputs_path = self._construct_inputs_path(input_key.version_key)
        return inputs_path / input_key.input_id
    
    def _construct_input_metadata_path(self, input_key: InputKey) -> Path:
        input_path = self._construct_input_path(input_key)
        return input_path / 'metadata.json'
    
    def _construct_output_path(self, input_key: InputKey) -> Path:
        input_path = self._construct_input_path(input_key)
        return input_path / 'output'

    # version methods

    def list_version_keys(self, function_key: FunctionKey) -> List[VersionKey]:
        versions_path = self._construct_versions_path(function_key)
        return [
            VersionKey(
                function_id=function_key.function_id,
                version_id=folder.name
            ) for folder in versions_path.glob("*/")
        ]

    def list_versions(self, function_key: FunctionKey) -> List[dict]:
        version_keys = self.list_version_keys(function_key)
        versions = [
            {
                **self.read_version_metadata(version_key),
                # "statistics": self._get_version_statistics(function_key, version_key)
            }
            for version_key in version_keys
        ]
        versions.sort(key=lambda x: x["created_at"])
        return versions

    def delete_version(self, version_key: VersionKey) -> None:
        pass

    def write_version_metadata(self, version_key: VersionKey, version_metadata: dict) -> None:
        version_metadata_path = self._construct_version_metadata_path(version_key)
        # will only write version metadata the first time
        if not version_metadata_path.exists():
            version_metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with open(version_metadata_path, "w", encoding="utf-8") as f:
                json.dump(version_metadata, f)
    
    # def _get_version_statistics(self, version_key: VersionKey) -> dict:
    #     """
    #     Get statistics for a given version: number of first-level subdirectories and total bytes.
    #     """
    #     inputs_path = self._construct_inputs_path(function_key, version_key)
    #     if inputs_path.exists():
    #         return {
    #             "count": len(list(inputs_path.glob("*/"))),
    #             "total_bytes": sum(
    #                 f.stat().st_size for f in inputs_path.glob("**/*") if f.is_file()
    #             ),
    #         }
    #     else:
    #         return {"count": 0, "total_bytes": 0}

    def read_version_metadata(self, version_key: VersionKey) -> dict:
        version_metadata_path = self._construct_version_metadata_path(version_key)
        with open(version_metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # input methods

    def list_input_keys(self, version_key: VersionKey) -> List[InputKey]:
        pass

    def delete_input(self, input_key: InputKey) -> None:
        pass

    def write_input_metadata(self, input_key: InputKey, input_metadata: dict) -> None:
        pass

    def read_input_metadata(self, input_key: InputKey) -> dict:
        pass
    
    # output methods

    def output_exists(self, input_key: InputKey) -> bool:
        output_path = self._construct_output_path(input_key)
        return output_path.exists() and output_path.is_dir()
    
    def read_output(self, input_key: InputKey) -> Any:
        try:
            output_path = self._construct_output_path(input_key)
            output = self._serializer.load(output_path)
            return output
        except Exception as error:
            raise LoadException(str(error)) from error
        
    def write_output(self, output: Any, input_key: InputKey) -> None:
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
            output_path = self._construct_output_path(input_key)
            with tempfile.TemporaryDirectory() as tmp_path:
                tmp_path = Path(tmp_path)
                self._serializer.dump(output, tmp_path)
                assert tmp_path.exists(), f"{tmp_path} (for {output_path}) does not exist."
                assert tmp_path.is_dir(), f"{tmp_path} (for {output_path}) is not a folder."
                assert any(
                    tmp_path.iterdir()
                ), f"{tmp_path} (for {output_path}) is an empty folder."
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if output_path.exists() and output_path.is_dir():
                    shutil.rmtree(output_path)
                shutil.copytree(tmp_path, output_path)
        except Exception as error:
            raise DumpException(str(error)) from error
