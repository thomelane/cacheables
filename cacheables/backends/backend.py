# We will start with a single backend called DiskBackend.
# We will use the filesystem to store the outputs from our cacheable functions (and metadata about them).

# A backend will be responsible for:
# - checking if a given key exists
# - listing all versions of a given function (given function_key)
# - loading/dumping version metadata (given function_key/version_key)
# - making a temporary/atomic resource for the output serializer to use (for both load and dump)
# - loading/dumping a function output (given function_key/version_key/input_key)



from abc import ABC, abstractmethod
from typing import List, Any

from ..keys import FunctionKey, VersionKey, InputKey


class Backend(ABC):

    # version methods

    @abstractmethod
    def list_version_keys(self, function_key: FunctionKey) -> List[VersionKey]:
        pass

    @abstractmethod
    def list_versions(self, function_key: FunctionKey) -> List[dict]:
        pass
    
    @abstractmethod
    def delete_version(self, version_key: VersionKey) -> None:
        pass
    
    @abstractmethod
    def write_version_metadata(self, version_key: VersionKey, version_metadata: dict) -> None:
        pass
    
    @abstractmethod
    def read_version_metadata(self, version_key: VersionKey) -> dict:
        pass

    # input methods

    @abstractmethod
    def list_input_keys(self, version_key: VersionKey) -> List[InputKey]:
        pass

    @abstractmethod
    def delete_input(self, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def write_input_metadata(self, input_key: InputKey, input_metadata: dict) -> None:
        pass
    
    @abstractmethod
    def read_input_metadata(self, input_key: InputKey) -> dict:
        pass
    
    # output methods

    @abstractmethod
    def output_exists(self, input_key: InputKey) -> bool:
        pass

    @abstractmethod
    def read_output(self, input_key: InputKey) -> Any:
        pass
    
    @abstractmethod
    def write_output(self, output: Any, input_key: InputKey) -> None:
        pass
