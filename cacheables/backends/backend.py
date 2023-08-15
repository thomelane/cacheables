# We will start with a single backend called DiskBackend.
# We will use the filesystem to store the outputs from our cacheable functions (and metadata about them).

# A backend will be responsible for:
# - checking if a given key exists
# - listing all inputs of a given function (given function_key)
# - loading/dumping input metadata (given input_key)
# - making a temporary/atomic resource for the output serializer to use (for both load and dump)
# - loading/dumping a function output (given input_key)



from abc import ABC, abstractmethod
from typing import List, Any

from ..keys import FunctionKey, InputKey


class Backend(ABC):

    # input methods

    @abstractmethod
    def list(self, function_key: FunctionKey) -> List[InputKey]:
        pass

    @abstractmethod
    def delete(self, input_key: InputKey) -> None:
        pass
    
    # output methods

    @abstractmethod
    def exists(self, input_key: InputKey) -> bool:
        pass

    @abstractmethod
    def read(self, input_key: InputKey) -> Any:
        pass

    @abstractmethod
    def read_metadata(self, input_key: InputKey) -> dict:
        pass
    
    @abstractmethod
    def write(self, output: Any, metadata: dict, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def write_metadata(self, metadata: dict, input_key: InputKey) -> None:
        pass
