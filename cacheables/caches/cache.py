# We will start with a single cache called DiskCache.
# We will use the filesystem to store the outputs from our cacheable functions (and metadata about them).

# An implemented cache will be responsible for:
# - checking if a given key exists
# - listing all inputs of a given function (given function_key)
# - loading/dumping input metadata (given input_key)
# - making a temporary/atomic resource for the output serializer to use (for both load and dump)
# - loading/dumping a function output (given input_key)


import contextlib
from abc import ABC, abstractmethod
from typing import List, Any, Optional



from ..keys import FunctionKey, InputKey


class Cache(ABC):

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
    def read_output(self, input_key: InputKey) -> Any:
        pass

    @abstractmethod
    def read_metadata(self, input_key: InputKey) -> dict:
        pass
    
    @abstractmethod
    def write_output(self, output: Any, metadata: dict, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def write_metadata(self, metadata: dict, input_key: InputKey) -> None:
        pass

    @abstractmethod
    def output_path(self, input_key: InputKey) -> Optional[str]:
        """
        path if it exists
        """
        pass


## fix __call__ from CacheableFunction
## add output hash to metadata
## call file the hash

# priority rules:
    # env > instance setting > global setting
    # disable > enable
    # local disable > env enable


# env disable (will trump everything)
# instance disable
# global disable
# env enable
# instance enable
# global enable
# env unset
# instance unset
# global unset
