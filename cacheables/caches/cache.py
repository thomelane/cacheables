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

import os
import warnings

from ..keys import FunctionKey, InputKey


class Cache(ABC):

    def __init__(self):
        self._controller = CacheController()

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

    def enable(self, read: bool = True, write: bool = True) -> contextlib.AbstractContextManager[None]:
        return self._controller.enable(read=read, write=write)

    def disable(self) -> contextlib.AbstractContextManager[None]:
        return self._controller.disable()

    def is_write_enabled(self) -> Optional[bool]:
        return self._controller.is_write_enabled()
    
    def is_read_enabled(self) -> Optional[bool]:
        return self._controller.is_read_enabled()


class CacheController:
    def __init__(self):
        self._read: Optional[bool] = None
        self._write: Optional[bool] = None
        self._global = GlobalCacheController()

    def enable(self, read: bool = True, write: bool = True) -> contextlib.AbstractContextManager[None]:
        previous_read, previous_write = self._read, self._write
        self._read, self._write = read, write

        @contextlib.contextmanager
        def context_manager():
            try:
                yield
            finally:
                self._read, self._write = previous_read, previous_write

        return context_manager()
    
    def disable(self) -> contextlib.AbstractContextManager[None]:
        return self.enable(read=False, write=False)

    def is_write_enabled(self) -> Optional[bool]:
        if (self._write is False) or (self._global.is_write_enabled() is False):
            return False
        elif (self._write is True) or (self._global.is_write_enabled() is True):
            return True
        else:
            return None

    def is_read_enabled(self) -> Optional[bool]:
        if (self._read is False) or (self._global.is_read_enabled() is False):
            return False
        elif (self._read is True) or (self._global.is_read_enabled() is True):
            return True
        else:
            return None


class GlobalCacheController:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalCacheController, cls).__new__(cls)
            cls._instance._read = None
            cls._instance._write = None
        return cls._instance
    
    def enable(self, read: bool = True, write: bool = True) -> contextlib.AbstractContextManager[None]:
        previous_read, previous_write = self._read, self._write
        self._read, self._write = read, write

        @contextlib.contextmanager
        def context_manager():
            try:
                yield
            finally:
                self._read, self._write = previous_read, previous_write

        return context_manager()
    
    def disable(self) -> contextlib.AbstractContextManager[None]:
        return self.enable(read=False, write=False)

    def is_read_enabled(self) -> Optional[bool]:
        env_var_enabled = self._env_var_enabled()
        if (self._read is False) or (env_var_enabled is False):
            return False
        elif (self._read is True) or (env_var_enabled is True):
            return True
        else:
            return None
    
    def is_write_enabled(self) -> Optional[bool]:
        env_var_enabled = self._env_var_enabled()
        if (self._write is False) or (env_var_enabled is False):
            return False
        elif (self._write is True) or (env_var_enabled is True):
            return True
        else:
            return None
    
    @staticmethod
    def _env_var_enabled() -> Optional[bool]:
        enabled = os.environ.get("CACHEABLES_ENABLED", "").lower() == "true"
        disabled = os.environ.get("CACHEABLES_DISABLED", "").lower() == "true"
        if enabled and disabled:
            warnings.warn("CACHEABLES_ENABLED and CACHEABLES_DISABLED are both set to true.")
            return False
        elif disabled:
            return False
        elif enabled:
            return True
        else:
            return None


enable_all_caches = GlobalCacheController().enable
disable_all_caches = GlobalCacheController().disable


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
