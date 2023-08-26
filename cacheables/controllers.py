import os
import warnings
from typing import Optional, Callable, Any
import contextlib


class CacheController:
    def __init__(self):
        self._read: Optional[bool] = None
        self._write: Optional[bool] = None
        self._filter: Optional[Callable] = None
        self._global = GlobalCacheController()

    def enable(
        self,
        read: bool = True,
        write: bool = True,
        filter: Optional[Callable] = lambda output: True,
    ) -> contextlib.AbstractContextManager[None]:
        previous_read = self._read
        previous_write = self._write
        previous_filter = self._filter
        self._read = read
        self._write = write
        self._filter = filter

        @contextlib.contextmanager
        def context_manager():
            try:
                yield
            finally:
                self._read = previous_read
                self._write = previous_write
                self._filter = previous_filter

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

    def is_passing_filter(self, output: Any) -> bool:
        if self._filter is None:
            return True
        else:
            return self._filter(output)


class GlobalCacheController:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalCacheController, cls).__new__(cls)
            cls._instance._read = None
            cls._instance._write = None
        return cls._instance

    def enable(
        self, read: bool = True, write: bool = True
    ) -> contextlib.AbstractContextManager[None]:
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
            warnings.warn(
                "CACHEABLES_ENABLED and CACHEABLES_DISABLED are both set to true."
            )
            return False
        elif disabled:
            return False
        elif enabled:
            return True
        else:
            return None


enable_all_caches = GlobalCacheController().enable
disable_all_caches = GlobalCacheController().disable
