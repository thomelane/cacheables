from dataclasses import dataclass


@dataclass
class FunctionKey:
    function_id: str


@dataclass
class VersionKey:
    function_id: str
    version_id: str

    @property
    def function_key(self) -> FunctionKey:
        return FunctionKey(function_id=self.function_id)


@dataclass
class InputKey:
    function_id: str
    version_id: str
    input_id: str

    @property
    def function_key(self) -> FunctionKey:
        return FunctionKey(function_id=self.function_id)
    
    @property
    def version_key(self) -> VersionKey:
        return VersionKey(function_id=self.function_id, version_id=self.version_id)
