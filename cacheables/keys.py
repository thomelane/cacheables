from dataclasses import dataclass


@dataclass
class FunctionKey:
    function_id: str


@dataclass
class InputKey:
    function_id: str
    input_id: str

    @property
    def function_key(self) -> FunctionKey:
        return FunctionKey(function_id=self.function_id)
