import json
import pickle
from pathlib import Path
from typing import Any
from abc import ABC, abstractmethod


class Serializer(ABC):
    @abstractmethod
    def dump(self, value: Any, path: Path):
        pass

    @abstractmethod
    def load(self, path: Path):
        pass


class JsonSerializer(Serializer):
    def _get_path(self, path: Path):
        return path / "output.json"

    def dump(self, value: Any, path: Path):
        filepath = self._get_path(path)
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(value, file, indent=4)

    def load(self, path: Path):
        filepath = self._get_path(path)
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)


class PickleSerializer(Serializer):
    def _get_path(self, path: Path):
        return path / "output.pickle"

    def dump(self, value: Any, path: Path):
        filepath = self._get_path(path)
        with open(filepath, "wb") as file:
            pickle.dump(value, file)

    def load(self, path: Path):
        filepath = self._get_path(path)
        with open(filepath, "rb") as file:
            return pickle.load(file)
