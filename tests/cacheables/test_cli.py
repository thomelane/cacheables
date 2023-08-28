import click
from click.testing import CliRunner
import pytest
import sys
from unittest.mock import patch

from cacheables import cacheable, CacheableFunction
from cacheables.cli import load_function_from_qualified_name, adopt, clear


@pytest.fixture
def runner():
    return CliRunner()


@cacheable
def foo():
    pass


# make above function importable (as `test_cli:foo`)
sys.path.append(".")


def test_load_function_from_qualified_name():
    fn = load_function_from_qualified_name("test_cli:foo")
    assert isinstance(fn, CacheableFunction)


def test_load_function_from_qualified_name_non_existent():
    with pytest.raises(click.BadParameter):
        load_function_from_qualified_name("non_existent_module:func")


def test_adopt(runner):
    # Mock the adopt_cache method to make it a no-op
    with patch("cacheables.CacheableFunction.adopt_cache") as mock_adopt:
        result = runner.invoke(adopt, ["sample_function_id", "test_cli:foo"])
        mock_adopt.assert_called_once_with("sample_function_id")
        assert result.exit_code == 0


def test_clear(runner):
    # Mock the adopt_cache method to make it a no-op
    with patch("cacheables.CacheableFunction.clear_cache") as mock_clear:
        result = runner.invoke(clear, ["test_cli:foo"])
        mock_clear.assert_called_once()
        assert result.exit_code == 0
