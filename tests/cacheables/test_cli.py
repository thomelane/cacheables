import sys
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from cacheables import cacheable, CacheableFunction
from cacheables.cli import load_function_from_qualified_name, cacheables, adopt, clear


@pytest.fixture
def runner():
    return CliRunner()


@cacheable
def foo():
    pass


def bar():
    pass


# make above functions importable (as `test_cli:(foo|bar)`)
sys.path.append(".")


def test_load_function_from_qualified_name():
    fn = load_function_from_qualified_name("tests.cacheables.test_cli:foo")
    assert isinstance(fn, CacheableFunction)


def test_load_function_from_qualified_name_invalid_format():
    with pytest.raises(click.BadParameter):
        load_function_from_qualified_name("foo")


def test_load_function_from_qualified_name_non_existent_module():
    with pytest.raises(click.BadParameter):
        load_function_from_qualified_name("tests.cacheables.non_existent_module:foo")


def test_load_function_from_qualified_name_non_existent_function():
    with pytest.raises(click.BadParameter):
        load_function_from_qualified_name("tests.cacheables.test_cli:baz")


def test_load_function_from_qualified_name_non_cacheable_function():
    with pytest.raises(click.BadParameter):
        load_function_from_qualified_name("tests.cacheables.test_cli:bar")


def test_group(runner):
    runner = CliRunner()
    result = runner.invoke(cacheables)
    assert result.exit_code == 0


def test_adopt(runner):
    # Mock the adopt_cache method to make it a no-op
    with patch("cacheables.CacheableFunction.adopt_cache") as mock_adopt:
        result = runner.invoke(adopt, ["sample_function_id", "tests.cacheables.test_cli:foo"])
        mock_adopt.assert_called_once_with("sample_function_id")
        assert result.exit_code == 0


def test_clear(runner):
    # Mock the adopt_cache method to make it a no-op
    with patch("cacheables.CacheableFunction.clear_cache") as mock_clear:
        result = runner.invoke(clear, ["tests.cacheables.test_cli:foo"])
        mock_clear.assert_called_once()
        assert result.exit_code == 0
