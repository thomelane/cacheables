# pylint: disable=C0103,C0104,C0116,W0621

import os
from pathlib import Path
from typing import Tuple
from unittest import mock

import pytest

from cacheables import (
    CacheableFunction,
    cacheable,
    disable_cache,
    enable_cache,
    pickle_dump,
    pickle_load,
)


@pytest.fixture
def observable_foo(
    tmp_path,
) -> Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]:
    load_fn = mock.Mock(side_effect=pickle_load)
    dump_fn = mock.Mock(side_effect=pickle_dump)
    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(base_path=tmp_path, load_fn=load_fn, dump_fn=dump_fn)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    return foo, inner_fn, load_fn, dump_fn


def test_cacheable(tmpdir):
    @cacheable(base_path=tmpdir)
    def foo(a: int, b: int) -> int:
        return a + b

    result = foo(1, 2)
    assert result == 3
    file_path = foo.get_path_from_inputs(1, 2)
    assert not os.path.exists(file_path)


def test_cacheable_with_complex_args(tmpdir):
    @cacheable(base_path=tmpdir)
    def foo(lst: list, dct: dict) -> int:
        return len(lst) + len(dct)

    result = foo([1, 2, 3], {"a": 1, "b": 2})
    assert result == 5


def test_cacheable_function_no_args(tmpdir):
    @cacheable(base_path=tmpdir)
    def foo() -> str:
        return "no arguments here"

    result = foo()
    foo.get_path_from_inputs()
    assert result == "no arguments here"


def test_cacheable_cache_enabled(tmpdir):
    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(base_path=tmpdir)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
        inner_fn.assert_called_once()

    with foo.enable_cache():
        assert foo(1, 2) == 3
        inner_fn.assert_called_once()

    path = foo.get_path_from_inputs(1, 2)
    assert os.path.exists(path)


def test_cacheable_cache_path(tmpdir):
    @cacheable(base_path=tmpdir)
    def foo(a: int, b: int) -> int:
        return a + b

    expected_path = Path(
        str(tmpdir),
        "functions",
        "foo",
        "versions",
        foo._version_id_fn(foo._name, foo._metadata),
        "inputs",
        foo._input_id_fn(1, 2),
        "outputs",
    )

    file_path = foo.get_path_from_inputs(1, 2)
    assert file_path == expected_path


def test_cacheable_version_id(tmpdir):
    @cacheable(base_path=tmpdir, name="shared", metadata={"version": 1})
    def foo(result):
        return result

    @cacheable(base_path=tmpdir, name="shared", metadata={"version": 1})
    def bar(result):
        return result

    @cacheable(base_path=tmpdir, name="shared", metadata={"version": 2})
    def baz(result):
        return result

    assert foo._get_version_id() == bar._get_version_id()
    assert foo._get_version_id() != baz._get_version_id()


def test_cacheable_change_metadata(tmpdir):
    @cacheable(base_path=tmpdir, metadata={"version": 1})
    def foo(_) -> int:
        return 1

    with foo.enable_cache():
        assert foo(1) == 1

    # changed implementation but didn't update version
    @cacheable(base_path=tmpdir, metadata={"version": 1})
    def foo(_) -> int:  # pylint: disable=function-redefined
        return 2

    with foo.enable_cache():
        assert foo(1) == 1  # should still return the old cached result

    # updated version this time
    @cacheable(base_path=tmpdir, metadata={"version": 2})
    def foo(_) -> int:  # pylint: disable=function-redefined
        return 2

    with foo.enable_cache():
        assert foo(1) == 2


def test_cacheable_with_version_id_fn_error(tmpdir):
    def version_id_fn(name: str, metadata: dict):
        raise ValueError("An error occurred in version_id_fn.")

    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(base_path=tmpdir, version_id_fn=version_id_fn)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert inner_fn.call_count == 2


def test_cacheable_with_version_id_fn_empty(tmpdir):
    def version_id_fn(_):
        return None

    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(base_path=tmpdir, version_id_fn=version_id_fn)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert inner_fn.call_count == 2


def test_cacheable_with_input_id_fn_error(tmpdir):
    def input_id_fn(*args, **kwargs):
        raise ValueError("An error occurred in input_id_fn.")

    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(base_path=tmpdir, input_id_fn=input_id_fn)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert inner_fn.call_count == 2


def test_cacheable_with_input_id_fn_empty(tmpdir):
    def input_id_fn(*args, **kwargs):  # pylint: disable=unused-argument
        return None

    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(base_path=tmpdir, input_id_fn=input_id_fn)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert inner_fn.call_count == 2


def test_cacheable_with_load_fn_error(tmpdir):
    def load_fn(path):
        raise ValueError("An error occurred in load_fn.")

    dump_fn = mock.Mock(side_effect=pickle_dump)

    @cacheable(base_path=tmpdir, load_fn=load_fn, dump_fn=dump_fn)
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert dump_fn.call_count == 2


def test_cacheable_with_dump_fn_noop(tmpdir):
    load_fn = mock.Mock(side_effect=lambda path: None)
    dump_fn = mock.Mock(side_effect=lambda obj, path: None)

    @cacheable(base_path=tmpdir, load_fn=load_fn, dump_fn=dump_fn)
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    load_fn.assert_not_called()


def test_cacheable_with_dump_fn_error(tmpdir):
    load_fn = mock.Mock(side_effect=lambda path: None)

    def dump_fn(obj, path):
        raise ValueError("An error occurred in dump_fn.")

    @cacheable(base_path=tmpdir, load_fn=load_fn, dump_fn=dump_fn)
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    load_fn.assert_not_called()


def test_cacheable_cache_read_only(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and dump_fn

    with foo.enable_cache(read=True, write=False):
        assert foo(3, 4) == 7  # call inner_fn
        assert foo(1, 2) == 3  # call load_fn

    assert inner_fn.call_count == 2
    assert load_fn.call_count == 1
    assert dump_fn.call_count == 1


def test_cacheable_cache_write_only(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and dump_fn

    with foo.enable_cache(read=False, write=True):
        assert foo(1, 2) == 3  # call inner_fn and dump_fn

    assert inner_fn.call_count == 2
    assert load_fn.call_count == 0
    assert dump_fn.call_count == 2


def test_cacheable_cache_disabled(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and dump_fn

    with foo.disable_cache():
        assert foo(1, 2) == 3  # call inner_fn

    with foo.enable_cache(read=False, write=False):
        assert foo(1, 2) == 3  # call inner_fn

    assert inner_fn.call_count == 3
    assert load_fn.call_count == 0
    assert dump_fn.call_count == 1


def test_cacheable_cache_override(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and dump_fn

    with foo.disable_cache():
        with foo.enable_cache():
            assert foo(1, 2) == 3  # call inner_fn

    with foo.enable_cache():
        with foo.disable_cache():
            assert foo(1, 2) == 3  # call inner_fn

    with foo.enable_cache(read=True, write=False):
        with foo.enable_cache(read=False, write=True):
            assert foo(1, 2) == 3  # call inner_fn

    assert inner_fn.call_count == 4
    assert load_fn.call_count == 0
    assert dump_fn.call_count == 1


def test_cacheable_disable_cache_globally(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with disable_cache():
        with foo.enable_cache():
            assert foo(1, 2) == 3  # call inner_fn
            assert foo(1, 2) == 3  # call inner_fn

    assert inner_fn.call_count == 2
    assert load_fn.call_count == 0
    assert dump_fn.call_count == 0


def test_cacheable_enable_cache_globally(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and dump_fn
        assert foo(1, 2) == 3  # call load_fn

    assert inner_fn.call_count == 1
    assert load_fn.call_count == 1
    assert dump_fn.call_count == 1


def test_cacheable_disable_cache_via_env_var(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, load_fn, dump_fn = observable_foo

    with mock.patch.dict("os.environ", {"DISABLE_CACHEABLE": "true"}):
        with foo.enable_cache():
            assert foo(1, 2) == 3  # call inner_fn, but not dump_fn
            assert foo(1, 2) == 3  # call inner_fn, but not load_fn

        assert inner_fn.call_count == 2
        assert load_fn.call_count == 0
        assert dump_fn.call_count == 0
