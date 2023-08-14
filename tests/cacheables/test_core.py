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
    PickleSerializer,
    DiskBackend
)


@pytest.fixture
def observable_foo(
    tmp_path,
) -> Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]:
    serializer = PickleSerializer()
    serializer.dump = mock.Mock(side_effect=serializer.dump)
    serializer.load = mock.Mock(side_effect=serializer.load)
    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(backend=DiskBackend(base_path=tmp_path, serializer=serializer))
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    return foo, inner_fn, serializer.load, serializer.dump


def test_cacheable(tmpdir):
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(a: int, b: int) -> int:
        return a + b

    output = foo(1, 2)
    assert output == 3


def test_cacheable_with_complex_args(tmpdir):
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(lst: list, dct: dict) -> int:
        return len(lst) + len(dct)

    result = foo([1, 2, 3], {"a": 1, "b": 2})
    assert result == 5


def test_cacheable_function_no_args(tmpdir):
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo() -> str:
        return "no arguments here"

    result = foo()
    assert result == "no arguments here"


def test_cacheable_cache_enabled(tmpdir):
    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
        inner_fn.assert_called_once()

    with foo.enable_cache():
        assert foo(1, 2) == 3
        inner_fn.assert_called_once()


def test_cacheable_cache_path(tmpdir):
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(a: int, b: int) -> int:
        return a + b

    expected_path = Path(
        str(tmpdir),
        "functions",
        foo._get_function_id(),
        "versions",
        foo._get_version_id(),
        "inputs",
        foo._get_input_id(1, 2),
        "output"
    )

    input_key = foo.get_input_key(1, 2)
    file_path = foo._backend._construct_output_path(input_key)
    assert file_path == expected_path


def test_cacheable_version_id(tmpdir):
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(a, b):
        return a + b

    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def bar(a, b):
        # different implementation, but signature is the same
        # so same version id
        return a * b

    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def baz(a, b, c):
        return a + b + c

    assert foo._get_version_id() == bar._get_version_id()
    assert foo._get_version_id() != baz._get_version_id()


def test_cacheable_change_metadata(tmpdir):
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(_) -> int:
        return 1

    with foo.enable_cache():
        assert foo(1) == 1

    # changed implementation but didn't update version/signature
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(_) -> int:  # pylint: disable=function-redefined
        return 2

    with foo.enable_cache():
        assert foo(1) == 1  # should still return the old cached result

    # updated version/signature this time
    @cacheable(backend=DiskBackend(base_path=tmpdir))
    def foo(_, blank=None) -> int:  # pylint: disable=function-redefined
        return 2

    with foo.enable_cache():
        assert foo(1) == 2


def test_cacheable_with_load_fn_error(tmpdir):
    def load_fn(path):
        raise ValueError("An error occurred in load_fn.")

    serializer = PickleSerializer()
    serializer.dump = mock.Mock(side_effect=serializer.dump)
    serializer.load = mock.Mock(side_effect=load_fn)

    @cacheable(backend=DiskBackend(base_path=tmpdir, serializer=serializer))
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert serializer.dump.call_count == 2


def test_cacheable_with_dump_fn_noop(tmpdir):
    def dump_fn(value, path):
        return None

    serializer = PickleSerializer()
    serializer.dump = mock.Mock(side_effect=dump_fn)
    serializer.load = mock.Mock(side_effect=serializer.load)

    @cacheable(backend=DiskBackend(base_path=tmpdir, serializer=serializer))
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    serializer.load.assert_not_called()


def test_cacheable_with_dump_fn_error(tmpdir):
    def dump_fn(value, path):
        raise ValueError("An error occurred in dump_fn.")

    serializer = PickleSerializer()
    serializer.dump = mock.Mock(side_effect=dump_fn)
    serializer.load = mock.Mock(side_effect=serializer.load)

    @cacheable(backend=DiskBackend(base_path=tmpdir, serializer=serializer))
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    serializer.load.assert_not_called()


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
