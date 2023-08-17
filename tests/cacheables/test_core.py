# pylint: disable=C0103,C0104,C0116,W0621

from pathlib import Path
from typing import Tuple, Any
from unittest import mock

import pytest

from cacheables import (
    CacheableFunction,
    cacheable,
    disable_all_caches,
    enable_all_caches,
    PickleSerializer,
    DiskCache
)


@pytest.fixture
def observable_foo(
    tmp_path,
) -> Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]:
    serializer = PickleSerializer()
    serializer.serialize = mock.Mock(side_effect=serializer.serialize)
    serializer.deserialize = mock.Mock(side_effect=serializer.deserialize)
    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(cache=DiskCache(base_path=tmp_path), serializer=serializer)
    def foo(a: int, b: int) -> int:
        return inner_fn(a, b)

    return foo, inner_fn, serializer.deserialize, serializer.serialize


def test_cacheable(tmpdir):
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo(a: int, b: int) -> int:
        return a + b

    output = foo(1, 2)
    assert output == 3


def test_cacheable_with_complex_args(tmpdir):
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo(lst: list, dct: dict) -> int:
        return len(lst) + len(dct)

    result = foo([1, 2, 3], {"a": 1, "b": 2})
    assert result == 5


def test_cacheable_function_no_args(tmpdir):
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo() -> str:
        return "no arguments here"

    result = foo()
    assert result == "no arguments here"


def test_cacheable_cache_enabled(tmpdir):
    inner_fn = mock.Mock(side_effect=lambda a, b: a + b)

    @cacheable(cache=DiskCache(base_path=tmpdir))
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
    @cacheable(
        cache=DiskCache(base_path=tmpdir),
        function_id="foo",
    )
    def foo(a: int, b: int) -> int:
        return a + b

    expected_path = Path(
        str(tmpdir),
        "functions",
        "foo",
        "inputs",
        "ad089d3d19511caa",
        "3ca08f64e96a37c2.pickle"
    )

    with foo.enable_cache():
        assert foo(1, 2) == 3

    assert expected_path.exists() and expected_path.is_file()


def test_cacheable_change_metadata(tmpdir):
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo(_) -> int:
        return 1

    with foo.enable_cache():
        assert foo(1) == 1

    # changed implementation but didn't update version/signature
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo(_) -> int:  # pylint: disable=function-redefined
        return 2

    with foo.enable_cache():
        assert foo(1) == 1  # should still return the old cached result

    # updated version/signature this time
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo(_, blank=None) -> int:  # pylint: disable=function-redefined
        return 2

    with foo.enable_cache():
        assert foo(1) == 2


def test_cacheable_with_deserialize_error(tmpdir):
    def deserialize(value: bytes) -> Any:
        raise ValueError("An error occurred in deserialize.")

    serializer = PickleSerializer()
    serializer.serialize = mock.Mock(side_effect=serializer.serialize)
    serializer.deserialize = mock.Mock(side_effect=deserialize)

    @cacheable(cache=DiskCache(base_path=tmpdir), serializer=serializer)
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    assert serializer.serialize.call_count == 2


def test_cacheable_with_serialize_error(tmpdir):
    def serialize(value: Any) -> bytes:
        raise ValueError("An error occurred in serialize.")

    serializer = PickleSerializer()
    serializer.serialize = mock.Mock(side_effect=serialize)
    serializer.deserialize = mock.Mock(side_effect=serializer.deserialize)

    @cacheable(cache=DiskCache(base_path=tmpdir), serializer=serializer)
    def foo(a: int, b: int) -> int:
        return a + b

    with foo.enable_cache():
        assert foo(1, 2) == 3
        assert foo(1, 2) == 3
    serializer.deserialize.assert_not_called()


def test_cacheable_cache_read_only(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and serialize

    with foo.enable_cache(read=True, write=False):
        assert foo(3, 4) == 7  # call inner_fn
        assert foo(1, 2) == 3  # call deserialize

    assert inner_fn.call_count == 2
    assert deserialize.call_count == 1
    assert serialize.call_count == 1


def test_cacheable_cache_write_only(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and serialize

    with foo.enable_cache(read=False, write=True):
        assert foo(1, 2) == 3  # call inner_fn and serialize

    assert inner_fn.call_count == 2
    assert deserialize.call_count == 0
    assert serialize.call_count == 2


def test_cacheable_cache_disabled(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and serialize

    with foo.disable_cache():
        assert foo(1, 2) == 3  # call inner_fn

    with foo.enable_cache(read=False, write=False):
        assert foo(1, 2) == 3  # call inner_fn

    assert inner_fn.call_count == 3
    assert deserialize.call_count == 0
    assert serialize.call_count == 1


def test_cacheable_cache_override(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with foo.enable_cache():
        assert foo(1, 2) == 3  # call inner_fn and serialize

    with foo.disable_cache():
        with foo.enable_cache():
            assert foo(1, 2) == 3  # call deserialize

    with foo.enable_cache():
        with foo.disable_cache():
            assert foo(1, 2) == 3  # call inner_fn

    with foo.enable_cache(read=True, write=False):
        with foo.enable_cache(read=False, write=True):
            assert foo(1, 2) == 3  # call inner_fn and serialize

    assert inner_fn.call_count == 3
    assert deserialize.call_count == 1
    assert serialize.call_count == 2


def test_cacheable_disable_cache_globally(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with disable_all_caches():
        with foo.enable_cache():
            assert foo(1, 2) == 3  # call inner_fn
            assert foo(1, 2) == 3  # call inner_fn

    assert inner_fn.call_count == 2
    assert deserialize.call_count == 0
    assert serialize.call_count == 0


def test_cacheable_enable_cache_globally(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with enable_all_caches():
        assert foo(1, 2) == 3  # call inner_fn and serialize
        assert foo(1, 2) == 3  # call deserialize

    assert inner_fn.call_count == 1
    assert deserialize.call_count == 1
    assert serialize.call_count == 1


def test_cacheable_disable_cache_via_env_var(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock]
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with mock.patch.dict("os.environ", {"CACHEABLES_DISABLED": "true"}):
        with foo.enable_cache():
            assert foo(1, 2) == 3  # call inner_fn, but not serialize
            assert foo(1, 2) == 3  # call inner_fn, but not deserialize

        assert inner_fn.call_count == 2
        assert deserialize.call_count == 0
        assert serialize.call_count == 0


def test_cacheable_read(tmpdir):
    @cacheable(cache=DiskCache(base_path=tmpdir))
    def foo(a: int, b: int) -> int:
        return a + b

    foo.enable_cache()
    assert foo(1, 2) == 3
    input_id = foo.get_input_id(1, 2)
    assert foo.load_output(input_id) == 3
