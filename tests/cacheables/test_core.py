# pylint: disable=C0103,C0104,C0116,W0621

from typing import Any, Tuple
from unittest import mock

import pytest

from cacheables import (
    CacheableFunction,
    DiskCache,
    PickleSerializer,
    cacheable,
    disable_all_caches,
    enable_all_caches,
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


def test_cacheable_with_custom_key_builder(tmpdir):
    def custom_key_builder(fn, args, kwargs):
        return f"test_{args[0]}"

    @cacheable(cache=DiskCache(base_path=tmpdir), key_builder=custom_key_builder)
    def foo(value: int) -> int:
        return value * 2

    # Test that it uses the custom key builder
    assert foo.get_input_id(42) == "test_42"

    # Test that caching works with custom key
    result1 = foo(42)
    result2 = foo(42)
    assert result1 == result2 == 84


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


@pytest.mark.filterwarnings("ignore:failed to load output")
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


@pytest.mark.filterwarnings("ignore:failed to dump output")
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
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
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
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
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
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
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
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
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
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
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
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
):
    foo, inner_fn, deserialize, serialize = observable_foo

    with enable_all_caches():
        assert foo(1, 2) == 3  # call inner_fn and serialize
        assert foo(1, 2) == 3  # call deserialize

    assert inner_fn.call_count == 1
    assert deserialize.call_count == 1
    assert serialize.call_count == 1


def test_cacheable_disable_cache_via_env_var(
    observable_foo: Tuple[CacheableFunction, mock.Mock, mock.Mock, mock.Mock],
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
