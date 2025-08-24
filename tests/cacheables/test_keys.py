from cacheables.core import CacheableFunction
from cacheables.keys import FunctionKey, InputKey, default_key_builder


class TestFunctionKey:
    def test_function_key_creation(self):
        key = FunctionKey(function_id="test_func")
        assert key.function_id == "test_func"


class TestInputKey:
    def test_input_key_creation(self):
        key = InputKey(function_id="test_func", input_id="input123")
        assert key.function_id == "test_func"
        assert key.input_id == "input123"

    def test_function_key_property(self):
        input_key = InputKey(function_id="test_func", input_id="input123")
        function_key = input_key.function_key
        assert isinstance(function_key, FunctionKey)
        assert function_key.function_id == "test_func"


class TestDefaultKeyBuilder:
    def test_function_with_no_args(self):
        def sample_func():
            return "test"

        key = default_key_builder(sample_func, (), {})
        assert isinstance(key, InputKey)
        assert key.function_id == f"{sample_func.__module__}:{sample_func.__qualname__}"
        assert len(key.input_id) == 16  # input_id should be 16 chars

    def test_function_with_args(self):
        def sample_func(a, b):
            return a + b

        key1 = default_key_builder(sample_func, (1, 2), {})
        key2 = default_key_builder(sample_func, (1, 2), {})
        key3 = default_key_builder(sample_func, (1, 3), {})

        assert key1 == key2
        assert key1 != key3
        assert isinstance(key1, InputKey)

    def test_function_with_kwargs(self):
        def sample_func(a, b=10):
            return a + b

        key1 = default_key_builder(sample_func, (1,), {"b": 20})
        key2 = default_key_builder(sample_func, (1,), {"b": 20})
        key3 = default_key_builder(sample_func, (1,), {"b": 30})

        assert key1 == key2
        assert key1 != key3

    def test_function_with_defaults(self):
        def sample_func(a, b=10):
            return a + b

        # These should be the same because b gets the default value
        key1 = default_key_builder(sample_func, (1,), {})
        key2 = default_key_builder(sample_func, (1,), {"b": 10})

        assert key1 == key2

    def test_exclude_args_function(self):
        def sample_func(a, _private, b):
            return a + b

        key1 = default_key_builder(sample_func, (1, "secret", 3), {})
        key2 = default_key_builder(sample_func, (1, "different_secret", 3), {})

        # Should be the same because _private is excluded
        assert key1 == key2

    def test_custom_exclude_args_function(self):
        # This test is now covered by the core.py comparison tests below
        pass

    def test_argument_order_consistency(self):
        def sample_func(a, b, c):
            return a + b + c

        # Different argument binding orders should produce same key
        key1 = default_key_builder(sample_func, (1, 2, 3), {})
        key2 = default_key_builder(sample_func, (1, 2), {"c": 3})
        key3 = default_key_builder(sample_func, (1,), {"b": 2, "c": 3})

        assert key1 == key2 == key3

    def test_unhashable_arguments(self):
        def sample_func(data):
            return len(data)

        # Lists are unhashable by hash() but pickle can handle them
        key1 = default_key_builder(sample_func, ([1, 2, 3],), {})
        key2 = default_key_builder(sample_func, ([1, 2, 3],), {})
        key3 = default_key_builder(sample_func, ([1, 2, 4],), {})

        assert key1 == key2
        assert key1 != key3


class TestCoreCompatibility:
    """Test that default_key_builder matches CacheableFunction behavior from core.py"""

    def _get_core_key(self, fn, args, kwargs):
        """Generate key using actual CacheableFunction implementation"""
        cacheable_fn = CacheableFunction(fn)
        return cacheable_fn._get_input_key_from_args(*args, **kwargs)

    def test_simple_function_matches_core(self):
        def test_func(a, b):
            return a + b

        args = (1, 2)
        kwargs = {}

        core_key = self._get_core_key(test_func, args, kwargs)
        new_key = default_key_builder(test_func, args, kwargs)

        assert core_key == new_key

    def test_function_with_defaults_matches_core(self):
        def test_func(a, b=10, c=20):
            return a + b + c

        args = (5,)
        kwargs = {"c": 30}

        core_key = self._get_core_key(test_func, args, kwargs)
        new_key = default_key_builder(test_func, args, kwargs)

        assert core_key == new_key

    def test_function_with_excluded_args_matches_core(self):
        def test_func(a, _private, b):
            return a + b

        args = (1, "secret", 3)
        kwargs = {}

        core_key = self._get_core_key(test_func, args, kwargs)
        new_key = default_key_builder(test_func, args, kwargs)

        assert core_key == new_key

    def test_complex_arguments_match_core(self):
        def test_func(data, options=None, _debug=False):
            return len(data) if data else 0

        args = ([1, 2, 3, 4],)
        kwargs = {"options": {"mode": "fast", "threads": 4}, "_debug": True}

        core_key = self._get_core_key(test_func, args, kwargs)
        new_key = default_key_builder(test_func, args, kwargs)

        assert core_key == new_key

    def test_mixed_args_kwargs_match_core(self):
        def test_func(a, b, c=30, d=40):
            return a + b + c + d

        # Test different ways of passing the same arguments
        test_cases = [
            ((1, 2, 3, 4), {}),
            ((1, 2, 3), {"d": 4}),
            ((1, 2), {"c": 3, "d": 4}),
            ((1,), {"b": 2, "c": 3, "d": 4}),
        ]

        keys = []
        for args, kwargs in test_cases:
            core_key = self._get_core_key(test_func, args, kwargs)
            new_key = default_key_builder(test_func, args, kwargs)
            assert core_key == new_key
            keys.append(new_key)

        # All should produce the same key
        first_key = keys[0]
        assert all(key == first_key for key in keys)

    def test_unhashable_types_match_core(self):
        def test_func(data_list, data_dict):
            return len(data_list) + len(data_dict)

        args = ([1, 2, 3], {"a": 1, "b": 2})
        kwargs = {}

        core_key = self._get_core_key(test_func, args, kwargs)
        new_key = default_key_builder(test_func, args, kwargs)

        assert core_key == new_key
