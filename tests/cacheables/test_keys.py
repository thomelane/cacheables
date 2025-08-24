from cacheables.core import CacheableFunction
from cacheables.keys import (
    FunctionKey,
    InputKey,
    create_key_builder,
    default_key_builder,
)


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

        input_id = default_key_builder(sample_func, (), {})
        assert isinstance(input_id, str)
        assert len(input_id) == 16  # input_id should be 16 chars

    def test_function_with_args(self):
        def sample_func(a, b):
            return a + b

        input_id1 = default_key_builder(sample_func, (1, 2), {})
        input_id2 = default_key_builder(sample_func, (1, 2), {})
        input_id3 = default_key_builder(sample_func, (1, 3), {})

        assert input_id1 == input_id2
        assert input_id1 != input_id3
        assert isinstance(input_id1, str)

    def test_function_with_kwargs(self):
        def sample_func(a, b=10):
            return a + b

        input_id1 = default_key_builder(sample_func, (1,), {"b": 20})
        input_id2 = default_key_builder(sample_func, (1,), {"b": 20})
        input_id3 = default_key_builder(sample_func, (1,), {"b": 30})

        assert input_id1 == input_id2
        assert input_id1 != input_id3

    def test_function_with_defaults(self):
        def sample_func(a, b=10):
            return a + b

        # These should be the same because b gets the default value
        input_id1 = default_key_builder(sample_func, (1,), {})
        input_id2 = default_key_builder(sample_func, (1,), {"b": 10})

        assert input_id1 == input_id2

    def test_exclude_args_function(self):
        def sample_func(a, _private, b):
            return a + b

        input_id1 = default_key_builder(sample_func, (1, "secret", 3), {})
        input_id2 = default_key_builder(sample_func, (1, "different_secret", 3), {})

        # Should be the same because _private is excluded
        assert input_id1 == input_id2

    def test_custom_exclude_args_function(self):
        def sample_func(exclude_me, keep_me):
            return keep_me

        def custom_exclude(arg_name):
            return arg_name == "exclude_me"

        custom_key_builder = create_key_builder(custom_exclude)
        input_id1 = custom_key_builder(sample_func, ("value1", "keep"), {})
        input_id2 = custom_key_builder(sample_func, ("value2", "keep"), {})

        # Should be the same because "exclude_me" is excluded
        assert input_id1 == input_id2

    def test_argument_order_consistency(self):
        def sample_func(a, b, c):
            return a + b + c

        # Different argument binding orders should produce same input_id
        input_id1 = default_key_builder(sample_func, (1, 2, 3), {})
        input_id2 = default_key_builder(sample_func, (1, 2), {"c": 3})
        input_id3 = default_key_builder(sample_func, (1,), {"b": 2, "c": 3})

        assert input_id1 == input_id2 == input_id3

    def test_unhashable_arguments(self):
        def sample_func(data):
            return len(data)

        # Lists are unhashable by hash() but pickle can handle them
        input_id1 = default_key_builder(sample_func, ([1, 2, 3],), {})
        input_id2 = default_key_builder(sample_func, ([1, 2, 3],), {})
        input_id3 = default_key_builder(sample_func, ([1, 2, 4],), {})

        assert input_id1 == input_id2
        assert input_id1 != input_id3


class TestCoreCompatibility:
    """Test that default_key_builder matches CacheableFunction behavior from core.py"""

    def _get_core_input_id(self, fn, args, kwargs):
        """Generate input_id using actual CacheableFunction implementation"""
        cacheable_fn = CacheableFunction(fn)
        return cacheable_fn.get_input_id(*args, **kwargs)

    def test_simple_function_matches_core(self):
        def test_func(a, b):
            return a + b

        args = (1, 2)
        kwargs = {}

        core_input_id = self._get_core_input_id(test_func, args, kwargs)
        new_input_id = default_key_builder(test_func, args, kwargs)

        assert core_input_id == new_input_id

    def test_function_with_defaults_matches_core(self):
        def test_func(a, b=10, c=20):
            return a + b + c

        args = (5,)
        kwargs = {"c": 30}

        core_input_id = self._get_core_input_id(test_func, args, kwargs)
        new_input_id = default_key_builder(test_func, args, kwargs)

        assert core_input_id == new_input_id

    def test_function_with_excluded_args_matches_core(self):
        def test_func(a, _private, b):
            return a + b

        args = (1, "secret", 3)
        kwargs = {}

        core_input_id = self._get_core_input_id(test_func, args, kwargs)
        new_input_id = default_key_builder(test_func, args, kwargs)

        assert core_input_id == new_input_id

    def test_complex_arguments_match_core(self):
        def test_func(data, options=None, _debug=False):
            return len(data) if data else 0

        args = ([1, 2, 3, 4],)
        kwargs = {"options": {"mode": "fast", "threads": 4}, "_debug": True}

        core_input_id = self._get_core_input_id(test_func, args, kwargs)
        new_input_id = default_key_builder(test_func, args, kwargs)

        assert core_input_id == new_input_id

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

        input_ids = []
        for args, kwargs in test_cases:
            core_input_id = self._get_core_input_id(test_func, args, kwargs)
            new_input_id = default_key_builder(test_func, args, kwargs)
            assert core_input_id == new_input_id
            input_ids.append(new_input_id)

        # All should produce the same input_id
        first_input_id = input_ids[0]
        assert all(input_id == first_input_id for input_id in input_ids)

    def test_unhashable_types_match_core(self):
        def test_func(data_list, data_dict):
            return len(data_list) + len(data_dict)

        args = ([1, 2, 3], {"a": 1, "b": 2})
        kwargs = {}

        core_input_id = self._get_core_input_id(test_func, args, kwargs)
        new_input_id = default_key_builder(test_func, args, kwargs)

        assert core_input_id == new_input_id


class TestPerformance:
    """Test performance optimizations like hash caching"""

    def test_hash_argument_caching(self):
        """Test that _hash_argument is cached and only computed once per unique argument"""
        import pickle
        from unittest.mock import patch

        from cacheables.keys import _hash_argument

        # Clear any existing cache
        _hash_argument.cache_clear()

        # Mock pickle.dumps to count how many times it's called
        with patch("cacheables.keys.pickle.dumps", wraps=pickle.dumps) as mock_dumps:
            # Call with the same hashable argument multiple times
            test_arg = (1, 2, 3, 4, 5)  # Use tuple (hashable) instead of list

            hash1 = _hash_argument(test_arg)
            hash2 = _hash_argument(test_arg)  # Should use cache
            hash3 = _hash_argument(test_arg)  # Should use cache

            # All hashes should be identical
            assert hash1 == hash2 == hash3

            # pickle.dumps should only be called once due to caching
            assert mock_dumps.call_count == 1

        # Test with a different argument - should call pickle.dumps again
        with patch("cacheables.keys.pickle.dumps", wraps=pickle.dumps) as mock_dumps:
            different_arg = (6, 7, 8)  # Different hashable tuple
            hash4 = _hash_argument(different_arg)

            # Should be different from previous hashes
            assert hash4 != hash1

            # pickle.dumps should be called once for the new argument
            assert mock_dumps.call_count == 1

        # Test unhashable arguments fall back correctly (no caching)
        with patch("cacheables.keys.pickle.dumps", wraps=pickle.dumps) as mock_dumps:
            unhashable_arg = [1, 2, 3]  # List is unhashable

            hash5 = _hash_argument(unhashable_arg)
            hash6 = _hash_argument(unhashable_arg)  # Should NOT use cache

            assert hash5 == hash6  # Same hash
            assert mock_dumps.call_count == 2  # Called twice since unhashable

    def test_default_key_builder_reuses_hashes(self):
        """Test that default_key_builder benefits from hash caching when same args appear"""
        import pickle
        from unittest.mock import patch

        from cacheables.keys import _hash_argument

        # Clear cache
        _hash_argument.cache_clear()

        def test_func(a, b, c):
            return a + b + c

        with patch("cacheables.keys.pickle.dumps", wraps=pickle.dumps) as mock_dumps:
            # First call
            input_id1 = default_key_builder(test_func, (1, 2, 3), {})
            first_call_count = mock_dumps.call_count

            # Second call with same args - should reuse cached hashes
            input_id2 = default_key_builder(test_func, (1, 2, 3), {})
            second_call_count = mock_dumps.call_count

            # Input IDs should be identical
            assert input_id1 == input_id2

            # Second call should use fewer pickle.dumps calls due to caching
            # (it will still call pickle.dumps for the final arguments tuple)
            assert (
                second_call_count < first_call_count + 3
            )  # Not +3 individual arg calls


class TestCacheableFunctionKeyBuilder:
    """Test CacheableFunction with custom key_builder parameter"""

    def test_custom_key_builder(self):
        """Test CacheableFunction with a custom key builder"""

        def custom_key_builder(fn, args, kwargs):
            return f"custom_{args[0]}"

        def test_func(x, y):
            return x + y

        cacheable_fn = CacheableFunction(test_func, key_builder=custom_key_builder)
        input_id = cacheable_fn.get_input_id(5, 10)
        assert input_id == "custom_5"

    def test_default_behavior_unchanged(self):
        """Test that default behavior is unchanged when key_builder=None"""

        def test_func(x, y):
            return x + y

        cacheable_fn1 = CacheableFunction(test_func, key_builder=None)
        cacheable_fn2 = CacheableFunction(test_func)

        input_id1 = cacheable_fn1.get_input_id(5, 10)
        input_id2 = cacheable_fn2.get_input_id(5, 10)

        assert input_id1 == input_id2
