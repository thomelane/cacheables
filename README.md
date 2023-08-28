# Cacheables

[![build](https://github.com/thomelane/cacheables/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/thomelane/cacheables/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/thomelane/cacheables/graph/badge.svg?token=3NNXBUBZIU)](https://codecov.io/gh/thomelane/cacheables)

Cacheables is a Python package that makes it easy to cache function outputs.

Cacheables is well suited to building efficient data workflows, because:

* functions will only recompute if their inputs have changed.
* everything is versioned: the functions, the inputs and the outputs.
* the cache is reused between different processes/executions (stored on [`DiskCache`](https://github.com/thomelane/cacheables/blob/21bf54fb67b7f9cb2699915da3969b36a2519d9c/cacheables/caches/disk.py#L13) by default).
* cached outputs are readable since you choose the file format ([`PickleSerializer`](https://github.com/thomelane/cacheables/blob/21bf54fb67b7f9cb2699915da3969b36a2519d9c/cacheables/serializers.py#L29C27-L29C27) is just a default).

## Install

```bash
pip install cacheables
```

## Basic Example

`@cacheable` is the decorator that makes a function cacheable.

```python
# basic_example.py
from cacheables import cacheable
from time import sleep

@cacheable
def foo(text: str) -> int:
    sleep(1)  # simulate a long running function
    return len(text)

if __name__ == "__main__":
    foo("hello")
    with foo.enable_cache():
        foo("world")

# python basic_example.py  # 2 seconds
# python basic_example.py  # 1 seconds (foo("world") used cache)
```

When the cache is enabled on a function, the following happens:

* an `input_key` will be calculated from the function arguments
* if the `input_key` exists in the cache
    * the output will be loaded from the cache
        * using `cache.read` and then `serializer.deserialize`
    * and the output will be returned
* if the `input_key` doesn't exist in the cache
    * the original function will execute to get an output
    * the output will be dumped in the cache
        * using `serializer.serialize` and then `cache.write`
    * and the output will be returned

## Standard Example

Cacheables is well suited to building efficient data workflows.

As a simple example, let's assume we have a data workflow that processes a
string by removing the vowels, reversing the output, and then finally
concatenating that output with the original string. We'll assume that two of
these steps are computationally expensive (`remove_vowels` and `concatenate`),
so we decorate them with `@cacheable`.

After running the workflow twice (showing that the cached results are used), we
modify the workflow by removing the `reverse` step. Only `concatenate` is run on
the third workflow execution, which is much more efficient than running the
whole workflow (including `remove_vowels`) again.

```python
# standard_example.py
from cacheables import cacheable, enable_all_caches
from time import sleep

@cacheable
def remove_vowels(text: str) -> str:
    sleep(1)  # simulate a long running function
    return ''.join([char for char in text if char not in "aeiou"])

def reverse(text: str) -> str:
    return text[::-1]

@cacheable
def concatenate(reversed_text: str, text: str) -> str:
    sleep(1)  # simulate a long running function
    return (reversed_text + text)

def run_workflow(text: str) -> int:
    t = remove_vowels(text)
    t = reverse(t)
    output = concatenate(t, text)
    return output


if __name__ == "__main__":
    enable_all_caches()

    run_workflow("cache this")  # 2 seconds
    run_workflow("cache this")  # 0 seconds

    def run_workflow(text: str) -> int:
        t = remove_vowels(text)
        # t = reverse(t)  # removed
        output = concatenate(t, text)
        return output

    run_workflow("cache this")  # 1 second

# python standard_example.py  # 5 seconds
# python standard_example.py  # 0 seconds (current and previous are still both cached)
```

## Advanced Example

Cacheables has many other features, a few of which are shown below.

```python
# advanced_example.py
from cacheables import cacheable, enable_all_caches, enable_logging
from cacheables.caches import DiskCache
from cacheables.serializers import JsonSerializer
from time import sleep

@cacheable(
    function_id="example",
    cache=DiskCache(base_path="~/.cache"),
    serializer=JsonSerializer(),
    exclude_args_fn=lambda e: e in ["verbose"]
)
def foo(text: str, verbose: bool = False) -> int:
    sleep(1)  # simulate a long running function
    return len(text)

if __name__ == "__main__":
    enable_all_caches()
    enable_logging()

    foo("cache this")  # 1 seconds
    foo("cache this", verbose=True)  # 0 seconds

    # manually write output to cache
    input_id = foo.get_input_id("and cache that")
    foo.dump_output(14, input_id)
    foo("and cache that")  # 0 seconds

    # manually read output from cache
    input_id = foo.get_input_id("cache this")
    foo.load_output(input_id)  # 0 seconds

    # show output path in cache
    foo.get_output_path(input_id)
    # ~/.cache/functions/example/inputs/cf5b2ab47064bd0e/aab3238922bcc25a.json

    # only use certain outputs in cache, recompute others
    with foo.enable_cache(filter=lambda output: output <= 10):
        foo("cache this")  # 0 seconds
        foo("and cache that")  # 1 seconds

    # overwrite cache
    with foo.enable_cache(read=False, write=True):
        foo("cache this")  # 1 seconds

# python advanced_example.py  # 3 seconds
# python advanced_example.py  # 2 seconds (first foo("cache this") used cache)
```

### PickleSerializer & DiskCache

When you use `@cacheable` without any argument, `PickleSerializer` and
`DiskCache` will be used by default.

After executing a function like `foo("hello")` with the cache enabled, you can
expect to see the following files on disk:

```
<cwd>/.cacheables
└── functions
    └── <function_id>
        └── inputs
            └── <input_id>
                ├── <output_id>.pickle
                └── metadata.json
```

* `function_id`
    * An `function_id` uniquely identifies a function. Unless specified using the `function_id` argument to `cacheable`, the `function_id` will take the following form: `module.submodule:foo`.
* `input_id`
    * An `input_id` uniquely identifies a set of inputs to a function. We assume that changes to the inputs of a function will result in a change to the output of the function. Under the hood, each `input_id` is created by first hashing each individual input argument (which is itself cached!) and then hashing all of the argument hashes into a single hash.
* `output_id`
    * An `output_id` uniquely identifies an output to a function. Similar to the `input_id`, it is a hash of the function's output.

## Other Documentation

See the [official documentation](https://thomelane.github.io/cacheables/) for more details.

Start by wrapping your function with the `@cacheable` decorator.

```python
@cacheable
def foo(text: str) -> int:
    sleep(10)  # simulate a long running function
    return len(text)
```

Customization is possible by passing in arguments to the decorator.

```python
@cacheable(
    function_id="example",
    cache=DiskCache(base_path="~/.cache"),
    serializer=JsonSerializer(),
    exclude_args_fn=lambda e: e in ["verbose"]
)
def foo(text: str, verbose: bool = False) -> int:
    sleep(10)  # simulate a long running function
    return len(text)
```

See the `@cacheable` docstring for more details.

#### Caching

Use `foo.enable_cache()` to enable the cache on a single function or
`enable_all_caches` to enable the cache on all functions.

```python
@cacheable
def foobar(text: str) -> int:
    sleep(10)  # simulate another long running function
    return len(text)

foo.clear_cache()
foo("hello")  # returns after 10 seconds
foo("hello")  # returns after 10 seconds

foo.enable_cache()
foo("hello")  # returns after 10 seconds (writes to cache)
foo("hello")  # returns immediately (reads from cache)
foobar("hello")  # returns after 10 seconds
foobar("hello")  # returns after 10 seconds

enable_all_caches()
foobar("hello")  # returns after 10 seconds (writes to cache)
foobar("hello")  # returns immediately (reads from cache)
```

You can also use both of these as context managers, if you only want to enable
the cache temporarily within a certain scope.

```python
foo.clear_cache()
foobar.clear_cache()

foo("hello")  # returns after 10 seconds
foo("hello")  # returns after 10 seconds

with foo.enable_cache():
    foo("hello")  # returns after 10 seconds (writes to cache)
    foo("hello")  # returns immediately (reads from cache)
foo("hello")  # returns after 10 seconds

with foo.enable_cache(), bar.enable_cache():
    foo("hello")  # returns immediately (reads from cache)
    foobar("hello")  # returns after 10 seconds (writes to cache)
    foobar("hello")  # returns immediately (reads from cache)
foo("hello")  # returns after 10 seconds
foobar("hello")  # returns after 10 seconds

with enable_all_caches():
    foo("hello")  # returns immediately (reads from cache)
    foobar("hello")  # returns immediately (reads from cache)
foo("hello")  # returns after 10 seconds
foobar("hello")  # returns after 10 seconds
```

#### Cache Setting

When a cacheable function is called after `enable_cache`, the cache will be
read from and written too. Sometimes you might need to leave the results in the
cache untouched, or even overwrite the results in the cache. You can do this by
specifying the `read` and `write` arguments.

```python
foo.enable_cache(read=False, write=True)
foo("hello")  # foo called, and result added to cache
foo("hello")  # foo called, and result re-added to cache
```

You have three levels of cache settings:

* Function: controlled by `foo.enable_cache`/`foo.disable_cache`
* Global: controlled by `enable_all_caches`/`disable_all_caches`
* Environment: controlled by `CACHEABLES_ENABLED`/`CACHEABLES_DISABLED`

When nothing is explicitly enabled/disabled (i.e. default), the cache will be disabled so that the cacheable function runs without any caching. When *any* level is explicitly set to disabled, the cache will be disabled, regardless of the other level settings (even if they are explicitly set to enabled).

#### Output load

Often you just want to load a result from the cache, but not execute it.
You can do this by using the `load_output` method.

```python
input_id = foo.get_input_id("hello")
output = foo.load_output(input_id)  # will error if result is not in cache
```

#### Output dump

Some more advanced use-cases might want to manually write results to the cache (e.g. batched processing). You can do this by using the `dump_output` method.

```python
input_id = foo.get_input_id("hello")
output = foo.dump_output(5, input_id)
```


## Development

```bash
poetry install
poetry run task test    # pytest
poetry run task format  # black
poetry run task lint    # ruff
```

Use pre-commit to automatically format and lint before each commit.

```bash
pre-commit install
```
