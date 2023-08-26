# Cacheables

[![build](https://github.com/thomelane/cacheables/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/thomelane/cacheables/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/thomelane/cacheables/graph/badge.svg?token=3NNXBUBZIU)](https://codecov.io/gh/thomelane/cacheables)

Cacheables is a Python package that makes it easy to cache function outputs.

Cacheables is well suited to building efficient data workflows, because:

* downstream steps will automatically recompute after making a change
* everything is versioned (the function, the inputs and the outputs)
* the cache is reused between different processes/executions (since it's on disk)
* cached outputs are readable (since you choose the file format)

`@cacheable` is the decorator that makes a function cacheable.

```python
@cacheable
def foo(text: str) -> int:
    sleep(10)  # simulate a long running function
    return len(str)

# will execute as normal by default
foo("hello")  # returns after 10 seconds
foo("hello")  # returns after 10 seconds

with foo.enable_cache():
    foo("world")  # returns after 10 seconds (writes to cache)
    foo("world")  # returns immediately (reads from cache)
```

When the cache is enabled, the following happens:

* an `input_key` will be calculated from the provided args
* if the `input_key` exists in the cache
    * the output will be loaded from the cache
        * using `cache.read` and then `serializer.deserialize`
    * and the output will be returned
* if the `input_key` doesn't exist in the cache
    * the original function will execute to get an output
    * the output will be dumped in the cache
        * using `serializer.serialize` and then `cache.write`
    * and the output will be returned

## PickleSerializer & DiskCache

When you use `@cacheable` without any argument, `PickleSerializer`
and `DiskCache` will be used by default. After executing a function
like `foo("hello")` with the cache enabled, you can expect to see the
following files on disk:

```
<cwd>/.cacheables/functions/<function_id>/inputs/<input_id>/<output_id>.pickle
<cwd>/.cacheables/functions/<function_id>/inputs/<input_id>/metadata.json
```

### `function_id`

An `function_id` uniquely identifies a function. Unless specified using the
`function_id` argument to `cacheable`, the `function_id` will take the following
form: `module.submodule:foo`.

### `input_id`

An `input_id` uniquely identifies a set of inputs to a function. We assume that
changes to the inputs of a function will result in a change to the output of the
function. Under the hood, each `input_id` is created by first hashing each
individual input argument (which is itself cached!) and then hashing all of the
argument hashes into a single hash.

### `output_id`

An `output_id` uniquely identifies an output to a function. Similar to the
`input_id`, it is a hash of the function's output.

## Usage

Start by wrapping your function with the `@cacheable` decorator.

```python
@cacheable
def foo(text: str) -> int:
    sleep(10)  # simulate a long running function
    return len(str)
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
    return len(str)
```

See the `@cacheable` docstring for more details.

### Caching

Use `foo.enable_cache()` to enable the cache on a single function or
`enable_all_caches` to enable the cache on all functions.

```python
@cacheable
def foobar(text: str) -> int:
    sleep(10)  # simulate another long running function
    return len(str)

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

### Cache Setting

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

### Output load

Often you just want to load a result from the cache, but not execute it.
You can do this by using the `load_output` method.

```python
input_id = foo.get_input_id("hello")
output = foo.load_output(input_id)  # will error if result is not in cache
```

### Output dump

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
