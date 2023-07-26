# Cacheable

Cacheable is a module that make it easy to cache function results. You'll be
able to experiment faster (by avoiding repeated work) and keep track of your
experiments with out-of-the-box versioning.

@cacheable is the decorator that makes a function cacheable.

A cacheable function executes just like a regular function by default, but gives
you a convenient way to cache the results to disk if needed. When you call the
cacheable function again (in the same process... or a completely different one
days later), the result will be loaded from disk instead of executing the
original function again.

```python
@cacheable
def foo(text: str) -> int:
    sleep(10)  # simulate a long running function
    return len(str)


foo("hello")  #  returns 5 after 10 seconds
foo("hello")  #  returns 5 after 10 seconds
with foo.enable_cache():
    foo("hello")  #  returns 5 after 10 seconds
    foo("hello")  #  returns 5 immediately
    foo("world")  #  returns 5 after 10 seconds
    foo("world")  #  returns 5 immediately

# same or different process

with foo.enable_cache():
    foo("hello")  #  returns 5 immediately
```

When the cache is enabled, the following happens:

* the cache path will be calculated
* if the cache path exists
    * the result will be loaded from the cache path (using a `load_fn`)
    * and the result will be returned
* if the cache path does not exist
    * the original function will execute to get a result
    * the result will be stored at the cache path (using a `dump_fn`)
    * and the result will be returned

## Cache Path

A cache path (which is always a directory) looks as follows:

`<base_path>/<name>/versions/<version_id>/inputs/<input_id>/outputs/`

### `input-id`

An `input-id` uniquely identifies a set of inputs to a function. We assume that
changes to the inputs of a function will result in a change to the output of the
function. You can customize how the `input-id` is calculated, but by default it's
the hash of the args and kwargs passed to the cacheable function.

### `version-id`

When a cacheable function's implementation changes, results we have in the cache may no
longer be valid. We can solve this by changing the `version-id` of the cacheable
function which, by default, is calculated by hashing the cacheable function's
metadata. When the `version-id` changes, the cache path changes, and we store
a new result instead of retrieving the old one.

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
    base_path="/tmp",
    name="string_lengths",
    metadata={"version": "2.1.3"},
    dump_fn=dump_to_txt,
    load_fn=load_from_txt,
)
def foo(text: str) -> int:
    sleep(10)  # simulate a long running function
    return len(str)
```

See the `@cacheable` docstring for more details.

### Caching

Use the `enable_cache` context manager:

```python
foo("hello")  #  returns 5 after 10 seconds
foo("hello")  #  returns 5 after 10 seconds
with foo.enable_cache():
    foo("hello")  #  returns 5 after 10 seconds
    foo("hello")  #  returns 5 immediately
```

You can use `enable_cache` on multiple cacheable functions at the same time:

```python
with foo.enable_cache(), bar.enable_cache():
    foo("hello")
    bar("world")
```

Or all cacheable functions at the same time:

```python
from cacheable import enable_cache

with enable_cache():
    foo("hello")
    bar("world")
```


### Cache Setting

When an cacheable function is called inside a `enable_cache` context manager, the
cache will be read from and written too. Sometimes you might need to leave the
results in the cache untouched, or even overwrite the results in the cache. You can
do this by specifying the `read` and `write` arguments.

```python
with foo.enable_cache(read=False, write=True):
    foo("hello")  # foo called, and result added to cache
    foo("hello")  # foo called, and result readded to cache
```

### Overriding Cache Settings

You might have a function which calls a cacheable function with the cache
enabled, but you might need to disable the cache without changing that original
function's code. e.g. you might be running code in a production environment, or
testing/debugging code.

You can call `disable_cache` in these situations (which is equivalent to calling
`enable_cache(read=False, write=False)`).

```python
def bar(text: str) -> int:
    with foo.enable_cache():
        return foo(text)

bar("something")  # foo called
bar("something")  # cached result for foo is used

with foo.disable_cache():
    bar("else")  # foo is called, and no result is added to cache
    bar("else")  # foo is called, and no result is added to cache
```

When different `enable_cache` settings are used, the most restrictive settings
will be used for `read` and `write`.

```python
with foo.enable_cache(read=False, write=True):
    with foo.enable_cache(read=True, write=False):
        foo("something")  # foo is called, and no result is added to cache
        foo("something")  # foo is called, and no result is added to cache
```

### Value load

Often you just want to load a result from the cache, but not execute it.
You can do this by using the `load` method.

```python
result = foo.load(1, "2")  # will load result from cache, and will error if result is not in cache
```
