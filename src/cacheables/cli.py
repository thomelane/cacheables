import click
import importlib
from .core import CacheableFunction


@click.group()
def cacheables():
    """Manage caches of cacheable functions."""
    pass


def load_function_from_qualified_name(qualified_name) -> CacheableFunction:
    """Load a Python function from a given a qualified name."""
    if ":" not in qualified_name:
        raise click.BadParameter(
            "qualified_name should be in the format 'module.submodule:function_name'."
        )
    module_path, func_name = qualified_name.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise click.BadParameter(f"Module '{module_path}' not found.") from exc
    func = getattr(module, func_name, None)
    if not func or not callable(func):
        raise click.BadParameter(
            f"Function '{func_name}' not found in module '{module_path}'."
        )
    if not isinstance(func, CacheableFunction):
        raise click.BadParameter(
            f"Function '{func_name}' is not a CacheableFunction. "
            "Must be decorated with @cacheable."
        )
    return func


@cacheables.command()
@click.argument("function_id")
@click.argument("function_qualified_name")
def adopt(function_id, function_qualified_name):
    """
    Adopt another CacheableFunction's cache.
    Useful after a function rename to reuse the existing cache.
    Assumes both caches are of the same type (i.e. both DiskCache).
    """
    fn = load_function_from_qualified_name(function_qualified_name)
    fn.adopt_cache(function_id)


@cacheables.command()
@click.argument("function_qualified_name")
def clear(function_qualified_name):
    """
    Clear the cache of a CacheableFunction.
    Useful for invalidating cache and/or reducing cache size.
    """
    fn = load_function_from_qualified_name(function_qualified_name)
    fn.clear_cache()


if __name__ == "__main__":
    cacheables()
