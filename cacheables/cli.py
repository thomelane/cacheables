import click
import importlib
from .core import CacheableFunction


@click.group()
def cacheables():
    """Manage cache for CacheableFunction."""
    pass


def load_function_from_path(path) -> callable:
    """Load a Python function from a given path."""
    if ":" not in path:
        raise click.BadParameter("path should be in the format 'module.submodule:function_name'.")
    module_path, func_name = path.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise click.BadParameter(f"Module '{module_path}' not found.") from exc
    func = getattr(module, func_name, None)
    if not func or not callable(func):
        raise click.BadParameter(f"Function '{func_name}' not found in module '{module_path}'.")
    if not isinstance(func, CacheableFunction):
        raise click.BadParameter(f"Function '{func_name}' is not a CacheableFunction. Must be decorated with @cacheable.")
    return func


@cacheables.command()
@click.argument('function_path')
def check(function_path):
    """
    Check a cacheable function is valid.
    """
    func = load_function_from_path(function_path)
    click.echo(f"Loaded function: {func.__name__} from {function_path}")


@cacheables.command()
@click.argument('function_id')
@click.argument('function_name')
def adopt(function_id, function_name):
    """
    Adopt another cacheable function's cache.
    Useful after a function rename to preserve the original cache.
    Assumes the other cache used the same backend.
    """
    _ = load_function_from_path(function_name)
    print(f"Renaming {function_id} to {function_name}")


@cacheables.command()
@click.argument('function_name')
def delete(function_name):
    """
    Delete (all or part of) a cacheable function's cache.
    Useful for invalidating cache and/or reducing cache size.
    """
    _ = load_function_from_path(function_name)
    print(f"Deleting cache for {function_name}")


if __name__ == "__main__":
    cacheables()
