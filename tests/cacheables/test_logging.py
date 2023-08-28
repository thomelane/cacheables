from cacheables import cacheable
from cacheables.logging import disable_logging, enable_logging


def test_disable_logging(capsys):
    @cacheable
    def foo(a, b):
        return a + b

    disable_logging()
    with foo.enable_cache():
        foo(1, 2)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""

    enable_logging()
    with foo.enable_cache():
        foo(1, 2)
    captured = capsys.readouterr()
    assert captured.out != ""
    assert captured.err == ""
