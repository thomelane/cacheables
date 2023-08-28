from pathlib import Path

from cacheables import cacheable, DiskCache


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
        "3ca08f64e96a37c2.pickle",
    )

    with foo.enable_cache():
        assert foo(1, 2) == 3

    assert expected_path.exists() and expected_path.is_file()
