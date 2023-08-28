import pytest

from cacheables.serializers import JsonSerializer, check_serializer


@pytest.mark.parametrize(
    "data", [{"name": "John", "age": 30}, ["apple", "banana", "cherry"]]
)
def test_serializer(data):
    serializer = JsonSerializer()
    check_serializer(serializer, data)
