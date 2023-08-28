import pytest

from cacheables.serializers import PickleSerializer, check_serializer


@pytest.mark.parametrize(
    "data", [{"name": "John", "age": 30}, ["apple", "banana", "cherry"]]
)
def test_serializer(data):
    serializer = PickleSerializer()
    check_serializer(serializer, data)
