import datetime
import json
import warnings
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import NoCredentialsError
from moto import mock_aws

from cacheables.caches.s3 import S3Cache, S3CacheLockingWarning
from cacheables.exceptions import InputKeyNotFoundError, WriteException
from cacheables.keys import FunctionKey, InputKey


class TestS3Cache:
    @pytest.fixture
    def s3_client(self):
        with mock_aws():
            yield boto3.client("s3", region_name="us-east-1")

    @pytest.fixture
    def s3_cache(self, s3_client):
        s3_client.create_bucket(Bucket="test-bucket")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", S3CacheLockingWarning)
            return S3Cache(bucket_name="test-bucket", prefix="test-prefix")

    @pytest.fixture
    def function_key(self):
        return FunctionKey(function_id="test_function")

    @pytest.fixture
    def input_key(self, function_key):
        return InputKey(function_id=function_key.function_id, input_id="test_input")

    def test_init_without_boto3(self):
        with patch("cacheables.caches.s3.boto3", None):
            with pytest.raises(ImportError, match="boto3 is required"):
                S3Cache(bucket_name="test-bucket")

    def test_init_with_credentials_error(self):
        with patch("cacheables.caches.s3.boto3") as mock_boto3:
            mock_boto3.client.side_effect = NoCredentialsError()
            with pytest.raises(WriteException, match="AWS credentials not found"):
                S3Cache(bucket_name="test-bucket")

    def test_init_with_bucket_not_found(self):
        with mock_aws():
            with pytest.raises(
                WriteException, match="S3 bucket 'nonexistent-bucket' not found"
            ):
                S3Cache(bucket_name="nonexistent-bucket")

    def test_init_issues_warning(self):
        with mock_aws():
            s3_client = boto3.client("s3", region_name="us-east-1")
            s3_client.create_bucket(Bucket="test-bucket")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                S3Cache(bucket_name="test-bucket")

                assert len(w) == 1
                assert "does not implement locking mechanisms" in str(w[0].message)
                assert issubclass(w[0].category, S3CacheLockingWarning)

    def test_construct_paths(self, s3_cache, input_key):
        function_prefix = s3_cache._construct_function_prefix(input_key.function_key)
        assert function_prefix == "test-prefix/functions/test_function"

        input_prefix = s3_cache._construct_input_prefix(input_key)
        assert input_prefix == "test-prefix/functions/test_function/inputs/test_input"

        metadata_key = s3_cache._construct_metadata_key(input_key)
        assert (
            metadata_key
            == "test-prefix/functions/test_function/inputs/test_input/metadata.json"
        )

        metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}
        output_key = s3_cache._construct_output_key(input_key, metadata)
        assert (
            output_key
            == "test-prefix/functions/test_function/inputs/test_input/output123.pickle"
        )

    def test_exists_false(self, s3_cache, input_key):
        assert s3_cache.exists(input_key) is False

    def test_exists_true(self, s3_cache, s3_client, input_key):
        # Create metadata file
        metadata = {"test": "value"}
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-prefix/functions/test_function/inputs/test_input/metadata.json",
            Body=json.dumps(metadata),
            ContentType="application/json",
        )

        assert s3_cache.exists(input_key) is True

    def test_list_empty(self, s3_cache, function_key):
        result = s3_cache.list(function_key)
        assert result == []

    def test_list_with_entries(self, s3_cache, s3_client, function_key):
        # Create some metadata files to simulate cached entries
        metadata = {"test": "value"}
        for input_id in ["input1", "input2"]:
            s3_client.put_object(
                Bucket="test-bucket",
                Key=f"test-prefix/functions/test_function/inputs/{input_id}/metadata.json",
                Body=json.dumps(metadata),
                ContentType="application/json",
            )

        result = s3_cache.list(function_key)

        assert len(result) == 2
        input_ids = {key.input_id for key in result}
        assert input_ids == {"input1", "input2"}
        assert all(key.function_id == "test_function" for key in result)

    def test_write_and_read_output(self, s3_cache, input_key):
        output_bytes = b"test_output_data"
        metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}

        # Write output
        s3_cache.write_output(output_bytes, metadata, input_key)

        # Read it back
        result = s3_cache.read_output(metadata, input_key)
        assert result == output_bytes

    def test_read_output_not_found(self, s3_cache, input_key):
        metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}

        with pytest.raises(InputKeyNotFoundError, match="Output not found"):
            s3_cache.read_output(metadata, input_key)

    def test_dump_and_load_metadata(self, s3_cache, input_key):
        metadata = {"test": "value", "number": 42}

        # Dump metadata
        s3_cache.dump_metadata(metadata, input_key)

        # Load it back
        result = s3_cache.load_metadata(input_key)
        assert result == metadata

    def test_load_metadata_not_found(self, s3_cache, input_key):
        with pytest.raises(InputKeyNotFoundError, match="Metadata not found"):
            s3_cache.load_metadata(input_key)

    def test_evict(self, s3_cache, input_key):
        # Create some files to evict
        metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}
        s3_cache.dump_metadata(metadata, input_key)
        s3_cache.write_output(b"test_data", metadata, input_key)

        # Verify files exist
        assert s3_cache.exists(input_key)

        # Evict
        s3_cache.evict(input_key)

        # Verify files are gone
        assert not s3_cache.exists(input_key)
        with pytest.raises(InputKeyNotFoundError):
            s3_cache.read_output(metadata, input_key)

    def test_clear(self, s3_cache, function_key):
        # Create entries for multiple inputs
        for input_id in ["input1", "input2"]:
            input_key = InputKey(
                function_id=function_key.function_id, input_id=input_id
            )
            metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}
            s3_cache.dump_metadata(metadata, input_key)
            s3_cache.write_output(b"test_data", metadata, input_key)

        # Verify entries exist
        entries = s3_cache.list(function_key)
        assert len(entries) == 2

        # Clear all
        s3_cache.clear(function_key)

        # Verify all entries are gone
        entries = s3_cache.list(function_key)
        assert len(entries) == 0

    def test_adopt(self, s3_cache):
        from_key = FunctionKey(function_id="old_function")
        to_key = FunctionKey(function_id="new_function")

        # Create entry for old function
        input_key = InputKey(function_id=from_key.function_id, input_id="test_input")
        metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}
        s3_cache.dump_metadata(metadata, input_key)
        s3_cache.write_output(b"test_data", metadata, input_key)

        # Verify old function has entry
        old_entries = s3_cache.list(from_key)
        assert len(old_entries) == 1

        # Verify new function has no entries
        new_entries = s3_cache.list(to_key)
        assert len(new_entries) == 0

        # Adopt
        s3_cache.adopt(from_key, to_key)

        # Verify old function has no entries
        old_entries = s3_cache.list(from_key)
        assert len(old_entries) == 0

        # Verify new function has the entry
        new_entries = s3_cache.list(to_key)
        assert len(new_entries) == 1

        # Verify the data was copied correctly
        new_input_key = InputKey(function_id=to_key.function_id, input_id="test_input")
        result_metadata = s3_cache.load_metadata(new_input_key)
        result_output = s3_cache.read_output(result_metadata, new_input_key)
        assert result_metadata == metadata
        assert result_output == b"test_data"

    def test_get_output_path(self, s3_cache, input_key):
        metadata = {"serializer": {"extension": "pickle"}, "output_id": "output123"}
        s3_cache.dump_metadata(metadata, input_key)

        result = s3_cache.get_output_path(input_key)

        expected = "s3://test-bucket/test-prefix/functions/test_function/inputs/test_input/output123.pickle"
        assert result == expected

    def test_get_output_path_not_found(self, s3_cache, input_key):
        with pytest.raises(InputKeyNotFoundError, match="InputKey.*not found in cache"):
            s3_cache.get_output_path(input_key)

    def test_update_last_accessed(self, s3_cache, input_key):
        original_metadata = {"test": "value"}
        s3_cache.dump_metadata(original_metadata, input_key)

        s3_cache.update_last_accessed(input_key)

        updated_metadata = s3_cache.load_metadata(input_key)
        assert "last_accessed" in updated_metadata
        assert updated_metadata["test"] == "value"  # Original data preserved

        # Verify timestamp format
        timestamp_str = updated_metadata["last_accessed"]
        parsed_timestamp = datetime.datetime.fromisoformat(timestamp_str)
        assert isinstance(parsed_timestamp, datetime.datetime)

    def test_get_last_accessed_with_timestamp(self, s3_cache, input_key):
        timestamp = "2023-01-01T12:00:00+00:00"
        metadata = {"last_accessed": timestamp}
        s3_cache.dump_metadata(metadata, input_key)

        result = s3_cache.get_last_accessed(input_key)

        expected = datetime.datetime.fromisoformat(timestamp)
        assert result == expected

    def test_get_last_accessed_no_timestamp(self, s3_cache, input_key):
        metadata = {"test": "value"}
        s3_cache.dump_metadata(metadata, input_key)

        result = s3_cache.get_last_accessed(input_key)

        assert result is None

    def test_integration_with_cacheable_pattern(self, s3_cache):
        """Test that demonstrates how S3Cache would work with the cacheable decorator pattern."""
        # This simulates what the cacheable decorator would do
        function_key = FunctionKey(function_id="my_function")
        input_key = InputKey(function_id="my_function", input_id="abc123")

        # Simulate caching a function result
        output_data = b"function_result_data"
        metadata = {
            "serializer": {"extension": "pickle"},
            "output_id": "result456",
            "cached_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        # Write to cache (what decorator would do on first call)
        s3_cache.write(output_data, metadata, input_key)

        # Check if cached (what decorator would check)
        assert s3_cache.exists(input_key)

        # Read from cache (what decorator would do on subsequent calls)
        cached_data = s3_cache.read(input_key)
        assert cached_data == output_data

        # List cached entries
        entries = s3_cache.list(function_key)
        assert len(entries) == 1
        assert entries[0].input_id == "abc123"
