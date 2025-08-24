import datetime
import json
import warnings
from typing import List, Optional

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    boto3 = None
    ClientError = Exception
    NoCredentialsError = Exception

from ..exceptions import InputKeyNotFoundError, ReadException, WriteException
from ..keys import FunctionKey, InputKey
from .base import BaseCache


class S3CacheLockingWarning(UserWarning):
    """Custom warning category for S3Cache locking warnings."""

    pass


# Configure warnings to show S3CacheLockingWarning only once per process
warnings.filterwarnings("once", category=S3CacheLockingWarning)


class S3Cache(BaseCache):
    def __init__(
        self,
        bucket_name: str,
        prefix: str = "cacheables",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        if boto3 is None:
            raise ImportError(
                "boto3 is required for S3Cache. Install it with: pip install cacheables[s3]"
            )

        super().__init__()
        self._bucket_name = bucket_name
        self._prefix = prefix.strip("/")

        # Issue warning about potential cache corruption (only once per process)
        warnings.warn(
            "S3Cache does not implement locking mechanisms. "
            "Cache corruption may occur when multiple processes access the same cache entries simultaneously. "
            "Use with caution in multi-process environments.",
            S3CacheLockingWarning,
            stacklevel=2,
        )

        # Initialize S3 client
        try:
            session_kwargs = {}
            if aws_access_key_id:
                session_kwargs["aws_access_key_id"] = aws_access_key_id
            if aws_secret_access_key:
                session_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if region_name:
                session_kwargs["region_name"] = region_name

            self._s3_client = boto3.client("s3", **session_kwargs)

            # Test connection and bucket access
            self._s3_client.head_bucket(Bucket=self._bucket_name)

        except NoCredentialsError as e:
            raise WriteException(f"AWS credentials not found: {e}") from e
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                raise WriteException(
                    f"S3 bucket '{self._bucket_name}' not found"
                ) from e
            elif error_code == "403":
                raise WriteException(
                    f"Access denied to S3 bucket '{self._bucket_name}'"
                ) from e
            else:
                raise WriteException(f"S3 error: {e}") from e

    def _construct_function_prefix(self, function_key: FunctionKey) -> str:
        return f"{self._prefix}/functions/{function_key.function_id}"

    def _construct_input_prefix(self, input_key: InputKey) -> str:
        function_prefix = self._construct_function_prefix(input_key.function_key)
        return f"{function_prefix}/inputs/{input_key.input_id}"

    def _construct_metadata_key(self, input_key: InputKey) -> str:
        input_prefix = self._construct_input_prefix(input_key)
        return f"{input_prefix}/metadata.json"

    def _construct_output_key(self, input_key: InputKey, metadata: dict) -> str:
        input_prefix = self._construct_input_prefix(input_key)
        extension = metadata["serializer"].get("extension", "bin")
        filename = f"{metadata['output_id']}.{extension}"
        return f"{input_prefix}/{filename}"

    def exists(self, input_key: InputKey) -> bool:
        metadata_key = self._construct_metadata_key(input_key)
        try:
            self._s3_client.head_object(Bucket=self._bucket_name, Key=metadata_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise ReadException(f"S3 error checking existence: {e}") from e

    def list(self, function_key: FunctionKey) -> List[InputKey]:
        function_prefix = self._construct_function_prefix(function_key)
        inputs_prefix = f"{function_prefix}/inputs/"

        try:
            response = self._s3_client.list_objects_v2(
                Bucket=self._bucket_name, Prefix=inputs_prefix, Delimiter="/"
            )

            input_keys = []
            for prefix_info in response.get("CommonPrefixes", []):
                prefix = prefix_info["Prefix"]
                # Extract input_id from prefix like "prefix/functions/func_id/inputs/input_id/"
                input_id = prefix.rstrip("/").split("/")[-1]
                input_keys.append(
                    InputKey(function_id=function_key.function_id, input_id=input_id)
                )

            return input_keys

        except ClientError as e:
            raise ReadException(f"S3 error listing inputs: {e}") from e

    def evict(self, input_key: InputKey) -> None:
        input_prefix = self._construct_input_prefix(input_key)

        try:
            # List all objects with the input prefix
            response = self._s3_client.list_objects_v2(
                Bucket=self._bucket_name, Prefix=f"{input_prefix}/"
            )

            # Delete all objects for this input
            if "Contents" in response:
                objects_to_delete = [
                    {"Key": obj["Key"]} for obj in response["Contents"]
                ]
                if objects_to_delete:
                    self._s3_client.delete_objects(
                        Bucket=self._bucket_name, Delete={"Objects": objects_to_delete}
                    )

        except ClientError as e:
            raise WriteException(f"S3 error evicting cache entry: {e}") from e

    def clear(self, function_key: FunctionKey) -> None:
        function_prefix = self._construct_function_prefix(function_key)

        try:
            # List all objects with the function prefix
            response = self._s3_client.list_objects_v2(
                Bucket=self._bucket_name, Prefix=f"{function_prefix}/"
            )

            # Delete all objects for this function
            if "Contents" in response:
                objects_to_delete = [
                    {"Key": obj["Key"]} for obj in response["Contents"]
                ]
                if objects_to_delete:
                    self._s3_client.delete_objects(
                        Bucket=self._bucket_name, Delete={"Objects": objects_to_delete}
                    )

        except ClientError as e:
            raise WriteException(f"S3 error clearing function cache: {e}") from e

    def adopt(
        self, from_function_key: FunctionKey, to_function_key: FunctionKey
    ) -> None:
        from_prefix = self._construct_function_prefix(from_function_key)
        to_prefix = self._construct_function_prefix(to_function_key)

        try:
            # List all objects with the from_function prefix
            response = self._s3_client.list_objects_v2(
                Bucket=self._bucket_name, Prefix=f"{from_prefix}/"
            )

            if "Contents" in response:
                # Copy all objects to the new function prefix
                for obj in response["Contents"]:
                    old_key = obj["Key"]
                    new_key = old_key.replace(from_prefix, to_prefix, 1)

                    self._s3_client.copy_object(
                        Bucket=self._bucket_name,
                        CopySource={"Bucket": self._bucket_name, "Key": old_key},
                        Key=new_key,
                    )

                # Delete old objects
                objects_to_delete = [
                    {"Key": obj["Key"]} for obj in response["Contents"]
                ]
                if objects_to_delete:
                    self._s3_client.delete_objects(
                        Bucket=self._bucket_name, Delete={"Objects": objects_to_delete}
                    )

        except ClientError as e:
            raise WriteException(f"S3 error adopting function cache: {e}") from e

    def read_output(self, metadata: dict, input_key: InputKey) -> bytes:
        output_key = self._construct_output_key(input_key, metadata)

        try:
            response = self._s3_client.get_object(
                Bucket=self._bucket_name, Key=output_key
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise InputKeyNotFoundError(f"Output not found for {input_key}") from e
            raise ReadException(f"S3 error reading output: {e}") from e

    def load_metadata(self, input_key: InputKey) -> dict:
        metadata_key = self._construct_metadata_key(input_key)

        try:
            response = self._s3_client.get_object(
                Bucket=self._bucket_name, Key=metadata_key
            )
            metadata_content = response["Body"].read().decode("utf-8")
            return json.loads(metadata_content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise InputKeyNotFoundError(
                    f"Metadata not found for {input_key}"
                ) from e
            raise ReadException(f"S3 error loading metadata: {e}") from e
        except json.JSONDecodeError as e:
            raise ReadException(f"Invalid JSON in metadata: {e}") from e

    def write_output(
        self, output_bytes: bytes, metadata: dict, input_key: InputKey
    ) -> None:
        output_key = self._construct_output_key(input_key, metadata)

        try:
            self._s3_client.put_object(
                Bucket=self._bucket_name, Key=output_key, Body=output_bytes
            )
        except ClientError as e:
            raise WriteException(f"S3 error writing output: {e}") from e

    def dump_metadata(self, metadata: dict, input_key: InputKey) -> None:
        metadata_key = self._construct_metadata_key(input_key)

        try:
            metadata_content = json.dumps(metadata, indent=4)
            self._s3_client.put_object(
                Bucket=self._bucket_name,
                Key=metadata_key,
                Body=metadata_content.encode("utf-8"),
                ContentType="application/json",
            )
        except ClientError as e:
            raise WriteException(f"S3 error writing metadata: {e}") from e

    def get_output_path(self, input_key: InputKey) -> str:
        if not self.exists(input_key):
            raise InputKeyNotFoundError(f"{input_key} not found in cache")
        metadata = self.load_metadata(input_key)
        output_key = self._construct_output_key(input_key, metadata)
        return f"s3://{self._bucket_name}/{output_key}"

    def update_last_accessed(self, input_key: InputKey) -> None:
        metadata = self.load_metadata(input_key)
        metadata["last_accessed"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        self.dump_metadata(metadata, input_key)

    def get_last_accessed(self, input_key: InputKey) -> Optional[datetime.datetime]:
        metadata = self.load_metadata(input_key)
        if "last_accessed" in metadata:
            return datetime.datetime.fromisoformat(metadata["last_accessed"])
        return None
