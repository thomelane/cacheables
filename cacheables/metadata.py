import datetime
import subprocess

from loguru import logger


def create_metadata(
    input_id: str,
    output_id: str,
    serializer_metadata: dict
) -> dict:
    metadata = {}
    metadata["input_id"] = input_id
    metadata["output_id"] = output_id
    metadata["created_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    metadata["serializer"] = serializer_metadata
    try:
        metadata["git_commit_hash"] = (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode("utf-8")
            .strip()
        )
        # check if there are any uncommitted changes (clean=False if there are)
        metadata["git_clean"] = (
            subprocess.check_output(["git", "status", "--porcelain"])
            .decode("utf-8")
            .strip()
            == ""
        )
    except subprocess.CalledProcessError as error:
        logger.warning(f"failed to get git metadata: {error}")
    return metadata
