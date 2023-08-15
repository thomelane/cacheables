import datetime
import subprocess

from loguru import logger

from .keys import InputKey


def create_metadata(
    input_key: InputKey
) -> dict:
    current_time = datetime.datetime.utcnow().isoformat() + "Z"
    metadata = {
        "function_id": input_key.function_id,
        "input_id": input_key.input_id,
        "created_at": current_time,
        "last_accessed_at": current_time,
    }
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
