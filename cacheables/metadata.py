import datetime
import subprocess

from loguru import logger

from .keys import VersionKey


def create_version_metadata(
    version_key: VersionKey
) -> dict:
    metadata = {
        "function_id": version_key.function_id,
        "version_id": version_key.version_id,
        "created_at": datetime.datetime.utcnow().isoformat() + "Z"
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
