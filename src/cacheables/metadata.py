import datetime
import subprocess
from functools import lru_cache

from loguru import logger


def _get_git_commit_hash() -> str:
    output = subprocess.check_output(["git", "rev-parse", "HEAD"])
    return output.decode("utf-8").strip()


def _is_git_clean() -> bool:
    """
    Check if there are any uncommitted changes (False if there are)
    """
    output = subprocess.check_output(["git", "status", "--porcelain"])
    return output.decode("utf-8").strip() == ""


@lru_cache(maxsize=None)
def _get_git_metadata() -> dict:
    metadata = {}
    try:
        metadata["git_commit_hash"] = _get_git_commit_hash()
        metadata["git_clean"] = _is_git_clean()
    except subprocess.CalledProcessError as error:
        logger.warning(f"failed to get git metadata: {error}")
    return metadata


def create_metadata(input_id: str, output_id: str, serializer_metadata: dict) -> dict:
    metadata = {}
    metadata["input_id"] = input_id
    metadata["output_id"] = output_id
    metadata["created_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    if serializer_metadata:
        metadata["serializer"] = serializer_metadata
    git_metadata = _get_git_metadata()
    if git_metadata:
        metadata["git"] = git_metadata
    return metadata
