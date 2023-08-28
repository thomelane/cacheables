from unittest.mock import patch
from subprocess import CalledProcessError
from cacheables.metadata import create_metadata, _get_git_metadata


def test_git_error():
    _get_git_metadata.cache_clear()

    with patch(
        "cacheables.metadata.subprocess.check_output",
        side_effect=CalledProcessError(1, "git"),
    ):
        metadata = create_metadata(
            input_id="input_id", output_id="output_id", serializer_metadata={}
        )
        assert isinstance(metadata, dict)
        assert "git" not in metadata

    _get_git_metadata.cache_clear()
