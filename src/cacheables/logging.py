import sys

from loguru import logger


def disable_logging():
    logger.disable(__package__)


def enable_logging(level: str = "DEBUG"):
    logger.enable(__package__)
    logger.remove()
    logger.add(
        sink=sys.stdout,
        level=level,
        format=" | ".join(
            [
                "<green>{time:HH:mm:ss.SSS}</green>",
                "<level>{level: <8}</level>",
                "<level>fn: {extra[function_id]}</level>",
                "<level>{message}</level>",
            ]
        ),
    )


disable_logging()
