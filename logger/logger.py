from __future__ import annotations
import logging
import os
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

DEFAULT_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
DEFAULT_LOGGER_DIR = os.environ.get("LOGGER_DIR", "./logs")
_logger: Optional[logging.Logger] = None

def get_logger(
    name: str = "smart_dataloader",
    filename: Optional[str] = None,
    level: Optional[str] = None,
    log_to_console: bool = True,
) -> logging.Logger:
    """Return a configured logger. Optionally log to console and/or a file.

    Args:
        name: Logger name (used for getLogger(name)). Defaults to 'smart_dataloader'.
        filename: If set, also write to ./logs/{filename}.log. If None, no file output.
        level: Log level (e.g. 'DEBUG', 'INFO'). Uses LOG_LEVEL env if not set.
        log_to_console: If True, add a StreamHandler so logs go to console.

    Returns:
        Configured logger instance.

    When filename is None, only console is used (if log_to_console). When filename is set,
    logs are written to ./logs/{filename}.log; if log_to_console is True, console output
    is also enabled. Handlers are added only once per logger to avoid duplicate lines.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, (level or DEFAULT_LOG_LEVEL).upper(), logging.INFO))
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # Add handlers only if this logger has none (avoid duplicates on repeated get_logger).
    if not logger.handlers:
        if log_to_console:
            console = logging.StreamHandler()
            console.setFormatter(formatter)
            logger.addHandler(console)
        if filename is not None:
            os.makedirs(DEFAULT_LOGGER_DIR, exist_ok=True)
            file_handler = logging.FileHandler(f"{DEFAULT_LOGGER_DIR}/{filename}.log", encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


def delete_logger_file(filename: str = "smart_dataloader") -> None:
    """Remove the log file for the given logger name. Closes file handlers first so the file can be deleted on Windows."""
    logger = logging.getLogger(filename)
    # Close and remove file handlers first so the file handle is released (required on Windows).
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            logger.removeHandler(handler)
    if os.path.exists(f"{DEFAULT_LOGGER_DIR}/{filename}.log"):
        os.remove(f"{DEFAULT_LOGGER_DIR}/{filename}.log")
