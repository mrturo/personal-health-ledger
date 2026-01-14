"""
Logging configuration and utilities.

Provides centralized logging setup for the application.
"""

import logging
import sys
from pathlib import Path

from personal_health_ledger.utils.parameters import LoggingConfig


def setup_logging(config: LoggingConfig, logger_name: str | None = None) -> logging.Logger:
    """
    Set up logging for the application.

    Args:
        config: Logging configuration.
        logger_name: Optional logger name. If None, returns root logger.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, config.level.upper()))

    logger.handlers.clear()

    formatter = logging.Formatter(config.format)

    if config.console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, config.level.upper()))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if config.file:
        log_file = Path(config.file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, config.level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance by name.

    Args:
        name: Logger name (typically __name__ of the module).

    Returns:
        Logger instance.
    """
    return logging.getLogger(name)
