# -*- code: utf-8 -*-

import datetime
import logging
import os
import tempfile
from logging import getLogger

import __main__


def set_logging_env(level=None, path=None):
    log = getLogger(__name__)
    log.info("Setting up log environment")

    # Setup log directory
    if not path:
        path = os.path.join(tempfile.gettempdir(), "cephci")
        log.info(f"Generating log directory - {path}")

    if not os.path.exists(path):
        log.info(f"Setting up log directory - {path}")
        os.makedirs(path)

    # Create log file path
    name = os.path.basename(os.path.splitext(__main__.__file__)[0])

    # Set path for log
    path = os.path.join(
        path, f"{name}-{datetime.datetime.now().strftime('%m%d%Y-%H%M%S')}.log"
    )
    log.info(f"Log path - {path}")

    # Add file handler for logger
    handler = logging.FileHandler(path)
    log.logger.addHandler(handler)

    # Set log level
    level = level.upper() if level else "DEBUG"
    log.logger.setLevel(level)
    log.info(f"Log level - {level}")

    return log
