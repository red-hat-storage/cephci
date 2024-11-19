# -*- coding: utf-8 -*-
"""Implements CephCI logging related functions."""
import glob
import logging
import os
import random
import string


FILE_SERVER = "http://magna002.ceph.redhat.com"
LOG_URL = f"{FILE_SERVER}/cephci-jenkins/"
LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(pathname)s:%(lineno)d - "
    "[%(levelname)s] - %(message)s"
)


def get_log_name(name) -> str:
    """Return a logger name.

    This method returns the given name if there are no file handlers attached
    to the provided logger. Otherwise it will return the name with a random 8
    character prefix.

    The primary reason we are doing this is to avoid duplicate log records when
    multiple file handlers are attached to the same module. In case of parallel
    test execution, there is a high probability that the same test module is
    used to execute other test cases.

    Creating a new log handler allows us to write logs to different files for
    each test case using the same module and is being executed in parallel.

    Args:
      name (str)    The name of the logger

    Returns:
        str The logger name.
    """
    rtn_value = name
    if logging.getLogger(name).hasHandlers():
        _prefix = "".join(random.choices(string.ascii_letters + string.digits, k=8))
        rtn_value = f"{name}.{_prefix}"

    return rtn_value


def log_configure_defaults(console: bool = False) -> None:
    """Configure logging defaults."""
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

    if not console:
        _logger = logging.getLogger()
        _logger.propagate = False


def log_add_file_handler(logger_name, file_path, file_name, format=None) -> None:
    """Adds a rotating file handler to the provided logger.

    The purpose of this method is to add the a rotating file handler to the
    given logger. There are two types of files created by this method. One for
    information and other for errors.

    Errors are logged separately so that they can be uploaded to ReportPortal.

    Args:
        logger_name (str)       The name of the logger to retrieve
        file_path (Path)        The base directory to store the log files
        file_name (str)         The prefix of the file name.
        format (str)            Logging formatter. Defaults to LOG_FORMAT.

    Returns
        No output is returned.
    """
    _logger = logging.getLogger(logger_name)
    if format:
        _logger.setFormatter(format)

    # Check if there exists log file
    _log_matches = glob.glob(os.path.join(file_path, f"{file_name}.*"))
    if len(_log_matches) > 0:
        _prefix = "".join(random.choices(string.ascii_letters + string.digits, k=8))
        file_name = f"{file_name}-{_prefix}"

    _log_file = os.path.join(file_path, f"{file_name}.log")
    _err_file = os.path.join(file_path, f"{file_name}.err")

    for _file in [_err_file, _log_file]:
        _handler = logging.handlers.RotatingFileHandler(
            _file,  # filename
            maxBytes=10 * 1024 * 1024,  # 10MB max size
            backupCount=20,  # keep up to 200MB
        )
        _logger.addHandler(_handler)


def log_close_file_handlers(name) -> None:
    """Close all file handlers."""
    # remove handlers from root
    logger = logging.getLogger(name)

    for _handler in logger.handlers:
        if isinstance(_handler, logging.FileHandler):
            _handler.close()
            logger.removeHandler(_handler)
