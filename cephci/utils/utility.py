import datetime
import logging
import os
import tempfile

import __main__
import requests
import yaml

from utility.log import Log

logger = Log(__name__)


def load_file(file_name):
    """Retrieve yaml data content from file.

    Args:
        file_name (str): Relative or absolute path of the file.

    Returns:
        The deserialized content of the file.
    """
    file_path = os.path.abspath(file_name)
    with open(file_path, "r") as conf_:
        content = yaml.safe_load(conf_)

    return content


def get_html_page(url: str) -> str:
    """Returns the content of the provided link.

    Args:
        url (str): The location to be read.

    Returns:
        str: The page content, or an empty string when it cannot be retrieved.
    """
    resp = requests.get(url, verify=False)

    try:
        if resp.ok:
            return resp.text
    except BaseException:  # noqa
        logger.debug(f"Failed to retrieve contents at {url}")

    return ""


def set_logging_env(level=None, path=None):
    log = Log()
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
