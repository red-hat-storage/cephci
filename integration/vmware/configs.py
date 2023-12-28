import os

import yaml

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".providerconf.yaml")
CONFIG = None


def get_configs(config=None):
    """Read configurations from yamlcode 

    Args:
        config (str): Config file path
    """
    global CONFIG
    if CONFIG:
        return CONFIG

    # Check for default config
    config = config if config else DEFAULT_CONFIG_PATH

    log.info(f"Loading config file - {config}")
    with open(config, "r") as _stream:
        try:
            CONFIG = yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{config}'")