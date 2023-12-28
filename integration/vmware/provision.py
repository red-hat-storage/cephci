import multiprocessing as mp

import yaml
from docopt import docopt
from cli.cloudproviders import CloudProvider
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log


log = Log(__name__)

RESOURCES = []


doc = """
Utility to cleanup env from cloud

    Usage:
            (--log-level <LOG>)
            (--config <YAML>)
            
    Options:
        -h --help                   Help
        -t --cloud-type <CLOUD>     Cloud type [vmware|openstack|aws]
        -l --log-level <LOG>        Log level for log utility
        --config <YAML>             Config file with cloud credentials
"""


def _set_log(level):
    """Set log level"""
    log.logger.setLevel(level.upper())


def _load_config(config):
    log.info(f"Loading config file - {config}")
    with open(config, "r") as _stream:
        try:
            return yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{config}'")


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    cloud = args.get("--cloud-type")
    config = args.get("--config")
    log_level = args.get("--log-level")

    # Set log level
    _set_log(log_level)

    # Read configuration for cloud
    get_configs(config)

    # Read configurations
    cloud_configs = get_cloud_credentials(cloud)

    # Connect to cloud
    cloud = CloudProvider(cloud, **cloud_configs)

    # Get timeout and interval
    timeout, retry = cloud_configs.pop("timeout"), cloud_configs.pop("retry")
    interval = int(timeout / retry) if timeout and retry else None
    # Log cluster configuration
    log.info(f"\nCluster configuration details -\n{data}")
