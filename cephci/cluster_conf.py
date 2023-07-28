import yaml
from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import ConfigError
from utility.log import Log

LOG = Log(__name__)

ROOT_PASSWD = "passwd"


doc = """
Utility to generate cluster conf based on global-conf

    Usage:
        cephci/cluster_conf.py --cloud-type <CLOUD>
            (--global-conf <YAML>)
            (--prefix <STR>)
            (--log-level <LOG>)
            (--cluster-conf <YAML>)
            [--config <YAML>]

        cephci/cluster_conf.py --help

    Options:
        -h --help                   Help
        -t --cloud-type <CLOUD>     Cloud type [openstack|ibmc|baremetal]
        --global-conf <YAML>        Global config file with node details
        -p --prefix <STR>           Resource name prefix
        -l --log-level <LOG>        Log level for log utility
        --cluster-conf <YAML>       Cluster config file path
        --config <YAML>             Config file with cloud credentials
"""


def _set_log(level):
    """Set log level"""
    LOG.logger.setLevel(level.upper())


def _load_config(config):
    LOG.info(f"Loading config file - {config}")
    with open(config, "r") as _stream:
        try:
            return yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{config}'")


def write_output(_file, data):
    LOG.info(f"Writing cluster details to {_file}")
    with open(_file, "w") as fp:
        try:
            yaml.dump(data, fp, default_flow_style=False)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{_file}'")


def collect_conf(cloud, prefix, config):
    _conf = {}
    for _c in config:
        cluster = _c.get("ceph-cluster")
        name = cluster.get("name")
        _conf[name] = {}

        LOG.info(f"Gathering info for cluster '{name}'")
        for key in cluster.keys():
            if "node" not in key:
                continue

            # Create node
            node_name = f"{name}-{prefix}-{key}"

            LOG.info(f"Collecting info for node '{node_name}'")
            node = Node(node_name, cloud)
            if not node.state:
                LOG.error(f"Failed to get status for node '{node_name}'")
                continue

            # Update node details
            _conf[name][node_name] = {}
            _conf[name][node_name]["hostname"] = node.name
            _conf[name][node_name]["id"] = key
            _conf[name][node_name]["credentials"] = [
                {"user": "root", "password": ROOT_PASSWD}
            ]
            _conf[name][node_name]["roles"] = cluster.get(key, {}).get("role")

            if node.private_ips:
                _conf[name][node_name]["private_ips"] = node.private_ips

            if node.public_ips:
                _conf[name][node_name]["public_ips"] = node.public_ips

            if node.volumes:
                _conf[name][node_name]["devices"] = [
                    d for v in node.volumes for d in Volume(v, cloud).device
                ]

    return _conf


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    config = args.get("--config")
    cloud = args.get("--cloud-type")
    global_conf = args.get("--global-conf")
    prefix = args.get("--prefix")
    log_level = args.get("--log-level")
    cluster_conf = args.get("--cluster-conf")

    # Set log level
    _set_log(log_level)

    # Read configuration for cloud
    get_configs(config)

    # Read configurations
    cloud_configs = get_cloud_credentials(cloud)
    global_configs = _load_config(global_conf).get("globals")

    # Connect to cloud
    cloud = CloudProvider(cloud, **cloud_configs)

    # Cleanup cluster
    data = collect_conf(cloud, prefix, global_configs)

    # Log cluster configuration
    LOG.info(f"\nCluster configuration details -\n{data}")

    # write output to yaml
    write_output(cluster_conf, data) if cluster_conf else None
