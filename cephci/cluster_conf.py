import yaml
from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import ConfigError
from utility.log import Log

LOG = Log(__name__)

ROOT_PASSWD = "passwd"
CEPH_PASSWD = "pass123"


doc = """
Utility to generate cluster configuration based on global-conf

    Usage:
        cephci/cluster_conf.py --cloud-type <CLOUD>
            (--prefix <STR>)
            (--global-conf <YAML>)
            (--cluster-conf <YAML>)
            [--config <YAML>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/cluster_conf.py --help

    Options:
        -h --help               Help
        --cloud-type <CLOUD>    Cloud type [openstack|ibmc|baremetal]
        --prefix <STR>          Resource name prefix
        --global-conf <YAML>    Global config file with node details
        --cluster-conf <YAML>   Cluster config file path
        --config <YAML>         Config file with cloud credentials
        --log-level <LOG>       Log level for log utility Default: DEBUG
        --log-dir <PATH>        Log directory for logs
"""


def _load_config(config):
    """Load cluster configration file"""
    LOG.info(f"Loading config file - {config}")
    with open(config, "r") as _stream:
        try:
            return yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{config}'")


def write_output(_file, data):
    """Write output data to file"""
    LOG.info(f"Writing cluster details to {_file}")
    with open(_file, "w") as fp:
        try:
            yaml.dump(data, fp, default_flow_style=False)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{_file}'")


def collect_conf(cloud, prefix, config):
    """Collect cluster information"""
    _conf = {}
    for _c in config:
        cluster = _c.get("ceph-cluster")
        name = cluster.get("name")
        _conf[name] = {}

        LOG.info(f"Gathering info for cluster '{name}'")
        for key in cluster.keys():
            if "node" not in key:
                continue

            # Create node name
            node_name = ""
            if str(cloud) == "openstack":
                node_name = f"{name}-{prefix}-{key}"
            elif str(cloud) == "baremetal":
                node_name = cluster.get(key).get("hostname")

            LOG.info(f"Collecting info for node '{node_name}'")
            node = Node(node_name, cloud)
            if not node.state:
                LOG.error(f"Failed to get status for node '{node_name}'")
                continue

            # Update node details
            _conf[name][node_name] = {}

            # Update hostname
            _conf[name][node_name]["hostname"] = node.id

            # Update users
            # TODO (vamahaja): Support for multiple users
            _conf[name][node_name]["users"] = [
                {"user": "root", "password": ROOT_PASSWD},
                {"user": "cephuser", "password": CEPH_PASSWD},
            ]

            # Update roles
            _conf[name][node_name]["roles"] = cluster.get(key, {}).get("role")

            # Update private and public IPs
            _conf[name][node_name]["ips"] = {}
            if node.private_ips:
                _conf[name][node_name]["ips"]["private"] = node.private_ips

            if node.public_ips:
                _conf[name][node_name]["ips"]["public"] = node.public_ips

            # Update volumes attached to node
            if node.volumes:
                _conf[name][node_name]["devices"] = [
                    d for v in node.volumes for d in Volume(v, cloud).device
                ]

    return _conf


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    cloud = args.get("--cloud-type")
    prefix = args.get("--prefix")
    global_conf = args.get("--global-conf")
    cluster_conf = args.get("--cluster-conf")
    config = args.get("--config")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

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
