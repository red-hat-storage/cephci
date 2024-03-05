from cluster_conf import collect_conf, write_output
from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.utilities.utils import load_config
from utility.log import Log

LOG = Log(__name__)


doc = """
Utility to provision cluster on cloud

    Usage:
        cephci/provision.py --cloud-type <CLOUD>
            (--global-conf <YAML>)
            (--inventory <YAML>)
            (--prefix <STR>)
            [--cluster-conf <YAML>]
            [--image <STR>]
            [--vmsize <STR>]
            [--network <STR>...]
            [--config <YAML>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/provision.py --help

    Options:
        -h --help               Help
        --cloud-type <CLOUD>    Cloud type [openstack|ibmc|baremetal]
        --global-conf <YAML>    Global config file with node details
        --inventory <YAML>      Cluster details config
        --prefix <STR>          Resource name prefix
        --image <STR>           Cloud images to be used for node
        --network <STR>         Cloud network to be attached to node
        --vmsize <STR>          Cloud image flavor
        --cluster-conf <YAML>   Cluster config file path
        --config <YAML>         Config file with cloud credentials
        --log-level <LOG>       Log level for log utility Default: DEBUG
        --log-dir <PATH>        Log directory for logs
"""


def provision_osp_volume(name, volcount, volsize):
    """Provision volumes on OSP"""
    # Provision volumes to be attached to node
    volumes = []
    for i in range(volcount):
        # Create volume object
        volname = f"{name}-vol-{i}"
        volume = Volume(volname, cloud)

        # Provision volume
        LOG.info(f"Provisioning volume '{volname}'")
        volume.create(volsize, timeout, interval)
        LOG.info(
            f"Volume '{volume.name}' with id '{volume.id}' provisioned successfully"
        )

        volumes.append(volname)

    return volumes


def provision_node(name, cloud, config, node, timeout=600, interval=10):
    """Provision node on cloud"""
    # Provision OSP volumes
    if str(cloud) == "openstack":
        volcount, volsize = node.get("no-of-volumes", 0), node.get("disk-size", 0)
        config["volumes"] = provision_osp_volume(name, volcount, volsize)

    # Create node object
    node = Node(name, cloud)

    # Provision node
    node.create(timeout=timeout, interval=interval, **config)
    LOG.info(f"Node '{node.name}' with id '{node.id}' provisioned successfully")

    # Attach volumes provisioned to node
    node.attach_volume(config.get("volumes"))

    return True


def provision(cloud, prefix, global_configs, node_configs, timeout=600, interval=10):
    """Provision cluster with global configs"""
    # Create cluster configs
    for _c in global_configs:
        cluster = _c.get("ceph-cluster")
        name = cluster.get("name")

        LOG.info(f"Provisioning cluster '{name}'")
        for key in cluster.keys():
            if "node" not in key:
                continue

            # Create node name
            node_name = f"{name}-{prefix}-{key}"

            # Get node details
            node = cluster.get(key)
            if not node_configs.get("networks"):
                node_configs["networks"] = node.get("networks")

            provision_node(node_name, cloud, node_configs, node, timeout, interval)

        LOG.info(f"Cluster '{name}' provisioned successfully")


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    cloud = args.get("--cloud-type")
    global_conf = args.get("--global-conf")
    inventory = args.get("--inventory")
    prefix = args.get("--prefix")
    network = args.get("--network")
    image = args.get("--image")
    vmsize = args.get("--vmsize")
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
    global_configs = load_config(global_conf).get("globals")

    # Connect to cloud
    cloud = CloudProvider(cloud, **cloud_configs)

    # Get timeout and interval
    timeout, retry = cloud_configs.pop("timeout"), cloud_configs.pop("retry")
    interval = int(timeout / retry) if timeout and retry else None

    # Get node configs based on inventory
    node_configs = cloud.get_inventory(inventory, image, vmsize, network)

    # Provision cluster
    provision(cloud, prefix, global_configs, node_configs, timeout, interval)

    # Collect cluster config
    data = collect_conf(cloud, prefix, global_configs)

    # Log cluster configuration
    LOG.info(f"\nCluster configuration details -\n{data}")

    # write output to yaml
    write_output(cluster_conf, data) if cluster_conf else None
