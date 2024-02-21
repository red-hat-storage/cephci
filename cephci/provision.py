import multiprocessing as mp

from cluster_conf import collect_conf, write_output
from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import OperationFailedError
from cli.utilities.utils import load_config
from utility.log import Log

LOG = Log(__name__)

RESOURCES = []


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


def provision_node(name, cloud, config, disk, timeout=600, interval=10):
    """Provision node on cloud"""
    volcount, volsize = disk.get("no-of-volumes", 0), disk.get("disk-size", 0)

    # Provision volumes to be attached to node
    volumes = []
    for i in range(volcount):
        # Create volume object
        volname = f"{name}-vol-{i}"
        volume = Volume(volname, cloud)

        # Add volume to resource bucket
        RESOURCES.append(volume)

        LOG.info(f"Provisioning volume '{volname}'")

        # Provision volume
        volume.create(volsize, timeout, interval)
        LOG.info(
            f"Volume '{volume.name}' with id '{volume.id}' provisioned successfully"
        )

        volumes.append(volname)

    # Get node configs
    cloud_data = config.get("cloud_data")
    image = config.get("image")
    vmsize = config.get("vmsize")
    network = config.get("network")

    # Create node object
    node = Node(name, cloud)

    # Add volume to resource bucket
    RESOURCES.append(node)

    LOG.info(f"Provisioning node '{name}'")

    # Provision node
    node.create(image, vmsize, cloud_data, network, timeout, interval)
    LOG.info(f"Node '{node.name}' with id '{node.id}' provisioned successfully")

    # Attach volumes provisioned to node
    node.attach_volume(volumes)

    return True


def provision(cloud, prefix, cluster_configs, node_configs, timeout=600, interval=10):
    """Provision cluster with global configs"""
    # Create cluster configs
    for _c in cluster_configs:
        cluster, procs = _c.get("ceph-cluster"), []
        name = cluster.get("name")

        LOG.info(f"Provisioning cluster '{name}'")
        for key in cluster.keys():
            if "node" not in key:
                continue

            # Create node name
            node_name = f"{name}-{prefix}-{key}"

            # Get node details
            node = cluster.get(key)
            if not node_configs.get("network"):
                node_configs["network"] = node.get("networks")

            # Provision node with details
            proc = mp.Process(
                target=provision_node,
                args=(node_name, cloud, node_configs, node, timeout, interval),
            )

            # Start process and append process to the list
            proc.start()
            procs.append(proc)

        # Wait till all nodes gets cleaned
        [p.join() for p in procs]

        # Check if any resource deletion failed
        stale = [r.name for r in RESOURCES if r.state]
        if stale:
            msg = f"Failed to provision resources {', '.join(stale)}"
            LOG.error(msg)
            raise OperationFailedError(msg)

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
