import multiprocessing as mp

from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import OperationFailedError
from utility.log import Log

LOG = Log(__name__)


doc = """
Utility to cleanup cluster from cloud

    Usage:
        cephci/cleanup.py --cloud-type <CLOUD>
            (--prefix <STR>)
            [--config <CRED>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/cleanup.py --help

    Options:
        -h --help                Help
        --cloud-type <CLOUD>     Cloud type [openstack|ibmc|baremetal]
        --prefix <STR>           Resource name prefix
        --config <CRED>          Config file with cloud credentials
        --log-level <LOG>        Log level for log utility
        --log-dir <PATH>         Log directory for logs
"""

RECYCLE = []


def delete_volume(name, cloud, timeout=600, interval=10):
    """Delete volume"""
    LOG.info(f"Deleting volume '{name}'")

    # Connect to OSP cloud for volume
    volume = Volume(name, cloud)

    # Add volume to recycle bucket
    RECYCLE.append(volume)

    # Delete volume
    volume.delete(timeout, interval)


def delete_node(name, cloud, timeout=600, interval=10):
    """Delete node"""
    global RECYCLE

    LOG.info(f"Deleting node '{name}'")

    # Connect to OSP cloud for node
    node = Node(name, cloud)

    # Add node to recycle bucket
    RECYCLE.append(node)

    # Delete volumes attached to node
    volumes = node.volumes
    if volumes:
        LOG.info(f"Deleting volumes {', '.join(volumes)} attached to node '{name}'")
        for volume in volumes:
            delete_volume(volume, cloud, timeout, interval)

    else:
        LOG.info(f"No volumes are attached to the node '{node.name}'")

    # Delete node from OSP
    node.delete(timeout, interval)


def cleanup(cloud, prefix, timeout=600, interval=10):
    """Cleanup nodes with prefix"""
    global RECYCLE

    # Get nodes with prefix
    procs, nodes = [], cloud.get_nodes_by_prefix(prefix)
    if nodes:
        LOG.info(f"Nodes with prefix '{prefix}' are {', '.join(nodes)}")
    else:
        LOG.error(f"No nodes are available with prefix '{prefix}'")

    # Start deleting nodes in parallel
    for node in nodes:
        proc = mp.Process(target=delete_node, args=(node, cloud, timeout, interval))
        proc.start()
        procs.append(proc)

    # Wait till all nodes gets cleaned
    [p.join() for p in procs]

    # Get nodes woth prefix
    procs, volumes = [], cloud.get_volumes_by_prefix(prefix)
    if volumes:
        LOG.info(f"Volumes with prefix '{prefix}' are {', '.join(volumes)}")
    else:
        LOG.error(f"No volumes available with prefix '{prefix}'.")

    # Start deleting volumes in parallel
    for volume in volumes:
        proc = mp.Process(target=delete_volume, args=(volume, cloud, timeout, interval))
        proc.start()
        procs.append(proc)

    # Wait till all volumes gets cleaned
    [p.join() for p in procs]

    # Check if any resource deletion failed
    stale = [r.name for r in RECYCLE if r.state]
    if not stale:
        return True

    # Fail script in-case stale resources are present
    msg = f"Failed to clean resources {', '.join(stale)}"
    LOG.error(msg)
    raise OperationFailedError(msg)


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    config = args.get("--config")
    cloud = args.get("--cloud-type")
    prefix = args.get("--prefix")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

    # Read configuration for cloud
    get_configs(config)

    # Read configurations
    cloud_configs = get_cloud_credentials(cloud)

    # Connect to cloud
    cloud = CloudProvider(cloud, **cloud_configs)

    # Get timeout and interval
    timeout, retry = cloud_configs.pop("timeout"), cloud_configs.pop("retry")
    interval = int(timeout / retry) if timeout and retry else None

    # Cleanup cluster
    cleanup(cloud, prefix, timeout, interval)
