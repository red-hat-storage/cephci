import multiprocessing as mp
import sys

from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


doc = """
Utility to cleanup cluster from cloud

    Usage:
        cephci/cleanup.py --cloud-type <CLOUD>
            (--prefix <STR>)
            (--log-level <LOG>)
            [--config <CRED>]

        cephci/cleanup.py --help

    Options:
        -h --help                   Help
        -t --cloud-type <CLOUD>     Cloud type [openstack|ibmc|baremetal]
        -p --prefix <STR>           Resource name prefix
        -l --log-level <LOG>        Log level for log utility
        -c --config <CRED>          Config file with cloud credentials
"""

RECYCLE = []


def _set_log(level):
    """Set log level"""
    log.logger.setLevel(level.upper())


def _get_nodes(prefix, cloud, configs):
    """Get nodes with prefix"""
    nodes = [n for n, _ in CloudProvider(cloud, **configs).nodes(prefix)]

    # Check for nodes available with prefix
    if not nodes:
        log.error(f"No resources available with prefix '{prefix}'. Exiting....")
        sys.exit(1)

    log.info(f"Nodes with prefix '{prefix}' are {', '.join(nodes)}")
    return nodes


def delete_node(node, cloud, timeout, interval, configs):
    """Delete node"""
    global RECYCLE

    log.info(f"Deleting node '{node}'")

    # Connect to OSP cloud
    node = Node(node, cloud, **configs)

    # Add node to recycle bucket
    RECYCLE.append(node)

    # Delete volumes attached to node
    volumes = node.volumes
    if volumes:
        log.info(f"Volumes attached to the node '{node.name}' are {', '.join(volumes)}")
        for volume in volumes:
            log.info(f"Deleting volume {volume}")
            _volume = Volume(volume, cloud, **configs)

            # Add volume to recycle bucket
            RECYCLE.append(_volume)

            _volume.delete(timeout, interval)

    else:
        log.info(f"No volumes are attached to the node '{node.name}'")

    # Delete node from OSP
    node.delete(timeout, interval)


def cleanup(cloud, prefix):
    """Cleanup nodes with prefix"""
    global RECYCLE

    configs = get_cloud_credentials(cloud)

    # Get timeout and interval
    timeout, retry = configs.get("timeout"), configs.get("retry")
    interval = int(timeout / retry)

    # Get nodes with prefix
    procs, nodes = [], _get_nodes(prefix, cloud, configs)

    # Start deleting nodes in parallel
    for node in nodes:
        proc = mp.Process(
            target=delete_node,
            args=(
                node,
                cloud,
                timeout,
                interval,
                configs,
            ),
        )
        proc.start()
        procs.append(proc)

    # Wait till all nodes gets cleaned
    [p.join() for p in procs]

    # Check if any resource deletion failed
    stale = [r.name for r in RECYCLE if r.state]
    if stale:
        msg = f"Failed to clean resources {', '.join(stale)}"
        log.error(msg)
        raise OperationFailedError(msg)

    return True


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    config = args.get("--config")
    cloud = args.get("--cloud-type")
    prefix = args.get("--prefix")
    log_level = args.get("--log-level")

    # Set log level
    _set_log(log_level)

    # Read configuration for cloud
    get_configs(config)

    # Cleanup cluster
    cleanup(cloud, prefix)
    log.info(f"Cleaned cluster with prefix '{prefix}' sucessfully")
