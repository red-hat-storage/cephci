import multiprocessing as mp
import sys

from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs

from cli.cluster.node import Node
from cli.exceptions import UnexpectedStateError
from cli.utilities.waiter import WaitUntil
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


def _set_log(level):
    """Set log level"""
    log.logger.setLevel(level.upper())


def _get_nodes(prefix, cloud, configs):
    """Get nodes with prefix"""
    # Get nodes with prefix
    nodes = Node.nodes(prefix, cloud, **configs)

    # Check for nodes available with prefix
    if not nodes:
        log.info(f"No resources available with prefix '{prefix}'. Exiting ....")
        sys.exit(1)

    log.info(f"Nodes with prefix '{prefix}' are {', '.join([n for n, _ in nodes])}")
    return nodes


def delete_node(node, cloud, configs):
    """Delete node"""
    log.info(f"Deleting node '{node}'")

    # Connect to OSP cloud
    node = Node(node, cloud, **configs)

    # Get volumes attached to node
    volumes = node.volumes()
    log.info(f"Volumes attached to the node '{node.name}' are {volumes}")

    # Delete node from OSP
    node.delete()

    # Get timeout and interval
    timeout = configs.get("timeout")
    retry = configs.get("retry")
    interval = int(timeout / retry)

    # Wait until node get deleted
    for w in WaitUntil(timeout=timeout, interval=interval):
        state = node.state()
        if not state:
            log.info(f"Node '{node.name}' deleted successfully")
            return True

        log.info(f"Node '{node.name}' in '{state}' state, retry after {interval} sec")

    # Check status for operation
    if w.expired:
        raise UnexpectedStateError(
            f"Failed to delete node '{node.name}' even after {timeout} sec"
        )


def cleanup(cloud, prefix):
    """Cleanup nodes with prefix"""
    # Get cloud configuration
    configs = get_cloud_credentials(cloud)

    # Get nodes with prefix
    procs, nodes = [], _get_nodes(prefix, cloud, configs)

    # Start deleting nodes in parallel
    for node, _ in nodes:
        proc = mp.Process(
            target=delete_node,
            args=(
                node,
                cloud,
                configs,
            ),
        )
        proc.start()
        procs.append(proc)

    # Wait till all nodes gets cleaned
    [p.join() for p in procs]

    log.info(f"Cleaned cluster with prefix '{prefix}' sucessfully")
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
