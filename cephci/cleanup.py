import multiprocessing
import sys

import yaml
from docopt import docopt

from cli.cluster.node import Node
from cli.exceptions import ConfigError, UnexpectedStateError
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)
cloud = None
configs = None


doc = """
Utility to cleanup cluster from cloud

    Usage:
        cephci/cleanup.py --log-level <LOG> --cloud-type <CLOUD> --cloud-creds <CRED> --pattern <STR>
        cephci/cleanup.py --help

    Options:
        -h --help                  Help
        -l --log-level <LOG>       Log level for log utility
        -t --cloud-type <CLOUD>    Cloud type [openstack|ibmc|baremetal]
        -c --cloud-creds <CRED>    Credentials for cloud
        -p --pattern <STR>         Resource name pattern
"""


def _set_log(level):
    log.logger.setLevel(level.upper())


def _get_configs(creds):
    with open(creds, "r") as _stream:
        try:
            _yaml = yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid credentials file '{creds}'")

    try:
        return _yaml["credentials"]
    except KeyError:
        raise ConfigError("Config file doesn't include credentials for Cloud provider")


def _get_nodes(pattern):
    nodes = Node.nodes(pattern, cloud, **configs)
    if not nodes:
        log.info(f"No resources available with pattern '{pattern}'. Exiting ....")
        sys.exit(1)

    log.info(f"Nodes with pattern '{pattern}' are {[n for n, _ in nodes]}")
    return nodes


def delete_node(node):
    log.info(f"Deleting node '{node}'")
    node = Node(node, cloud, **configs)

    volumes = node.volumes()
    log.info(f"Volumes attached to the node '{node.name}' are {volumes}")

    node.delete()

    timeout, interval = 300, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        state = node.state()
        if not state:
            log.info(f"Node '{node.name}' deleted successfully")
            break

        log.info(f"Node '{node.name}' in '{state}' state, retry after {interval} sec")

    if w.expired:
        raise UnexpectedStateError(
            f"Failed to delete node '{node.name}' even after {timeout} sec"
        )


def cleanup(nodes):
    procs = [
        multiprocessing.Process(target=delete_node, args=(node,)) for node, _ in nodes
    ]
    [p.start() for p in procs]
    [p.join() for p in procs]


if __name__ == "__main__":
    args = docopt(doc)

    log_level = args.get("--log-level")
    cloud = args.get("--cloud-type")
    creds = args.get("--cloud-creds")
    pattern = args.get("--pattern")

    _set_log(log_level)

    configs = _get_configs(creds)
    nodes = _get_nodes(pattern)

    cleanup(nodes)
