"""This module helps configure network delays using linux tc."""
from typing import Dict, Optional, Tuple

from ceph.ceph import Ceph, CephNode
from ceph.utils import get_nodes_by_ids
from utility.log import Log

LOG = Log(__name__)


def exec_command(
    node: CephNode,
    command: str,
    sudo: Optional[bool] = False,
    check_ec: Optional[bool] = True,
) -> Tuple:
    """Executes the given command on the provided node."""
    out, err = node.exec_command(sudo=sudo, cmd=command, check_ec=check_ec)
    LOG.debug(f"Output: {out}")
    LOG.debug(f"Error: {err}")

    return out, err


def get_network_device(node: CephNode) -> str:
    """Return the interface configured for default route."""
    out, _ = exec_command(
        node=node,
        sudo=True,
        command="ip route show | awk '/default/ { print $5 }' | uniq",
    )
    return out.strip()


def run(ceph_cluster: Ceph, config: Dict, **kwargs) -> int:
    """CephCI framework entry point for the module.

    This method tunes the network based on the given set of configurations for the
    mentioned set of nodes.

    Note: Exercise the test module before deploying RHCS.

    Examples:
      # Apply network delay for the given nodes
        config:
          nodes:
            - node1
            - node2
          network-device: eth0
          rule: root netem delay 100ms 50ms distribution normal

      # Apply network delay for the given CephNode role
        config:
          roles:
            - rgw
            - client
          network-device: eth0
          rule: root netem loss 0.5%
          modify: true

    Args:
        ceph_cluster (Ceph):    Ceph Cluster participating in the test environment.
        config (dict):          Configuration that needs to be applied
        kwargs (dict):          Key-value pairs that can be leveraged.

    Returns:
        0 on success else 1 on failures
    """
    LOG.info("Executing network shaping workflow.")
    rc = 0

    nodes = list()
    if "roles" in config.keys():
        for role in config["roles"]:
            nodes += ceph_cluster.get_nodes(role=role)

    if "nodes" in config.keys():
        nodes += get_nodes_by_ids(ceph_cluster, config["nodes"])

    for node in nodes:
        dev = get_network_device(node)

        if config.get("network-device", dev) != dev:
            LOG.debug(f"The default network device is {dev}")
            LOG.error(f"{config['network-device']} is not found in {node.vmshortname}")
            rc = 1
            continue

        verb = "change" if config.get("modify", False) else "add"
        rule = config["rule"]

        try:
            exec_command(
                node=node,
                check_ec=True,
                sudo=True,
                command=f"tc qdisc {verb} dev {dev} {rule}",
            )
        except Exception as be:  # no-qa
            LOG.debug(f"Failed to apply tc rule on {node.vmshortname}")
            LOG.warning(be)
            rc = 1

    return rc
