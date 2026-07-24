import json

from cli.utilities.containers import Container


def get_iscsi_container(node, all=False):
    """Get iSCSI container.

    Args:
        node: Ceph Node object
    """
    container = Container(node)
    containers, _ = container.ps(all=all, filter={"name": "iscsi"}, format="json")
    containers = json.loads(containers)

    if len(containers) == 1:
        return containers[0]

    for cont in containers:
        if "tcmu" in cont["CIDFile"]:
            continue
        return cont
