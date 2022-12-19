import json

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm


class LabelNoScheduleError(Exception):
    pass


def run(ceph_cluster, **kw):
    """verify "__no_schedule" label
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    installer_node = ceph_cluster.get_nodes(role="installer")
    config = kw.get("config")
    # node = ceph_cluster.get_nodes()[1]
    # daemon_type, node_name, label = "osd", node.hostname, "_no_schedule"
    daemon_type, label = "osd", "_no_schedule"

    for node in config.get("nodes"):
        node_name = ceph_cluster.get_nodes()[int(node[-1]) - 1].hostname
        exp_out = f"Added label _no_schedule to host {node_name}"
        # Adding "__no_schedule" label and checking daemons to node.
        result = CephAdm(installer_node).ceph.orch.label.add(node_name, label)
        result = list(result.values())[0][0].strip()
        if exp_out != result:
            raise LabelNoScheduleError("Failed to add _no_schedule label to node")
        # Verfiy label added to host
        result = CephAdm(installer_node).ceph.orch.host.ls(
            format="json", host_pattern=node_name
        )
        result = list(result.values())[0][0].strip()
        if label not in result:
            raise LabelNoScheduleError("Failed to add _no_schedule label to node")
        # Checking all the deamons in node.
        timeout, interval = 60, 5
        for w in WaitUntil(timeout=timeout, interval=interval):
            result = CephAdm(installer_node).ceph.orch.ps(
                hostname=node_name, format="json"
            )
            all_running_daemons = [
                data["daemon_type"]
                for data in json.loads(result[installer_node[0].hostname][0])
            ]
            if all(item == daemon_type for item in all_running_daemons):
                break
        if w.expired:
            raise LabelNoScheduleError("Deamons check failed")
    return 0
