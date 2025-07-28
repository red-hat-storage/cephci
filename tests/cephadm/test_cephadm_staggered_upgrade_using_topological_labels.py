import json

from ceph.ceph_admin.orch import Orch
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import UnexpectedStateError
from cli.utilities.operations import wait_for_cluster_health
from utility.log import Log

log = Log(__name__)


class StaggeredUpgradeError(Exception):
    pass


def get_daemon_status(node, daemon):
    """
    Verify demon status.
    Args:
        node (str): installer node object
        daemon (str): daemon name
    """
    timeout, interval = 60, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        result = CephAdm(node).ceph.orch.ps(format="json")
        all_running_daemons = [
            data.get("daemon_name") for data in json.loads(result[node[0].hostname][0])
        ]
        if (item == daemon for item in all_running_daemons):
            break
    if w.expired:
        raise UnexpectedStateError("Daemon is not running")


def run(ceph_cluster, **kw):
    """Staggered upgrade
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
        kw: test data
        e.g:
        test:
            name: Staggered upgrade for MGR on datacenter A
            desc: Staggered upgrade with daemon_types mgr, DC-A
            module: test_cephadm_staggered_upgrade_using_topological_labels.py
            polarion-id: CEPH-83623930
            config:
                action: "all_combination"
                osd_flags:
                - noout
                - noscrub
                - nodeep-scrub
                daemon_types: mgr
                topological_label: datacenter=A
    """
    config = kw.get("config")
    action = config.get("action")
    osd_flags = config.get("osd_flags")
    target_image = config.get("container_image")
    node = ceph_cluster.get_nodes(role="mon")[0]
    orch = Orch(cluster=ceph_cluster, **config)
    client = ceph_cluster.get_nodes(role="client")
    # Check cluster health before upgrade
    health = wait_for_cluster_health(client, "HEALTH_OK", 300, 10)
    if not health:
        raise StaggeredUpgradeError("Cluster not in 'HEALTH_OK' state")
    # Set osd flags
    for flag in osd_flags:
        if CephAdm(node).ceph.osd.set(flag):
            raise StaggeredUpgradeError("Unable to set osd flag")
    # Check target image
    if CephAdm(node).ceph.orch.upgrade.check(image=target_image):
        raise StaggeredUpgradeError("Upgrade image check failed")
    # Staggered upgrade with topological_labels
    if action == "topological_labels":
        labels = config.get("topological_labels")
        if CephAdm(node).ceph.orch.upgrade.start(
            image=target_image, topological_labels=labels
        ):
            raise StaggeredUpgradeError(
                "Unable to start upgrade with topological_labels"
            )
    # Staggered upgrade with all combinations
    if action == "all_combination":
        daemon_types = config.get("daemon_types")
        labels = config.get("topological_labels")
        if CephAdm(node).ceph.orch.upgrade.start(
            image=target_image, daemon_types=daemon_types, topological_labels=labels
        ):
            raise StaggeredUpgradeError("Unable to start upgrade with all combinations")
    # Check upgrade status
    orch.monitor_upgrade_status()
    # Unset osd flags
    for flag in osd_flags:
        if CephAdm(node).ceph.osd.unset(flag):
            raise StaggeredUpgradeError("Unable to set osd flag")
    # Check cluster health after upgrade
    health = wait_for_cluster_health(client, "HEALTH_OK", 300, 10)
    if not health:
        raise StaggeredUpgradeError("Cluster not in 'HEALTH_OK' state")
    return 0
