import json

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


class StaggeredUpgradeError(Exception):
    pass


def check_upgrade_status(node):
    """Check upgrade status
    Args:
        node: installer node
    """
    timeout, interval = 600, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(node).ceph.orch.upgrade.status()
        if not json.loads(out)["in_progress"]:
            log.info("Upgrade completed")
            return
        log.info(f"Upgrade still In Progress. Retry for status after '{interval}' sec")
    if w.expired:
        log.error("Cluster upgrade not completed")
        raise StaggeredUpgradeError("Cluster upgrade not completed")


def run(ceph_cluster, **kw):
    """Staggered upgrade
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
        kw: test data
        e.g:
        test:
            name: Staggered upgrade with daemon types mgr,mon
            desc: Staggered upgrade with daemon types mgr,mon
            module: test_cephadm_staggered_upgrade.py
            polarion-id: CEPH-83575554
            config:
                action: "daemon_types"
                osd_flags:
                - noout
                - noscrub
                - nodeep-scrub
                daemon_types: mgr,mon
    """
    config = kw.get("config")
    osd_flags = config.get("osd_flags")
    target_image = config.get("container_image")
    action = config.get("action")
    node = ceph_cluster.get_nodes(role="mon")[0]
    # Check cluster health before upgrade
    if CephAdm(node).ceph.health() != "HEALTH_OK":
        raise StaggeredUpgradeError("Cluster not in 'HEALTH_OK' state")
    # Set osd flags
    for flag in osd_flags:
        if CephAdm(node).ceph._osd.set(flag):
            raise StaggeredUpgradeError("Unable to set osd flag")
    # Check target image
    if CephAdm(node).ceph.orch.upgrade.check(image=target_image):
        raise StaggeredUpgradeError("Upgrade image check failed")
    # Staggered upgrade with daemon_types
    if action == "daemon_types":
        daemon_types = config.get("daemon_types")
        if daemon_types == "osd":
            limit = config.get("limit")
            if CephAdm(node).ceph.orch.upgrade.start(
                image=target_image, daemon_types=daemon_types, limit=limit
            ):
                raise StaggeredUpgradeError("Unable to start upgrade with daemon_types")
        else:
            if CephAdm(node).ceph.orch.upgrade.start(
                image=target_image, daemon_types=daemon_types
            ):
                raise StaggeredUpgradeError("Unable to start upgrade with daemon_types")
    # Staggered upgrade with services
    if action == "services":
        services = config.get("services")
        if services == "osd.all_available_devices":
            limit = config.get("limit")
            if CephAdm(node).ceph.orch.upgrade.start(
                image=target_image, services=services, limit=limit
            ):
                raise StaggeredUpgradeError("Unable to start upgrade with services")
        else:
            if CephAdm(node).ceph.orch.upgrade.start(
                image=target_image, services=services
            ):
                raise StaggeredUpgradeError("Unable to start upgrade with services")
    # Staggered upgrade with hosts
    if action == "hosts":
        nodes = config.get("nodes")
        hosts = ",".join(
            [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]
        )
        if CephAdm(node).ceph.orch.upgrade.start(image=target_image, hosts=hosts):
            raise StaggeredUpgradeError("Unable to start upgrade with hosts")
    # Staggered upgrade with all combinations
    if action == "all_combination":
        nodes = config.get("nodes")
        daemon_types = config.get("daemon_types")
        limit = config.get("limit")
        services = config.get("services")
        hosts = ",".join(
            [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]
        )
        if CephAdm(node).ceph.orch.upgrade.start(
            image=target_image, daemon_types=daemon_types, limit=limit, hosts=hosts
        ):
            raise StaggeredUpgradeError("Unable to start upgrade with all combinations")
    # Check upgrade status
    check_upgrade_status(node)
    # Unset osd flags
    for flag in osd_flags:
        if CephAdm(node).ceph._osd.unset(flag):
            raise StaggeredUpgradeError("Unable to set osd flag")
    # Check cluster health after upgrade
    if CephAdm(node).ceph.health() != "HEALTH_OK":
        raise StaggeredUpgradeError("Cluster not in 'HEALTH_OK' state")
    return 0
