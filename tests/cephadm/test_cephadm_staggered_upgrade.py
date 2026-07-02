from ceph.ceph_admin.orch import Orch
from cli.cephadm.cephadm import CephAdm
from cli.utilities.operations import wait_for_cluster_health
from utility.log import Log

log = Log(__name__)


class StaggeredUpgradeError(Exception):
    pass


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
                flags:
                - automatically-accept-license
                daemon_types: mgr,mon
    """
    config = kw.get("config")
    osd_flags = config.get("osd_flags", [])
    flags = config.get("flags", [])
    target_image = config.get("container_image")
    action = config.get("action")
    installer = ceph_cluster.get_ceph_object("installer")
    orch = Orch(cluster=ceph_cluster, **config)
    client = ceph_cluster.get_nodes(role="client")
    # Check cluster health before upgrade
    health = wait_for_cluster_health(client, "HEALTH_OK", 300, 10)
    if not health:
        raise StaggeredUpgradeError("Cluster not in 'HEALTH_OK' state")
    # Set osd flags
    for flag in osd_flags:
        if CephAdm(installer).ceph.osd.set(flag):
            raise StaggeredUpgradeError("Unable to set osd flag")
    # Check target image
    if CephAdm(installer).ceph.orch.upgrade.check(image=target_image):
        raise StaggeredUpgradeError("Upgrade image check failed")
    # Staggered upgrade with daemon_types
    if action == "daemon_types":
        daemon_types = config.get("daemon_types")
        upgrade_kwargs = {"image": target_image, "daemon_types": daemon_types}

        # Add flags if provided
        for flag in flags:
            upgrade_kwargs[flag] = True

        if daemon_types == "osd":
            limit = config.get("limit")
            upgrade_kwargs["limit"] = limit
            if CephAdm(installer).ceph.orch.upgrade.start(**upgrade_kwargs):
                raise StaggeredUpgradeError("Unable to start upgrade with daemon_types")
        else:
            if CephAdm(installer).ceph.orch.upgrade.start(**upgrade_kwargs):
                raise StaggeredUpgradeError("Unable to start upgrade with daemon_types")
    # Staggered upgrade with services
    if action == "services":
        services = config.get("services")
        upgrade_kwargs = {"image": target_image, "services": services}

        # Add flags if provided
        for flag in flags:
            upgrade_kwargs[flag] = True

        if services == "osd.all_available_devices":
            limit = config.get("limit")
            upgrade_kwargs["limit"] = limit
            if CephAdm(installer).ceph.orch.upgrade.start(**upgrade_kwargs):
                raise StaggeredUpgradeError("Unable to start upgrade with services")
        else:
            if CephAdm(installer).ceph.orch.upgrade.start(**upgrade_kwargs):
                raise StaggeredUpgradeError("Unable to start upgrade with services")
    # Staggered upgrade with hosts
    if action == "hosts":
        nodes = config.get("nodes")
        hosts = ",".join(
            [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]
        )
        upgrade_kwargs = {"image": target_image, "hosts": hosts}

        # Add flags if provided
        for flag in flags:
            upgrade_kwargs[flag] = True

        if CephAdm(installer).ceph.orch.upgrade.start(**upgrade_kwargs):
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
        upgrade_kwargs = {
            "image": target_image,
            "daemon_types": daemon_types,
            "limit": limit,
            "hosts": hosts,
        }

        # Add flags if provided
        for flag in flags:
            upgrade_kwargs[flag] = True

        if CephAdm(installer).ceph.orch.upgrade.start(**upgrade_kwargs):
            raise StaggeredUpgradeError("Unable to start upgrade with all combinations")
    # Check upgrade status
    orch.monitor_upgrade_status()
    # Unset osd flags
    for flag in osd_flags:
        if CephAdm(installer).ceph.osd.unset(flag):
            raise StaggeredUpgradeError("Unable to set osd flag")
    # Check cluster health after upgrade (skip if configured)
    skip_health_check = config.get("skip_health_check", False)
    if not skip_health_check:
        health = wait_for_cluster_health(client, "HEALTH_OK", 300, 10)
        if not health:
            raise StaggeredUpgradeError("Cluster not in 'HEALTH_OK' state")
    return 0
