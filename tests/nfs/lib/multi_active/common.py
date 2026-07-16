"""Shared helpers for NFS Multi-Active suite tests."""

from datetime import datetime

from cli.exceptions import ConfigError
from cli.utilities.utils import check_coredump_generated
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.lib.multi_active.constants import DEFAULT_HEALTH_WAIT
from utility.log import Log

log = Log(__name__)

NFS_COREDUMP_PATH = "/var/lib/systemd/coredump"
_SKIP_COREDUMP_CONTEXTS = frozenset({"post-NEG-5"})


def init_crash_monitoring(config):
    """Record suite start time for per-TC NFS coredump windows."""
    config.setdefault("coredump_since", datetime.now())


def check_for_crashes(ceph_cluster, config=None, *, context="inter-session"):
    """Fail when ceph crash archive or NFS coredumps are present.

    Set ``skip_health_check: true`` in the test config to skip (same as health).
    """
    config = config or {}
    if config.get("skip_health_check"):
        log.info(
            "Skipping %s crash check (skip_health_check=true)",
            context,
        )
        return

    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        raise ConfigError(f"{context} crash check requires at least one client node")

    client = clients[0]
    out, _ = client.exec_command(sudo=True, cmd="ceph crash ls-new", check_ec=False)
    if out and out.strip():
        log.error(
            "Ceph crash archive has entries (%s):\n%s",
            context,
            out.strip(),
        )
        client.exec_command(sudo=True, cmd="ceph crash archive-all", check_ec=False)
        raise ConfigError(f"Ceph crash detected before health check ({context})")

    if context in _SKIP_COREDUMP_CONTEXTS:
        log.info(
            "Skipping NFS coredump check for %s (expected daemon crash TC)",
            context,
        )
        return

    since = config.get("_last_crash_check_at") or config.get("coredump_since")
    if since is None:
        log.warning(
            "coredump_since not set; call init_crash_monitoring() at suite run start "
            "(%s)",
            context,
        )
        return

    nfs_nodes = ceph_cluster.get_nodes("nfs")
    for nfs_node in nfs_nodes:
        if check_coredump_generated(nfs_node, NFS_COREDUMP_PATH, since):
            raise ConfigError(
                f"NFS coredump on {nfs_node.hostname} since {since} ({context})"
            )

    log.info("No ceph crashes or NFS coredumps detected (%s)", context)


def wait_for_ceph_health(ceph_cluster, config=None, *, context="inter-session"):
    """Check for crashes, then wait for Ceph HEALTH_OK.

    Set ``skip_health_check: true`` in the test config to skip.
    """
    config = config or {}
    if config.get("skip_health_check"):
        log.info(
            "Skipping %s Ceph health check (skip_health_check=true)",
            context,
        )
        return

    check_for_crashes(ceph_cluster, config, context=context)

    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        raise ConfigError(f"{context} health check requires at least one client node")

    wait_time = int(config.get("health_wait", DEFAULT_HEALTH_WAIT))
    cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
    log.info("Proceed only if Ceph health is OK (%s)", context)
    if cephfs_common_utils.wait_for_healthy_ceph(clients[0], wait_time):
        raise ConfigError(
            f"Cluster health is not OK even after waiting for {wait_time}s ({context})"
        )
    log.info("Cluster is HEALTH_OK (%s)", context)
    config["_last_crash_check_at"] = datetime.now()
