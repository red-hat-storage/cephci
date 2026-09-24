"""Smallfile workload on a dedicated NFS export while a cluster upgrade runs.

Runs beside the existing upgrade I/O test. That test creates, exercises, and
deletes its own exports. This one keeps a separate export and mount so the two
workloads do not remove each other's files.
"""

import json
import time

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from tests.nfs.nfs_operations import (
    init_cluster_health_check,
    log_cluster_health_and_check_crashes,
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
    remove_exports_and_unmount,
)
from utility.log import Log

log = Log(__name__)

SMALLFILE_BIN = "/home/cephuser/smallfile/smallfile_cli.py"
SMALLFILE_OPS = ("create", "read", "append", "rename", "delete", "cleanup")
NO_UPGRADE_MSG = "There are no upgrades in progress currently."


def _ensure_smallfile(client):
    """Clone smallfile under cephuser when it is not already present."""
    cmd = (
        "if [ ! -f {bin} ]; then "
        "rm -rf /home/cephuser/smallfile; "
        "sudo -u cephuser git clone "
        "https://github.com/distributed-system-analysis/smallfile.git "
        "/home/cephuser/smallfile; "
        "fi"
    ).format(bin=SMALLFILE_BIN)
    client.exec_command(sudo=True, cmd=cmd, long_running=True)


def _upgrade_in_progress(client):
    """Return True while ``ceph orch upgrade status`` reports an active upgrade."""
    try:
        out, _ = client.exec_command(sudo=True, cmd="ceph orch upgrade status")
    except Exception as exc:
        log.warning("Upgrade status check failed; treating upgrade as active: %s", exc)
        return True
    text = out if isinstance(out, str) else str(out)
    if NO_UPGRADE_MSG in text:
        return False
    try:
        return bool(json.loads(text).get("in_progress"))
    except (TypeError, ValueError):
        return True


def _run_smallfile_op(client, mount, operation, threads, file_size, files, sudo):
    sync_dir = "/var/tmp/nfs_smallfile_upgrade"
    client.exec_command(sudo=True, cmd=f"mkdir -p {sync_dir}")
    if not sudo:
        client.exec_command(sudo=True, cmd=f"chown cephuser:cephuser {sync_dir}")
    top = f"{mount}/smallfile"
    client.exec_command(sudo=sudo, cmd=f"mkdir -p {top}")
    cmd = (
        f"python3 {SMALLFILE_BIN} --operation {operation} --threads {threads} "
        f"--file-size {file_size} --files {files} --top {top} "
        f"--network-sync-dir {sync_dir}"
    )
    client.exec_command(sudo=sudo, cmd=cmd, long_running=True, check_ec=True)


def run(ceph_cluster, **kw):
    """
    Mount one dedicated NFS export and loop smallfile until the upgrade ends.

    The loop stops after an in-progress upgrade is observed and then finishes,
    or when ``max_time`` seconds elapse.
    """
    config = kw.get("config") or {}
    clients = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", 1))
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    # Last client(s): the sibling I/O test already drives the earlier clients.
    clients = clients[-no_clients:]
    client = clients[0]
    sudo = bool(config.get("sudo", False))
    threads = int(config.get("threads", 4))
    file_size = int(config.get("file_size", 4))
    files = int(config.get("files", 50))
    max_time = int(config.get("max_time", 10800))
    max_consecutive_failures = int(config.get("max_consecutive_failures", 5))
    version = config.get("nfs_version", "4.2")
    port = config.get("port", "2049")

    installer_node = ceph_cluster.get_nodes("installer")[0]
    rados_obj, start_time = init_cluster_health_check(ceph_cluster, config)
    nfs_cluster_name = Ceph(client).nfs.cluster.ls()[0]
    nfs_hostname = Ceph(client).nfs.cluster.info(nfs_cluster_name)[nfs_cluster_name][
        "backend"
    ][0]["hostname"]
    nfs_export = "/export/smallfile_upgrade"
    nfs_mount = "/mnt/nfs_smallfile_upgrade"
    mount_dict = None
    result = 0
    seen_upgrade = False
    cycles = 0
    consecutive_failures = 0

    try:
        _ensure_smallfile(client)
        mount_dict = create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            1,
            fs_name="cephfs",
            nfs_name=nfs_cluster_name,
            fs="cephfs",
            port=port,
            version=version,
            nfs_server=nfs_hostname,
            chown_cephuser=not sudo,
            installer_node=installer_node,
            during_upgrade=True,
            nfs_wait_timeout=config.get("nfs_wait_timeout", 300),
            mount_timeout=config.get("mount_timeout", 120),
            mount_tries=config.get("mount_tries", 2),
        )
        mount = mount_dict[client]["mount"][0]
        deadline = time.time() + max_time
        log.info(
            "Starting smallfile on %s (%s) until upgrade completes or %ss elapse",
            client.hostname,
            mount,
            max_time,
        )
        while time.time() < deadline:
            for operation in SMALLFILE_OPS:
                if time.time() >= deadline:
                    break
                try:
                    log.info("smallfile %s cycle %s", operation, cycles + 1)
                    _run_smallfile_op(
                        client, mount, operation, threads, file_size, files, sudo
                    )
                    consecutive_failures = 0
                except Exception as exc:
                    consecutive_failures += 1
                    log.warning(
                        "smallfile %s failed (%s/%s): %s",
                        operation,
                        consecutive_failures,
                        max_consecutive_failures,
                        exc,
                    )
                    if consecutive_failures >= max_consecutive_failures:
                        raise
            cycles += 1
            in_progress = _upgrade_in_progress(client)
            seen_upgrade = seen_upgrade or in_progress
            if seen_upgrade and not in_progress:
                log.info("Upgrade finished after %s smallfile cycle(s)", cycles)
                break
        else:
            if not seen_upgrade:
                log.error(
                    "Smallfile ran for %ss without observing an upgrade", max_time
                )
                result = 1
            else:
                log.info(
                    "Smallfile reached max_time=%ss after %s cycle(s); "
                    "upgrade still active",
                    max_time,
                    cycles,
                )
        if cycles == 0:
            log.error("Smallfile completed no cycles")
            result = 1
    except Exception as exc:
        log.error("Smallfile workload during upgrade failed: %s", exc)
        result = 1
    finally:
        if mount_dict is not None:
            try:
                remove_exports_and_unmount(mount_dict, clients, nfs_cluster_name)
            except Exception as exc:
                log.error("Failed to remove smallfile export: %s", exc)
                result = 1
        if log_cluster_health_and_check_crashes(rados_obj, start_time):
            result = 1
    if result == 0:
        log.info("TEST PASSED - smallfile workload during upgrade (%s cycles)", cycles)
    return result
