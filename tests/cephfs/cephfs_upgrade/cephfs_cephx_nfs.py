"""
Combined CephFS+NFS CephX suite NFS steps (config.action).

  setup — create 2-node NFS on volume ``cephfs``, mount clients, write NfsCephX
  io    — probe/seed IO on existing mounts (optional remount_if_missing)

CephX also uses cephfs2. Imports shared key-helper paths/exit_status only;
does not call rotate helpers.
"""

import json
import shlex
import traceback

from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.cephfs_upgrade.cephfs_cephx_key_helpers import (
    DEFAULT_UPGRADE_CONFIG_PATH,
    _read_upgrade_json,
    _remote_exit_status,
    ensure_ceph_fs,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.nfs.nfs_operations import cleanup_cluster, mount_retry, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

NFS_CEPHX_KEY = "NfsCephX"
DEFAULT_NFS_NAME = "cephfs-nfs-cephx"
# Shared with CephX first FS; export.create hardcodes subvolume ops on "cephfs".
DEFAULT_FS_NAME = "cephfs"
DEFAULT_EXPORT_BASE = "/export_cephx"
DEFAULT_MOUNT = "/mnt/nfs_cephx"
DEFAULT_VERSION = "4.1"
DEFAULT_PORT = "2049"


def _write_nfs_section(client, payload, path):
    data = {}
    out, _ = client.exec_command(
        sudo=True, cmd=f"cat {shlex.quote(path)}", check_ec=False
    )
    if _remote_exit_status(client) == 0 and (out or "").strip():
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            data = {}
    if not isinstance(data, dict):
        data = {}
    data[NFS_CEPHX_KEY] = payload
    blob = json.dumps(data, indent=4) + "\n"
    remote = client.remote_file(sudo=True, file_name=path, file_mode="w")
    remote.write(blob)
    remote.flush()
    remote.close()


def _ensure_fstab(client, server, export, mount_point, version):
    """Append _netdev NFS fstab so C2/C3 kernel/reboot paths remount NFS."""
    opts = f"defaults,_netdev,vers={version}"
    entry = f"{server}:{export} {mount_point} nfs {opts} 0 0"
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"grep -F {shlex.quote(mount_point)} /etc/fstab",
        check_ec=False,
    )
    if _remote_exit_status(client) == 0 and mount_point in (out or ""):
        log.info("fstab already has %s on %s", mount_point, client.hostname)
        return
    client.exec_command(
        sudo=True,
        cmd=f"printf '%s\\n' {shlex.quote(entry)} >> /etc/fstab",
    )
    log.info("Added NFS fstab on %s: %s", client.hostname, entry)


def _load_nfs_section(client, path):
    data = _read_upgrade_json(client, path)
    if not data:
        raise OperationFailedError(f"Missing upgrade config {path}")
    section = data.get(NFS_CEPHX_KEY) or {}
    if not section.get("nfs_mount") or not section.get("nfs_export"):
        raise OperationFailedError(f"No {NFS_CEPHX_KEY} hand-off in {path}")
    return section


def _assert_mounted(client, mount_point, export):
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"findmnt -n -o SOURCE --target {shlex.quote(mount_point)}",
        check_ec=False,
        timeout=60,
    )
    source = (out or "").strip()
    if _remote_exit_status(client) != 0 or not source.endswith(f":{export}"):
        return False
    return True


def _nfs_probe_io(client, mount_point, tag):
    probe = f"{mount_point}/{tag}"
    qprobe = shlex.quote(probe)
    qtag = shlex.quote(tag)
    client.exec_command(
        sudo=True,
        cmd=(
            f"printf '%s\\n' {qtag} > {qprobe} && "
            f"grep -q {qtag} {qprobe} && rm -f {qprobe}"
        ),
    )


def _setup(ceph_cluster, **kw):
    """
    Config (optional):
        nfs_name, fs_name (default cephfs), nfs_export_base, nfs_mount,
        nfs_version, port, clients (int, default 4), upgrade_config_path
    """
    config = kw.get("config") or {}
    clients = []
    nfs_name = DEFAULT_NFS_NAME
    mount_point = DEFAULT_MOUNT
    export = f"{DEFAULT_EXPORT_BASE}_0"
    nfs_created = False
    try:
        path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
        nfs_name = config.get("nfs_name", DEFAULT_NFS_NAME)
        fs_name = config.get("fs_name", DEFAULT_FS_NAME)
        export_base = config.get("nfs_export_base", DEFAULT_EXPORT_BASE)
        mount_point = config.get("nfs_mount", DEFAULT_MOUNT)
        nfs_version = str(config.get("nfs_version", DEFAULT_VERSION))
        nfs_port = str(config.get("port", DEFAULT_PORT))
        no_clients = int(config.get("clients", 4))
        # single_export creates {export_base}_0
        export = f"{export_base}_0"

        nfs_nodes = ceph_cluster.get_nodes("nfs")
        clients = ceph_cluster.get_nodes("client")
        if len(nfs_nodes) < 2:
            raise ConfigError(
                f"Need 2 nfs nodes for combined suite; found {len(nfs_nodes)}"
            )
        if no_clients > len(clients):
            raise ConfigError(f"Need {no_clients} clients; found {len(clients)}")
        clients = clients[:no_clients]
        nfs_server_hostnames = [n.hostname for n in nfs_nodes]

        fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
        ensure_ceph_fs(fs_util, clients[0], fs_name)

        log.info(
            "Creating 2-node NFS %s on %s servers=%s mount=%s",
            nfs_name,
            fs_name,
            nfs_server_hostnames,
            mount_point,
        )
        setup_nfs_cluster(
            clients,
            nfs_server_hostnames,
            nfs_port,
            nfs_version,
            nfs_name,
            mount_point,
            fs_name,
            export_base,
            fs_name,
            ceph_cluster=ceph_cluster,
            single_export=True,
        )
        nfs_created = True
        server = nfs_server_hostnames[0]

        for client in clients:
            _ensure_fstab(client, server, export, mount_point, nfs_version)

        payload = {
            "nfs_name": nfs_name,
            "fs_name": fs_name,
            "nfs_export": export,
            "nfs_mount": mount_point,
            "nfs_version": nfs_version,
            "port": nfs_port,
            "server": server,
            "clients": no_clients,
        }
        _write_nfs_section(clients[0], payload, path)
        log.info(
            "NFS CephX setup PASSED — %s mounted on %d clients", export, no_clients
        )
    except Exception:
        if nfs_created and clients:
            try:
                log.warning(
                    "NFS setup failed after cluster create; cleaning %s", nfs_name
                )
                cleanup_cluster(clients, mount_point, nfs_name, export)
            except Exception as cleanup_exc:
                log.warning("NFS cleanup_cluster failed: %s", cleanup_exc)
        raise


def _io(ceph_cluster, **kw):
    """
    Config:
        tag (str): probe filename stem (default nfs_cephx_probe)
        remount_if_missing (bool): remount via hand-off if findmnt fails
        upgrade_config_path, clients (override count from hand-off)
    """
    config = kw.get("config") or {}
    path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
    tag = config.get("tag", "nfs_cephx_probe")
    remount_if_missing = bool(config.get("remount_if_missing", False))

    clients = ceph_cluster.get_nodes("client")
    section = _load_nfs_section(clients[0], path)
    mount_point = section["nfs_mount"]
    export = section["nfs_export"]
    nfs_version = str(section.get("nfs_version", "4.1"))
    nfs_port = str(section.get("port", "2049"))
    server = section.get("server")
    no_clients = int(config.get("clients", section.get("clients", 4)))
    if no_clients > len(clients):
        raise OperationFailedError(f"Need {no_clients} clients; found {len(clients)}")
    clients = clients[:no_clients]

    for idx, client in enumerate(clients, start=1):
        if not _assert_mounted(client, mount_point, export):
            if not remount_if_missing or not server:
                raise OperationFailedError(
                    f"client.{idx} ({client.hostname}): expected NFS "
                    f"{export} at {mount_point} (no remount)"
                )
            log.warning(
                "client.%d mount missing; remounting %s:%s -> %s",
                idx,
                server,
                export,
                mount_point,
            )
            client.create_dirs(dir_path=mount_point, sudo=True)
            mount_retry(client, mount_point, nfs_version, nfs_port, server, export)
            if not _assert_mounted(client, mount_point, export):
                raise OperationFailedError(
                    f"client.{idx} remount failed for {mount_point}"
                )

        pre = f"{mount_point}/nfs_cephx_seed.txt"
        qpre = shlex.quote(pre)
        if config.get("seed"):
            client.exec_command(
                sudo=True,
                cmd=f"printf '%s\\n' 'nfs-cephx-seed' > {qpre}",
            )
        else:
            # Post-upgrade / post-rotate: seed must still be readable
            # (missing file is a real data-path failure, not a soft warn).
            client.exec_command(
                sudo=True,
                cmd=f"test -f {qpre} && cat {qpre} > /dev/null",
            )
        _nfs_probe_io(client, mount_point, f"{tag}_c{idx}")
        log.info("OK NFS IO client.%d on %s", idx, mount_point)

    log.info("NFS mount IO PASSED on %d clients", no_clients)


ACTIONS = {
    "setup": _setup,
    "io": _io,
}


def run(ceph_cluster, **kw):
    """Config keys: action (setup|io), plus action-specific keys."""
    try:
        config = kw.get("config") or {}
        action = config.get("action")
        if action not in ACTIONS:
            raise OperationFailedError(f"Unknown action {action!r}; use setup|io")
        ACTIONS[action](ceph_cluster, **kw)
        log.info("CephX NFS PASSED action=%s", action)
        return 0
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        return 1
