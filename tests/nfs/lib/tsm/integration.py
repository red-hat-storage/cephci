"""Helpers for TSM cross-feature integration tests.

Reuses:
  - ``create_nfs_via_file_and_verify`` (nfs_operations) for orch apply
  - ``create_or_replace_export_with_delegation`` for exports
  - ``wait_new_container`` (helpers) re-exported for callers
  - ``Ceph`` CLI wrappers for cluster info / orch ps

Kept here (integration-specific):
  - placement-count wait, multi-export mounts, MDS session checks
"""

import json

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount
from tests.nfs.lib.tsm.helpers import wait_new_container  # noqa: F401 — re-export
from tests.nfs.nfs_delegation_operations import create_or_replace_export_with_delegation
from tests.nfs.nfs_operations import create_nfs_via_file_and_verify
from utility.log import Log

log = Log(__name__)

EXPORT_COUNT = 3
# None = omit --delegations on export create; str values use --delegations <mode>
DELEG_MODES = (None, "none", "rw")
APPLY_TIMEOUT = 300
DAEMON_WAIT_TIMEOUT = 300


def resolve_nfs_nodes(ceph_cluster):
    """All NFS-role nodes from cluster conf (same as test_nfs_tsm_basic)."""
    return sorted(ceph_cluster.get_nodes("nfs"), key=lambda n: n.hostname)


def _json_cmd(client, cmd):
    out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    try:
        return json.loads((out or "").strip() or "null")
    except json.JSONDecodeError:
        return None


def _parse_cli_json(raw):
    if not raw:
        return None
    text = raw.strip() if isinstance(raw, str) else raw.read().decode().strip()
    try:
        return json.loads(text or "null")
    except json.JSONDecodeError:
        return None


def nfs_cluster_info(client, nfs_name):
    info = Ceph(client).nfs.cluster.info(nfs_name)
    if not info:
        return None
    return info.get(nfs_name) or info


def record_ganesha_id(client, nfs_name):
    info = nfs_cluster_info(client, nfs_name) or {}
    for key in ("id", "cluster_id", "ganesha_id"):
        if info.get(key):
            return info[key]
    return None


def assert_ganesha_id_unchanged(pre_id, post_id, label):
    if pre_id and post_id and str(pre_id) != str(post_id):
        log.error("[%s] ganesha id changed %s -> %s", label, pre_id, post_id)
        return 1
    return 0


def orch_nfs_daemons(client, nfs_name):
    raw = Ceph(client).orch.ps(service_name=f"nfs.{nfs_name}", format="json")
    data = _parse_cli_json(raw)
    return data if isinstance(data, list) else []


def daemon_on_host(client, nfs_name, hostname):
    return next(
        (
            d
            for d in orch_nfs_daemons(client, nfs_name)
            if d.get("hostname") == hostname
        ),
        None,
    )


def running_nfs_hosts(client, nfs_name):
    return {
        d["hostname"]
        for d in orch_nfs_daemons(client, nfs_name)
        if d.get("status_desc") == "running" and d.get("hostname")
    }


def wait_nfs_running_count(client, nfs_name, count, timeout=DAEMON_WAIT_TIMEOUT):
    """Wait until >= count daemons are running (TC-10 count placement may leave a spare host)."""
    for _ in WaitUntil(timeout=timeout, interval=5):
        if len(running_nfs_hosts(client, nfs_name)) >= count:
            return True
    return False


def active_mds_rank(client, fs_name="cephfs"):
    data = _json_cmd(client, f"ceph fs status {fs_name} --format json") or {}
    for mds in data.get("mdsmap") or []:
        if mds.get("state") == "up:active" and mds.get("rank") is not None:
            return int(mds["rank"])
    return 0


def mds_nfs_sessions(client, fs_name="cephfs"):
    rank = active_mds_rank(client, fs_name)
    sessions = (
        _json_cmd(client, f"ceph tell mds.{fs_name}:{rank} session ls --format json")
        or []
    )
    out = [
        s
        for s in sessions
        if "ganesha" in json.dumps(s).lower() or "nfs" in json.dumps(s).lower()
    ]
    log.info("MDS nfs sessions (rank %s): %s", rank, len(out))
    return out


def snapshot_mds_sessions(client, label):
    sessions = mds_nfs_sessions(client)
    log.info("[%s] MDS sessions: %s", label, len(sessions))
    return sessions


def assert_mds_sessions_ok(pre, post, label):
    if len(post) < 1:
        log.error("[%s] no MDS nfs sessions after disruption", label)
        return 1
    log.info("[%s] MDS sessions pre=%s post=%s", label, len(pre), len(post))
    return 0


def wait_orch_hosts(client, nfs_name, hostnames, timeout=300):
    want = set(hostnames)
    for _ in WaitUntil(timeout=timeout, interval=5):
        if want <= running_nfs_hosts(client, nfs_name):
            return True
    return False


def wait_daemon_running(client, nfs_name, hostname, timeout=DAEMON_WAIT_TIMEOUT):
    for _ in WaitUntil(timeout=timeout, interval=5):
        d = daemon_on_host(client, nfs_name, hostname)
        if d and d.get("status_desc") == "running":
            return True
    return False


def wait_daemon_stopped(client, nfs_name, hostname, timeout=120):
    for _ in WaitUntil(timeout=timeout, interval=5):
        d = daemon_on_host(client, nfs_name, hostname)
        if not d:
            return True
        status = str(d.get("status_desc", "")).strip().lower()
        if status == "stopped" or (d.get("status") == 0 and status != "running"):
            return True
    return False


def _orch_daemon_action(client, nfs_name, hostname, action, *, wait_running=False):
    daemon = daemon_on_host(client, nfs_name, hostname)
    if not daemon or not daemon.get("daemon_name"):
        return False
    name = daemon["daemon_name"]
    if action == "redeploy":
        cmd = f"ceph orch daemon redeploy {name}"
    else:
        cmd = f"ceph orch daemon {action} {name} --force"
    client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return wait_daemon_running(client, nfs_name, hostname) if wait_running else True


def remove_nfs_daemon(client, nfs_name, hostname):
    return _orch_daemon_action(client, nfs_name, hostname, "rm")


def restart_nfs_daemon(client, nfs_name, hostname):
    return _orch_daemon_action(client, nfs_name, hostname, "restart", wait_running=True)


def redeploy_nfs_daemon(client, nfs_name, hostname):
    return _orch_daemon_action(
        client, nfs_name, hostname, "redeploy", wait_running=True
    )


def reapply_nfs_spec(installer, spec, nfs_nodes):
    return create_nfs_via_file_and_verify(
        installer, [spec], APPLY_TIMEOUT, nfs_nodes=nfs_nodes
    )


def setup_three_exports(client, nfs_name, tag="integ"):
    group = f"tsm_integ_{tag}"
    client.exec_command(
        sudo=True, cmd=f"ceph fs subvolumegroup create cephfs {group}", check_ec=False
    )
    exports = []
    for i in range(EXPORT_COUNT):
        sv = f"sv_{tag}_{i}"
        client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume create cephfs {sv} --group_name {group}",
            check_ec=False,
        )
        path, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath cephfs {sv} --group_name {group}",
            check_ec=False,
        )
        path = (path or "").strip()
        pseudo = f"/export_integ_{i}"
        try:
            create_or_replace_export_with_delegation(
                client,
                "cephfs",
                nfs_name,
                pseudo,
                path,
                DELEG_MODES[i],
                use_cephadm=False,
            )
        except (OperationFailedError, CommandFailed) as exc:
            log.error("export %s failed: %s", pseudo, exc)
            return None
        exports.append({"subvolume": sv, "group": group, "export": pseudo})
    return exports


def mount_three_exports(clients, exports, server, nfs_port, nfs_version):
    """Mount client i on export_integ_i (one client per export)."""
    mounts = []
    for i, exp in enumerate(exports):
        if i >= len(clients):
            break
        client = clients[i]
        mnt = f"/mnt/integ_exp{i}"
        client.create_dirs(dir_path=mnt, sudo=True)
        if Mount(client).nfs(
            mount=mnt,
            version=str(nfs_version),
            port=str(nfs_port),
            server=server.hostname,
            export=exp["export"],
        ):
            return None
        mounts.append(mnt)
    return mounts


def teardown_three_exports(client, clients, mounts, exports):
    for cl, mnt in zip(clients, mounts):
        cl.exec_command(
            sudo=True,
            cmd="timeout 30 umount -l %s 2>/dev/null || true" % mnt,
            check_ec=False,
            timeout=45,
        )
    group = exports[0]["group"] if exports else None
    for exp in exports:
        client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume rm cephfs {exp['subvolume']} --group_name {exp['group']} 2>/dev/null",
            check_ec=False,
        )
    if group:
        client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm cephfs {group} 2>/dev/null",
            check_ec=False,
        )
