"""
Shared NFSv4 delegation test infrastructure.

Cluster/export template (config-key), cephadm shell helpers, Ganesha container log I/O.
Scenario-specific validation and multi-client recall logic live in the test modules.

Recommended suite order (tier1-nfs-ganesha-v4-2.yaml):
  1. nfs_verify_cluster_level_delegations (optional cluster/export toggles)
  2. nfs_verify_delegation_rw_none_scenarios (may auto-create NFS cluster)
  3. nfs_verify_delegation_revoke_recall / nfs_verify_delegation_conflict_scenarios
     (require ``nfs_name`` cluster unless ``auto_create_nfs_cluster: true``)
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from nfs_operations import setup_nfs_cluster, verify_nfs_ganesha_service

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

# CephNode / installer / client nodes (ceph.ceph.CephNode)
CephNode = Any
ConfigDict = Dict[str, Any]
ExportSpec = Tuple[str, str]  # (pseudo_path, delegation_mode)

log = Log(__name__)

CONF_KEY = "mgr/cephadm/services/nfs/ganesha.conf"
DEFAULT_TEMPLATE_PATH = (
    "/usr/share/ceph/mgr/cephadm/templates/services/nfs/ganesha.conf.j2"
)
MOUNTED_TEMPLATE_PATH = "/var/lib/ceph/ganesha.conf"

# Unique per interpreter run to avoid /tmp clashes across parallel delegation tests.
_DELEG_RUN_ID = "%s_%s" % (os.getpid(), int(time.time() * 1000))


def _delegation_tmp_path(suffix: str) -> str:
    return "/tmp/ganesha_delegation_%s_%s" % (_DELEG_RUN_ID, suffix)


WORK_TEMPLATE_PATH = _delegation_tmp_path("work.conf.j2")
BACKUP_TEMPLATE_PATH = _delegation_tmp_path("backup.conf.j2")
LOG_TEMPLATE_WORK_PATH = _delegation_tmp_path("logging_work.conf.j2")
LOG_TEMPLATE_BACKUP_PATH = _delegation_tmp_path("logging_backup.conf.j2")
GANESHA_FULL_DEBUG_LOG_BLOCK = """LOG {
    Default_Log_Level = FULL_DEBUG;
    Facility {
        name = FILE;
        destination = "/var/log/ganesha.log";
        enable = active;
    }
    Components {
        CONFIG = FULL_DEBUG;
        FSAL = FULL_DEBUG;
        NFS4 = FULL_DEBUG;
        STATE = FULL_DEBUG;
    }
}"""

DELEGATIONS_PATTERN = re.compile(r"Delegations\s*=\s*(true|false)\s*;", re.I)
SUPPORTED_DELEGATIONS = frozenset({"rw", "ro", "none"})

# Ganesha log validation (compiled once; used by scenario test modules).
RE_DELEGATION_RECALLING = re.compile(r"Recalling", re.I)
RE_DELEGATION_RECALLED = re.compile(r"successfully recalled", re.I)
RE_DELEGATION_ATTEMPT_GRANT = re.compile(r"Attempting to grant delegation", re.I)
RE_DELEGATION_TYPE_NOT_SUPPORTED = re.compile(r"Delegation type not supported", re.I)
RE_DELEGATION_TYPE_VALUE = re.compile(r"delegation_type\s+(\d+)", re.I)
RE_DELEGATION_TYPE_ZERO = re.compile(r"delegation_type\s+0\b", re.I)
RE_DELEGATION_TYPE_READ_WRITE = re.compile(r"delegation_type\s+[45]\b", re.I)
NFS4_OP_OPEN_END_MARKER = "END OF nfs4_op_open"

GANESHA_DELEGATION_TAILF_GREP_E = (
    "OPEN handler|END OF nfs4_op_open|END OF|CLOSE handler|successfully recalled|"
    "Successful exit|Share Access|DELEGRETURN|OP_DELEGRETURN|Recalling|Revoking|"
    "NFS4ERR_RECLAIM_BAD|do_delegation|delegation_type|Attempting to grant delegation|"
    "Delegation type not supported"
)
GANESHA_DELEGATION_TAILF_CAPTURE = _delegation_tmp_path("tailf.log")
GANESHA_DELEGATION_TAILF_PID = _delegation_tmp_path("tailf.pid")

# Delegation test timing (seconds). Ganesha NFSv4 delegations are lease-driven; many
# clusters use a ~90s lease. These values add margin for async grant/recall and for
# FULL_DEBUG lines to reach the in-container tail -f | grep capture.
#
# After seed IO (OPEN/CLOSE): let any prior delegation return before the next capture.
DELEGATION_EXIT_WAIT_SECONDS_DEFAULT = 120
# Background OPEN hold: keep the delegation active while a peer runs conflicting IO.
DELEGATION_HOLD_SECONDS_DEFAULT = 120
# After starting the hold OPEN, before peer IO, so grant can complete asynchronously.
DELEGATION_HOLD_READY_SECONDS_DEFAULT = 8
# After scenario IO: wait for recall/grant lines in the filtered ganesha.log capture.
DELEGATION_LOG_SETTLE_SECONDS_DEFAULT = 90
# clamp_delegation_log_settle_seconds() bounds when callers use that helper.
DELEGATION_LOG_SETTLE_SECONDS_CLAMP_LO = 75
DELEGATION_LOG_SETTLE_SECONDS_CLAMP_HI = 90
DELEGATION_LOG_SETTLE_FALLBACK_DEFAULT = 82


def parse_nfs4_open_delegation_types(window_text):
    """Return delegation_type integers from nfs4_op_open END lines in *window_text*."""
    types = []
    for ln in (window_text or "").splitlines():
        if NFS4_OP_OPEN_END_MARKER not in ln:
            continue
        match = RE_DELEGATION_TYPE_VALUE.search(ln)
        if match:
            types.append(int(match.group(1)))
    return types


def validate_delegation_recall_ganesha_capture(scenario_name, hits, expect_recall):
    """Validate tail -f capture for delegation recall (or absence of recall)."""
    if not hits:
        raise OperationFailedError("Scenario %s: empty tail -f capture" % scenario_name)
    text = "\n".join(hits)
    if expect_recall:
        if not RE_DELEGATION_RECALLING.search(text):
            raise OperationFailedError(
                "Scenario %s: missing 'Recalling' in capture" % scenario_name
            )
        if not RE_DELEGATION_RECALLED.search(text):
            raise OperationFailedError(
                "Scenario %s: missing 'successfully recalled' in capture"
                % scenario_name
            )
    elif RE_DELEGATION_RECALLING.search(text) or RE_DELEGATION_RECALLED.search(text):
        raise OperationFailedError(
            "Scenario %s: unexpected recall in ganesha.log" % scenario_name
        )


def validate_delegation_ganesha_window(delegation_mode, window_text):
    """Return (ok, reason) for rw/ro/none export log window validation."""
    text = window_text or ""
    types = parse_nfs4_open_delegation_types(text)
    if delegation_mode == "rw":
        if not RE_DELEGATION_ATTEMPT_GRANT.search(text):
            return (
                False,
                "missing 'Attempting to grant delegation' (write delegation path)",
            )
        if 5 not in types:
            return (
                False,
                "expected delegation_type 5 (write deleg) on END OF nfs4_op_open, "
                "parsed types=%r" % types,
            )
        return True, ""
    if delegation_mode == "ro":
        if not RE_DELEGATION_TYPE_NOT_SUPPORTED.search(text):
            return (
                False,
                "missing 'Delegation type not supported' (expected on write OPEN for ro export)",
            )
        if not RE_DELEGATION_ATTEMPT_GRANT.search(text):
            return (
                False,
                "missing 'Attempting to grant delegation' (expected on read OPEN for ro export)",
            )
        if 4 not in types:
            return (
                False,
                "expected delegation_type 4 (read deleg) on END OF nfs4_op_open, "
                "parsed types=%r" % types,
            )
        if 5 in types:
            return (
                False,
                "unexpected delegation_type 5 (write deleg) on read-only export",
            )
        return True, ""
    if delegation_mode == "none":
        if not RE_DELEGATION_TYPE_NOT_SUPPORTED.search(text):
            return (
                False,
                "missing 'Delegation type not supported' for delegations=none export",
            )
        if any(t in (4, 5) for t in types):
            return (
                False,
                "delegations=none but saw read/write delegation types in nfs4_op_open: %r"
                % types,
            )
        if not types:
            if RE_DELEGATION_TYPE_ZERO.search(
                text
            ) and not RE_DELEGATION_TYPE_READ_WRITE.search(text):
                return True, ""
            return (
                False,
                "could not parse delegation_type from END OF nfs4_op_open lines",
            )
        if not all(t == 0 for t in types):
            return (
                False,
                "expected only delegation_type 0 on delegations=none export, got %r"
                % types,
            )
        return True, ""
    return False, "unknown delegations mode %r" % delegation_mode


def enable_ganesha_debug_logging(
    cmd_host,
    log_block=GANESHA_FULL_DEBUG_LOG_BLOCK,
    config_key=CONF_KEY,
):
    """
    Enable Ganesha debug logging by appending a LOG block to the cephadm NFS template.

    Returns:
        bool: True if a prior custom template backup exists, False otherwise.
    """
    backup_exists = backup_ganesha_template(
        cmd_host, LOG_TEMPLATE_BACKUP_PATH, config_key
    )

    run_cephadm_shell(
        cmd_host,
        "ceph config-key get %s > %s" % (config_key, LOG_TEMPLATE_WORK_PATH),
        check_ec=False,
    )
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "test -s %s || cephadm shell -- cat %s > %s"
            % (LOG_TEMPLATE_WORK_PATH, DEFAULT_TEMPLATE_PATH, LOG_TEMPLATE_WORK_PATH)
        ),
    )
    ready, _ = cmd_host.exec_command(
        sudo=True,
        cmd="test -s %s && echo ok" % LOG_TEMPLATE_WORK_PATH,
        check_ec=False,
    )
    if "ok" not in (ready or ""):
        raise OperationFailedError("Unable to read Ganesha template for logging update")

    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "grep -q 'Default_Log_Level = FULL_DEBUG' %s "
            "|| cat >> %s <<'EOF'\n%s\nEOF"
            % (LOG_TEMPLATE_WORK_PATH, LOG_TEMPLATE_WORK_PATH, log_block)
        ),
    )

    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "cephadm shell --mount %s:%s -- ceph config-key set %s -i %s"
            % (
                LOG_TEMPLATE_WORK_PATH,
                MOUNTED_TEMPLATE_PATH,
                config_key,
                MOUNTED_TEMPLATE_PATH,
            )
        ),
    )
    verify_out, _ = cmd_host.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph config-key get %s" % config_key,
    )
    if "Default_Log_Level = FULL_DEBUG;" not in (verify_out or ""):
        raise OperationFailedError(
            "Failed to enable Ganesha debug logging in ceph config-key template"
        )
    return backup_exists


def ensure_ceph_conf_and_admin_keyring_on_hosts(source_node, target_nodes):
    """
    Place minimal ceph.conf and client.admin keyring on NFS host(s).

    ``target_nodes`` may be a single node or a list. Mirrors test_client behavior so
    ``cephadm shell`` and ``ceph`` work on NFS hosts.
    """
    nodes = target_nodes if isinstance(target_nodes, list) else [target_nodes]
    source_name = getattr(source_node, "hostname", source_node)

    ceph_conf, _ = source_node.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph config generate-minimal-conf",
        timeout=300,
    )
    if not ceph_conf or not str(ceph_conf).strip():
        raise OperationFailedError(
            "ceph config generate-minimal-conf returned empty output on %s"
            % source_name
        )

    admin_keyring, _ = source_node.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph auth get client.admin",
        timeout=300,
    )
    if not admin_keyring or not str(admin_keyring).strip():
        raise OperationFailedError(
            "ceph auth get client.admin returned empty output on %s" % source_name
        )

    for target in nodes:
        target.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")
        with target.remote_file(
            sudo=True, file_name="/etc/ceph/ceph.conf", file_mode="w"
        ) as conf_fp:
            conf_fp.write(ceph_conf)
            conf_fp.flush()
        with target.remote_file(
            sudo=True,
            file_name="/etc/ceph/ceph.client.admin.keyring",
            file_mode="w",
        ) as key_fp:
            key_fp.write(admin_keyring)
            key_fp.flush()
        log.info(
            "Installed /etc/ceph/ceph.conf and ceph.client.admin.keyring on %s",
            target.hostname,
        )


def mount_delegation_export(
    client: CephNode,
    mount_point: str,
    nfs_version: str,
    port: str,
    server: str,
    export_path: str,
) -> None:
    """Create mount point, best-effort unmount, and mount an NFS export for delegation tests."""
    from nfs_operations import mount_retry

    from cli.utilities.filesys import Unmount

    client.exec_command(sudo=True, cmd="mkdir -p %s" % mount_point, check_ec=False)
    try:
        Unmount(client).unmount(mount_point)
    except Exception:
        pass
    mount_retry(client, mount_point, nfs_version, port, server, export_path)


def unmount_delegation_export(
    client: CephNode,
    mount_point: str,
    *,
    remove_mount_dir: bool = False,
    test_files: Optional[Sequence[str]] = None,
) -> None:
    """Unmount a delegation test mount and optionally remove test files / mount directory."""
    from cli.utilities.filesys import Unmount

    try:
        Unmount(client).unmount(mount_point)
    except Exception:
        pass
    if test_files:
        for name in test_files:
            client.exec_command(
                sudo=True, cmd="rm -f %s/%s" % (mount_point, name), check_ec=False
            )
            client.exec_command(
                sudo=True,
                cmd="rm -f %s/%s.renamed" % (mount_point, name),
                check_ec=False,
            )
    if remove_mount_dir:
        client.exec_command(sudo=True, cmd="rm -rf %s" % mount_point, check_ec=False)


def _delegation_hold_pidfile(client: CephNode) -> str:
    host = str(getattr(client, "hostname", "client")).replace("/", "_")
    return _delegation_tmp_path("hold_%s.pids" % host)


def _q_delegation_hold_path(path: str) -> str:
    return path.replace("'", "'\"'\"'")


def hold_delegation_open(client: CephNode, path: str, mode: str, seconds: int) -> None:
    """Keep a file OPEN in the background; record its PID for kill_delegation_holds()."""
    safe = _q_delegation_hold_path(path)
    pidfile = _delegation_hold_pidfile(client)
    client.exec_command(
        sudo=True,
        cmd=(
            "nohup python3 -c "
            "\"f=open('%s','%s'); import time; time.sleep(%s)\" >/dev/null 2>&1 & "
            "echo $! >> %s" % (safe, mode, int(seconds), pidfile)
        ),
        check_ec=False,
    )


def kill_delegation_holds(client: CephNode) -> None:
    """Kill hold OPEN processes started by hold_delegation_open() on this client only."""
    pidfile = _delegation_hold_pidfile(client)
    client.exec_command(
        sudo=True,
        cmd=(
            "if [ -f %s ]; then while read -r pid; do "
            '[ -n "$pid" ] && kill "$pid" 2>/dev/null || true; '
            "done < %s; rm -f %s; fi"
        )
        % (pidfile, pidfile, pidfile),
        check_ec=False,
    )


def ensure_nfs_cluster_for_delegation_test(
    cephadm: Any,
    nfs_name: str,
    config: Mapping[str, Any],
    *,
    client: CephNode,
    nfs_nodes: Sequence[CephNode],
    installer: CephNode,
    ceph_cluster: Any,
) -> Tuple[List[str], bool]:
    """
    Ensure ``nfs.{nfs_name}`` exists before multi-client delegation scenarios.

    Returns:
        (cluster_names, created_cluster): ``created_cluster`` is True when
        ``auto_create_nfs_cluster`` triggered ``setup_nfs_cluster``.
    """
    nfs_clusters = cephadm.nfs.cluster.ls()
    if nfs_name in nfs_clusters:
        return nfs_clusters, False

    if not config.get("auto_create_nfs_cluster", False):
        raise ConfigError(
            (
                "NFS cluster %r not found. Run "
                "nfs_verify_cluster_level_delegations or "
                "nfs_verify_delegation_rw_none_scenarios first, or set "
                "auto_create_nfs_cluster: true in the test config."
            )
            % nfs_name
        )

    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    nfs_mount = config.get("nfs_mount", "/mnt/delegation_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_delegation_boot")
    fs_name = config.get("fs_name", "cephfs")
    service_wait_timeout = int(config.get("service_wait_timeout", 300))

    setup_nfs_cluster(
        clients=[client],
        nfs_server=[n.hostname for n in nfs_nodes],
        port=nfs_port,
        version=nfs_version,
        nfs_name=nfs_name,
        nfs_mount=nfs_mount,
        fs_name=fs_name,
        export=bootstrap_export,
        fs=fs_name,
        ceph_cluster=ceph_cluster,
        single_export=True,
    )
    verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)
    nfs_clusters = cephadm.nfs.cluster.ls()
    if nfs_name not in nfs_clusters:
        raise OperationFailedError(
            "Failed to create NFS cluster %s: %s" % (nfs_name, nfs_clusters)
        )
    return nfs_clusters, True


def restore_delegation_ganesha_templates(
    nfs_cmd_host: CephNode,
    delegation_template_backup: bool,
    logging_template_backup: bool,
    cephadm: Any,
    installer: CephNode,
    redeploy_wait: int,
    service_wait_timeout: int,
) -> None:
    """Restore Ganesha config-key templates after delegation tests."""
    restore_ganesha_template(
        nfs_cmd_host, logging_template_backup, LOG_TEMPLATE_BACKUP_PATH
    )
    restore_ganesha_template(
        nfs_cmd_host, delegation_template_backup, BACKUP_TEMPLATE_PATH
    )
    redeploy_nfs_clusters(
        cephadm,
        cephadm.nfs.cluster.ls(),
        installer,
        redeploy_wait,
        service_wait_timeout,
    )


def run_cephadm_shell(
    node: CephNode, command: str, check_ec: bool = True
) -> Tuple[str, str]:
    return node.exec_command(
        sudo=True,
        cmd="cephadm shell -- %s" % command,
        check_ec=check_ec,
    )


def backup_ganesha_template(
    cmd_host, backup_path=BACKUP_TEMPLATE_PATH, config_key=CONF_KEY
):
    """Backup config-key template; return True if a prior custom template existed."""
    run_cephadm_shell(
        cmd_host,
        "ceph config-key get %s > %s" % (config_key, backup_path),
        check_ec=False,
    )
    out, _ = cmd_host.exec_command(
        sudo=True,
        cmd="test -s %s && echo present || echo absent" % backup_path,
        check_ec=False,
    )
    return "present" in (out or "")


def restore_ganesha_template(
    cmd_host, backup_exists, backup_path=BACKUP_TEMPLATE_PATH, config_key=CONF_KEY
):
    """Restore backed-up template or remove config-key override."""
    if backup_exists:
        cmd_host.exec_command(
            sudo=True,
            cmd=(
                "cephadm shell --mount %s:%s -- ceph config-key set %s -i %s"
                % (
                    backup_path,
                    MOUNTED_TEMPLATE_PATH,
                    config_key,
                    MOUNTED_TEMPLATE_PATH,
                )
            ),
        )
    else:
        run_cephadm_shell(
            cmd_host, "ceph config-key rm %s" % config_key, check_ec=False
        )


def set_cluster_delegation(cmd_host, delegation):
    run_cephadm_shell(
        cmd_host,
        "ceph config-key get %s > %s" % (CONF_KEY, WORK_TEMPLATE_PATH),
        check_ec=False,
    )
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "test -s %s || cephadm shell -- cat %s > %s"
            % (WORK_TEMPLATE_PATH, DEFAULT_TEMPLATE_PATH, WORK_TEMPLATE_PATH)
        ),
    )
    expected = "Delegations = %s;" % delegation
    out, _ = cmd_host.exec_command(
        sudo=True,
        cmd=(
            "sed -i -E "
            "'s/(Delegations[[:space:]]*=[[:space:]]*)(true|false)([[:space:]]*;)/"
            "\\1%s\\3/' %s && grep -F %r %s"
            % (delegation, WORK_TEMPLATE_PATH, expected, WORK_TEMPLATE_PATH)
        ),
    )
    if expected not in (out or ""):
        raise OperationFailedError("Failed to set %s in ganesha template" % expected)
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "cephadm shell --mount %s:%s -- ceph config-key set %s -i %s"
            % (
                WORK_TEMPLATE_PATH,
                MOUNTED_TEMPLATE_PATH,
                CONF_KEY,
                MOUNTED_TEMPLATE_PATH,
            )
        ),
    )


def redeploy_nfs_clusters(
    cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
):
    for cluster_name in nfs_clusters:
        cephadm.orch.redeploy("nfs.%s" % cluster_name)
    time.sleep(redeploy_wait)
    verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)


def _expect_delegation(conf, label, expected):
    match = DELEGATIONS_PATTERN.search(conf or "")
    if not match:
        raise OperationFailedError("Delegations entry not found in %s" % label)
    observed = match.group(1).lower()
    if observed != expected:
        raise OperationFailedError(
            "Delegations mismatch on %s: expected %s, got %s"
            % (label, expected, observed)
        )


def verify_container_delegation(nfs_nodes, daemons, nfs_name, delegation):
    by_short = {(d.get("hostname") or "").split(".")[0]: d for d in daemons}
    for node in nfs_nodes:
        short = node.hostname.split(".")[0]
        cid = by_short.get(short, {}).get("container_id")
        if not cid:
            raise OperationFailedError(
                "No nfs daemon on %s for nfs.%s" % (node.hostname, nfs_name)
            )
        conf, _ = node.exec_command(
            sudo=True, cmd="podman exec %s cat /etc/ganesha/ganesha.conf" % cid
        )
        _expect_delegation(conf, "%s container %s" % (node.hostname, cid), delegation)
        log.info(
            "Verified Delegations = %s on %s (container %s)",
            delegation,
            node.hostname,
            cid,
        )


def verify_host_conf_delegation(nfs_nodes, nfs_name, delegation):
    for node in nfs_nodes:
        fsids = node.get_dir_list("/var/lib/ceph", sudo=True)
        if not fsids:
            raise OperationFailedError(
                "Unable to locate /var/lib/ceph fsid path on %s" % node.hostname
            )
        base = "/var/lib/ceph/%s" % fsids[0]
        instances = [n for n in node.get_dir_list(base, sudo=True) if nfs_name in n]
        if not instances:
            raise OperationFailedError(
                "No nfs instance directory found for %s on %s"
                % (nfs_name, node.hostname)
            )
        conf = ""
        for inst in instances:
            path = "%s/%s/etc/ganesha/ganesha.conf" % (base, inst)
            out, _ = node.exec_command(sudo=True, cmd="cat %s" % path, check_ec=False)
            if out:
                conf = out
                break
        if not conf:
            raise OperationFailedError(
                "Unable to read ganesha.conf for %s on %s" % (nfs_name, node.hostname)
            )
        _expect_delegation(conf, "host ganesha.conf on %s" % node.hostname, delegation)


def _expect_export_delegation(cmd_host, nfs_name, export_name, expected):
    out, _ = run_cephadm_shell(
        cmd_host,
        "ceph nfs export get %s %s --format json" % (nfs_name, export_name),
    )
    content = (out or "").strip()
    if not content:
        raise OperationFailedError("Empty output from `ceph nfs export get`")
    try:
        data = json.loads(content)
    except json.JSONDecodeError as err:
        raise OperationFailedError(
            "Unable to parse export get output as json: %s; output=%r" % (err, content)
        )
    observed = str(data.get("delegations", "")).strip().lower()
    if observed != expected:
        raise OperationFailedError(
            "Delegations mismatch for %s: expected %r, got %r"
            % (export_name, expected, observed)
        )


def create_or_replace_export_with_delegation(
    cmd_host, fs_name, nfs_name, export_name, export_path, delegation
):
    run_cephadm_shell(
        cmd_host,
        "ceph nfs export delete %s %s" % (nfs_name, export_name),
        check_ec=False,
    )
    run_cephadm_shell(
        cmd_host,
        "ceph nfs export create %s %s %s %s %s --delegations %s"
        % (fs_name, nfs_name, export_name, fs_name, export_path, delegation),
    )
    _expect_export_delegation(cmd_host, nfs_name, export_name, delegation)


def update_export_delegation(cmd_host, nfs_name, export_name, delegation):
    run_cephadm_shell(
        cmd_host,
        "ceph nfs export update %s %s '%s' --format json"
        % (nfs_name, export_name, delegation),
    )
    _expect_export_delegation(cmd_host, nfs_name, export_name, delegation)


def run_negative_export_delegation_checks(
    cmd_host, fs_name, nfs_name, export_name, export_path, config
):
    invalid_values = config.get("negative_delegation_values", ["invalid", "bad"])
    invalid_values = [str(v).strip() for v in invalid_values if str(v).strip()]
    if not invalid_values:
        raise ConfigError("negative_delegation_values must contain at least one value")

    seed = str(config.get("export_update_seed_delegation", "rw")).strip().lower()
    if seed not in SUPPORTED_DELEGATIONS:
        seed = "rw"

    create_or_replace_export_with_delegation(
        cmd_host, fs_name, nfs_name, export_name, export_path, seed
    )

    for invalid in invalid_values:
        cases = (
            (
                "ceph nfs export create %s %s %s %s %s --delegations %s"
                % (fs_name, nfs_name, export_name, fs_name, export_path, invalid),
                "export create with invalid delegations=%r" % invalid,
            ),
            (
                "ceph nfs export update %s %s '%s' --format json"
                % (nfs_name, export_name, invalid),
                "export update with invalid delegations=%r" % invalid,
            ),
        )
        for command, action in cases:
            run_cephadm_shell(cmd_host, command, check_ec=False)
            if int(getattr(cmd_host, "exit_status", 0)) == 0:
                raise OperationFailedError(
                    "Negative test failed for %s: command unexpectedly succeeded"
                    % action
                )
            log.info("Negative test passed for %s: command failed as expected", action)


def _supported_delegation_config_values(
    config: Mapping[str, Any], key: str, default: List[str]
) -> List[str]:
    vals = [str(v).strip().lower() for v in config.get(key, default)]
    if any(v not in SUPPORTED_DELEGATIONS for v in vals):
        raise ConfigError("%s accepts only rw, ro, none" % key)
    return vals


def _provision_export_check_subvolume(
    cmd_host: CephNode, fs_name: str, group: str, subvolume_name: str
) -> str:
    run_cephadm_shell(
        cmd_host,
        "ceph fs subvolumegroup create %s %s" % (fs_name, group),
        check_ec=False,
    )
    run_cephadm_shell(
        cmd_host, "ceph fs subvolume create %s %s %s" % (fs_name, subvolume_name, group)
    )
    out, _ = run_cephadm_shell(
        cmd_host,
        "ceph fs subvolume getpath %s %s --group_name %s"
        % (fs_name, subvolume_name, group),
    )
    path = (out or "").strip()
    if not path:
        raise OperationFailedError(
            "Unable to fetch subvolume path for %s" % subvolume_name
        )
    return path


def _teardown_export_check_subvolume(
    cmd_host: CephNode,
    fs_name: str,
    nfs_name: str,
    export_name: str,
    group: str,
    subvolume_name: str,
) -> None:
    run_cephadm_shell(
        cmd_host,
        "ceph nfs export delete %s %s" % (nfs_name, export_name),
        check_ec=False,
    )
    run_cephadm_shell(
        cmd_host,
        "ceph fs subvolume rm %s %s --group_name %s" % (fs_name, subvolume_name, group),
        check_ec=False,
    )


def _run_export_create_delegation_checks(
    cmd_host: CephNode,
    fs_name: str,
    nfs_name: str,
    export_name: str,
    subvolume_path: str,
    delegation_modes: Iterable[str],
) -> None:
    for delegation_mode in delegation_modes:
        create_or_replace_export_with_delegation(
            cmd_host, fs_name, nfs_name, export_name, subvolume_path, delegation_mode
        )


def _run_export_update_delegation_checks(
    cmd_host: CephNode,
    fs_name: str,
    nfs_name: str,
    export_name: str,
    subvolume_path: str,
    config: Mapping[str, Any],
    update_modes: Iterable[str],
) -> None:
    seed = str(config.get("export_update_seed_delegation", "rw")).strip().lower()
    if seed not in SUPPORTED_DELEGATIONS:
        raise ConfigError("export_update_seed_delegation accepts only rw, ro, none")
    create_or_replace_export_with_delegation(
        cmd_host, fs_name, nfs_name, export_name, subvolume_path, seed
    )
    for delegation_mode in update_modes:
        update_export_delegation(cmd_host, nfs_name, export_name, delegation_mode)


def run_export_level_delegation_checks(
    cmd_host: CephNode, fs_name: str, nfs_name: str, config: Mapping[str, Any]
) -> None:
    """Run export create/update/negative checks for rw, ro, and none delegation modes."""
    group = config.get("subvolume_group", "ganeshagroup")
    subvolume_name = config.get(
        "subvolume_name", "subvol_delegation_%s" % int(time.time())
    )
    export_name = config.get("export_name", "/export1")
    do_create = config.get("run_export_create_checks", True)
    do_update = config.get("run_export_update_checks", True)
    if not do_create and not do_update:
        raise ConfigError(
            "At least one of run_export_create_checks/run_export_update_checks must be true"
        )

    subvolume_path = _provision_export_check_subvolume(
        cmd_host, fs_name, group, subvolume_name
    )
    try:
        if do_create:
            _run_export_create_delegation_checks(
                cmd_host,
                fs_name,
                nfs_name,
                export_name,
                subvolume_path,
                _supported_delegation_config_values(
                    config, "delegation_values", ["rw", "ro", "none"]
                ),
            )
        if do_update:
            _run_export_update_delegation_checks(
                cmd_host,
                fs_name,
                nfs_name,
                export_name,
                subvolume_path,
                config,
                _supported_delegation_config_values(
                    config, "export_update_values", ["ro", "rw", "none"]
                ),
            )
        if config.get("run_negative_export_checks", True):
            run_negative_export_delegation_checks(
                cmd_host, fs_name, nfs_name, export_name, subvolume_path, config
            )
    finally:
        _teardown_export_check_subvolume(
            cmd_host, fs_name, nfs_name, export_name, group, subvolume_name
        )


def truncate_ganesha_container_log(nfs_node, container_id):
    nfs_node.exec_command(
        sudo=True,
        cmd="podman exec %s sh -c ':> /var/log/ganesha.log'" % container_id,
        check_ec=False,
    )


def start_ganesha_delegation_tailf_follow(
    nfs_node: CephNode,
    container_id: str,
    capture_path: Optional[str] = None,
    grep_e: Optional[str] = None,
) -> None:
    capture = capture_path or GANESHA_DELEGATION_TAILF_CAPTURE
    pattern = grep_e or GANESHA_DELEGATION_TAILF_GREP_E
    pid_file = GANESHA_DELEGATION_TAILF_PID
    nfs_node.exec_command(
        sudo=True,
        cmd=(
            "podman exec %s bash -c "
            "'rm -f %s %s; nohup bash -c \"tail -f /var/log/ganesha.log 2>/dev/null | "
            'grep --line-buffered -E \\"%s\\" >> %s" >/dev/null 2>&1 & echo $! > %s\''
            % (container_id, capture, pid_file, pattern, capture, pid_file)
        ),
        check_ec=False,
    )
    time.sleep(2)
    log.info(
        "Started tail -f | grep --line-buffered -E on container %s -> %s",
        container_id,
        capture,
    )


def stop_ganesha_delegation_tailf_follow(nfs_node: CephNode, container_id: str) -> None:
    pid_file = GANESHA_DELEGATION_TAILF_PID
    nfs_node.exec_command(
        sudo=True,
        cmd=(
            "podman exec %s bash -c "
            "'if [ -f %s ]; then pid=$(cat %s); "
            '[ -n "$pid" ] && kill "$pid" 2>/dev/null || true; '
            '[ -n "$pid" ] && pkill -P "$pid" 2>/dev/null || true; '
            "rm -f %s; fi'" % (container_id, pid_file, pid_file, pid_file)
        ),
        check_ec=False,
    )
    time.sleep(3)


def read_ganesha_delegation_tailf_capture(
    nfs_node: CephNode, container_id: str, capture_path: Optional[str] = None
) -> List[str]:
    capture = capture_path or GANESHA_DELEGATION_TAILF_CAPTURE
    raw, _ = nfs_node.exec_command(
        sudo=True,
        cmd="podman exec %s cat %s" % (container_id, capture),
        check_ec=False,
    )
    if not raw:
        return []
    return [ln for ln in raw.splitlines() if ln.strip()]


def clamp_delegation_log_settle_seconds(
    config: Mapping[str, Any],
    default=DELEGATION_LOG_SETTLE_FALLBACK_DEFAULT,
    lo=DELEGATION_LOG_SETTLE_SECONDS_CLAMP_LO,
    hi=DELEGATION_LOG_SETTLE_SECONDS_CLAMP_HI,
):
    """
    Return delegation_log_settle_seconds from *config*, clamped to [lo, hi].

    Fallback default (82) is the midpoint of the clamp range when the suite omits
    an explicit value; tier1 YAML usually sets 90 (~one NFSv4 lease) for capture.
    """
    settle = int(config.get("delegation_log_settle_seconds", default))
    return max(lo, min(hi, settle))


def create_delegation_exports(
    cmd_host: CephNode,
    fs_name: str,
    nfs_name: str,
    subvolume_group: str,
    exports: Iterable[ExportSpec],
) -> List[str]:
    """Create subvolumes + exports for (pseudo, delegation_mode) pairs; return subvolume names."""
    run_cephadm_shell(
        cmd_host,
        "ceph fs subvolumegroup create %s %s" % (fs_name, subvolume_group),
        check_ec=False,
    )
    subvol_names: List[str] = []
    ts = int(time.time())
    for pseudo, delegation_mode in exports:
        sv = "sv_%s_%s" % (pseudo.strip("/").replace("-", "_"), ts)
        subvol_names.append(sv)
        run_cephadm_shell(
            cmd_host,
            "ceph fs subvolume create %s %s %s" % (fs_name, sv, subvolume_group),
        )
        out, _ = run_cephadm_shell(
            cmd_host,
            "ceph fs subvolume getpath %s %s --group_name %s"
            % (fs_name, sv, subvolume_group),
        )
        path = (out or "").strip()
        if not path:
            raise OperationFailedError("Empty subvolume path for %s" % sv)
        create_or_replace_export_with_delegation(
            cmd_host, fs_name, nfs_name, pseudo, path, delegation_mode
        )
    return subvol_names


def teardown_delegation_exports(
    cmd_host, fs_name, nfs_name, subvolume_group, export_pseudos, subvol_names
):
    for pseudo in export_pseudos:
        run_cephadm_shell(
            cmd_host,
            "ceph nfs export delete %s %s" % (nfs_name, pseudo),
            check_ec=False,
        )
    for sv in reversed(subvol_names):
        run_cephadm_shell(
            cmd_host,
            "ceph fs subvolume rm %s %s --group_name %s"
            % (fs_name, sv, subvolume_group),
            check_ec=False,
        )
    run_cephadm_shell(
        cmd_host,
        "ceph fs subvolumegroup rm %s %s --force" % (fs_name, subvolume_group),
        check_ec=False,
    )


def enable_cluster_delegation_and_debug_logging(
    nfs_cmd_host, cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
):
    """Enable cluster Delegations=true and Ganesha FULL_DEBUG logging; returns template backups."""
    delegation_template_backup = backup_ganesha_template(nfs_cmd_host)
    log.info("Enabling cluster-level Delegations=true")
    set_cluster_delegation(nfs_cmd_host, "true")
    redeploy_nfs_clusters(
        cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
    )
    log.info("Enabling Ganesha FULL_DEBUG file logging to /var/log/ganesha.log")
    logging_template_backup = enable_ganesha_debug_logging(nfs_cmd_host)
    redeploy_nfs_clusters(
        cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
    )
    return delegation_template_backup, logging_template_backup


# Backward-compatible aliases for older imports.
start_ganesha_deleg_tailf_follow = start_ganesha_delegation_tailf_follow
stop_ganesha_deleg_tailf_follow = stop_ganesha_delegation_tailf_follow
read_ganesha_deleg_tailf_capture = read_ganesha_delegation_tailf_capture
GANESHA_DELEG_TAILF_GREP_E = GANESHA_DELEGATION_TAILF_GREP_E
GANESHA_DELEG_TAILF_CAPTURE = GANESHA_DELEGATION_TAILF_CAPTURE
GANESHA_DELEG_TAILF_PID = GANESHA_DELEGATION_TAILF_PID
