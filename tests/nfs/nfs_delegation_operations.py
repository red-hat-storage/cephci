"""
Shared NFSv4 delegation test infrastructure.

Cluster/export template (config-key), cephadm shell helpers, Ganesha container log I/O.
Scenario-specific validation and multi-client recall logic live in the test modules.

"""

from __future__ import annotations

import json
import os
import re
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)

from looseversion import LooseVersion
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
RE_DELEGATION_REVOKING = re.compile(r"Revoking", re.I)
RE_DELEGATION_DELEGRETURN = re.compile(r"DELEGRETURN|OP_DELEGRETURN", re.I)
RE_NFS4_CLOSE_HANDLER = re.compile(r"CLOSE handler", re.I)
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

DELEGATION_MIN_UPSTREAM_BUILD = "20.2.1"
DELEGATION_MIN_RHCS_BUILD = "9.1"


def delegation_supported_for_rhbuild(rhbuild: Optional[str]) -> bool:
    """
    Return True when *rhbuild* (--rhbuild / config rhbuild) is RHCS 9.1+.

    ``run.py`` sets ``config['rhbuild']`` to ``{rhbuild}-{platform}`` (e.g.
    ``9.1-rhel-9``). Values with major version >= 10 are treated as upstream Ceph
    and compared against ``DELEGATION_MIN_UPSTREAM_BUILD``.
    """
    if not rhbuild:
        return False
    token = str(rhbuild).split("-")[0]
    if not token:
        return False
    major_str = token.split(".")[0]
    if not major_str.isdigit():
        return False
    major = int(major_str)
    lv = LooseVersion(token)
    if major < 10:
        return lv >= LooseVersion(DELEGATION_MIN_RHCS_BUILD)
    return lv >= LooseVersion(DELEGATION_MIN_UPSTREAM_BUILD)


def skip_delegation_tests_unless_supported(config: Optional[ConfigDict] = None) -> bool:
    """
    Return True when NFSv4 delegation tests should be skipped.

    Uses only ``config['rhbuild']`` (from cephci ``--rhbuild``). Callers should
    ``return 0`` immediately when this is True.
    """
    config = config or {}
    rhbuild = config.get("rhbuild")
    if delegation_supported_for_rhbuild(rhbuild):
        return False
    log.info(
        "Skipping NFSv4 delegation test: requires --rhbuild >= %s (rhbuild=%s)",
        DELEGATION_MIN_RHCS_BUILD,
        rhbuild,
    )
    return True


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
# After client A returns a delegation, before peer conflict IO (revoke scenarios).
DELEGATION_RETURN_WAIT_SECONDS_DEFAULT = 15
# After scenario IO: wait for recall/grant lines in the filtered ganesha.log capture.
DELEGATION_LOG_SETTLE_SECONDS_DEFAULT = 90
# Poll attempts when waiting for a seeded file to appear on an NFS mount.
DELEGATION_PATH_WAIT_RETRIES_DEFAULT = 40

# Loose stress capture: grant-ish and recall/return-ish line patterns.
RE_DELEGATION_STRESS_GRANT_ACTIVITY = re.compile(
    r"Attempting to grant delegation|delegation_type|do_delegation", re.I
)
RE_DELEGATION_STRESS_RECALL_ACTIVITY = re.compile(
    r"Recalling|successfully recalled|DELEGRETURN|OP_DELEGRETURN|Revoking", re.I
)


class DelegationTiming(NamedTuple):
    """Unified delegation timing values parsed from a suite config mapping."""

    exit_wait_seconds: int
    hold_seconds: int
    hold_ready_seconds: int
    return_wait_seconds: int
    log_settle_seconds: int
    path_wait_retries: int


def delegation_timing_from_config(config: Mapping[str, Any]) -> DelegationTiming:
    """
    Parse common delegation timing keys from a cephci suite ``config`` mapping.

    Keys: ``delegation_exit_wait_seconds``, ``delegation_hold_seconds``,
    ``delegation_hold_ready_seconds``, ``delegation_return_wait_seconds``,
    ``delegation_log_settle_seconds``, ``path_wait_retries``.
    """
    cfg = config or {}
    timing = DelegationTiming(
        exit_wait_seconds=int(
            cfg.get(
                "delegation_exit_wait_seconds", DELEGATION_EXIT_WAIT_SECONDS_DEFAULT
            )
        ),
        hold_seconds=int(
            cfg.get("delegation_hold_seconds", DELEGATION_HOLD_SECONDS_DEFAULT)
        ),
        hold_ready_seconds=int(
            cfg.get(
                "delegation_hold_ready_seconds",
                DELEGATION_HOLD_READY_SECONDS_DEFAULT,
            )
        ),
        return_wait_seconds=int(
            cfg.get(
                "delegation_return_wait_seconds",
                DELEGATION_RETURN_WAIT_SECONDS_DEFAULT,
            )
        ),
        log_settle_seconds=int(
            cfg.get(
                "delegation_log_settle_seconds", DELEGATION_LOG_SETTLE_SECONDS_DEFAULT
            )
        ),
        path_wait_retries=int(
            cfg.get("path_wait_retries", DELEGATION_PATH_WAIT_RETRIES_DEFAULT)
        ),
    )
    log.info(
        "delegation timing: exit_wait=%ss hold=%ss ready=%ss return_wait=%ss "
        "settle=%ss path_retries=%s",
        timing.exit_wait_seconds,
        timing.hold_seconds,
        timing.hold_ready_seconds,
        timing.return_wait_seconds,
        timing.log_settle_seconds,
        timing.path_wait_retries,
    )
    return timing


def q_delegation_shell(s: str) -> str:
    """Quote *s* for embedding in remote ``bash -c '...'`` commands."""
    return s.replace("'", "'\"'\"'")


def write_delegation_file(
    client: CephNode, path: str, content: str, append: bool = False
) -> None:
    """
    Write *content* to *path* on *client* using remote python3.

    Avoids shell ``echo`` interpolation of user-controlled text.
    """
    mode = "a" if append else "w"
    client.exec_command(
        sudo=True,
        cmd=(
            "python3 <<'PY'\n"
            "path = %r\n"
            "content = %r\n"
            "with open(path, '%s') as f:\n"
            "    f.write(content)\n"
            "import os\n"
            "os.sync()\n"
            "PY" % (path, content, mode)
        ),
    )
    log.info(
        "wrote delegation file %s on %s (append=%s, bytes=%d)",
        path,
        getattr(client, "hostname", client),
        append,
        len(content),
    )


def delegation_path_exists(client: CephNode, path: str) -> bool:
    """Return True if *path* exists as a regular file on *client*."""
    client.exec_command(sudo=True, cmd="test -f %s" % path, check_ec=False)
    return int(getattr(client, "exit_status", 1)) == 0


def wait_for_delegation_path(
    client: CephNode, path: str, label: str, retries: Optional[int] = None
) -> None:
    """
    Poll until *path* is visible on *client*.

    Raises:
        OperationFailedError: if *path* is not visible after *retries* attempts.
    """
    if retries is None:
        retries = DELEGATION_PATH_WAIT_RETRIES_DEFAULT
    parent = path.rsplit("/", 1)[0] or "/"
    for n in range(1, int(retries) + 1):
        client.exec_command(sudo=True, cmd="ls -la %s" % parent, check_ec=False)
        if delegation_path_exists(client, path):
            log.info("%s: %s visible (attempt %d)", label, path, n)
            return
        time.sleep(1)
    raise OperationFailedError(
        "%s: %s not visible on %s" % (label, path, client.hostname)
    )


def validate_delegation_stress_loose_capture(hits: Iterable[str]) -> None:
    """
    Validate post-IO capture contains delegation grant and recall/return markers.

    Used by stress/scale tests when ``strict_log_validation`` is false.
    """
    text = "\n".join(hits or [])
    if not text.strip():
        raise OperationFailedError("empty ganesha tail capture after stress IO")
    if not RE_DELEGATION_STRESS_GRANT_ACTIVITY.search(text):
        raise OperationFailedError(
            "no delegation grant activity in ganesha capture after IO"
        )
    if not RE_DELEGATION_STRESS_RECALL_ACTIVITY.search(text):
        raise OperationFailedError(
            "no recall/return activity in ganesha capture after IO"
        )
    log.info("stress capture: grant and recall/return activity present")


def validate_delegation_stress_strict_capture(
    hits: Iterable[str], hold_mode: str = "write"
) -> None:
    """
    Strict stress validation: write/read grant plus completed recall in capture.

    Used when suite config sets ``strict_log_validation: true``.
    """
    hit_list = list(hits or [])
    validate_delegation_grant_capture("delegation_stress", hit_list, hold_mode)
    validate_delegation_recall_ganesha_capture("delegation_stress", hit_list, True)
    log.info("stress capture: strict grant and recall validation passed")


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


def _hits_after_last_delegreturn(hits):
    """Return capture lines following the last DELEGRETURN/OP_DELEGRETURN marker."""
    last_idx = -1
    for idx, ln in enumerate(hits or []):
        if RE_DELEGATION_DELEGRETURN.search(ln):
            last_idx = idx
    if last_idx < 0:
        return hits or []
    return (hits or [])[last_idx + 1 :]


def count_nfs4_open_end_markers(hits):
    """Count ``END OF nfs4_op_open`` lines in a log capture."""
    return sum(1 for ln in (hits or []) if NFS4_OP_OPEN_END_MARKER in ln)


def validate_delegation_grant_capture(scenario_name, hits, hold_mode="write"):
    """Assert a read/write delegation was granted in *hits*."""
    if not hits:
        raise OperationFailedError("Scenario %s: empty tail -f capture" % scenario_name)
    text = "\n".join(hits)
    if not RE_DELEGATION_ATTEMPT_GRANT.search(text):
        raise OperationFailedError(
            "Scenario %s: missing 'Attempting to grant delegation'" % scenario_name
        )
    expected = 5 if hold_mode == "write" else 4
    types = parse_nfs4_open_delegation_types(text)
    if expected not in types:
        raise OperationFailedError(
            "Scenario %s: expected delegation_type %s, parsed %r"
            % (scenario_name, expected, types)
        )


def log_delegation_open_types(scenario_name, hits) -> List[int]:
    """Log delegation_type values from nfs4_op_open END lines; return parsed types."""
    types = parse_nfs4_open_delegation_types("\n".join(hits or []))
    log.info(
        "Scenario %s: nfs4_op_open delegation_type values %r",
        scenario_name,
        types,
    )
    return types


def capture_delegation_ganesha_log_phase(
    nfs_node: CephNode,
    container_id: str,
    settle_seconds: int,
    phase_action: Callable[[], None],
) -> List[str]:
    """Truncate ganesha.log, run *phase_action*, capture delegation lines for one phase."""
    truncate_ganesha_container_log(nfs_node, container_id)
    start_ganesha_delegation_tailf_follow(nfs_node, container_id)
    phase_action()
    time.sleep(int(settle_seconds))
    stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
    return read_ganesha_delegation_tailf_capture(nfs_node, container_id)


def hold_new_file_write_open(client: CephNode, path: str, seconds: int) -> None:
    """Create *path* with mode ``w`` and keep it OPEN in the background."""
    safe = q_delegation_shell(path)
    pidfile = _delegation_hold_pidfile(client)
    client.exec_command(
        sudo=True,
        cmd=(
            "nohup python3 -c "
            "\"f=open('%s','w'); f.write('new-file\\\\n'); import time; time.sleep(%s)\" "
            ">/dev/null 2>&1 & echo $! >> %s" % (safe, int(seconds), pidfile)
        ),
        check_ec=False,
    )
    log.info(
        "Holding new-file write OPEN on %s for %ss from %s",
        path,
        int(seconds),
        getattr(client, "hostname", client),
    )


def validate_delegation_open_close_cycles_capture(
    scenario_name, hits, open_close_cycles, hold_mode="write"
):
    """Delegation held; extra OPEN RPCs stay bounded during local open/close cycles."""
    validate_delegation_grant_capture(scenario_name, hits, hold_mode)
    opens = count_nfs4_open_end_markers(hits)
    max_allowed = int(open_close_cycles) + 2
    if opens > max_allowed:
        raise OperationFailedError(
            "Scenario %s: saw %d nfs4_op_open END lines (expected <= %d for %d local cycles)"
            % (scenario_name, opens, max_allowed, open_close_cycles)
        )


def validate_delegation_rw_io_recall_capture(scenario_name, hits, hold_mode="write"):
    """Delegated IO on client A, recall from client B, grant on delegated path."""
    validate_delegation_recall_ganesha_capture(scenario_name, hits, True)
    validate_delegation_grant_capture(scenario_name, hits, hold_mode)


def validate_delegation_lock_capture(scenario_name, hits, hold_mode="write"):
    """Delegation grant with locks; optional recall if peer IO follows lock conflict."""
    validate_delegation_grant_capture(scenario_name, hits, hold_mode)
    text = "\n".join(hits or [])
    if RE_DELEGATION_RECALLING.search(text):
        validate_delegation_recall_ganesha_capture(scenario_name, hits, True)


def validate_delegation_setattr_capture(scenario_name, hits, hold_mode="write"):
    """Delegation grant during setattr/truncate metadata operations."""
    validate_delegation_grant_capture(scenario_name, hits, hold_mode)


def validate_delegation_close_and_return_capture(
    scenario_name: str, hits: Iterable[str], *, require_close: bool = False
) -> None:
    """Assert *hits* contain DELEGRETURN after client close/unmount/shutdown."""
    hit_list = list(hits or [])
    if not hit_list:
        raise OperationFailedError("Scenario %s: empty tail -f capture" % scenario_name)
    text = "\n".join(hit_list)
    if not RE_DELEGATION_DELEGRETURN.search(text):
        raise OperationFailedError(
            "Scenario %s: missing DELEGRETURN/OP_DELEGRETURN in capture" % scenario_name
        )
    if require_close and not RE_NFS4_CLOSE_HANDLER.search(text):
        raise OperationFailedError(
            "Scenario %s: missing CLOSE handler before DELEGRETURN" % scenario_name
        )
    log.info(
        "Scenario %s: delegation return observed (DELEGRETURN%s)",
        scenario_name,
        ", CLOSE handler" if RE_NFS4_CLOSE_HANDLER.search(text) else "",
    )


def validate_delegation_revoke_ganesha_capture(
    scenario_name, hits, expect_revoke, require_delegreturn=True
):
    """Validate revoke path in tail -f capture."""
    if not hits:
        raise OperationFailedError("Scenario %s: empty tail -f capture" % scenario_name)
    text = "\n".join(hits)
    if expect_revoke:
        if not RE_DELEGATION_RECALLING.search(text):
            raise OperationFailedError(
                "Scenario %s: missing 'Recalling' in capture (revoke path)"
                % scenario_name
            )
        if not (
            RE_DELEGATION_REVOKING.search(text) or RE_DELEGATION_RECALLED.search(text)
        ):
            raise OperationFailedError(
                "Scenario %s: missing 'Revoking' or 'successfully recalled' "
                "(delegation not revoked after unresponsive client)" % scenario_name
            )
    else:
        if require_delegreturn and not RE_DELEGATION_DELEGRETURN.search(text):
            raise OperationFailedError(
                "Scenario %s: missing DELEGRETURN before peer IO" % scenario_name
            )
        peer_hits = (
            _hits_after_last_delegreturn(hits) if require_delegreturn else (hits or [])
        )
        peer_text = "\n".join(peer_hits)
        if (
            RE_DELEGATION_RECALLING.search(peer_text)
            or RE_DELEGATION_REVOKING.search(peer_text)
            or RE_DELEGATION_RECALLED.search(peer_text)
        ):
            raise OperationFailedError(
                "Scenario %s: unexpected recall/revoke after DELEGRETURN "
                "(delegation should have been returned before conflict)" % scenario_name
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


def lazy_unmount_delegation_export(client: CephNode, mount_point: str) -> None:
    """Lazy-unmount an NFS export while local opens may still be active."""
    client.exec_command(sudo=True, cmd="umount -l %s" % mount_point, check_ec=False)
    log.info(
        "Lazy-unmounted %s on %s",
        mount_point,
        getattr(client, "hostname", client),
    )


def _delegation_hold_pidfile(client: CephNode) -> str:
    host = str(getattr(client, "hostname", "client")).replace("/", "_")
    return _delegation_tmp_path("hold_%s.pids" % host)


def hold_delegation_open(client: CephNode, path: str, mode: str, seconds: int) -> None:
    """Keep a file OPEN in the background; record its PID for kill_delegation_holds()."""
    safe = q_delegation_shell(path)
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


def release_delegation_hold_gracefully(
    client: CephNode, term_wait_seconds: int = 5
) -> None:
    """SIGTERM hold OPEN processes so NFS CLOSE/DELEGRETURN can complete."""
    pidfile = _delegation_hold_pidfile(client)
    client.exec_command(
        sudo=True,
        cmd=(
            "if [ -f %s ]; then while read -r pid; do "
            '[ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null || true; '
            "done < %s; fi"
        )
        % (pidfile, pidfile),
        check_ec=False,
    )
    time.sleep(int(term_wait_seconds))
    kill_delegation_holds(client)


def wait_for_delegreturn_in_ganesha_capture(
    nfs_node, container_id, timeout_seconds: int = 45
) -> bool:
    """Poll filtered tail capture until DELEGRETURN appears or *timeout_seconds* elapses."""
    deadline = time.time() + int(timeout_seconds)
    while time.time() < deadline:
        hits = read_ganesha_delegation_tailf_capture(nfs_node, container_id)
        if RE_DELEGATION_DELEGRETURN.search("\n".join(hits or [])):
            return True
        time.sleep(2)
    return False


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


def wait_for_nfs_cluster_daemons_running(
    cephadm,
    nfs_clusters: Sequence[str],
    timeout_seconds: int = 300,
    interval: int = 5,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Poll `ceph orch ps` until every nfs daemon is running with a container_id.

    `verify_nfs_ganesha_service` uses aggregate `orch ls` counts, which can
    report success while individual daemons are still in `starting` state.
    """
    deadline = time.time() + int(timeout_seconds)
    results: Dict[str, List[Dict[str, Any]]] = {}
    while time.time() < deadline:
        all_ready = True
        results = {}
        for cluster_name in nfs_clusters:
            service = "nfs.%s" % cluster_name
            raw = cephadm.orch.ps(service_name=service, format="json")
            daemons = json.loads(raw) if raw else []
            results[cluster_name] = daemons
            if not daemons:
                all_ready = False
                log.info("Waiting for %s: orch ps returned no daemons", service)
                break
            pending = [
                d
                for d in daemons
                if str(d.get("status_desc", "")).strip().lower() != "running"
                or not d.get("container_id")
            ]
            if pending:
                all_ready = False
                for daemon in pending:
                    log.info(
                        "Waiting for %s daemon on %s: status=%s container_id=%s",
                        service,
                        daemon.get("hostname"),
                        daemon.get("status_desc"),
                        daemon.get("container_id") or "none",
                    )
        if all_ready:
            log.info(
                "All NFS daemons running with container ids for: %s",
                ", ".join(nfs_clusters),
            )
            return results
        time.sleep(interval)
    raise OperationFailedError(
        "Timed out after %ss waiting for NFS daemons to reach running state "
        "with container ids for %s" % (timeout_seconds, ", ".join(nfs_clusters))
    )


def redeploy_nfs_clusters(
    cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
):
    for cluster_name in nfs_clusters:
        cephadm.orch.redeploy("nfs.%s" % cluster_name)
    time.sleep(redeploy_wait)
    verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)
    wait_for_nfs_cluster_daemons_running(
        cephadm, nfs_clusters, timeout_seconds=service_wait_timeout
    )


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


def _nfs_node_for_daemon(
    nfs_nodes: Sequence[CephNode], daemon: Mapping[str, Any]
) -> Optional[CephNode]:
    """Return the CephNode matching an orch ps NFS daemon entry."""
    daemon_short = (daemon.get("hostname") or "").split(".")[0]
    if not daemon_short:
        return None
    by_short = {node.hostname.split(".")[0]: node for node in nfs_nodes}
    node = by_short.get(daemon_short)
    if node:
        return node
    for short, candidate in by_short.items():
        if short == daemon_short or short in daemon_short or daemon_short in short:
            return candidate
    return None


def _nfs_nodes_with_daemons(
    nfs_nodes: Sequence[CephNode], daemons: Sequence[Mapping[str, Any]]
) -> List[CephNode]:
    """Return nfs nodes that host at least one orch ps daemon (deduplicated)."""
    matched: List[CephNode] = []
    seen = set()
    for daemon in daemons:
        node = _nfs_node_for_daemon(nfs_nodes, daemon)
        if not node:
            continue
        short = node.hostname.split(".")[0]
        if short in seen:
            continue
        seen.add(short)
        matched.append(node)
    return matched


def verify_container_delegation(nfs_nodes, daemons, nfs_name, delegation):
    if not daemons:
        raise OperationFailedError(
            "No nfs daemons in orch ps for nfs.%s container verify" % nfs_name
        )
    active_nodes = _nfs_nodes_with_daemons(nfs_nodes, daemons)
    for daemon in daemons:
        node = _nfs_node_for_daemon(nfs_nodes, daemon)
        if not node:
            raise OperationFailedError(
                "No nfs node in test inventory for orch daemon %s (nfs.%s)"
                % (daemon.get("hostname"), nfs_name)
            )
        cid = daemon.get("container_id")
        if not cid:
            status = daemon.get("status_desc", "missing")
            raise OperationFailedError(
                "No running nfs daemon container on %s for nfs.%s "
                "(status=%s; wait for redeploy to finish)"
                % (node.hostname, nfs_name, status)
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
    inventory_only = [
        node.hostname
        for node in nfs_nodes
        if node.hostname.split(".")[0]
        not in {n.hostname.split(".")[0] for n in active_nodes}
    ]
    if inventory_only:
        log.info(
            "Skipping container delegation verify on nfs nodes without orch daemons: %s",
            ", ".join(inventory_only),
        )


def verify_host_conf_delegation(nfs_nodes, daemons, nfs_name, delegation):
    nodes = _nfs_nodes_with_daemons(nfs_nodes, daemons)
    if not nodes:
        raise OperationFailedError(
            "No nfs nodes matched orch ps daemons for nfs.%s host conf verify"
            % nfs_name
        )
    for node in nodes:
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


def verify_cluster_delegation_on_nfs_nodes(
    cephadm,
    nfs_nodes,
    nfs_clusters,
    delegation: str,
    timeout_seconds: int = 120,
    poll_interval: int = 5,
) -> None:
    """
    Poll ``ceph orch ps`` and verify Delegations in container and host ganesha.conf.

    After redeploy, per-daemon container ids can lag behind aggregate service-ready
    checks; retry until every NFS node is inspectable or *timeout_seconds* elapses.
    """
    deadline = time.time() + int(timeout_seconds)
    last_error = None
    while time.time() < deadline:
        try:
            for cluster_name in nfs_clusters:
                raw = cephadm.orch.ps(
                    service_name="nfs.%s" % cluster_name, format="json"
                )
                daemons = json.loads(raw) if raw else []
                if not daemons:
                    raise OperationFailedError(
                        "No nfs daemons found in orch ps for nfs.%s" % cluster_name
                    )
                verify_container_delegation(
                    nfs_nodes, daemons, cluster_name, delegation
                )
                verify_host_conf_delegation(
                    nfs_nodes, daemons, cluster_name, delegation
                )
            return
        except OperationFailedError as err:
            last_error = err
            log.info(
                "Cluster delegation verify not ready, retrying in %ss: %s",
                poll_interval,
                err,
            )
            time.sleep(int(poll_interval))
    raise last_error or OperationFailedError(
        "Timed out after %ss verifying cluster delegation on nfs nodes"
        % timeout_seconds
    )


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
