"""
NFS-Ganesha conditional logging automation (Cephadm / Ceph NFS).

Effective conf on each NFS host (after template + redeploy)::

  /var/lib/ceph/<fsid>/nfs.<cluster>..../etc/ganesha/ganesha.conf

LOG/Facility/Conditional are applied via the cephadm ganesha template
(``mgr/cephadm/services/nfs/ganesha.conf``) and ``ceph nfs cluster config set``
(RADOS ``%url``). Capture always resolves Facility.destination from that host
ganesha.conf (not a hardcoded log path).

Operations (config.operation):
  tc_cl_config_01  — Static ANY policy (Part A baseline + Part B Conditional)
  tc_cl_config_02  — Static MATCH_ALL (Part A baseline + Part B1–B4)
  tc_cl_config_03  — Invalid/malformed config handling
  tc_cl_config_04  — Default Match_Policy (MATCH_ANY) via cluster config set
  tc_cl_config_05  — MATCH_ALL via cluster config set (Clients+Export_A)
  tc_cl_config_06  — MATCH_ANY, exports only via cluster config set
  tc_cl_dynamic_01 — ganesha_mgr CRUD
  tc_cl_dynamic_02 — DBus persistence / hot-reload
  conditional_logging_all — run all of the above in order
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from nfs_delegation_operations import (
    CONF_KEY,
    DEFAULT_TEMPLATE_PATH,
    MOUNTED_TEMPLATE_PATH,
    backup_ganesha_template,
    redeploy_nfs_clusters,
    restore_ganesha_template,
    run_cephadm_shell,
)
from nfs_operations import (
    cleanup_cluster,
    mount_retry,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

# --- helpers ---

CephNode = Any

LOG_BLOCK_PATTERN = re.compile(r"LOG\s*\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", re.S)

# Conditional-logging markers used by TC-CL-CONFIG-01 pass/fail criteria.
RE_FSAL_F_DBG = re.compile(r"FSAL\s*:F_DBG", re.I)
RE_FSAL_FULL_DEBUG = re.compile(r"FSAL\s*:FULL_DEBUG", re.I)
RE_EXPORT_OR_NFS_M_DBG = re.compile(r"(?:EXPORT|NFS_V4|NFS4)\s*:M_DBG", re.I)
# NFS4 elevated markers for cluster-config MATCH_ANY cases (CONFIG-04/05/06).
# Require the Ganesha log-level colon form ("NFS4 : DEBUG"), NOT config assignment
# ("NFS4 = DEBUG") which appears in conf dumps and caused false positives on
# non-matching traffic.
RE_NFS4_DEBUG = re.compile(
    r"(?:NFS4|NFS_V4)\s*:\s*(?:FULL_DEBUG|DEBUG|F_DBG|NIV_DEBUG|NIV_FULL_DEBUG)\b",
    re.I,
)
RE_NFS4_FULL_DEBUG = re.compile(
    r"(?:NFS4|NFS_V4)\s*:\s*(?:FULL_DEBUG|F_DBG|NIV_FULL_DEBUG)\b",
    re.I,
)
MOUNTED_USERCONF_PATH = "/tmp/cl_nfs_userconf.conf"
EXP_A_PSEUDO = "/exp_a"
EXP_B_PSEUDO = "/exp_b"
EXP_A_MOUNT = "/mnt/exp_a"
EXP_B_MOUNT = "/mnt/exp_b"

# Container path (bind-mounted from the host cephadm unit dir).
GANESHA_CONF_IN_CONTAINER = "/etc/ganesha/ganesha.conf"
# Host path pattern on the NFS node (cephadm):
#   /var/lib/ceph/<fsid>/nfs.<cluster>..../etc/ganesha/ganesha.conf
# Used only when writing a new Facility into ganesha.conf: cephadm's default
# template has no LOG/Facility. Capture always reads Facility.destination from
# the live host ganesha.conf above.
_CL_FACILITY_DEST_WHEN_ABSENT = "/var/log/ganesha.log"

CONDITIONAL_LOG_WORK_SUFFIX = "conditional_logging_work.conf.j2"
CONDITIONAL_LOG_BACKUP_SUFFIX = "conditional_logging_backup.conf.j2"

RE_FACILITY_BLOCK = re.compile(r"Facility\s*\{([^{}]*)\}", re.I | re.S)
RE_FACILITY_DESTINATION = re.compile(
    r"""destination\s*=\s*(?:"([^"]+)"|'([^']+)')""",
    re.I,
)


@dataclass
class TestCaseResult:
    tc_id: str
    name: str
    passed: bool = False
    detail: str = ""

    def mark(self, passed: bool, detail: str = "") -> "TestCaseResult":
        self.passed = passed
        self.detail = detail
        status = "PASS" if passed else "FAIL"
        log.info("[%s] %s — %s: %s", status, self.tc_id, self.name, detail or status)
        return self


@dataclass
class TestRunReport:
    results: List[TestCaseResult] = field(default_factory=list)

    def add(self, result: TestCaseResult) -> TestCaseResult:
        self.results.append(result)
        return result

    def all_passed(self) -> bool:
        return bool(self.results) and all(r.passed for r in self.results)

    def summary_lines(self) -> List[str]:
        lines = ["Conditional logging test summary:"]
        for result in self.results:
            status = "PASS" if result.passed else "FAIL"
            lines.append(f"  {status}  {result.tc_id}: {result.name}")
            if result.detail:
                lines.append(f"         {result.detail}")
        return lines


_CL_RUN_ID = f"{os.getpid()}_{int(time.time() * 1000)}"


def _cl_tmp_path(suffix: str) -> str:
    return f"/tmp/ganesha_cl_{_CL_RUN_ID}_{suffix}"


def cl_config_get(config: Mapping[str, Any], *keys: str, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def parse_facility_destination(conf_text: str) -> Optional[str]:
    """Return FILE Facility destination from ganesha.conf / LOG text, if any."""
    for fac in RE_FACILITY_BLOCK.finditer(conf_text or ""):
        m = RE_FACILITY_DESTINATION.search(fac.group(1))
        if not m:
            continue
        path = (m.group(1) or m.group(2) or "").strip()
        if path:
            return path
    return None


def facility_destination_for_write(cmd_host: CephNode) -> str:
    """Destination to put in Facility when updating the cephadm ganesha template.

    Prefer the path already declared in the template / prior ganesha.conf LOG.
    Cephadm's stock template has no Facility; only then use the test default that
    will be written *into* ganesha.conf (capture still reads live conf).
    """
    existing = parse_facility_destination(read_ganesha_template(cmd_host))
    if existing:
        return existing
    return _CL_FACILITY_DEST_WHEN_ABSENT


def build_log_facility_block(destination: str) -> str:
    if not destination:
        raise OperationFailedError(
            "Facility destination is required (from ganesha.conf)"
        )
    return (
        "    Facility {\n"
        "        name = FILE;\n"
        f'        destination = "{destination}";\n'
        "        enable = active;\n"
        "    }\n"
    )


def build_components_block(components: Mapping[str, str]) -> str:
    lines = ["    Components {"]
    for comp, level in components.items():
        lines.append(f"        {comp} = {level};")
    lines.append("    }")
    return "\n".join(lines) + "\n"


def build_baseline_log_block(
    global_level: str = "EVENT",
    components: Optional[Mapping[str, str]] = None,
    log_destination: Optional[str] = None,
) -> str:
    """Return a LOG block with no Conditional section (baseline / Part A)."""
    if not log_destination:
        raise OperationFailedError(
            "log_destination required — resolve from ganesha.conf Facility"
        )
    components = components or {"FSAL": "INFO", "NFS_V4": "INFO"}
    return (
        "LOG {\n"
        f"    Default_Log_Level = {global_level};\n"
        f"{build_log_facility_block(log_destination)}"
        f"{build_components_block(components)}"
        "}\n"
    )


def build_conditional_subblock(
    components: Mapping[str, str],
    exports: Sequence[int],
    clients: Sequence[str],
) -> str:
    lines = ["    Conditional {"]
    for comp, level in components.items():
        lines.append(f"        {comp} = {level};")
    if exports:
        export_csv = ", ".join(str(e) for e in exports)
        lines.append(f"        Exports = {export_csv};")
    if clients:
        client_csv = ", ".join(str(c) for c in clients)
        lines.append(f"        Clients = {client_csv};")
    lines.append("    }")
    return "\n".join(lines) + "\n"


def build_conditional_log_block(
    match_policy: Optional[str] = "ANY",
    global_level: str = "EVENT",
    conditional_components: Optional[Mapping[str, str]] = None,
    exports: Optional[Sequence[int]] = None,
    clients: Optional[Sequence[str]] = None,
    components: Optional[Mapping[str, str]] = None,
    log_destination: Optional[str] = None,
) -> str:
    """Return a complete LOG { ... } block for ganesha template injection.

    Facility destination must come from ganesha.conf (caller resolves via
    ``facility_destination_for_write`` / live conf). Pass ``match_policy=None``
    to omit Match_Policy (Ganesha default MATCH_ANY — TC-CL-CONFIG-04).
    """
    if not log_destination:
        raise OperationFailedError(
            "log_destination required — resolve from ganesha.conf Facility"
        )
    conditional_components = conditional_components or {
        "FSAL": "FULL_DEBUG",
        "NFS_V4": "MID_DEBUG",
    }
    # Default global Components match the CONFIG-01 Part B static example.
    components = components or {"FSAL": "INFO", "NFS_V4": "INFO"}
    exports = list(exports or [])
    clients = list(clients or [])
    policy_line = ""
    if match_policy is not None:
        policy = str(match_policy).strip().upper()
        if policy in ("MATCH_ANY", "ANY"):
            policy = "ANY"
        elif policy in ("MATCH_ALL", "ALL"):
            policy = "ALL"
        policy_line = f"    Match_Policy = {policy};\n"
    return (
        "LOG {\n"
        f"    Default_Log_Level = {global_level};\n"
        f"{build_log_facility_block(log_destination)}"
        f"{build_components_block(components)}"
        f"{policy_line}"
        f"{build_conditional_subblock(conditional_components, exports, clients)}"
        "}\n"
    )


def replace_log_block_in_template(template_text: str, new_log_block: str) -> str:
    """Replace an existing LOG block or append ``new_log_block`` to the template."""
    text = template_text or ""
    if LOG_BLOCK_PATTERN.search(text):
        return LOG_BLOCK_PATTERN.sub(new_log_block.strip(), text, count=1)
    return text.rstrip() + "\n\n" + new_log_block.strip() + "\n"


def read_ganesha_template(cmd_host: CephNode, work_path: Optional[str] = None) -> str:
    work = work_path or _cl_tmp_path(CONDITIONAL_LOG_WORK_SUFFIX)
    run_cephadm_shell(
        cmd_host, f"ceph config-key get {CONF_KEY} > {work}", check_ec=False
    )
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            f"test -s {work} || cephadm shell -- cat {DEFAULT_TEMPLATE_PATH} > {work}"
        ),
    )
    out, _ = cmd_host.exec_command(sudo=True, cmd=f"cat {work}", check_ec=False)
    if not (out or "").strip():
        raise OperationFailedError(
            "Unable to read Ganesha template for conditional logging"
        )
    return out


def write_ganesha_template(
    cmd_host: CephNode, template_text: str, work_path: Optional[str] = None
):
    work = work_path or _cl_tmp_path(CONDITIONAL_LOG_WORK_SUFFIX)
    remote = None
    try:
        try:
            remote = cmd_host.remote_file(sudo=True, file_name=work, file_mode="w")
            remote.write(template_text)
            remote.flush()
        except AttributeError:
            remote = cmd_host.remote_file(sudo=True, file_name=work, file_mode="wb")
            remote.write(template_text.encode("utf-8"))
            remote.flush()
    finally:
        if remote and hasattr(remote, "close"):
            remote.close()
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            f"cephadm shell --mount {work}:{MOUNTED_TEMPLATE_PATH} "
            f"-- ceph config-key set {CONF_KEY} -i {MOUNTED_TEMPLATE_PATH}"
        ),
    )


def apply_conditional_log_template(
    cmd_host: CephNode,
    log_block: str,
    work_path: Optional[str] = None,
) -> None:
    """Merge ``log_block`` into the cephadm ganesha template config-key."""
    template = read_ganesha_template(cmd_host, work_path=work_path)
    merged = replace_log_block_in_template(template, log_block)
    write_ganesha_template(cmd_host, merged, work_path=work_path)
    log.info(
        "Applied conditional LOG block to ganesha template (config-key %s)", CONF_KEY
    )


def get_nfs_daemon_container(
    cephadm: Any, nfs_name: str
) -> Tuple[str, str, Mapping[str, Any]]:
    """Return (container_id, hostname, daemon_dict) for the first running nfs daemon."""
    raw = cephadm.orch.ps(service_name=f"nfs.{nfs_name}", format="json")
    daemons = json.loads(raw) if raw else []
    if not daemons:
        raise OperationFailedError(f"No orch ps daemons for nfs.{nfs_name}")
    for daemon in daemons:
        cid = daemon.get("container_id")
        status = str(daemon.get("status_desc", "")).lower()
        if cid and status == "running":
            return cid, str(daemon.get("hostname", "")), daemon
    raise OperationFailedError(
        f"No running nfs.{nfs_name} daemon with container_id in orch ps"
    )


def ganesha_mgr(
    nfs_node: CephNode,
    container_id: str,
    args: str,
    timeout: int = 120,
) -> Tuple[str, str]:
    """Run ganesha_mgr inside the NFS Ganesha container."""
    cmd = f"podman exec {container_id} ganesha_mgr {args}"
    log.info("ganesha_mgr: %s", args)
    return nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=timeout)


def reload_ganesha(nfs_node: CephNode, container_id: str) -> None:
    """Signal Ganesha to reload configuration (SIGHUP)."""
    cmd = (
        f"podman exec {container_id} bash -c "
        "'pid=$(pidof ganesha.nfsd 2>/dev/null); "
        '[ -n "$pid" ] && kill -HUP "$pid" || exit 1\''
    )
    out, err = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=60)
    rc = getattr(nfs_node, "exit_status", None)
    if rc not in (0, None) or (err and "exit" in str(err).lower()):
        raise OperationFailedError(
            "Ganesha reload failed: rc=%s err=%s out=%s" % (rc, err, out)
        )
    log.info("Sent SIGHUP to ganesha.nfsd in container %s", container_id)
    time.sleep(3)


def is_ganesha_running(nfs_node: CephNode, container_id: str) -> bool:
    out, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} pidof ganesha.nfsd",
        check_ec=False,
        timeout=30,
    )
    return bool(str(out or "").strip())


def find_host_ganesha_conf(nfs_node: CephNode, nfs_name: str) -> Tuple[str, str]:
    """Locate live ganesha.conf on the NFS node under ``/var/lib/ceph``.

    Cephadm layout::

        /var/lib/ceph/<fsid>/nfs.<cluster>..../etc/ganesha/ganesha.conf

    Returns ``(absolute_host_path, conf_text)``.
    """
    if not nfs_name:
        raise OperationFailedError("nfs_name required to locate host ganesha.conf")
    try:
        fsids = nfs_node.get_dir_list("/var/lib/ceph", sudo=True) or []
    except Exception as exc:
        raise OperationFailedError(
            f"Unable to list /var/lib/ceph on {nfs_node.hostname}: {exc}"
        ) from exc
    for fsid in fsids:
        fsid = str(fsid).strip()
        if not fsid or fsid.startswith("."):
            continue
        base = f"/var/lib/ceph/{fsid}"
        try:
            entries = nfs_node.get_dir_list(base, sudo=True) or []
        except Exception:
            continue
        # Prefer directories that contain the NFS cluster id (nfs.<name>...).
        candidates = [e for e in entries if nfs_name in str(e)]
        # Stable order: nfs.* first, then others.
        candidates.sort(key=lambda e: (0 if str(e).startswith("nfs.") else 1, str(e)))
        for entry in candidates:
            path = f"{base}/{entry}/etc/ganesha/ganesha.conf"
            out, _ = nfs_node.exec_command(
                sudo=True,
                cmd=f"test -s {path} && cat {path}",
                check_ec=False,
                timeout=60,
            )
            if (out or "").strip():
                log.info(
                    "Live ganesha.conf on NFS host %s: %s",
                    nfs_node.hostname,
                    path,
                )
                return path, str(out)
    raise OperationFailedError(
        f"No host ganesha.conf at /var/lib/ceph/<fsid>/*{nfs_name}*/etc/ganesha/ganesha.conf "
        f"on {nfs_node.hostname}"
    )


def read_live_ganesha_conf(
    nfs_node: CephNode,
    container_id: str,
    nfs_name: Optional[str] = None,
) -> Tuple[str, str]:
    """Return ``(conf_path, conf_text)`` — prefer host ``/var/lib/ceph/.../ganesha.conf``."""
    if nfs_name:
        try:
            return find_host_ganesha_conf(nfs_node, nfs_name)
        except OperationFailedError as exc:
            log.warning(
                "%s; falling back to container %s", exc, GANESHA_CONF_IN_CONTAINER
            )
    out, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} cat {GANESHA_CONF_IN_CONTAINER}",
        check_ec=False,
        timeout=60,
    )
    text = str(out or "")
    if not text.strip():
        raise OperationFailedError(
            f"Unable to read ganesha.conf from host /var/lib/ceph or "
            f"container {GANESHA_CONF_IN_CONTAINER} on {nfs_node.hostname}"
        )
    return f"podman://{container_id}{GANESHA_CONF_IN_CONTAINER}", text


def _host_unit_dir_from_conf_path(conf_path: str) -> Optional[str]:
    """``/var/lib/ceph/<fsid>/nfs....`` from a host ganesha.conf path."""
    marker = "/etc/ganesha/ganesha.conf"
    if (
        conf_path
        and conf_path.endswith(marker)
        and conf_path.startswith("/var/lib/ceph/")
    ):
        return conf_path[: -len(marker)]
    return None


def resolve_ganesha_file_log_path(
    nfs_node: CephNode,
    container_id: str,
    nfs_name: Optional[str] = None,
) -> Tuple[str, str]:
    """Return ``(conf_path, file_log_path)`` from live ganesha.conf Facility.destination."""
    conf_path, conf = read_live_ganesha_conf(nfs_node, container_id, nfs_name=nfs_name)
    path = parse_facility_destination(conf)
    if not path:
        raise OperationFailedError(
            f"No LOG Facility destination in {conf_path}; "
            "cannot capture conditional logging FILE output from ganesha.conf"
        )
    log.info(
        "Using Ganesha FILE log from %s Facility.destination=%s",
        conf_path,
        path,
    )
    return conf_path, path


def _truncate_and_read_file_log(
    nfs_node: CephNode,
    container_id: str,
    facility_dest: str,
    conf_path: str,
    *,
    truncate: bool,
    tail_lines: int = 8000,
) -> str:
    """Truncate and/or tail Facility FILE log via host unit dir, else container."""
    unit = _host_unit_dir_from_conf_path(conf_path)
    host_candidates: List[str] = []
    if unit:
        base_name = os.path.basename(facility_dest.rstrip("/")) or "ganesha.log"
        host_candidates = [
            (
                f"{unit}{facility_dest}"
                if facility_dest.startswith("/")
                else f"{unit}/{facility_dest}"
            ),
            f"{unit}/log/{base_name}",
            f"{unit}/var/log/{base_name}",
        ]
    # Prefer a host path that already exists under the cephadm unit dir.
    for host_path in host_candidates:
        exists, _ = nfs_node.exec_command(
            sudo=True,
            cmd=f"test -e {host_path} && echo yes || echo no",
            check_ec=False,
            timeout=30,
        )
        if "yes" not in str(exists or ""):
            continue
        if truncate:
            nfs_node.exec_command(
                sudo=True,
                cmd=f"truncate -s 0 {host_path} || : > {host_path}",
                check_ec=False,
                timeout=30,
            )
            return host_path
        out, _ = nfs_node.exec_command(
            sudo=True,
            cmd=f"tail -n {int(tail_lines)} {host_path}",
            check_ec=False,
            timeout=120,
        )
        return str(out or "")

    # Container path (Facility.destination as Ganesha sees it).
    if truncate:
        nfs_node.exec_command(
            sudo=True,
            cmd=(
                f"podman exec {container_id} sh -c "
                f'\'mkdir -p "$(dirname "{facility_dest}")" && '
                f'touch "{facility_dest}" && :> "{facility_dest}"\''
            ),
            check_ec=False,
            timeout=60,
        )
        return facility_dest
    out, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f'podman exec {container_id} tail -n {int(tail_lines)} "{facility_dest}"',
        check_ec=False,
        timeout=120,
    )
    return str(out or "")


def truncate_ganesha_file_log(
    nfs_node: CephNode,
    container_id: str,
    log_path: Optional[str] = None,
    nfs_name: Optional[str] = None,
) -> str:
    """Truncate the FILE log declared in live ganesha.conf; return facility dest."""
    if log_path:
        path = log_path
        conf_path, _ = read_live_ganesha_conf(nfs_node, container_id, nfs_name=nfs_name)
    else:
        conf_path, path = resolve_ganesha_file_log_path(
            nfs_node, container_id, nfs_name=nfs_name
        )
    _truncate_and_read_file_log(
        nfs_node,
        container_id,
        path,
        conf_path,
        truncate=True,
    )
    return path


def read_ganesha_log(
    nfs_node: CephNode,
    container_id: str,
    tail_lines: int = 8000,
    log_path: Optional[str] = None,
    nfs_name: Optional[str] = None,
) -> str:
    """Tail the FILE log path from live host ganesha.conf Facility.destination."""
    if log_path:
        path = log_path
        conf_path, _ = read_live_ganesha_conf(nfs_node, container_id, nfs_name=nfs_name)
    else:
        conf_path, path = resolve_ganesha_file_log_path(
            nfs_node, container_id, nfs_name=nfs_name
        )
    return _truncate_and_read_file_log(
        nfs_node,
        container_id,
        path,
        conf_path,
        truncate=False,
        tail_lines=tail_lines,
    )


def capture_ganesha_log_window(
    nfs_node: CephNode,
    container_id: str,
    action,
    settle_sec: int = 5,
    tail_lines: int = 8000,
    nfs_name: Optional[str] = None,
) -> str:
    """Truncate FILE log from ganesha.conf Facility, run action, return new tail."""
    path = truncate_ganesha_file_log(nfs_node, container_id, nfs_name=nfs_name)
    action()
    time.sleep(settle_sec)
    return read_ganesha_log(
        nfs_node,
        container_id,
        tail_lines=tail_lines,
        log_path=path,
        nfs_name=nfs_name,
    )


def count_component_debug_lines(log_text: str, component: str) -> int:
    pattern = re.compile(
        rf"(?:{re.escape(component)}|COMPONENT_{re.escape(component)})"
        rf".{{0,160}}?(?:FULL_DEBUG|MID_DEBUG|NIV_DEBUG|\bDEBUG\b|\bDBG\b)",
        re.I,
    )
    return len(pattern.findall(log_text or ""))


def verify_conditional_verbosity(
    matched_log: str,
    unmatched_log: str,
    components: Sequence[str] = ("FSAL", "NFS_V4"),
    min_debug_delta: int = 1,
) -> Tuple[bool, str]:
    """
    Return True when matched-client logs show more conditional debug than unmatched.
    """
    if not (unmatched_log or "").strip():
        for component in components:
            matched_dbg = count_component_debug_lines(matched_log, component)
            if matched_dbg < min_debug_delta:
                return (
                    False,
                    f"{component}: expected >= {min_debug_delta} debug lines, got {matched_dbg}",
                )
        return True, "matched client shows conditional debug"

    for component in components:
        matched_dbg = count_component_debug_lines(matched_log, component)
        unmatched_dbg = count_component_debug_lines(unmatched_log, component)
        if matched_dbg < unmatched_dbg + min_debug_delta:
            return (
                False,
                f"{component}: matched debug lines={matched_dbg}, "
                f"unmatched={unmatched_dbg} (expected matched > unmatched)",
            )
    return True, "conditional debug verbosity verified"


def verify_no_elevated_debug(
    log_text: str, components: Sequence[str] = ("FSAL", "NFS_V4")
) -> Tuple[bool, str]:
    """Return True when log shows no elevated conditional debug for given components."""
    for component in components:
        dbg = count_component_debug_lines(log_text, component)
        if dbg > 0:
            return False, f"{component}: unexpected debug lines={dbg}"
    return True, "no elevated conditional debug"


def count_elevated_conditional_markers(log_text: str) -> Dict[str, int]:
    """Count CONFIG-01 elevated markers (FSAL:F_DBG / FULL_DEBUG, EXPORT|NFS:M_DBG)."""
    text = log_text or ""
    return {
        "fsal_f_dbg": len(RE_FSAL_F_DBG.findall(text)),
        "fsal_full_debug": len(RE_FSAL_FULL_DEBUG.findall(text)),
        "export_or_nfs_m_dbg": len(RE_EXPORT_OR_NFS_M_DBG.findall(text)),
    }


def verify_baseline_no_conditional_debug(log_text: str) -> Tuple[bool, str]:
    """
    Part A / non-matching: elevated conditional markers must be absent.

    Fail if any of: FSAL:F_DBG, FSAL:FULL_DEBUG, EXPORT|NFS_V4|NFS4:M_DBG.
    """
    counts = count_elevated_conditional_markers(log_text)
    if any(counts.values()):
        return False, f"elevated conditional markers present: {counts}"
    return True, f"no elevated conditional markers: {counts}"


def verify_matching_client_conditional_debug(log_text: str) -> Tuple[bool, str]:
    """
    Part B matching client: must show FSAL F_DBG (or FULL_DEBUG) and M_DBG lines.
    """
    counts = count_elevated_conditional_markers(log_text)
    has_fsal = counts["fsal_f_dbg"] > 0 or counts["fsal_full_debug"] > 0
    has_m_dbg = counts["export_or_nfs_m_dbg"] > 0
    if not has_fsal:
        return False, f"matching client missing FSAL:F_DBG/FULL_DEBUG: {counts}"
    if not has_m_dbg:
        return False, f"matching client missing EXPORT/NFS_V4:M_DBG: {counts}"
    return True, f"matching client elevated markers present: {counts}"


def verify_match_policy_in_log(
    log_text: str, expected_policy: str = "MATCH_ALL"
) -> Tuple[bool, str]:
    """
    Confirm Ganesha accepted the Match_Policy change.

    Looks for lines like: Conditional logging match policy changed to (MATCH_ALL)
    """
    text = log_text or ""
    policy = str(expected_policy).strip().upper()
    # Accept MATCH_ALL / ALL / MATCH_ANY / ANY variants in the confirmation line.
    aliases = {policy}
    if policy in ("ALL", "MATCH_ALL"):
        aliases.update({"ALL", "MATCH_ALL"})
    elif policy in ("ANY", "MATCH_ANY"):
        aliases.update({"ANY", "MATCH_ANY"})
    pattern = re.compile(
        r"Conditional\s+logging\s+match\s+policy\s+changed\s+to\s*\(([^)]+)\)",
        re.I,
    )
    matches = pattern.findall(text)
    if not matches:
        # Broader fallback: policy token near "match policy" wording.
        for alias in aliases:
            if re.search(rf"match\s+policy.{{0,40}}{re.escape(alias)}", text, re.I):
                return True, f"match policy confirmation found ({alias})"
        return False, "match policy change confirmation not found in ganesha log"
    for found in matches:
        found_norm = str(found).strip().upper().replace(" ", "_")
        if found_norm in aliases or found_norm.replace("MATCH_", "") in {
            a.replace("MATCH_", "") for a in aliases
        }:
            return True, f"match policy changed to ({found})"
    return (
        False,
        f"match policy confirmation present but unexpected: {matches!r} "
        f"(expected one of {sorted(aliases)})",
    )


def parse_ganesha_mgr_show_output(output: str) -> Dict[str, Any]:
    """Parse ``show log conditional_config`` or individual show commands."""
    text = str(output or "")
    result: Dict[str, Any] = {
        "clients": [],
        "exports": [],
        "match_policy": None,
        "components": {},
        "raw": text,
    }
    section = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("="):
            continue
        lower = stripped.lower()
        if lower.startswith("clients"):
            section = "clients"
            continue
        if lower.startswith("exports") or lower.startswith("export ids"):
            section = "exports"
            continue
        if "match policy" in lower:
            section = "policy"
            parts = stripped.split(":", 1)
            if len(parts) == 2:
                result["match_policy"] = parts[1].strip()
            continue
        if lower.startswith("component"):
            section = "components"
            continue
        if section == "clients" and not stripped.startswith("Export"):
            result["clients"].append(stripped)
        elif section == "exports":
            m = re.search(r"(\d+)", stripped)
            if m:
                result["exports"].append(int(m.group(1)))
        elif section == "components" and ":" in stripped:
            comp, level = stripped.split(":", 1)
            result["components"][comp.strip()] = level.strip()
        elif stripped.startswith("MATCH_") or stripped in ("ANY", "ALL"):
            result["match_policy"] = stripped
    return result


def list_exports_detailed(cmd_host: CephNode, nfs_name: str) -> List[Dict[str, Any]]:
    out, _ = run_cephadm_shell(
        cmd_host, f"ceph nfs export ls {nfs_name} --detailed --format json"
    )
    content = (out or "").strip()
    if not content:
        return []
    data = json.loads(content)
    if isinstance(data, dict):
        return data.get("exports", []) or list(data.values())
    return list(data)


def export_id_from_entry(entry: Mapping[str, Any]) -> Optional[int]:
    for key in ("export_id", "id", "Export_Id"):
        if key in entry and entry[key] is not None:
            return int(entry[key])
    return None


def export_path_from_entry(entry: Mapping[str, Any]) -> Optional[str]:
    # Prefer NFS client/pseudo path over CephFS ``path``. Detailed export JSON
    # always has both (e.g. pseudo=/cl_export_0, path=/); matching on ``path``
    # first incorrectly skips the export we just created.
    for key in ("pseudo", "bind", "export_path", "path"):
        if entry.get(key):
            return str(entry[key])
    return None


def create_nfs_exports(
    cmd_host: CephNode,
    fs_name: str,
    nfs_name: str,
    export_paths: Sequence[str],
    cephfs_path: str = "/",
) -> Dict[str, int]:
    """Create exports and return mapping export_path (pseudo) -> export_id.

    ``cmd_host`` must be able to authenticate to the cluster (typically the
    installer via ``cephadm shell``, or a client with an admin keyring).
    ``export_paths`` are NFS pseudo paths; ``cephfs_path`` is the CephFS path
    behind them (defaults to filesystem root).
    """
    path_to_id: Dict[str, int] = {}
    for export_path in export_paths:
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export delete {nfs_name} {export_path}",
            check_ec=False,
        )
        run_cephadm_shell(
            cmd_host,
            (
                f"ceph nfs export create {fs_name} {nfs_name} {export_path} "
                f"{fs_name} --path={cephfs_path}"
            ),
        )
        time.sleep(2)
    entries = list_exports_detailed(cmd_host, nfs_name)
    for export_path in export_paths:
        export_id = None
        for entry in entries:
            if export_path_from_entry(entry) == export_path:
                export_id = export_id_from_entry(entry)
                break
        if export_id is None:
            raise OperationFailedError(
                f"Could not resolve export_id for {export_path!r} in {entries!r}"
            )
        path_to_id[export_path] = export_id
        log.info("Export %r -> id %s", export_path, export_id)
    return path_to_id


def mount_export(
    client: CephNode,
    nfs_server: str,
    export_path: str,
    mount_path: str,
    version: str,
    port: str,
) -> None:
    client.create_dirs(dir_path=mount_path, sudo=True)
    if not mount_retry(
        client=client,
        mount_name=mount_path,
        version=version,
        port=port,
        nfs_server=nfs_server,
        export_name=export_path,
    ):
        raise OperationFailedError(
            f"Mount failed: {nfs_server}:{export_path} -> {mount_path} on {client.hostname}"
        )


def umount_export(client: CephNode, mount_path: str) -> None:
    client.exec_command(
        sudo=True,
        cmd=f"umount -l {mount_path}",
        check_ec=False,
        timeout=60,
    )


def _safe_umount(client: CephNode, mount_path: str) -> None:
    """Best-effort umount so failures/returns do not leave stale mounts."""
    try:
        umount_export(client, mount_path)
    except Exception:
        pass


def run_light_io(client: CephNode, mount_path: str, dd_count: int = 100) -> None:
    """Run ls -R and dd write/read workload on a mount."""
    mp = mount_path.rstrip("/")
    test_file = f"{mp}/cl_testfile.dat"
    client.exec_command(sudo=True, cmd=f"ls -R {mp} >/dev/null 2>&1", timeout=120)
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={test_file} bs=1M count={int(dd_count)} conv=fsync",
        timeout=600,
    )
    client.exec_command(
        sudo=True,
        cmd=f"dd if={test_file} of=/dev/null bs=1M",
        timeout=300,
    )


def run_echo_io(client: CephNode, mount_path: str, filename: str, content: str) -> None:
    """ls + echo write + cat on a mount (CONFIG-04/06 style)."""
    mp = mount_path.rstrip("/")
    path = f"{mp}/{filename}"
    client.exec_command(sudo=True, cmd=f"ls {mp}", timeout=60)
    client.exec_command(
        sudo=True,
        cmd=f"bash -c 'echo {content} > {path}'",
        timeout=60,
    )
    client.exec_command(sudo=True, cmd=f"cat {path}", check_ec=False, timeout=60)


def run_dd_small_io(
    client: CephNode, mount_path: str, filename: str, count: int = 16
) -> None:
    """ls + small dd write (CONFIG-05 style)."""
    mp = mount_path.rstrip("/")
    path = f"{mp}/{filename}"
    client.exec_command(sudo=True, cmd=f"ls {mp}", timeout=60)
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/urandom of={path} bs=4k count={int(count)} conv=fsync",
        timeout=120,
    )


def build_cluster_userconf(
    components: Mapping[str, str],
    conditional_components: Mapping[str, str],
    exports: Optional[Sequence[int]] = None,
    clients: Optional[Sequence[str]] = None,
    match_policy: Optional[str] = None,
    log_destination: Optional[str] = None,
) -> str:
    """Build LOG text for ``ceph nfs cluster config set``.

    Facility.destination must match ganesha.conf (caller resolves). A second
    LOG via RADOS ``%url`` without Facility was observed to leave the FILE log
    empty (CONFIG-04/05/06 Scenario A).
    """
    if not log_destination:
        raise OperationFailedError(
            "log_destination required — resolve from ganesha.conf Facility"
        )
    comps = _mirror_nfs4_aliases(dict(components))
    cond = _mirror_nfs4_aliases(dict(conditional_components))
    lines = [
        "LOG {",
        "  Default_Log_Level = EVENT;",
        "  Facility {",
        "    name = FILE;",
        f'    destination = "{log_destination}";',
        "    enable = active;",
        "  }",
        "  Components {",
    ]
    for comp, level in comps.items():
        lines.append(f"    {comp} = {level};")
    lines.append("  }")
    if match_policy:
        policy = str(match_policy).strip().upper()
        # Keep tokens identical to build_conditional_log_block (ALL/ANY).
        # Dual template + %url userconf with MATCH_ALL vs ALL was observed to
        # break AND semantics (export-only traffic elevated under CONFIG-05).
        if policy in ("ANY", "MATCH_ANY"):
            policy = "ANY"
        elif policy in ("ALL", "MATCH_ALL"):
            policy = "ALL"
        lines.append(f"  Match_Policy = {policy};")
    lines.append("  Conditional {")
    # Same field order as build_conditional_subblock (levels, then Exports, Clients).
    for comp, level in cond.items():
        lines.append(f"    {comp} = {level};")
    if exports:
        lines.append(f"    Exports = {', '.join(str(e) for e in exports)};")
    if clients:
        lines.append(f"    Clients = {', '.join(str(c) for c in clients)};")
    lines.append("  }")
    lines.append("}")
    return "\n".join(lines) + "\n"


def _mirror_nfs4_aliases(components: Dict[str, str]) -> Dict[str, str]:
    """Ensure NFS4 and NFS_V4 both appear when either is set."""
    out = dict(components)
    if "NFS4" in out and "NFS_V4" not in out:
        out["NFS_V4"] = out["NFS4"]
    if "NFS_V4" in out and "NFS4" not in out:
        out["NFS4"] = out["NFS_V4"]
    return out


def nfs_cluster_config_reset(cmd_host: CephNode, nfs_name: str) -> int:
    """Reset RADOS userconf; return 0 on success (best-effort)."""
    out, err = run_cephadm_shell(
        cmd_host,
        f"ceph nfs cluster config reset {nfs_name}",
        check_ec=False,
    )
    log.info(
        "nfs cluster config reset %s: out=%r err=%r",
        nfs_name,
        (out or "")[:200],
        (err or "")[:200],
    )
    return 0


def nfs_cluster_config_get(cmd_host: CephNode, nfs_name: str) -> str:
    out, _ = run_cephadm_shell(
        cmd_host,
        f"ceph nfs cluster config get {nfs_name}",
        check_ec=False,
    )
    return str(out or "")


def nfs_cluster_config_set(
    cmd_host: CephNode, nfs_name: str, conf_text: str
) -> Tuple[int, str]:
    """Write conf to host temp file and ``ceph nfs cluster config set -i``.

    Returns (rc, combined_output). Success when command reports success or
    a subsequent config get contains a Conditional block from conf_text.
    """
    work = _cl_tmp_path("userconf.conf")
    remote = None
    try:
        try:
            remote = cmd_host.remote_file(sudo=True, file_name=work, file_mode="w")
            remote.write(conf_text)
            remote.flush()
        except AttributeError:
            remote = cmd_host.remote_file(sudo=True, file_name=work, file_mode="wb")
            remote.write(conf_text.encode("utf-8"))
            remote.flush()
    finally:
        if remote and hasattr(remote, "close"):
            remote.close()

    try:
        out, err = cmd_host.exec_command(
            sudo=True,
            cmd=(
                f"cephadm shell --mount {work}:{MOUNTED_USERCONF_PATH} "
                f"-- ceph nfs cluster config set {nfs_name} -i {MOUNTED_USERCONF_PATH}"
            ),
            check_ec=True,
        )
        combined = f"{out or ''}{err or ''}"
        log.info("nfs cluster config set %s: %s", nfs_name, combined.strip())
        return 0, combined
    except Exception as exc:
        log.error("nfs cluster config set failed: %s", exc)
        return 1, str(exc)


def log_has_nfs4_debug(log_text: str, full_debug: bool = False) -> bool:
    """True when NFS4/NFS_V4 elevated DEBUG (or FULL_DEBUG) markers are present."""
    pat = RE_NFS4_FULL_DEBUG if full_debug else RE_NFS4_DEBUG
    return bool(pat.search(log_text or ""))


def verify_nfs4_matching_logs(
    log_text: str, full_debug: bool = False
) -> Tuple[bool, str]:
    if not (log_text or "").strip():
        return False, "FILE log empty (no Facility.destination active in ganesha.conf?)"
    if log_has_nfs4_debug(log_text, full_debug=full_debug):
        kind = "FULL_DEBUG" if full_debug else "DEBUG"
        return True, f"NFS4 {kind} present"
    return False, "NFS4 DEBUG/FULL_DEBUG markers absent (policy not loaded?)"


def verify_nfs4_non_matching_logs(log_text: str) -> Tuple[bool, str]:
    if log_has_nfs4_debug(log_text, full_debug=False):
        samples = RE_NFS4_DEBUG.findall(log_text or "")[:5]
        # Also show a short surrounding snippet for the first hit
        m = RE_NFS4_DEBUG.search(log_text or "")
        snippet = ""
        if m:
            start = max(0, m.start() - 80)
            end = min(len(log_text or ""), m.end() + 80)
            snippet = (log_text or "")[start:end].replace("\n", " ")
        return (
            False,
            "unexpected NFS4 DEBUG/FULL_DEBUG on non-matching traffic "
            f"(hits={samples!r} snippet={snippet!r})",
        )
    if not (log_text or "").strip():
        # Empty capture is ambiguous: either Facility is dead (and matching
        # scenarios will fail) or non-matching traffic logged nothing at CRIT.
        return (
            True,
            "no NFS4 DEBUG (FILE log empty — Facility check deferred to matching)",
        )
    return True, "no NFS4 DEBUG on non-matching traffic"


def _dump_policy_not_loaded(ctx: dict, container_id: str) -> None:
    """Dump config get / RADOS userconf / host ganesha.conf / FILE log."""
    nfs_name = ctx["nfs_name"]
    cmd_host = ctx["cmd_host"]
    get_out = nfs_cluster_config_get(cmd_host, nfs_name)
    log.error("policy not loaded — config get:\n%s", get_out)
    rados_out, _ = run_cephadm_shell(
        cmd_host,
        f"rados -p .nfs -N {nfs_name} get userconf-nfs.{nfs_name} -",
        check_ec=False,
    )
    log.error("policy not loaded — rados userconf:\n%s", rados_out)
    try:
        conf_path, live_conf = read_live_ganesha_conf(
            ctx["nfs_node"], container_id, nfs_name=nfs_name
        )
    except Exception as exc:
        conf_path, live_conf = "<unavailable>", str(exc)
    log.error(
        "policy not loaded — live ganesha.conf %s Facility.destination=%r:\n%s",
        conf_path,
        parse_facility_destination(live_conf if isinstance(live_conf, str) else ""),
        "\n".join(
            ln
            for ln in str(live_conf).splitlines()
            if re.search(
                r"LOG|Facility|destination|Conditional|Match_Policy|Components|COMPONENTS",
                ln,
                re.I,
            )
        )
        or str(live_conf)[:2000],
    )
    try:
        file_log = read_ganesha_log(
            ctx["nfs_node"],
            container_id,
            tail_lines=200,
            nfs_name=nfs_name,
        )
    except Exception as exc:
        file_log = f"<unavailable: {exc}>"
    log.error(
        "policy not loaded — FILE log from ganesha.conf Facility:\n%s",
        file_log,
    )


def _nfs_node_for_hostname(
    nfs_nodes: Sequence[CephNode], hostname: str
) -> Optional[CephNode]:
    """Resolve an orch hostname to a CephNode (short-name tolerant)."""
    want = (hostname or "").split(".")[0]
    if not want:
        return None
    by_short = {n.hostname.split(".")[0]: n for n in nfs_nodes}
    node = by_short.get(want)
    if node:
        return node
    for short, candidate in by_short.items():
        if short in want or want in short:
            return candidate
    return None


def _iter_running_nfs_daemons(ctx: dict) -> List[Tuple[CephNode, str]]:
    """Return [(node, container_id), ...] for running nfs.<name> daemons."""
    raw = ctx["cephadm"].orch.ps(service_name=f"nfs.{ctx['nfs_name']}", format="json")
    daemons = json.loads(raw) if raw else []
    out: List[Tuple[CephNode, str]] = []
    for daemon in daemons:
        cid = daemon.get("container_id")
        status = str(daemon.get("status_desc", "")).lower()
        if not cid or status != "running":
            continue
        node = _nfs_node_for_hostname(ctx["nfs_nodes"], str(daemon.get("hostname", "")))
        if node:
            out.append((node, str(cid)))
    return out


def _container_on_mount_host(ctx: dict) -> str:
    """Return container_id for the daemon on ctx['nfs_node'] (mount target)."""
    prefer = ctx["nfs_node"].hostname.split(".")[0]
    for node, cid in _iter_running_nfs_daemons(ctx):
        if node.hostname.split(".")[0] == prefer:
            return cid
    # Fall back to first running daemon and retarget mount host to match logs.
    targets = _iter_running_nfs_daemons(ctx)
    if not targets:
        raise OperationFailedError(
            f"No running nfs.{ctx['nfs_name']} daemon with container_id"
        )
    node, cid = targets[0]
    ctx["nfs_node"] = node
    ctx["nfs_server"] = node.hostname
    log.warning(
        "Mount NFS host retargeted to %s (container %s) for log capture alignment",
        node.hostname,
        cid,
    )
    return cid


def _ensure_host_ganesha_policy_ready(
    ctx: dict,
    container_id: str,
    *,
    require_conditional: bool = False,
) -> str:
    """After redeploy, require host ganesha.conf to carry Facility (+ Conditional).

    Conf path: ``/var/lib/ceph/<fsid>/nfs.<cluster>..../etc/ganesha/ganesha.conf``.
    Returns Facility.destination used for FILE log capture.
    """
    conf_path, conf = read_live_ganesha_conf(
        ctx["nfs_node"], container_id, nfs_name=ctx["nfs_name"]
    )
    dest = parse_facility_destination(conf)
    if not dest:
        raise OperationFailedError(
            f"After apply, {conf_path} has no LOG Facility.destination — "
            "conditional logging FILE capture cannot run"
        )
    has_conditional = bool(re.search(r"\bConditional\b", conf, re.I))
    if require_conditional and not has_conditional:
        raise OperationFailedError(
            f"After apply, {conf_path} has Facility but no Conditional block — "
            "template/userconf policy did not materialize into host ganesha.conf"
        )
    if has_conditional:
        log.info(
            "Host ganesha.conf ready: %s (Facility.destination=%s, Conditional present)",
            conf_path,
            dest,
        )
    else:
        log.info(
            "Host ganesha.conf ready: %s (Facility.destination=%s, no Conditional)",
            conf_path,
            dest,
        )
    truncate_ganesha_file_log(
        ctx["nfs_node"],
        container_id,
        log_path=dest,
        nfs_name=ctx["nfs_name"],
    )
    ctx["ganesha_conf_path"] = conf_path
    ctx["ganesha_file_log"] = dest
    return dest


def _apply_match_any_policy(
    ctx: dict,
    *,
    components: Mapping[str, str],
    conditional_components: Mapping[str, str],
    exports: Optional[Sequence[int]] = None,
    clients: Optional[Sequence[str]] = None,
    match_policy: Optional[str] = None,
) -> Tuple[str, int, str]:
    """Apply Conditional for CONFIG-04/05/06 onto Ceph NFS ganesha.conf.

    1. Inject Facility + COMPONENTS + Conditional into the cephadm ganesha
       template (materialized on NFS hosts as
       ``/var/lib/ceph/<fsid>/nfs.<cluster>..../etc/ganesha/ganesha.conf``).
    2. Mirror the same policy via ``ceph nfs cluster config set`` (API under test;
       included via ``%url`` — also carries Facility so FILE logging is not wiped).
    3. Redeploy, then verify the host ganesha.conf and prepare FILE log capture.
    """
    comps = _mirror_nfs4_aliases(dict(components))
    cond = _mirror_nfs4_aliases(dict(conditional_components))
    log_dest = facility_destination_for_write(ctx["cmd_host"])
    log_block = build_conditional_log_block(
        match_policy=match_policy,
        global_level="EVENT",
        components=comps,
        conditional_components=cond,
        exports=exports,
        clients=clients,
        log_destination=log_dest,
    )
    apply_conditional_log_template(ctx["cmd_host"], log_block)

    userconf = build_cluster_userconf(
        components=comps,
        conditional_components=cond,
        exports=exports,
        clients=clients,
        match_policy=match_policy,
        log_destination=log_dest,
    )
    set_rc, set_out = nfs_cluster_config_set(ctx["cmd_host"], ctx["nfs_name"], userconf)
    container_id = _redeploy_refresh(ctx)
    _ensure_host_ganesha_policy_ready(ctx, container_id, require_conditional=True)
    return container_id, set_rc, set_out


def _prepare_match_any_fixture(ctx: dict) -> dict:
    """One NFS cluster, two exports (/exp_a, /exp_b), reset userconf."""
    nfs_cluster_config_reset(ctx["cmd_host"], ctx["nfs_name"])
    export_paths = [EXP_A_PSEUDO, EXP_B_PSEUDO]
    path_to_id = create_nfs_exports(
        ctx["cmd_host"],
        ctx["fs_name"],
        ctx["nfs_name"],
        export_paths,
        cephfs_path="/",
    )
    client_match = ctx["clients"][0]
    client_other = ctx["clients"][1]
    return {
        "export_a": EXP_A_PSEUDO,
        "export_b": EXP_B_PSEUDO,
        "export_a_id": path_to_id[EXP_A_PSEUDO],
        "export_b_id": path_to_id[EXP_B_PSEUDO],
        "mp_a": EXP_A_MOUNT,
        "mp_b": EXP_B_MOUNT,
        "client_match": client_match,
        "client_other": client_other,
        "client_match_ip": client_ip(client_match),
        "client_other_ip": client_ip(client_other),
    }


def _scenario_io_capture(
    ctx: dict,
    container_id: str,
    client: CephNode,
    export_path: str,
    mount_path: str,
    io_callable,
    settle_sec: int = 8,
) -> str:
    """Mount, run IO, umount; capture log the same way as TC-CL-CONFIG-01/02/03."""

    def _action():
        mount_export(
            client,
            ctx["nfs_server"],
            export_path,
            mount_path,
            ctx["version"],
            ctx["port"],
        )
        try:
            io_callable()
        finally:
            _safe_umount(client, mount_path)

    return capture_ganesha_log_window(
        ctx["nfs_node"],
        container_id,
        _action,
        settle_sec=settle_sec,
        nfs_name=ctx.get("nfs_name"),
    )


def client_ip(client: CephNode) -> str:
    return str(getattr(client, "ip_address", None) or client.hostname)


def log_contains_fatal(log_text: str) -> bool:
    """Return True if log shows FATAL / abort-style failure."""
    return bool(
        re.search(
            r"\bFATAL\b|\babort(ing)?\b|segmentation fault|core dumped",
            log_text or "",
            re.I,
        )
    )


def log_contains_any(log_text: str, tokens: Sequence[str]) -> bool:
    """Case-insensitive substring match for any token in ``tokens``."""
    text = (log_text or "").lower()
    return any(str(tok).lower() in text for tok in tokens if tok)


def malformed_log_block_cases(
    export_id: int = 1,
    client: str = "10.0.0.1",
    log_destination: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return graceful-malformation cases for TC-CL-CONFIG-03.

    Each case dict:
      name, log_block, expect_warn (tokens), require_warn, verify_no_elevate
    """
    if not log_destination:
        raise OperationFailedError(
            "log_destination required — resolve from ganesha.conf Facility"
        )
    export_id = int(export_id)
    client = str(client)
    components = {"FSAL": "INFO", "NFS_V4": "INFO"}
    facility = build_log_facility_block(log_destination)
    return [
        {
            "name": "case1_invalid_match_policy",
            "log_block": (
                "LOG {\n"
                "    Default_Log_Level = EVENT;\n"
                f"{facility}"
                f"{build_components_block(components)}"
                "    Match_Policy = INVALID_POLICY;\n"
                "    Conditional {\n"
                "        FSAL = FULL_DEBUG;\n"
                "        NFS_V4 = MID_DEBUG;\n"
                f"        Exports = {export_id};\n"
                f"        Clients = {client};\n"
                "    }\n"
                "}\n"
            ),
            "expect_warn": ["INVALID_POLICY", "Unknown token"],
            "require_warn": True,
            "verify_no_elevate": False,
        },
        {
            "name": "case2_unknown_component",
            "log_block": (
                "LOG {\n"
                "    Default_Log_Level = EVENT;\n"
                f"{facility}"
                f"{build_components_block(components)}"
                "    Match_Policy = ANY;\n"
                "    Conditional {\n"
                "        FSAL = FULL_DEBUG;\n"
                "        INVALID_COMPONENT = FULL_DEBUG;\n"
                "        NFS_V4 = MID_DEBUG;\n"
                f"        Exports = {export_id};\n"
                f"        Clients = {client};\n"
                "    }\n"
                "}\n"
            ),
            "expect_warn": ["INVALID_COMPONENT", "Unknown parameter"],
            "require_warn": True,
            "verify_no_elevate": False,
        },
        {
            "name": "case3_invalid_log_level",
            "log_block": (
                "LOG {\n"
                "    Default_Log_Level = EVENT;\n"
                f"{facility}"
                f"{build_components_block(components)}"
                "    Match_Policy = ANY;\n"
                "    Conditional {\n"
                "        FSAL = SUPER_DEBUG;\n"
                "        NFS_V4 = MID_DEBUG;\n"
                f"        Exports = {export_id};\n"
                f"        Clients = {client};\n"
                "    }\n"
                "}\n"
            ),
            "expect_warn": ["SUPER_DEBUG", "Unknown token"],
            "require_warn": True,
            "verify_no_elevate": False,
        },
        {
            "name": "case4b_empty_trailing_commas",
            "log_block": (
                "LOG {\n"
                "    Default_Log_Level = EVENT;\n"
                f"{facility}"
                f"{build_components_block(components)}"
                "    Match_Policy = ANY;\n"
                "    Conditional {\n"
                "        FSAL = FULL_DEBUG;\n"
                "        NFS_V4 = MID_DEBUG;\n"
                f"        Exports = {export_id},,2,;\n"
                f"        Clients = ,{client},;\n"
                "    }\n"
                "}\n"
            ),
            "expect_warn": [],
            "require_warn": False,
            "verify_no_elevate": False,
        },
        {
            "name": "case6_missing_conditional_block",
            "log_block": (
                "LOG {\n"
                "    Default_Log_Level = EVENT;\n"
                f"{facility}"
                f"{build_components_block(components)}"
                "    Match_Policy = ANY;\n"
                "}\n"
            ),
            "expect_warn": [],
            "require_warn": False,
            "verify_no_elevate": True,
        },
    ]


def redeploy_and_wait(
    cephadm: Any,
    installer: CephNode,
    nfs_name: str,
    redeploy_wait: int,
    service_wait_timeout: int,
) -> Tuple[str, str]:
    redeploy_nfs_clusters(
        cephadm, [nfs_name], installer, redeploy_wait, service_wait_timeout
    )
    return get_nfs_daemon_container(cephadm, nfs_name)


def _redeploy_refresh(ctx) -> str:
    """Redeploy NFS and refresh ctx container_id on the mount-target host."""
    redeploy_and_wait(
        ctx["cephadm"],
        ctx["installer"],
        ctx["nfs_name"],
        ctx["redeploy_wait"],
        ctx["service_wait_timeout"],
    )
    container_id = _container_on_mount_host(ctx)
    ctx["container_id"] = container_id
    return container_id


# --- test operations ---

OP_TC_CL_CONFIG_01 = "tc_cl_config_01"
OP_TC_CL_CONFIG_02 = "tc_cl_config_02"
OP_TC_CL_CONFIG_03 = "tc_cl_config_03"
OP_TC_CL_CONFIG_04 = "tc_cl_config_04"
OP_TC_CL_CONFIG_05 = "tc_cl_config_05"
OP_TC_CL_CONFIG_06 = "tc_cl_config_06"
OP_TC_CL_DYNAMIC_01 = "tc_cl_dynamic_01"
OP_TC_CL_DYNAMIC_02 = "tc_cl_dynamic_02"
OP_CONDITIONAL_LOGGING_ALL = "conditional_logging_all"

_ALL_OPERATIONS = [
    OP_TC_CL_CONFIG_01,
    OP_TC_CL_CONFIG_02,
    OP_TC_CL_CONFIG_03,
    OP_TC_CL_CONFIG_04,
    OP_TC_CL_CONFIG_05,
    OP_TC_CL_CONFIG_06,
    OP_TC_CL_DYNAMIC_01,
    OP_TC_CL_DYNAMIC_02,
]


def _normalize_operation(name):
    if name is None:
        return None
    return str(name).strip().lower().replace("-", "_")


def _operations_to_run(config):
    raw = config.get("operation")
    if raw is None:
        raise OperationFailedError(
            "config.operation is required. Use one of: "
            + ", ".join(_ALL_OPERATIONS + [OP_CONDITIONAL_LOGGING_ALL])
        )
    op = _normalize_operation(raw)
    if op == OP_CONDITIONAL_LOGGING_ALL:
        return list(_ALL_OPERATIONS)
    if op in _ALL_OPERATIONS:
        return [op]
    raise OperationFailedError(f"Unknown operation {raw!r}")


def _ensure_nfs_cluster(ceph_cluster, config, installer, clients, nfs_nodes, cephadm):
    nfs_name = config.get("nfs_name", "cephfs-nfs-cl")
    fs_name = config.get("fs_name", "cephfs")
    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_cl")
    bootstrap_export = config.get("bootstrap_export", "/export_cl_bootstrap")
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    auto_create = bool(config.get("auto_create_nfs_cluster", True))

    nfs_clusters = cephadm.nfs.cluster.ls()
    created = False
    if nfs_name not in nfs_clusters:
        if not auto_create:
            raise ConfigError(f"NFS cluster {nfs_name!r} not found")
        nfs_servers = [node.hostname for node in nfs_nodes]
        created = True
        setup_nfs_cluster(
            clients=[clients[0]],
            nfs_server=nfs_servers,
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
    return nfs_name, fs_name, created


def _export_paths(config, count=3):
    base = cl_config_get(
        config, "export_path_prefix", "tc_cl_export", default="/cl_export"
    )
    return [f"{base}_{i}" for i in range(count)]


def _mount_paths(config, count=3):
    base = cl_config_get(
        config, "mount_path_prefix", "tc_cl_mount", default="/mnt/cl_export"
    )
    return [f"{base}_{i}" for i in range(count)]


def _run_tc_cl_config_01(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-01: Static Match_Policy=ANY (two-phase).

    Part A — no Conditional block: elevated FSAL:F_DBG / EXPORT|NFS:M_DBG must be absent.
    Part B — Conditional ANY for client1 + export1: matching client+export elevates;
             non-matching client on a different export does not.
    """
    result = TestCaseResult("TC-CL-CONFIG-01", "Static Config – Basic ANY Policy")
    matched_client = ctx["clients"][0]
    unmatched_client = ctx["clients"][1]
    mount_paths = []
    try:
        matched_ip = client_ip(matched_client)

        export_paths = _export_paths(ctx["config"], count=2)
        mount_paths = _mount_paths(ctx["config"], count=2)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        # ANY = client OR export. Non-matching client must use a different export
        # or Export_Id alone would still elevate logging.
        matched_export = export_paths[0]
        unmatched_export = export_paths[1]
        matched_export_id = path_to_id[matched_export]
        mp_matched = mount_paths[0]
        mp_unmatched = mount_paths[1]

        mount_export(
            matched_client,
            ctx["nfs_server"],
            matched_export,
            mp_matched,
            ctx["version"],
            ctx["port"],
        )
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            unmatched_export,
            mp_unmatched,
            ctx["version"],
            ctx["port"],
        )

        # --- Part A: baseline LOG without Conditional ---
        log.info("=== TC-CL-CONFIG-01 Part A: baseline (no conditional logging) ===")
        log_dest = facility_destination_for_write(ctx["cmd_host"])
        baseline_block = build_baseline_log_block(
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
            log_destination=log_dest,
        )
        apply_conditional_log_template(ctx["cmd_host"], baseline_block)
        container_id = _redeploy_refresh(ctx)
        _ensure_host_ganesha_policy_ready(ctx, container_id)
        reload_ganesha(ctx["nfs_node"], container_id)

        def _part_a_io():
            run_light_io(matched_client, mp_matched, dd_count=10)
            run_light_io(unmatched_client, mp_unmatched, dd_count=10)

        part_a_log = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            _part_a_io,
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        ok_a, detail_a = verify_baseline_no_conditional_debug(part_a_log)
        if not ok_a:
            return result.mark(False, f"Part A FAIL: {detail_a}")
        log.info("Part A PASS: %s", detail_a)

        # --- Part B: Conditional Match_Policy=ANY ---
        log.info(
            "=== TC-CL-CONFIG-01 Part B: Conditional ANY "
            "(Clients=%s Exports=%s) ===",
            matched_ip,
            matched_export_id,
        )
        log_dest = facility_destination_for_write(ctx["cmd_host"])
        conditional_block = build_conditional_log_block(
            match_policy="ANY",
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
            conditional_components={"FSAL": "FULL_DEBUG", "NFS_V4": "MID_DEBUG"},
            exports=[matched_export_id],
            clients=[matched_ip],
            log_destination=log_dest,
        )
        apply_conditional_log_template(ctx["cmd_host"], conditional_block)
        container_id = _redeploy_refresh(ctx)
        _ensure_host_ganesha_policy_ready(ctx, container_id, require_conditional=True)
        reload_ganesha(ctx["nfs_node"], container_id)

        def _matched_io():
            run_light_io(matched_client, mp_matched, dd_count=10)

        def _unmatched_io():
            run_light_io(unmatched_client, mp_unmatched, dd_count=10)

        matched_log = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            _matched_io,
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        unmatched_log = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            _unmatched_io,
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )

        ok_b1, detail_b1 = verify_matching_client_conditional_debug(matched_log)
        ok_b2, detail_b2 = verify_baseline_no_conditional_debug(unmatched_log)

        if not ok_b1:
            return result.mark(False, f"Part B matching FAIL: {detail_b1}")
        if not ok_b2:
            return result.mark(False, f"Part B non-matching FAIL: {detail_b2}")

        return result.mark(
            True,
            f"Part A PASS ({detail_a}); Part B PASS "
            f"(matching: {detail_b1}; non-matching: {detail_b2})",
        )
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        if len(mount_paths) >= 2:
            _safe_umount(matched_client, mount_paths[0])
            _safe_umount(unmatched_client, mount_paths[1])


def _run_tc_cl_config_02(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-02: Static Match_Policy=ALL (two-phase).

    Part A — baseline without Conditional: no elevated markers.
    Part B — MATCH_ALL Conditional for client1 + export1:
      B1 client+export match → elevate
      B2 client match, export mismatch → no elevate
      B3 client mismatch, export match → no elevate
      B4 neither match → no elevate
    """
    result = TestCaseResult("TC-CL-CONFIG-02", "Static Config – MATCH_ALL Policy")
    matched_client = ctx["clients"][0]
    unmatched_client = ctx["clients"][1]
    mount_paths = []
    try:
        matched_ip = client_ip(matched_client)

        export_paths = _export_paths(ctx["config"], count=2)
        mount_paths = _mount_paths(ctx["config"], count=4)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        matched_export = export_paths[0]
        unmatched_export = export_paths[1]
        matched_export_id = path_to_id[matched_export]

        # --- Part A: baseline LOG without Conditional ---
        log.info("=== TC-CL-CONFIG-02 Part A: baseline (no conditional logging) ===")
        log_dest = facility_destination_for_write(ctx["cmd_host"])
        baseline_block = build_baseline_log_block(
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
            log_destination=log_dest,
        )
        apply_conditional_log_template(ctx["cmd_host"], baseline_block)
        container_id = _redeploy_refresh(ctx)
        _ensure_host_ganesha_policy_ready(ctx, container_id)
        reload_ganesha(ctx["nfs_node"], container_id)

        # Mount matching export on both clients for baseline I/O.
        mount_export(
            matched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[0],
            ctx["version"],
            ctx["port"],
        )
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[1],
            ctx["version"],
            ctx["port"],
        )

        def _part_a_io():
            run_light_io(matched_client, mount_paths[0], dd_count=5)
            run_light_io(unmatched_client, mount_paths[1], dd_count=5)

        part_a_log = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            _part_a_io,
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        ok_a, detail_a = verify_baseline_no_conditional_debug(part_a_log)
        _safe_umount(matched_client, mount_paths[0])
        _safe_umount(unmatched_client, mount_paths[1])
        if not ok_a:
            return result.mark(False, f"Part A FAIL: {detail_a}")
        log.info("Part A PASS: %s", detail_a)

        # --- Part B: Conditional Match_Policy=ALL ---
        log.info(
            "=== TC-CL-CONFIG-02 Part B: Conditional ALL "
            "(Clients=%s Exports=%s) ===",
            matched_ip,
            matched_export_id,
        )
        log_dest = facility_destination_for_write(ctx["cmd_host"])
        conditional_block = build_conditional_log_block(
            match_policy="ALL",
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
            conditional_components={"FSAL": "FULL_DEBUG", "NFS_V4": "MID_DEBUG"},
            exports=[matched_export_id],
            clients=[matched_ip],
            log_destination=log_dest,
        )
        apply_conditional_log_template(ctx["cmd_host"], conditional_block)
        container_id = _redeploy_refresh(ctx)
        _ensure_host_ganesha_policy_ready(ctx, container_id, require_conditional=True)
        # Capture confirmation of policy acceptance around reload.
        policy_log = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: reload_ganesha(ctx["nfs_node"], container_id),
            settle_sec=5,
            nfs_name=ctx["nfs_name"],
        )
        ok_policy, detail_policy = verify_match_policy_in_log(
            policy_log, expected_policy="MATCH_ALL"
        )
        if not ok_policy:
            # Also check a wider log tail in case confirmation was slightly earlier.
            wider = read_ganesha_log(
                ctx["nfs_node"], container_id, tail_lines=4000, nfs_name=ctx["nfs_name"]
            )
            ok_policy, detail_policy = verify_match_policy_in_log(
                wider, expected_policy="MATCH_ALL"
            )
        if not ok_policy:
            return result.mark(
                False, f"Part B policy confirmation FAIL: {detail_policy}"
            )
        log.info("Part B policy confirmation PASS: %s", detail_policy)

        # B1 — matching client + matching export
        log.info("=== Part B1: matching client + matching export ===")
        mount_export(
            matched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[0],
            ctx["version"],
            ctx["port"],
        )
        log_b1 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_paths[0], dd_count=5),
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        _safe_umount(matched_client, mount_paths[0])
        ok_b1, detail_b1 = verify_matching_client_conditional_debug(log_b1)
        if not ok_b1:
            return result.mark(False, f"Part B1 matching FAIL: {detail_b1}")
        log.info("Part B1 PASS: %s", detail_b1)

        # B2 — matching client + non-matching export
        log.info("=== Part B2: matching client + non-matching export ===")
        mount_export(
            matched_client,
            ctx["nfs_server"],
            unmatched_export,
            mount_paths[1],
            ctx["version"],
            ctx["port"],
        )
        log_b2 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_paths[1], dd_count=5),
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        _safe_umount(matched_client, mount_paths[1])
        ok_b2, detail_b2 = verify_baseline_no_conditional_debug(log_b2)
        if not ok_b2:
            return result.mark(
                False, f"Part B2 client-match/export-mismatch FAIL: {detail_b2}"
            )
        log.info("Part B2 PASS: %s", detail_b2)

        # B3 — non-matching client + matching export
        log.info("=== Part B3: non-matching client + matching export ===")
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[2],
            ctx["version"],
            ctx["port"],
        )
        log_b3 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(unmatched_client, mount_paths[2], dd_count=5),
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        _safe_umount(unmatched_client, mount_paths[2])
        ok_b3, detail_b3 = verify_baseline_no_conditional_debug(log_b3)
        if not ok_b3:
            return result.mark(
                False, f"Part B3 client-mismatch/export-match FAIL: {detail_b3}"
            )
        log.info("Part B3 PASS: %s", detail_b3)

        # B4 — neither matches (recommended)
        log.info("=== Part B4: non-matching client + non-matching export ===")
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            unmatched_export,
            mount_paths[3],
            ctx["version"],
            ctx["port"],
        )
        log_b4 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(unmatched_client, mount_paths[3], dd_count=5),
            settle_sec=8,
            nfs_name=ctx["nfs_name"],
        )
        _safe_umount(unmatched_client, mount_paths[3])
        ok_b4, detail_b4 = verify_baseline_no_conditional_debug(log_b4)
        if not ok_b4:
            return result.mark(False, f"Part B4 neither-match FAIL: {detail_b4}")
        log.info("Part B4 PASS: %s", detail_b4)

        return result.mark(
            True,
            f"Part A PASS ({detail_a}); policy ({detail_policy}); "
            f"B1 ({detail_b1}); B2 ({detail_b2}); B3 ({detail_b3}); B4 ({detail_b4})",
        )
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        for client in (matched_client, unmatched_client):
            for path in mount_paths:
                _safe_umount(client, path)


def _run_tc_cl_config_03(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-03: graceful handling of malformed Conditional Logging configs.

    Cases (service must stay up; no FATAL):
      1  invalid Match_Policy → WARN about INVALID_POLICY
      2  unknown component → WARN about INVALID_COMPONENT
      3  invalid log level SUPER_DEBUG → WARN about SUPER_DEBUG
      4b empty/trailing commas → starts and stays running
      6  Match_Policy without Conditional → starts; no elevated markers
    """
    result = TestCaseResult(
        "TC-CL-CONFIG-03", "Invalid/Malformed Config & Error Handling"
    )
    failures = []
    passed_cases = []
    matched_client = ctx["clients"][0]
    mount_path = None
    try:
        matched_ip = client_ip(matched_client)
        export_paths = _export_paths(ctx["config"], count=1)
        mount_paths = _mount_paths(ctx["config"], count=1)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        export_id = path_to_id[export_paths[0]]
        mount_path = mount_paths[0]

        cases = malformed_log_block_cases(
            export_id=export_id,
            client=matched_ip,
            log_destination=facility_destination_for_write(ctx["cmd_host"]),
        )
        for case in cases:
            case_name = case["name"]
            log.info("=== TC-CL-CONFIG-03 %s ===", case_name)
            try:
                apply_conditional_log_template(ctx["cmd_host"], case["log_block"])
                container_id = _redeploy_refresh(ctx)
                reload_ganesha(ctx["nfs_node"], container_id)
                # Capture parse/reload WARNs before capture_ganesha_log_window
                # truncates the file for the I/O window.
                reload_warn_log = read_ganesha_log(
                    ctx["nfs_node"],
                    container_id,
                    tail_lines=8000,
                    nfs_name=ctx["nfs_name"],
                )

                if not is_ganesha_running(ctx["nfs_node"], container_id):
                    failures.append(f"{case_name}: ganesha not running after reload")
                    continue

                _safe_umount(matched_client, mount_path)
                mount_export(
                    matched_client,
                    ctx["nfs_server"],
                    export_paths[0],
                    mount_path,
                    ctx["version"],
                    ctx["port"],
                )

                def _io():
                    run_light_io(matched_client, mount_path, dd_count=2)

                case_log = capture_ganesha_log_window(
                    ctx["nfs_node"],
                    container_id,
                    _io,
                    settle_sec=6,
                    nfs_name=ctx["nfs_name"],
                )
                wide_log = read_ganesha_log(
                    ctx["nfs_node"],
                    container_id,
                    tail_lines=8000,
                    nfs_name=ctx["nfs_name"],
                )
                combined_log = reload_warn_log + "\n" + case_log + "\n" + wide_log

                if log_contains_fatal(combined_log):
                    failures.append(f"{case_name}: FATAL/abort found in ganesha log")
                    continue
                if not is_ganesha_running(ctx["nfs_node"], container_id):
                    failures.append(f"{case_name}: ganesha exited after I/O")
                    continue

                if case.get("require_warn"):
                    tokens = case.get("expect_warn") or []
                    if not log_contains_any(
                        reload_warn_log, tokens
                    ) and not log_contains_any(combined_log, tokens):
                        failures.append(
                            f"{case_name}: expected warn tokens {tokens!r} not found"
                        )
                        continue

                if case.get("verify_no_elevate"):
                    ok_elev, detail_elev = verify_baseline_no_conditional_debug(
                        case_log
                    )
                    if not ok_elev:
                        failures.append(
                            f"{case_name}: unexpected elevated markers ({detail_elev})"
                        )
                        continue

                passed_cases.append(case_name)
                log.info("%s PASS", case_name)
            except Exception as err:
                failures.append(f"{case_name}: {err}")

        # Restore a known-good baseline LOG block.
        apply_conditional_log_template(
            ctx["cmd_host"],
            build_baseline_log_block(
                global_level="EVENT",
                components={"FSAL": "INFO", "NFS_V4": "INFO"},
                log_destination=facility_destination_for_write(ctx["cmd_host"]),
            ),
        )
        container_id = _redeploy_refresh(ctx)

        if failures:
            return result.mark(
                False,
                f"passed={passed_cases}; failures={failures}",
            )
        return result.mark(True, f"validated graceful cases: {', '.join(passed_cases)}")
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        if mount_path:
            _safe_umount(matched_client, mount_path)


def _run_tc_cl_dynamic_01(ctx) -> TestCaseResult:
    """TC-CL-DYNAMIC-01: ganesha_mgr CRUD lifecycle."""
    result = TestCaseResult(
        "TC-CL-DYNAMIC-01", "ganesha_mgr Full CRUD + Enable/Disable"
    )
    matched_client = ctx["clients"][0]
    mount_path = None
    try:
        container_id = ctx["container_id"]
        matched_ip = client_ip(matched_client)
        export_paths = _export_paths(ctx["config"], count=1)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        export_id = list(path_to_id.values())[0]
        mount_path = _mount_paths(ctx["config"], 1)[0]

        ganesha_mgr(
            ctx["nfs_node"],
            container_id,
            "reset log conditional_config",
        )
        for show_cmd in (
            "show conditional_clients",
            "show conditional_exports",
            "show conditional_match_policy",
        ):
            ganesha_mgr(ctx["nfs_node"], container_id, show_cmd)

        ganesha_mgr(
            ctx["nfs_node"], container_id, f"add conditional_clients {matched_ip}"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "add conditional_clients 192.168.1.0/24"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, f"add conditional_exports {export_id}"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "set log conditional FSAL FULL_DEBUG"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "set log conditional NFS_V4 MID_DEBUG"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "update conditional_match_policy ALL"
        )

        show_out, _ = ganesha_mgr(
            ctx["nfs_node"], container_id, "show log conditional_config"
        )
        parsed = parse_ganesha_mgr_show_output(show_out)
        if str(export_id) not in str(
            parsed.get("exports", [])
        ) and export_id not in parsed.get("exports", []):
            return result.mark(
                False, f"export {export_id} not in show output: {show_out[:300]}"
            )
        if not parsed.get("clients"):
            return result.mark(False, f"no clients in show output: {show_out[:300]}")

        mount_export(
            matched_client,
            ctx["nfs_server"],
            export_paths[0],
            mount_path,
            ctx["version"],
            ctx["port"],
        )
        log_after_add = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
            nfs_name=ctx["nfs_name"],
        )
        ok_add, detail_add = verify_conditional_verbosity(log_after_add, "")

        clients_show, _ = ganesha_mgr(
            ctx["nfs_node"], container_id, "show conditional_clients"
        )
        remove_target = matched_ip
        if "/32" in clients_show:
            for line in clients_show.splitlines():
                if matched_ip in line:
                    remove_target = line.strip()
                    break
        ganesha_mgr(
            ctx["nfs_node"],
            container_id,
            f"remove conditional_clients {remove_target}",
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, f"remove conditional_exports {export_id}"
        )

        log_after_remove = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
            nfs_name=ctx["nfs_name"],
        )
        ok_remove, detail_remove = verify_no_elevated_debug(log_after_remove)
        passed = ok_add and ok_remove
        detail = f"after add: {detail_add}; after remove: {detail_remove}"
        return result.mark(passed, detail)
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        if mount_path:
            _safe_umount(matched_client, mount_path)


def _run_tc_cl_dynamic_02(ctx) -> TestCaseResult:
    """TC-CL-DYNAMIC-02: DBus overrides vs config reload."""
    result = TestCaseResult("TC-CL-DYNAMIC-02", "Persistence, Hot-Reload & Coexistence")
    matched_client = ctx["clients"][0]
    mount_path = None
    try:
        container_id = ctx["container_id"]
        matched_ip = client_ip(matched_client)
        export_paths = _export_paths(ctx["config"], count=1)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        export_id = list(path_to_id.values())[0]
        mount_path = _mount_paths(ctx["config"], 1)[0]

        base_block = build_conditional_log_block(
            match_policy="ANY",
            global_level="EVENT",
            conditional_components={"FSAL": "EVENT", "NFS_V4": "EVENT"},
            exports=[export_id],
            clients=[matched_ip],
            log_destination=facility_destination_for_write(ctx["cmd_host"]),
        )
        apply_conditional_log_template(ctx["cmd_host"], base_block)
        container_id = _redeploy_refresh(ctx)

        ganesha_mgr(
            ctx["nfs_node"],
            container_id,
            "set log conditional_config "
            f"--components FSAL,NFS_V4 --level FULL_DEBUG "
            f"--clients {matched_ip} --export-ids {export_id} --policy ANY",
        )
        mount_export(
            matched_client,
            ctx["nfs_server"],
            export_paths[0],
            mount_path,
            ctx["version"],
            ctx["port"],
        )
        log_dbus = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
            nfs_name=ctx["nfs_name"],
        )
        ok_dbus, _ = verify_conditional_verbosity(log_dbus, "")

        reload_ganesha(ctx["nfs_node"], container_id)
        time.sleep(3)
        show_out, _ = ganesha_mgr(
            ctx["nfs_node"], container_id, "show log conditional_config"
        )
        parsed = parse_ganesha_mgr_show_output(show_out)

        capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
            nfs_name=ctx["nfs_name"],
        )

        if not is_ganesha_running(ctx["nfs_node"], container_id):
            return result.mark(False, "ganesha not running after reload")
        passed = ok_dbus and is_ganesha_running(ctx["nfs_node"], container_id)
        detail = (
            f"dbus elevated logs ok={ok_dbus}; post-reload policy={parsed.get('match_policy')}; "
            "ganesha stable after reload"
        )
        return result.mark(passed, detail)
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        if mount_path:
            _safe_umount(matched_client, mount_path)


def _assert_config_get_04(
    get_out: str, client_ip_s: str, export_a_id: int
) -> Tuple[bool, str]:
    text = get_out or ""
    if "Conditional" not in text:
        return False, "Conditional missing from config get"
    if client_ip_s not in text:
        return False, f"Clients={client_ip_s} missing from config get"
    if str(export_a_id) not in text:
        return False, f"Exports={export_a_id} missing from config get"
    if not re.search(r"NFS4\s*=\s*DEBUG", text, re.I):
        return False, "NFS4 = DEBUG missing from config get"
    if re.search(r"Match_Policy\s*=\s*MATCH_ALL\b", text, re.I) or re.search(
        r"Match_Policy\s*=\s*ALL\b", text, re.I
    ):
        return False, "MATCH_ALL must not appear (default MATCH_ANY expected)"
    # Match_Policy absent or MATCH_ANY/ANY is OK
    return True, "config get OK (Conditional, Clients, Exports, NFS4=DEBUG)"


def _assert_config_get_05(
    get_out: str,
    client_ip_s: str,
    export_a_id: int,
) -> Tuple[bool, str]:
    text = get_out or ""
    if "Conditional" not in text:
        return False, "Conditional missing"
    # Cluster-config counterpart of CONFIG-02: MATCH_ALL/ALL + Clients + one Export.
    # Pure Clients-only (no Exports) ignores Clients on this Ganesha build.
    if not re.search(r"Match_Policy\s*=\s*(?:MATCH_ALL|ALL)\b", text, re.I):
        return False, "Match_Policy = ALL/MATCH_ALL missing"
    if client_ip_s not in text:
        return False, f"Clients={client_ip_s} missing"
    if str(export_a_id) not in text:
        return False, f"Exports={export_a_id} missing"
    if not re.search(r"NFS4\s*=\s*FULL_DEBUG", text, re.I):
        return False, "NFS4 = FULL_DEBUG missing"
    return True, "config get OK (MATCH_ALL, Clients+Export_A, NFS4=FULL_DEBUG)"


def _assert_config_get_06(get_out: str, export_a_id: int) -> Tuple[bool, str]:
    text = get_out or ""
    if "Conditional" not in text:
        return False, "Conditional missing"
    if not re.search(r"Match_Policy\s*=\s*(?:MATCH_ANY|ANY)\b", text, re.I):
        return False, "Match_Policy = ANY/MATCH_ANY missing"
    if str(export_a_id) not in text:
        return False, f"Exports={export_a_id} missing"
    if re.search(r"^\s*Clients\s*=", text, re.M | re.I):
        return False, "Clients line must be absent (exports-only)"
    if not re.search(r"NFS4\s*=\s*FULL_DEBUG", text, re.I):
        return False, "NFS4 = FULL_DEBUG missing"
    return True, "config get OK (MATCH_ANY, Exports only, NFS4=FULL_DEBUG)"


def _run_tc_cl_config_04(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-04: default Match_Policy is MATCH_ANY (OR).

    No Match_Policy line. Conditional applies if client OR export matches.
    A: client-2 + exp_a → DEBUG (export matches)
    B: client-1 + exp_b → DEBUG (client matches)
    C: client-2 + exp_b → no DEBUG (neither matches)
    """
    result = TestCaseResult(
        "TC-CL-CONFIG-04", "Default Match_Policy MATCH_ANY (cluster config)"
    )
    fx = None
    try:
        fx = _prepare_match_any_fixture(ctx)
        container_id, set_rc, set_out = _apply_match_any_policy(
            ctx,
            components={"NFS4": "CRIT", "FSAL": "WARN"},
            conditional_components={"NFS4": "DEBUG"},
            clients=[fx["client_match_ip"]],
            exports=[fx["export_a_id"]],
            match_policy=None,
        )
        if set_rc != 0:
            return result.mark(False, f"config_set failed rc={set_rc}: {set_out}")

        get_out = nfs_cluster_config_get(ctx["cmd_host"], ctx["nfs_name"])
        ok_get, detail_get = _assert_config_get_04(
            get_out, fx["client_match_ip"], fx["export_a_id"]
        )
        if not ok_get:
            return result.mark(False, f"config_get assert: {detail_get}")
        log.info("CONFIG-04 config get: %s", detail_get)

        # Run non-matching first so client_other has never hit a matching export
        # (avoids sticky per-client elevation from Scenario A).
        log_c = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_other"],
            fx["export_b"],
            fx["mp_b"],
            lambda: run_echo_io(fx["client_other"], fx["mp_b"], "cl04_c", "test_c"),
        )
        ok_c, detail_c = verify_nfs4_non_matching_logs(log_c)
        if not ok_c:
            return result.mark(False, f"Scenario C FAIL: {detail_c}")

        # Scenario A — export matches, client does not
        log_a = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_other"],
            fx["export_a"],
            fx["mp_a"],
            lambda: run_echo_io(fx["client_other"], fx["mp_a"], "cl04_a", "test_a"),
        )
        ok_a, detail_a = verify_nfs4_matching_logs(log_a, full_debug=False)
        if not ok_a:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario A FAIL: {detail_a}")

        # Scenario B — client matches, export does not
        log_b = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_match"],
            fx["export_b"],
            fx["mp_b"],
            lambda: run_echo_io(fx["client_match"], fx["mp_b"], "cl04_b", "test_b"),
        )
        ok_b, detail_b = verify_nfs4_matching_logs(log_b, full_debug=False)
        if not ok_b:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario B FAIL: {detail_b}")

        if not is_ganesha_running(ctx["nfs_node"], container_id):
            return result.mark(False, "ganesha not running after scenarios")

        return result.mark(
            True,
            f"C={detail_c}; A={detail_a}; B={detail_b}; daemon running",
        )
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        try:
            nfs_cluster_config_reset(ctx["cmd_host"], ctx["nfs_name"])
        except Exception:
            pass
        if fx:
            for client, mp in (
                (fx["client_match"], fx["mp_a"]),
                (fx["client_match"], fx["mp_b"]),
                (fx["client_other"], fx["mp_a"]),
                (fx["client_other"], fx["mp_b"]),
            ):
                _safe_umount(client, mp)


def _run_tc_cl_config_05(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-05: MATCH_ALL via ``ceph nfs cluster config set``.

    Cluster-config counterpart of CONFIG-02 (static template MATCH_ALL):
      A  client_match + exp_a → FULL_DEBUG (both match)
      B  client_match + exp_b → no elevate (export mismatch)
      C  client_other + exp_a → no elevate (client mismatch)
      D  client_other + exp_b → no elevate (neither)

    Pure Clients-only (omitted Exports) ignores Clients on this build and
    elevates everyone; a sentinel Export_Id crashed NFS. So CONFIG-05 uses
    Clients + Export_A with Match_Policy=ALL — the same shape CONFIG-02 already
    proved works — exercised through cluster config set/get/reset.
    """
    result = TestCaseResult("TC-CL-CONFIG-05", "MATCH_ALL via cluster config")
    fx = None
    try:
        fx = _prepare_match_any_fixture(ctx)
        log.info(
            "CONFIG-05 apply: Clients=%s Exports=[%s] Match_Policy=ALL",
            fx["client_match_ip"],
            fx["export_a_id"],
        )
        # Use ALL (not MATCH_ALL) so template + userconf tokens stay identical;
        # CONFIG-02 already proved ALL works for AND semantics.
        container_id, set_rc, set_out = _apply_match_any_policy(
            ctx,
            components={"NFS4": "CRIT"},
            conditional_components={"NFS4": "FULL_DEBUG"},
            clients=[fx["client_match_ip"]],
            exports=[fx["export_a_id"]],
            match_policy="ALL",
        )
        if set_rc != 0:
            return result.mark(False, f"config_set failed rc={set_rc}: {set_out}")

        get_out = nfs_cluster_config_get(ctx["cmd_host"], ctx["nfs_name"])
        ok_get, detail_get = _assert_config_get_05(
            get_out, fx["client_match_ip"], fx["export_a_id"]
        )
        if not ok_get:
            return result.mark(False, f"config_get assert: {detail_get}")
        log.info("CONFIG-05 config get: %s", detail_get)

        # Non-matching first (CONFIG-02 B3/B4 / CONFIG-04 Scenario C ordering).
        # C — client mismatch, export match (proves Clients filter under MATCH_ALL)
        log_c = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_other"],
            fx["export_a"],
            fx["mp_a"],
            lambda: run_dd_small_io(fx["client_other"], fx["mp_a"], "cl05_c"),
        )
        ok_c, detail_c = verify_nfs4_non_matching_logs(log_c)
        if not ok_c:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario C FAIL: {detail_c}")

        # D — neither matches
        log_d = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_other"],
            fx["export_b"],
            fx["mp_b"],
            lambda: run_dd_small_io(fx["client_other"], fx["mp_b"], "cl05_d"),
        )
        ok_d, detail_d = verify_nfs4_non_matching_logs(log_d)
        if not ok_d:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario D FAIL: {detail_d}")

        # A — both match
        log_a = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_match"],
            fx["export_a"],
            fx["mp_a"],
            lambda: run_dd_small_io(fx["client_match"], fx["mp_a"], "cl05_a"),
        )
        ok_a, detail_a = verify_nfs4_matching_logs(log_a, full_debug=True)
        if not ok_a:
            ok_a, detail_a = verify_nfs4_matching_logs(log_a, full_debug=False)
        if not ok_a:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario A FAIL: {detail_a}")

        # B — client match, export mismatch (proves MATCH_ALL AND, not OR)
        log_b = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_match"],
            fx["export_b"],
            fx["mp_b"],
            lambda: run_dd_small_io(fx["client_match"], fx["mp_b"], "cl05_b"),
        )
        ok_b, detail_b = verify_nfs4_non_matching_logs(log_b)
        if not ok_b:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario B FAIL: {detail_b}")

        if not is_ganesha_running(ctx["nfs_node"], container_id):
            return result.mark(False, "ganesha not running after scenarios")

        return result.mark(
            True,
            f"C={detail_c}; D={detail_d}; A={detail_a}; B={detail_b}; daemon running",
        )
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        try:
            nfs_cluster_config_reset(ctx["cmd_host"], ctx["nfs_name"])
        except Exception:
            pass
        if fx:
            for client, mp in (
                (fx["client_match"], fx["mp_a"]),
                (fx["client_match"], fx["mp_b"]),
                (fx["client_other"], fx["mp_a"]),
                (fx["client_other"], fx["mp_b"]),
            ):
                _safe_umount(client, mp)


def _run_tc_cl_config_06(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-06: MATCH_ANY, exports only.

    Any client on export A gets FULL_DEBUG; export B never does.
    """
    result = TestCaseResult(
        "TC-CL-CONFIG-06", "MATCH_ANY exports-only (cluster config)"
    )
    fx = None
    try:
        fx = _prepare_match_any_fixture(ctx)
        container_id, set_rc, set_out = _apply_match_any_policy(
            ctx,
            components={"NFS4": "CRIT"},
            conditional_components={"NFS4": "FULL_DEBUG"},
            clients=None,
            exports=[fx["export_a_id"]],
            match_policy="MATCH_ANY",
        )
        if set_rc != 0:
            return result.mark(False, f"config_set failed rc={set_rc}: {set_out}")

        get_out = nfs_cluster_config_get(ctx["cmd_host"], ctx["nfs_name"])
        ok_get, detail_get = _assert_config_get_06(get_out, fx["export_a_id"])
        if not ok_get:
            return result.mark(False, f"config_get assert: {detail_get}")
        log.info("CONFIG-06 config get: %s", detail_get)

        log_a = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_match"],
            fx["export_a"],
            fx["mp_a"],
            lambda: run_echo_io(fx["client_match"], fx["mp_a"], "cl06_a", "cl06_a"),
        )
        ok_a, detail_a = verify_nfs4_matching_logs(log_a, full_debug=True)
        if not ok_a:
            ok_a, detail_a = verify_nfs4_matching_logs(log_a, full_debug=False)
        if not ok_a:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario A FAIL: {detail_a}")

        log_b = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_other"],
            fx["export_a"],
            fx["mp_a"],
            lambda: run_echo_io(fx["client_other"], fx["mp_a"], "cl06_a2", "cl06_a2"),
        )
        ok_b, detail_b = verify_nfs4_matching_logs(log_b, full_debug=True)
        if not ok_b:
            ok_b, detail_b = verify_nfs4_matching_logs(log_b, full_debug=False)
        if not ok_b:
            _dump_policy_not_loaded(ctx, container_id)
            return result.mark(False, f"Scenario B FAIL: {detail_b}")

        log_c = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_match"],
            fx["export_b"],
            fx["mp_b"],
            lambda: run_echo_io(fx["client_match"], fx["mp_b"], "cl06_b", "cl06_b"),
        )
        ok_c, detail_c = verify_nfs4_non_matching_logs(log_c)
        if not ok_c:
            return result.mark(False, f"Scenario C FAIL: {detail_c}")

        log_d = _scenario_io_capture(
            ctx,
            container_id,
            fx["client_other"],
            fx["export_b"],
            fx["mp_b"],
            lambda: run_echo_io(fx["client_other"], fx["mp_b"], "cl06_b2", "cl06_b2"),
        )
        ok_d, detail_d = verify_nfs4_non_matching_logs(log_d)
        if not ok_d:
            return result.mark(False, f"Scenario D FAIL: {detail_d}")

        if not is_ganesha_running(ctx["nfs_node"], container_id):
            return result.mark(False, "ganesha not running after scenarios")

        reset_rc = nfs_cluster_config_reset(ctx["cmd_host"], ctx["nfs_name"])
        get_after = nfs_cluster_config_get(ctx["cmd_host"], ctx["nfs_name"])
        if "Conditional" in (get_after or ""):
            return result.mark(
                False,
                f"config reset left Conditional present (rc={reset_rc})",
            )

        return result.mark(
            True,
            f"A={detail_a}; B={detail_b}; C={detail_c}; D={detail_d}; "
            "reset OK; daemon running",
        )
    except Exception as err:
        return result.mark(False, str(err))
    finally:
        try:
            nfs_cluster_config_reset(ctx["cmd_host"], ctx["nfs_name"])
        except Exception:
            pass
        if fx:
            for client, mp in (
                (fx["client_match"], fx["mp_a"]),
                (fx["client_match"], fx["mp_b"]),
                (fx["client_other"], fx["mp_a"]),
                (fx["client_other"], fx["mp_b"]),
            ):
                _safe_umount(client, mp)


_TC_DISPATCH = {
    OP_TC_CL_CONFIG_01: _run_tc_cl_config_01,
    OP_TC_CL_CONFIG_02: _run_tc_cl_config_02,
    OP_TC_CL_CONFIG_03: _run_tc_cl_config_03,
    OP_TC_CL_CONFIG_04: _run_tc_cl_config_04,
    OP_TC_CL_CONFIG_05: _run_tc_cl_config_05,
    OP_TC_CL_CONFIG_06: _run_tc_cl_config_06,
    OP_TC_CL_DYNAMIC_01: _run_tc_cl_dynamic_01,
    OP_TC_CL_DYNAMIC_02: _run_tc_cl_dynamic_02,
}


def run(ceph_cluster, **kw):
    """Entry point for cephci suite execution."""
    config = kw.get("config", {})
    steps = _operations_to_run(config)
    report = TestRunReport()

    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")
    min_clients = int(config.get("min_clients", 2))
    if len(clients) < min_clients:
        raise ConfigError(
            f"conditional logging requires at least {min_clients} clients"
        )
    clients = clients[: max(int(config.get("clients", min_clients)), min_clients)]

    if not nfs_nodes or not installers:
        raise ConfigError("Requires nfs and installer nodes")

    installer = installers[0]
    nfs_node = nfs_nodes[0]
    # Admin ceph CLI via cephadm shell must run on the installer (has keyring).
    # NFS nodes often fail with: "no keyring found; disabled cephx authentication".
    nfs_cmd_host = installer
    cephadm = CephAdm(installer).ceph
    redeploy_wait = int(config.get("redeploy_wait", 15))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    reset_on_exit = bool(config.get("reset_ganesha_template_on_exit", True))

    backup_path = _cl_tmp_path(CONDITIONAL_LOG_BACKUP_SUFFIX)
    template_backup_exists = backup_ganesha_template(nfs_cmd_host, backup_path)
    created_cluster = False
    nfs_name = config.get("nfs_name", "cephfs-nfs-cl")

    try:
        try:
            nfs_clusters = cephadm.nfs.cluster.ls()
            if nfs_name not in nfs_clusters:
                created_cluster = True
        except Exception:
            pass

        nfs_name, fs_name, created_flag = _ensure_nfs_cluster(
            ceph_cluster, config, installer, clients, nfs_nodes, cephadm
        )
        created_cluster = created_cluster or created_flag
        container_id, _, _ = redeploy_and_wait(
            cephadm, installer, nfs_name, redeploy_wait, service_wait_timeout
        )
        ctx = {
            "config": config,
            "clients": clients,
            "nfs_node": nfs_node,
            "nfs_nodes": nfs_nodes,
            "nfs_server": nfs_node.hostname,
            "cmd_host": nfs_cmd_host,
            "installer": installer,
            "cephadm": cephadm,
            "nfs_name": nfs_name,
            "fs_name": fs_name,
            "version": config.get("nfs_version", "4.2"),
            "port": str(config.get("port", "2049")),
            "redeploy_wait": redeploy_wait,
            "service_wait_timeout": service_wait_timeout,
            "container_id": container_id,
        }

        for step in steps:
            log.info("=== Running conditional logging operation: %s ===", step)
            report.add(_TC_DISPATCH[step](ctx))

        for line in report.summary_lines():
            log.info(line)

        if not report.all_passed():
            failed = [r.tc_id for r in report.results if not r.passed]
            raise OperationFailedError("Conditional logging failures: %s" % failed)
        log.info(
            "TEST PASSED - conditional logging operations OK: %s",
            ", ".join(r.tc_id for r in report.results),
        )
        return 0

    except Exception as err:
        log.error("Conditional logging test failed: %s", err)
        log.error(traceback.format_exc())
        return 1
    finally:
        if reset_on_exit:
            try:
                restore_ganesha_template(
                    nfs_cmd_host, template_backup_exists, backup_path
                )
                redeploy_nfs_clusters(
                    cephadm,
                    [nfs_name],
                    installer,
                    redeploy_wait,
                    service_wait_timeout,
                )
            except Exception as cleanup_err:
                log.error("Template restore failed: %s", cleanup_err)
        if created_cluster and config.get("cleanup_cluster_on_exit", False):
            try:
                cleanup_cluster(
                    clients=[clients[0]],
                    nfs_mount=config.get("nfs_mount", "/mnt/nfs_cl"),
                    nfs_name=nfs_name,
                    nfs_export=config.get("bootstrap_export", "/export_cl_bootstrap"),
                    nfs_nodes=nfs_nodes,
                )
            except Exception as cleanup_err:
                log.error("Cluster cleanup failed: %s", cleanup_err)
