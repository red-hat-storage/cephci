"""
Helpers for NFS-Ganesha conditional logging tests.

Covers static config (ganesha template / config-key), runtime ganesha_mgr control,
log capture, and pass/fail reporting.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from nfs_delegation_operations import (
    CONF_KEY,
    DEFAULT_TEMPLATE_PATH,
    MOUNTED_TEMPLATE_PATH,
    redeploy_nfs_clusters,
    run_cephadm_shell,
    truncate_ganesha_container_log,
    wait_for_nfs_cluster_daemons_running,
)
from nfs_operations import mount_retry, verify_nfs_ganesha_service

from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)

CephNode = Any

LOG_BLOCK_START = re.compile(r"^\s*LOG\s*\{", re.M)
LOG_BLOCK_PATTERN = re.compile(r"LOG\s*\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", re.S)

RE_PARSE_ERROR = re.compile(
    r"(parse|syntax|invalid|unknown|error|failed|DEBUGX|malformed)",
    re.I,
)
RE_COMPONENT_DEBUG = re.compile(
    r"(?:FSAL|NFS[_ ]?V?4|COMPONENT_FSAL|COMPONENT_NFS_V4)"
    r".{0,120}?(?:FULL_DEBUG|MID_DEBUG|NIV_DEBUG|\bDEBUG\b|\bDBG\b)",
    re.I,
)
RE_COMPONENT_INFO = re.compile(
    r"(?:FSAL|NFS[_ ]?V?4|COMPONENT_FSAL|COMPONENT_NFS_V4)"
    r".{0,120}?(?:\bINFO\b|\bEVENT\b|\bNIV_EVENT\b)",
    re.I,
)
# Conditional-logging markers used by TC-CL-CONFIG-01 pass/fail criteria.
RE_FSAL_F_DBG = re.compile(r"FSAL\s*:F_DBG", re.I)
RE_FSAL_FULL_DEBUG = re.compile(r"FSAL\s*:FULL_DEBUG", re.I)
RE_EXPORT_OR_NFS_M_DBG = re.compile(r"(?:EXPORT|NFS_V4|NFS4)\s*:M_DBG", re.I)

CONDITIONAL_LOG_WORK_SUFFIX = "conditional_logging_work.conf.j2"
CONDITIONAL_LOG_BACKUP_SUFFIX = "conditional_logging_backup.conf.j2"


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


def _cl_tmp_path(suffix: str) -> str:
    return f"/tmp/ganesha_cl_{suffix}"


def cl_config_get(config: Mapping[str, Any], *keys: str, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def build_log_facility_block(destination: str = "/var/log/ganesha.log") -> str:
    return (
        "    Facility {\n"
        "        name = FILE;\n"
        f'        destination = "{destination}";\n'
        "        enable = active;\n"
        "    }\n"
    )


def build_global_components_block(level: str = "EVENT") -> str:
    return (
        "    Components {\n"
        f"        ALL = {level};\n"
        f"        FSAL = {level};\n"
        f"        NFS_V4 = {level};\n"
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
    log_destination: str = "/var/log/ganesha.log",
) -> str:
    """Return a LOG block with no Conditional section (baseline / Part A)."""
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
    match_policy: str = "ANY",
    global_level: str = "EVENT",
    conditional_components: Optional[Mapping[str, str]] = None,
    exports: Optional[Sequence[int]] = None,
    clients: Optional[Sequence[str]] = None,
    components: Optional[Mapping[str, str]] = None,
    log_destination: str = "/var/log/ganesha.log",
) -> str:
    """Return a complete LOG { ... } block for ganesha template injection."""
    conditional_components = conditional_components or {
        "FSAL": "FULL_DEBUG",
        "NFS_V4": "MID_DEBUG",
    }
    # Default global Components match the CONFIG-01 Part B static example.
    components = components or {"FSAL": "INFO", "NFS_V4": "INFO"}
    exports = list(exports or [])
    clients = list(clients or [])
    policy = str(match_policy).strip().upper()
    if policy in ("MATCH_ANY", "ANY"):
        policy = "ANY"
    elif policy in ("MATCH_ALL", "ALL"):
        policy = "ALL"
    return (
        "LOG {\n"
        f"    Default_Log_Level = {global_level};\n"
        f"{build_log_facility_block(log_destination)}"
        f"{build_components_block(components)}"
        f"    Match_Policy = {policy};\n"
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
    try:
        remote = cmd_host.remote_file(sudo=True, file_name=work, file_mode="w")
        remote.write(template_text)
        remote.flush()
        if hasattr(remote, "close"):
            remote.close()
    except AttributeError:
        remote = cmd_host.remote_file(sudo=True, file_name=work, file_mode="wb")
        remote.write(template_text.encode("utf-8"))
        remote.flush()
        if hasattr(remote, "close"):
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
    if err and "exit" in str(err).lower():
        raise OperationFailedError(f"Ganesha reload failed: {err or out}")
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


def read_ganesha_log(
    nfs_node: CephNode, container_id: str, tail_lines: int = 8000
) -> str:
    out, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} tail -n {int(tail_lines)} /var/log/ganesha.log",
        check_ec=False,
        timeout=120,
    )
    return str(out or "")


def capture_ganesha_log_window(
    nfs_node: CephNode,
    container_id: str,
    action,
    settle_sec: int = 5,
    tail_lines: int = 8000,
) -> str:
    """Truncate log, run ``action()``, wait, return new log tail."""
    truncate_ganesha_container_log(nfs_node, container_id)
    action()
    time.sleep(settle_sec)
    return read_ganesha_log(nfs_node, container_id, tail_lines=tail_lines)


def count_component_debug_lines(log_text: str, component: str) -> int:
    pattern = re.compile(
        rf"(?:{re.escape(component)}|COMPONENT_{re.escape(component)})"
        rf".{{0,160}}?(?:FULL_DEBUG|MID_DEBUG|NIV_DEBUG|\bDEBUG\b|\bDBG\b)",
        re.I,
    )
    return len(pattern.findall(log_text or ""))


def count_component_info_lines(log_text: str, component: str) -> int:
    pattern = re.compile(
        rf"(?:{re.escape(component)}|COMPONENT_{re.escape(component)})"
        rf".{{0,160}}?(?:\bINFO\b|\bEVENT\b|\bNIV_EVENT\b)",
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
    for key in ("path", "pseudo", "export_path", "bind"):
        if entry.get(key):
            return str(entry[key])
    return None


def create_nfs_exports(
    cmd_host: CephNode,
    fs_name: str,
    nfs_name: str,
    export_paths: Sequence[str],
) -> Dict[str, int]:
    """Create exports and return mapping export_path -> export_id."""
    path_to_id: Dict[str, int] = {}
    for export_path in export_paths:
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export delete {nfs_name} {export_path}",
            check_ec=False,
        )
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export create {fs_name} {nfs_name} {export_path} {fs_name} {export_path}",
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


def run_light_io(client: CephNode, mount_path: str, dd_count: int = 100) -> None:
    """Run ls -R and dd write/read workload on a mount."""
    mp = mount_path.rstrip("/")
    client.exec_command(sudo=True, cmd=f"ls -R {mp} >/dev/null 2>&1", check_ec=False)
    test_file = f"{mp}/cl_testfile.dat"
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={test_file} bs=1M count={int(dd_count)} conv=fsync",
        check_ec=False,
        timeout=600,
    )
    client.exec_command(
        sudo=True,
        cmd=f"dd if={test_file} of=/dev/null bs=1M",
        check_ec=False,
        timeout=300,
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
) -> List[Dict[str, Any]]:
    """
    Return graceful-malformation cases for TC-CL-CONFIG-03.

    Each case dict:
      name, log_block, expect_warn (tokens), require_warn, verify_no_elevate
    """
    export_id = int(export_id)
    client = str(client)
    components = {"FSAL": "INFO", "NFS_V4": "INFO"}
    return [
        {
            "name": "case1_invalid_match_policy",
            "log_block": (
                "LOG {\n"
                "    Default_Log_Level = EVENT;\n"
                f"{build_log_facility_block()}"
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
                f"{build_log_facility_block()}"
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
                f"{build_log_facility_block()}"
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
                f"{build_log_facility_block()}"
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
                f"{build_log_facility_block()}"
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
    wait_for_nfs_cluster_daemons_running(
        cephadm, [nfs_name], timeout_seconds=service_wait_timeout
    )
    verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)
    return get_nfs_daemon_container(cephadm, nfs_name)
