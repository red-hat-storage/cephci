"""
ODF / Rook-like Ceph defaults helpers for standalone CephCI (cephadm) clusters.

Opt-in via run.py::

    --custom-config apply-odf-defaults=true
    --custom-config apply-odf-topology=true

``apply-odf-defaults`` merges a shared YAML profile into bootstrap ``args.config``
so ``cephadm bootstrap --config`` seeds the mon store and OSDMap ratios, including
``ms_bind_msgr1=false`` and ``rbd_default_map_options``. At the end of
``bootstrap.py`` (after bootstrap succeeds) it sets monmap to v2-only addresses via
``ceph mon set-addrs`` (``[v2:IP:3300/0]``). Mons often ignore ``ms_bind_msgr1`` for
bind ports and follow the monmap (tracker #70457), so both steps are required.

``apply-odf-topology`` alone injects ``rbd_default_map_options`` at bootstrap.
Post-OSD it applies CRUSH / container-limit / device-class steps.
"""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from os.path import abspath, dirname
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from utility.log import Log

LOG = Log(__name__)

_REPO_ROOT = dirname(dirname(abspath(__file__)))
DEFAULT_ODF_PROFILE_PATH = os.path.join(
    _REPO_ROOT, "conf", "tentacle", "rook", "odf_rook_defaults.yaml"
)

# CLI override keys (from --custom-config key=value)
APPLY_ODF_DEFAULTS_KEY = "apply-odf-defaults"
APPLY_ODF_TOPOLOGY_KEY = "apply-odf-topology"

# Applied at bootstrap when apply-odf-topology is set without the full defaults profile
MSGR2_BOOTSTRAP_CONFIG: Dict[str, Dict[str, Any]] = {
    "global": {
        "ms_bind_msgr1": "false",
        "rbd_default_map_options": "ms_mode=prefer-crc",
    }
}

_TRUTHY = {"1", "true", "yes", "y", "on"}
_IP_PORT_RE = re.compile(r"^([^:]+):(\d+)$")


def is_truthy(value: Any) -> bool:
    """Return True for common truthy CLI / YAML values."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in _TRUTHY


def overrides_enabled(overrides: Optional[Dict], key: str) -> bool:
    """Return True if *key* is present and truthy in custom-config overrides."""
    if not overrides:
        return False
    return is_truthy(overrides.get(key))


def is_msgr1_disabled(client) -> bool:
    """
    Return True if ``ceph config get mon ms_bind_msgr1`` is false.

    Used by kernel CephFS mounts to choose msgr2 (:3300 + ms_mode=crc) vs
    legacy (:6789 / bare IP).
    """
    try:
        out, _ = client.exec_command(sudo=True, cmd="ceph config get mon ms_bind_msgr1")
        return (out or "").strip().lower() in ("false", "0", "no")
    except Exception:  # noqa: BLE001
        return False


def _normalize_kernel_mon_part(part: str) -> str:
    """Strip ``v1:``/``v2:`` and ``/nonce``; leave ``IP`` or ``IP:port``."""
    text = str(part).strip().strip("[]")
    if text.startswith("v2:") or text.startswith("v1:"):
        text = text.split(":", 1)[1]
    if "/" in text:
        text = text.split("/", 1)[0]
    return text


def format_mons_for_kernel_mount(mon_node_ip, use_msgr2: bool) -> str:
    """
    Format mon device string for ``mount -t ceph``.

    Args:
        mon_node_ip: Comma-separated IPs, list/tuple of IPs, or single IP
                     (optional ``:port`` already present). Also accepts
                     mon-dump style ``v2:IP:3300/0`` strings.
        use_msgr2: When True, ensure each mon uses port 3300.

    Returns:
        Comma-separated mon string suitable for mount device.
    """
    if isinstance(mon_node_ip, (list, tuple)):
        parts = [_normalize_kernel_mon_part(x) for x in mon_node_ip if str(x).strip()]
    else:
        parts = [
            _normalize_kernel_mon_part(p)
            for p in str(mon_node_ip).replace(" ", "").split(",")
            if p.strip()
        ]
    parts = [p for p in parts if p]
    if not use_msgr2:
        return ",".join(parts)
    formatted = []
    for part in parts:
        # Bare IP → :3300; rewrite legacy :6789; keep existing :3300 / other ports
        if part.count(":") == 0:
            formatted.append(f"{part}:3300")
        elif part.endswith(":6789"):
            formatted.append(f"{part[:-5]}:3300")
        else:
            formatted.append(part)
    return ",".join(formatted)


def kernel_ms_mode_opt(use_msgr2: bool, existing: str = "") -> str:
    """Return ``,ms_mode=crc`` when msgr2 is required and not already set."""
    if use_msgr2 and "ms_mode=" not in (existing or ""):
        return ",ms_mode=crc"
    return ""


def load_odf_defaults_profile(
    path: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Load the ODF defaults profile YAML (section -> key -> value).

    Args:
        path: Optional absolute/relative path; defaults to conf/tentacle/rook profile.
    """
    profile_path = path or DEFAULT_ODF_PROFILE_PATH
    if not os.path.isfile(profile_path):
        raise FileNotFoundError(f"ODF defaults profile not found: {profile_path}")
    with open(profile_path) as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"ODF defaults profile must be a mapping: {profile_path}")
    LOG.info("Loaded ODF defaults profile from %s", profile_path)
    return data


def merge_odf_into_bootstrap_config(
    suite_config: Optional[Dict[str, Dict[str, Any]]],
    odf_profile: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Deep-merge ODF profile under suite bootstrap config.

    Suite section keys win on conflict (e.g. public_network from the suite).
    """
    merged: Dict[str, Dict[str, Any]] = deepcopy(odf_profile) if odf_profile else {}
    suite_config = suite_config or {}
    for section, values in suite_config.items():
        if not isinstance(values, dict):
            merged[section] = values
            continue
        section_dict = merged.setdefault(section, {})
        if not isinstance(section_dict, dict):
            merged[section] = deepcopy(values)
            continue
        section_dict.update(values)
    return merged


def apply_odf_defaults_to_bootstrap_config(
    args: Dict[str, Any],
    overrides: Optional[Dict] = None,
    profile_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Merge ODF Rook settings into bootstrap ``args["config"]``.

    - ``apply-odf-defaults=true``: full profile from YAML.
    - ``apply-odf-topology=true`` alone: ``rbd_default_map_options`` only.

    Mutates and returns *args*. No-op when neither flag is set.
    """
    apply_defaults = overrides_enabled(overrides, APPLY_ODF_DEFAULTS_KEY)
    apply_topology = overrides_enabled(overrides, APPLY_ODF_TOPOLOGY_KEY)
    if not apply_defaults and not apply_topology:
        return args

    existing = args.get("config")
    if existing is not None and not isinstance(existing, dict):
        LOG.warning(
            "bootstrap args.config is not a dict (%s); skipping ODF defaults merge",
            type(existing),
        )
        return args

    if apply_defaults:
        profile = load_odf_defaults_profile(profile_path)
        args["config"] = merge_odf_into_bootstrap_config(existing, profile)
        LOG.info(
            "Merged ODF Rook defaults into bootstrap --config "
            "(suite keys take precedence)"
        )
    else:
        # Topology-only: still seed RBD ms_mode at bootstrap
        args["config"] = merge_odf_into_bootstrap_config(
            existing, MSGR2_BOOTSTRAP_CONFIG
        )
        LOG.info(
            "Merged RBD ms_mode bootstrap key into --config "
            "(apply-odf-topology without full defaults)"
        )

    LOG.debug("Bootstrap config after ODF merge: %s", args["config"])
    return args


def _shell(shell_fn: Callable, *cmd: str, check_status: bool = True) -> str:
    """Run a cephadm shell command and return stdout."""
    result = shell_fn(args=list(cmd), check_status=check_status)
    if isinstance(result, tuple):
        out, _err = result
    else:
        out = result
    if isinstance(out, bytes):
        out = out.decode()
    return (out or "").strip()


def _ip_from_addr_string(addr: str) -> Optional[str]:
    """Extract host/IP from ``IP:port`` or ``v2:IP:port/0`` style strings."""
    if not addr:
        return None
    text = addr.strip().strip("[]")
    # v2:10.0.0.1:3300/0 or v1:10.0.0.1:6789/0
    if text.startswith("v2:") or text.startswith("v1:"):
        text = text.split(":", 1)[1]
    if "/" in text:
        text = text.split("/", 1)[0]
    match = _IP_PORT_RE.match(text)
    if match:
        return match.group(1)
    if ":" not in text:
        return text
    # last-resort: strip trailing :port
    return text.rsplit(":", 1)[0]


def _mon_name_and_ip(mon: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Return (mon_name, ip) from a mon dump entry, preferring v2 addr IP."""
    name = mon.get("name")
    if not name:
        return None
    addrs = mon.get("public_addrs") or {}
    vec = addrs.get("addrvec") if isinstance(addrs, dict) else None
    if vec:
        for prefer in ("v2", "v1"):
            for entry in vec:
                if entry.get("type") == prefer:
                    ip = _ip_from_addr_string(entry.get("addr") or "")
                    if ip:
                        return name, ip
    for key in ("public_addr", "addr"):
        ip = _ip_from_addr_string(str(mon.get(key) or ""))
        if ip:
            return name, ip
    return None


def apply_v2_only_mon_addrs(shell_fn: Callable) -> None:
    """
    Set each mon public addr to v2-only (ODF-style monmap).

    Equivalent to::

        ceph mon set-addrs <name> '[v2:<ip>:3300/0]'

    Leaves ``ms_bind_msgr1`` unchanged (ODF keeps it true).
    """
    LOG.info("Setting monmap to v2-only addresses (ODF requireMsgr2 style)")
    raw = _shell(shell_fn, "ceph", "mon", "dump", "-f", "json")
    try:
        dump = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError) as exc:
        LOG.warning("Failed to parse mon dump JSON: %s", exc)
        return

    mons = dump.get("mons") or []
    if not mons:
        LOG.warning("No mons in mon dump; skipping set-addrs")
        return

    for mon in mons:
        parsed = _mon_name_and_ip(mon)
        if not parsed:
            LOG.warning("Could not resolve name/IP for mon entry: %s", mon)
            continue
        name, ip = parsed
        addrvec = f"[v2:{ip}:3300/0]"
        LOG.info("ceph mon set-addrs %s %s", name, addrvec)
        try:
            _shell(shell_fn, "ceph", "mon", "set-addrs", name, addrvec)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("mon set-addrs %s failed: %s", name, exc)

    try:
        after = _shell(shell_fn, "ceph", "mon", "dump")
        LOG.info("mon dump after v2-only set-addrs:\n%s", after)
    except Exception as exc:  # noqa: BLE001
        LOG.debug("Could not re-dump monmap: %s", exc)


def verify_odf_defaults(shell_fn, profile: Optional[Dict] = None) -> List[str]:
    """
    Verify ODF defaults against ``ceph config dump`` and ``ceph osd dump``.

    Also checks monmap is v2-only when apply-odf-defaults path is used.

    Args:
        shell_fn: Callable that runs a ceph command list and returns (out, err)
                  e.g. ``cephadm.shell``.
        profile: Optional profile dict; loaded from disk if omitted.

    Returns:
        List of human-readable mismatch strings (empty if all matched).
    """
    profile = profile or load_odf_defaults_profile()
    mismatches: List[str] = []

    out, _ = shell_fn(args=["ceph", "config", "dump", "--format", "json"])
    try:
        dump = json.loads(out) if out else []
    except (TypeError, json.JSONDecodeError):
        dump = []
    # config dump JSON is a list of {section, name, value, ...}
    by_key = {}
    if isinstance(dump, list):
        for entry in dump:
            section = entry.get("section") or entry.get("who") or "global"
            name = entry.get("name") or entry.get("option")
            if name:
                by_key[(section, name)] = str(entry.get("value", ""))

    for section, options in profile.items():
        if not isinstance(options, dict):
            continue
        for name, expected in options.items():
            if name.startswith("mon_osd_") and section == "global":
                # Ratios verified via osd dump below
                continue
            actual = by_key.get((section, name))
            if actual is None and section != "global":
                # Some keys may appear under global in dump
                actual = by_key.get(("global", name))
            if actual is None:
                mismatches.append(f"missing {section}/{name} (expected {expected})")
                continue
            if str(expected) != str(actual) and _normalize_num(
                expected
            ) != _normalize_num(actual):
                mismatches.append(
                    f"{section}/{name}: expected {expected}, got {actual}"
                )

    out, _ = shell_fn(args=["ceph", "osd", "dump", "--format", "json"])
    try:
        osd_dump = json.loads(out) if out else {}
    except (TypeError, json.JSONDecodeError):
        osd_dump = {}
    ratio_map = {
        "mon_osd_full_ratio": "full_ratio",
        "mon_osd_backfillfull_ratio": "backfillfull_ratio",
        "mon_osd_nearfull_ratio": "nearfull_ratio",
    }
    global_opts = profile.get("global", {})
    for conf_key, dump_key in ratio_map.items():
        if conf_key not in global_opts:
            continue
        expected = float(global_opts[conf_key])
        actual = float(osd_dump.get(dump_key, -1))
        if abs(expected - actual) > 0.011:
            mismatches.append(f"osd dump {dump_key}: expected {expected}, got {actual}")

    # ODF-style: monmap should advertise v2 only
    out, _ = shell_fn(args=["ceph", "mon", "dump", "-f", "json"])
    try:
        mon_dump = json.loads(out) if out else {}
    except (TypeError, json.JSONDecodeError):
        mon_dump = {}
    for mon in mon_dump.get("mons") or []:
        name = mon.get("name", "?")
        addrs = mon.get("public_addrs") or {}
        vec = addrs.get("addrvec") if isinstance(addrs, dict) else None
        if vec:
            types = {a.get("type") for a in vec}
            if "v1" in types:
                mismatches.append(f"mon.{name} still has v1 in addrvec: {vec}")
            if "v2" not in types:
                mismatches.append(f"mon.{name} missing v2 in addrvec: {vec}")
        else:
            public = str(mon.get("public_addr") or mon.get("addr") or "")
            if "v1:" in public or ":6789" in public:
                mismatches.append(f"mon.{name} public addr still has v1: {public}")
            if "v2:" not in public and ":3300" not in public:
                mismatches.append(f"mon.{name} public addr not v2: {public}")

    return mismatches


def _normalize_num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
