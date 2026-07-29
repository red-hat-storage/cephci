"""
Post-OSD ODF-like topology / platform settings for standalone CephCI.

Opt-in via::

    --custom-config apply-odf-topology=true

Applies (when possible):
  - Zone-based CRUSH failure domains
  - Per-pool zone CRUSH rules (for newly named helper pools only by default)
  - cephadm container resource limits (OSD/MON/MDS)
  - SSD device class label on OSDs

msgr2 monmap (v2-only ``set-addrs``) and ``rbd_default_map_options`` are applied
via ``utility.odf_defaults`` (``apply-odf-defaults``). Pass topology ``steps``
including ``msgr2`` only for an explicit post-deploy re-apply.
"""

from __future__ import annotations

import tempfile
from typing import Any, Callable, Dict, List, Optional, Sequence

from utility.log import Log
from utility.odf_defaults import (
    APPLY_ODF_TOPOLOGY_KEY,
    apply_v2_only_mon_addrs,
    overrides_enabled,
)

LOG = Log(__name__)

# ODF-like container caps (cephadm extra_container_args)
OSD_CPUS = "2"
OSD_MEMORY = "5g"
MON_CPUS = "1"
MON_MEMORY = "2g"
MDS_CPUS = "2"
MDS_MEMORY = "6g"

# Default post-OSD steps (msgr2 belongs in bootstrap --config)
DEFAULT_TOPOLOGY_STEPS = (
    "zones",
    "crush_rules",
    "container_limits",
    "ssd_class",
)


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


def _crush(shell_fn: Callable, *parts: str) -> str:
    """Best-effort crush command (ignores non-zero for re-entrant applies)."""
    return _shell(shell_fn, "ceph", "osd", "crush", *parts, check_status=False)


def _hostnames_from_cluster(ceph_cluster) -> List[str]:
    """Return short hostnames for OSD/mon nodes suitable for CRUSH moves."""
    names: List[str] = []
    seen = set()
    for role in ("osd", "mon", "manager", "installer"):
        for node in ceph_cluster.get_nodes(role=role) or []:
            name = getattr(node, "hostname", None) or getattr(node, "shortname", None)
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    if not names:
        for node in ceph_cluster.get_nodes() or []:
            name = getattr(node, "hostname", None) or getattr(node, "shortname", None)
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    return names


def apply_zone_failure_domains(shell_fn: Callable, hosts: Sequence[str]) -> None:
    """Create region/zone CRUSH hierarchy and move hosts under zones."""
    if len(hosts) < 3:
        LOG.warning(
            "Need at least 3 hosts for zone failure domains (have %s); skipping zones",
            len(hosts),
        )
        return

    zone_hosts = list(hosts[:3])
    LOG.info("Applying zone-based CRUSH hierarchy for hosts %s", zone_hosts)

    _crush(shell_fn, "add-bucket", "region-1", "region")
    _crush(shell_fn, "move", "region-1", "root=default")
    for zone in ("zone-a", "zone-b", "zone-c"):
        _crush(shell_fn, "add-bucket", zone, "zone")
        _crush(shell_fn, "move", zone, "region=region-1")

    for host, zone in zip(zone_hosts, ("zone-a", "zone-b", "zone-c")):
        # Host bucket name in CRUSH is typically the short hostname
        short = host.split(".")[0]
        out = _crush(shell_fn, "move", short, f"zone={zone}")
        LOG.debug("crush move %s -> %s: %s", short, zone, out)


def apply_per_pool_crush_rules(shell_fn: Callable) -> None:
    """Create zone-aware replicated CRUSH rules (pools left to callers/suites)."""
    LOG.info("Creating zone-aware CRUSH rules odf-block / odf-fs-meta / odf-fs-data")
    for rule in ("odf-block", "odf-fs-meta", "odf-fs-data"):
        out = _shell(
            shell_fn,
            "ceph",
            "osd",
            "crush",
            "rule",
            "create-replicated",
            rule,
            "default",
            "zone",
            check_status=False,
        )
        LOG.debug("create-replicated %s: %s", rule, out)


def apply_container_resource_limits(shell_fn: Callable, installer_node) -> None:
    """Apply ODF-like memory/CPU limits via cephadm orch service specs."""
    LOG.info("Applying ODF-like container resource limits via orch specs")
    spec = f"""service_type: osd
service_id: all-available-devices
placement:
  host_pattern: "*"
extra_container_args:
  - "--cpus={OSD_CPUS}"
  - "--memory={OSD_MEMORY}"
---
service_type: mon
service_name: mon
placement:
  host_pattern: "*"
extra_container_args:
  - "--cpus={MON_CPUS}"
  - "--memory={MON_MEMORY}"
---
service_type: mds
service_id: cephfs
placement:
  label: mds
extra_container_args:
  - "--cpus={MDS_CPUS}"
  - "--memory={MDS_MEMORY}"
"""
    temp = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w")
    try:
        temp.write(spec)
        temp.flush()
        # installer may be CephObject (has .node) or a raw CephNode
        remote_node = getattr(installer_node, "node", installer_node)
        remote = remote_node.remote_file(sudo=True, file_name=temp.name, file_mode="w")
        remote.write(spec)
        remote.flush()
        _shell(shell_fn, "ceph", "orch", "apply", "-i", temp.name, check_status=False)
    except Exception as exc:  # noqa: BLE001
        LOG.warning(
            "Container resource limit apply failed (may OOM on small VMs): %s", exc
        )
    finally:
        temp.close()


def apply_msgr2_only(shell_fn: Callable) -> None:
    """
    Post-deploy msgr2 helper (optional topology step ``msgr2``).

    Sets monmap to v2-only addrs (ODF style) and ``rbd_default_map_options``.
    Does not set ``ms_bind_msgr1=false`` (ODF leaves that true).
    Prefer ``apply-odf-defaults`` which runs ``apply_v2_only_mon_addrs`` after deploy.
    """
    LOG.info("Configuring msgr2-only style settings (post-deploy)")
    apply_v2_only_mon_addrs(shell_fn)
    _shell(
        shell_fn,
        "ceph",
        "config",
        "set",
        "global",
        "rbd_default_map_options",
        "ms_mode=prefer-crc",
    )


def apply_ssd_device_class(shell_fn: Callable) -> None:
    """Set CRUSH device class label to ssd for all OSDs (label only)."""
    LOG.info("Setting OSD CRUSH device class to ssd (label only)")
    try:
        out = _shell(shell_fn, "ceph", "osd", "ls")
        osd_ids = out.split()
        if not osd_ids:
            LOG.warning("No OSDs found for device class update")
            return
        _shell(shell_fn, "ceph", "osd", "crush", "set-device-class", "ssd", *osd_ids)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("SSD device class apply failed: %s", exc)


def apply_odf_topology(
    ceph_cluster,
    shell_fn: Callable,
    installer_node=None,
    overrides: Optional[Dict] = None,
    steps: Optional[Sequence[str]] = None,
) -> None:
    """
    Apply ODF-like topology/platform settings when ``apply-odf-topology`` is set.

    Args:
        ceph_cluster: Ceph cluster object
        shell_fn: cephadm.shell-compatible callable
        installer_node: installer CephNode (for writing orch specs)
        overrides: custom_config_dict / config overrides
        steps: optional subset of
               ``zones``, ``crush_rules``, ``container_limits``, ``ssd_class``,
               and optionally ``msgr2`` (post-deploy; prefer bootstrap for msgr2)
    """
    if overrides is not None and not overrides_enabled(
        overrides, APPLY_ODF_TOPOLOGY_KEY
    ):
        return

    wanted = set(steps or DEFAULT_TOPOLOGY_STEPS)
    hosts = _hostnames_from_cluster(ceph_cluster)
    LOG.info("Applying ODF topology steps %s on hosts %s", sorted(wanted), hosts)

    if "zones" in wanted:
        apply_zone_failure_domains(shell_fn, hosts)
    if "crush_rules" in wanted:
        apply_per_pool_crush_rules(shell_fn)
    if "container_limits" in wanted:
        node = installer_node
        if node is None:
            nodes = ceph_cluster.get_nodes(role="installer")
            node = nodes[0] if nodes else None
        if node is None:
            LOG.warning("No installer node for container limits; skipping")
        else:
            apply_container_resource_limits(shell_fn, node)
    if "msgr2" in wanted:
        apply_msgr2_only(shell_fn)
    if "ssd_class" in wanted:
        apply_ssd_device_class(shell_fn)

    LOG.info("ODF topology apply complete")


def topology_status_snapshot(shell_fn: Callable) -> Dict[str, Any]:
    """Return a small JSON-friendly snapshot for verification/logging."""
    snap: Dict[str, Any] = {}
    try:
        snap["osd_tree"] = _shell(shell_fn, "ceph", "osd", "tree")
    except Exception as exc:  # noqa: BLE001
        snap["osd_tree_error"] = str(exc)
    try:
        snap["crush_rules"] = _shell(shell_fn, "ceph", "osd", "crush", "rule", "ls")
    except Exception as exc:  # noqa: BLE001
        snap["crush_rules_error"] = str(exc)
    try:
        snap["device_classes"] = _shell(shell_fn, "ceph", "osd", "crush", "class", "ls")
    except Exception as exc:  # noqa: BLE001
        snap["device_classes_error"] = str(exc)
    try:
        snap["mon_dump"] = _shell(shell_fn, "ceph", "mon", "dump")
    except Exception as exc:  # noqa: BLE001
        snap["mon_dump_error"] = str(exc)
    try:
        snap["rbd_default_map_options"] = _shell(
            shell_fn, "ceph", "config", "get", "global", "rbd_default_map_options"
        )
    except Exception as exc:  # noqa: BLE001
        snap["rbd_default_map_options_error"] = str(exc)
    return snap
