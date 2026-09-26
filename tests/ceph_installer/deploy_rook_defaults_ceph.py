"""
Post-bootstrap deploy that aligns a cephadm cluster with compact ODF/Rook.

Does not bootstrap. Suite YAML is vanilla; Rook/ODF *config* is opt-in via::

    run.py ... -c apply-odf-defaults=true

This module:

  1. Fails if OSD-role nodes have no volumes, or if OSDs already exist
  2. Applies host specs (labels + rack location on OSD roles)
  3. Applies mon (label) and v2-only set-addrs when the defaults flag is on
  4. Applies mgr (label)
  5. Applies OSDs with crush_device_class ssd (creates default~ssd)
  6. Creates .mgr_rack_ssd (take default~ssd, chooseleaf rack) and binds .mgr

Does **not** apply or re-verify ``odf_rook_defaults.yaml`` (bootstrap +
``-c apply-odf-defaults=true`` / ``-c verify-odf-defaults=true``).

Do not rewrite replicated_rule. Do not crush-move after OSDs exist.
Do **not** use ``-c apply-odf-topology=true`` with this path (do-not-use;
legacy zones conflict with rack topology).
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from utility.log import Log
from utility.odf_defaults import (
    APPLY_ODF_DEFAULTS_KEY,
    apply_v2_only_mon_addrs,
    overrides_enabled,
)
from utility.odf_topology import apply_ssd_device_class

LOG = Log(__name__)

MGR_RACK_SSD_RULE = ".mgr_rack_ssd"
DEVICE_REFRESH_SLEEP = 120
HOST_WAIT_TIMEOUT = 300
OSD_CLASS_WAIT_TIMEOUT = 300


def _role_list(node) -> List[str]:
    role = getattr(node, "role", None)
    if role is None:
        return []
    if hasattr(role, "role_list"):
        return [str(r) for r in role.role_list]
    if isinstance(role, (list, tuple)):
        return [str(r) for r in role]
    return [str(role)]


def _is_client_only(node) -> bool:
    roles = _role_list(node)
    return roles == ["client"]


def _has_osd_role(node) -> bool:
    return "osd" in _role_list(node)


def _node_lookup_name(node) -> str:
    return (
        getattr(node, "id", None) or getattr(node, "shortname", None) or node.hostname
    )


def _volume_count(node) -> int:
    volumes = getattr(node, "volume_list", None) or []
    return len(volumes)


def osd_nodes_missing_volumes(nodes: Sequence) -> List[str]:
    """Return messages for OSD-role nodes with no data disks."""
    missing = []
    for node in nodes:
        if not _has_osd_role(node):
            continue
        count = _volume_count(node)
        if count <= 0:
            name = getattr(node, "hostname", None) or _node_lookup_name(node)
            missing.append(
                f"{name}: osd role but no volumes "
                f"(roles={_role_list(node)}, volume_count={count})"
            )
    return missing


def host_specs_for_cluster(nodes: Sequence) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Build cephadm host specs: labels on non-client nodes, rack location on OSD roles.

    Returns (specs, expected_hostnames).
    """
    specs: List[Dict[str, Any]] = []
    hostnames: List[str] = []
    rack_idx = 0
    for node in nodes:
        if _is_client_only(node):
            continue
        spec: Dict[str, Any] = {
            "service_type": "host",
            "address": True,
            "labels": "apply-all-labels",
            "nodes": [_node_lookup_name(node)],
        }
        if _has_osd_role(node):
            spec["location"] = {"root": "default", "rack": f"rack{rack_idx}"}
            rack_idx += 1
        specs.append(spec)
        hostnames.append(node.hostname)
    return specs, hostnames


def _stdout(result) -> str:
    if isinstance(result, tuple):
        return (result[0] or "").strip()
    return (result or "").strip()


def _json_cmd(cephadm, *parts: str) -> Any:
    out = _stdout(cephadm.shell(args=list(parts)))
    if not out:
        return None
    return json.loads(out)


class DeployRookDefaultsError(Exception):
    """Root error for Rook/ODF-aligned cephadm deploy."""


class ClusterUnreachableError(DeployRookDefaultsError):
    """Bootstrap missing or ``ceph status`` failed."""


class MissingOsdVolumesError(DeployRookDefaultsError):
    """An OSD-role node has no data volumes in the cluster spec."""


class OsdsAlreadyPresentError(DeployRookDefaultsError):
    """Refuse host location apply because OSDs already exist."""


class HostDeployError(DeployRookDefaultsError):
    """Host spec apply or host wait failed."""


class MonDeployError(DeployRookDefaultsError):
    """Mon apply or v2-only set-addrs failed."""


class MgrDeployError(DeployRookDefaultsError):
    """Mgr apply failed."""


class CrashDeployError(DeployRookDefaultsError):
    """Crash daemon apply failed."""


class OsdDeployError(DeployRookDefaultsError):
    """OSD DriveGroup apply failed or no OSDs came up."""


class CrushDeviceClassError(DeployRookDefaultsError):
    """OSDs were not classed ``ssd`` / ``default~ssd`` missing."""


class CrushTopologyError(DeployRookDefaultsError):
    """Crush rule create, pool bind, or topology verify failed."""


class ClusterHealthError(DeployRookDefaultsError):
    """Cluster health check failed after deploy."""


def _assert_cluster_reachable(cephadm) -> None:
    try:
        out = _stdout(cephadm.shell(args=["ceph", "status"]))
    except Exception as exc:  # noqa: BLE001
        raise ClusterUnreachableError(
            f"Cluster unreachable (bootstrap first): {exc}"
        ) from exc
    if not out:
        raise ClusterUnreachableError("ceph status returned empty output")
    LOG.info("ceph status:\n%s", out)


def _assert_no_osds(cephadm) -> None:
    out = _stdout(cephadm.shell(args=["ceph", "osd", "ls"], check_status=False))
    osd_ids = out.split()
    if osd_ids:
        raise OsdsAlreadyPresentError(
            "OSDs already exist; refusing host location apply "
            f"(would require crush-move). osd ls: {osd_ids}"
        )


def _wait_for_hosts(
    cephadm, hostnames: Sequence[str], timeout: int = HOST_WAIT_TIMEOUT
) -> None:
    wanted = set(hostnames)
    deadline = time.time() + timeout
    last_names: set = set()
    while time.time() < deadline:
        raw = _stdout(cephadm.shell(args=["ceph", "orch", "host", "ls", "-f", "json"]))
        try:
            entries = json.loads(raw or "[]")
        except json.JSONDecodeError:
            entries = []
        last_names = {h.get("hostname") for h in entries if h.get("hostname")}
        missing = wanted - last_names
        if not missing:
            LOG.info("All expected hosts present: %s", sorted(wanted))
            return
        LOG.info("Waiting for hosts %s (have %s)", sorted(missing), sorted(last_names))
        time.sleep(10)
    raise HostDeployError(
        f"Hosts not in orch host ls after {timeout}s: missing {sorted(wanted - last_names)}"
    )


def _apply_hosts(orch, specs: List[Dict[str, Any]]) -> None:
    LOG.info("Applying %s host spec(s) with labels/racks", len(specs))
    try:
        orch.apply_spec({"specs": specs, "validate-spec-services": False})
    except Exception as exc:  # noqa: BLE001
        raise HostDeployError(f"host spec apply failed: {exc}") from exc


def _apply_label_service(
    service_cls, ceph_cluster, config: Dict, label: str, error_cls
) -> None:
    obj = service_cls(cluster=ceph_cluster, **config)
    try:
        obj.apply(
            {
                "command": "apply",
                "service": obj.SERVICE_NAME,
                "args": {"placement": {"label": label}},
            }
        )
    except Exception as exc:  # noqa: BLE001
        raise error_cls(
            f"{obj.SERVICE_NAME} apply (label:{label}) failed: {exc}"
        ) from exc


def _refresh_devices(cephadm, sleep_s: int = DEVICE_REFRESH_SLEEP) -> None:
    LOG.info("Refreshing device inventory (%ss)", sleep_s)
    cephadm.shell(args=["ceph", "orch", "device", "ls", "--refresh"])
    time.sleep(sleep_s)


def _apply_osds(orch) -> None:
    spec = {
        "service_type": "osd",
        "service_id": "all-available-devices",
        "placement": {"label": "osd"},
        "spec": {
            "data_devices": {"all": "true"},
            "crush_device_class": "ssd",
        },
    }
    LOG.info("Applying OSD DriveGroup with crush_device_class=ssd")
    try:
        orch.apply_spec({"specs": [spec], "validate-spec-services": True})
    except Exception as exc:  # noqa: BLE001
        raise OsdDeployError(f"OSD DriveGroup apply failed: {exc}") from exc


def _osd_ids(cephadm) -> List[str]:
    return _stdout(cephadm.shell(args=["ceph", "osd", "ls"])).split()


def _osd_classes_from_tree(cephadm) -> Dict[str, Optional[str]]:
    """Map osd.N -> device_class from osd tree JSON."""
    data = _json_cmd(cephadm, "ceph", "osd", "tree", "-f", "json") or {}
    classes: Dict[str, Optional[str]] = {}
    for node in data.get("nodes") or []:
        if node.get("type") == "osd":
            name = node.get("name") or f"osd.{node.get('id')}"
            classes[str(name)] = node.get("device_class")
    return classes


def _ensure_ssd_class(cephadm, timeout: int = OSD_CLASS_WAIT_TIMEOUT) -> None:
    deadline = time.time() + timeout
    fallback_done = False
    while time.time() < deadline:
        classes = _osd_classes_from_tree(cephadm)
        if classes and all(c == "ssd" for c in classes.values()):
            LOG.info("All OSDs have crush device class ssd: %s", classes)
            return
        LOG.info("OSD device classes (want ssd): %s", classes)
        if classes and not fallback_done:
            LOG.warning(
                "OSD spec did not class all devices as ssd; "
                "falling back to crush set-device-class"
            )
            apply_ssd_device_class(cephadm.shell)
            fallback_done = True
        time.sleep(10)
    raise CrushDeviceClassError(
        f"OSDs not classed as ssd after {timeout}s: {_osd_classes_from_tree(cephadm)}"
    )


def _crush_tree_text(cephadm, show_shadow: bool = False) -> str:
    args = ["ceph", "osd", "crush", "tree"]
    if show_shadow:
        args.append("--show-shadow")
    return _stdout(cephadm.shell(args=args, check_status=False))


def _create_mgr_rack_ssd_rule(cephadm) -> None:
    LOG.info(
        "Creating crush rule %s (root default, type rack, class ssd)",
        MGR_RACK_SSD_RULE,
    )
    result = cephadm.shell(
        args=[
            "ceph",
            "osd",
            "crush",
            "rule",
            "create-replicated",
            MGR_RACK_SSD_RULE,
            "default",
            "rack",
            "ssd",
        ],
        check_status=False,
    )
    if isinstance(result, tuple):
        out, err = result[0], result[1] if len(result) > 1 else ""
    else:
        out, err = result, ""
    combined = f"{out or ''} {err or ''}".lower()
    if "already exists" in combined or "eexist" in combined:
        LOG.info("Crush rule %s already exists", MGR_RACK_SSD_RULE)
        return
    if err and "error" in str(err).lower() and "already" not in combined:
        raise CrushTopologyError(
            f"create-replicated {MGR_RACK_SSD_RULE} failed: {out} {err}"
        )
    LOG.info("create-replicated output: %s %s", out, err)
    rules = _stdout(cephadm.shell(args=["ceph", "osd", "crush", "rule", "ls"]))
    if MGR_RACK_SSD_RULE not in rules.split():
        raise CrushTopologyError(
            f"{MGR_RACK_SSD_RULE} not in crush rule ls after create: {rules}"
        )


def _bind_pool_rule(cephadm, pool: str, rule: str) -> None:
    pools = _stdout(cephadm.shell(args=["ceph", "osd", "pool", "ls"])).split()
    if pool not in pools:
        LOG.warning("Pool %s not present; skip crush_rule bind", pool)
        return
    LOG.info("ceph osd pool set %s crush_rule %s", pool, rule)
    try:
        cephadm.shell(args=["ceph", "osd", "pool", "set", pool, "crush_rule", rule])
    except Exception as exc:  # noqa: BLE001
        raise CrushTopologyError(
            f"pool set {pool} crush_rule {rule} failed: {exc}"
        ) from exc


def _set_default_crush_rule(cephadm) -> None:
    dump = _json_cmd(
        cephadm, "ceph", "osd", "crush", "rule", "dump", MGR_RACK_SSD_RULE, "-f", "json"
    )
    if isinstance(dump, list):
        dump = dump[0] if dump else {}
    rule_id = (dump or {}).get("rule_id")
    if rule_id is None:
        raise CrushTopologyError(f"Could not resolve rule_id for {MGR_RACK_SSD_RULE}")
    LOG.info("ceph config set global osd_pool_default_crush_rule %s", rule_id)
    try:
        cephadm.shell(
            args=[
                "ceph",
                "config",
                "set",
                "global",
                "osd_pool_default_crush_rule",
                str(rule_id),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        raise CrushTopologyError(
            f"osd_pool_default_crush_rule set failed: {exc}"
        ) from exc


def _rule_steps(dump: Dict) -> List[Dict]:
    return list(dump.get("steps") or [])


def _verify_crush(cephadm, expected_racks: Sequence[str]) -> None:
    tree = _stdout(cephadm.shell(args=["ceph", "osd", "tree"]))
    LOG.info("ceph osd tree:\n%s", tree)
    shadow = _crush_tree_text(cephadm, show_shadow=True)
    LOG.info("ceph osd crush tree --show-shadow:\n%s", shadow)

    missing_racks = [r for r in expected_racks if r not in tree and r not in shadow]
    if missing_racks:
        raise CrushTopologyError(f"Missing racks in crush tree: {missing_racks}")
    if "default~ssd" not in shadow and "default~ssd" not in tree:
        # Some dumps print the class on OSD lines only
        classes = _osd_classes_from_tree(cephadm)
        if not classes or any(c != "ssd" for c in classes.values()):
            raise CrushDeviceClassError(
                "default~ssd not visible and OSDs are not class ssd"
            )
        LOG.warning("default~ssd not in tree text; OSD classes are ssd: %s", classes)

    replicated = _json_cmd(
        cephadm, "ceph", "osd", "crush", "rule", "dump", "replicated_rule", "-f", "json"
    )
    if isinstance(replicated, list):
        replicated = replicated[0] if replicated else {}
    steps = _rule_steps(replicated or {})
    choose = [s for s in steps if "type" in s and "take" not in s]
    # replicated_rule must remain host
    if any(s.get("type") == "rack" for s in choose):
        raise CrushTopologyError(
            f"replicated_rule was rewritten to rack; dump={replicated}"
        )

    mgr_rule = _json_cmd(
        cephadm, "ceph", "osd", "crush", "rule", "dump", MGR_RACK_SSD_RULE, "-f", "json"
    )
    if isinstance(mgr_rule, list):
        mgr_rule = mgr_rule[0] if mgr_rule else {}
    mgr_steps = _rule_steps(mgr_rule or {})
    take = next(
        (
            s.get("item_name") or s.get("item")
            for s in mgr_steps
            if s.get("op") == "take"
        ),
        None,
    )
    leaf = next(
        (s.get("type") for s in mgr_steps if "choose" in str(s.get("op", ""))), None
    )
    if take not in ("default~ssd", "default"):
        # create-replicated with class ssd should take default~ssd
        LOG.info("mgr rule steps: %s", mgr_steps)
        if take != "default~ssd":
            raise CrushTopologyError(
                f"{MGR_RACK_SSD_RULE} take step is {take!r}, expected default~ssd"
            )
    if leaf != "rack":
        raise CrushTopologyError(
            f"{MGR_RACK_SSD_RULE} chooseleaf type is {leaf!r}, expected rack"
        )

    crush_rule = _stdout(
        cephadm.shell(args=["ceph", "osd", "pool", "get", ".mgr", "crush_rule"])
    )
    if MGR_RACK_SSD_RULE not in crush_rule:
        raise CrushTopologyError(
            f".mgr crush_rule is {crush_rule!r}, expected {MGR_RACK_SSD_RULE}"
        )
    LOG.info(
        "Crush verify ok: racks=%s .mgr -> %s", list(expected_racks), MGR_RACK_SSD_RULE
    )


def run(ceph_cluster, **kwargs) -> int:
    """Deploy racks, mon/mgr/OSD, and ODF-like .mgr crush rule after bootstrap."""
    from ceph.ceph_admin import CephAdmin
    from ceph.ceph_admin.crash import Crash
    from ceph.ceph_admin.helper import get_cluster_state
    from ceph.ceph_admin.mgr import Mgr
    from ceph.ceph_admin.mon import Mon
    from ceph.ceph_admin.orch import Orch

    config = dict(kwargs.get("config") or {})
    overrides = kwargs.get("test_data", {}).get("custom_config_dict") or {}
    config.setdefault("overrides", overrides)

    skip_osd = bool(config.get("skip_osd"))
    skip_crush_rules = bool(config.get("skip_crush_rules"))
    apply_crash = bool(config.get("apply_crash"))
    set_default_rule = bool(config.get("osd_pool_default_crush_rule", True))
    apply_defaults = overrides_enabled(overrides, APPLY_ODF_DEFAULTS_KEY)

    build = config.get("build", config.get("rhbuild"))
    if build:
        ceph_cluster.rhcs_version = build

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    orch = Orch(cluster=ceph_cluster, **config)

    try:
        _assert_cluster_reachable(cephadm)

        nodes = ceph_cluster.get_nodes() or []
        missing = osd_nodes_missing_volumes(nodes)
        if missing:
            for msg in missing:
                LOG.error(msg)
            raise MissingOsdVolumesError(
                "OSD-role nodes require volumes: " + "; ".join(missing)
            )

        _assert_no_osds(cephadm)

        specs, hostnames = host_specs_for_cluster(nodes)
        if not specs:
            raise HostDeployError("No non-client nodes to add as hosts")
        LOG.info("Host specs: %s", specs)
        _apply_hosts(orch, specs)
        _wait_for_hosts(cephadm, hostnames)

        _apply_label_service(Mon, ceph_cluster, config, "mon", MonDeployError)
        if apply_defaults:
            LOG.info(
                "Re-applying v2-only mon addrs "
                "(--custom-config apply-odf-defaults=true)"
            )
            failures = apply_v2_only_mon_addrs(cephadm.shell)
            if failures:
                raise MonDeployError(
                    "v2-only mon set-addrs failed: " + "; ".join(failures)
                )

        _apply_label_service(Mgr, ceph_cluster, config, "mgr", MgrDeployError)

        if apply_crash:
            _apply_label_service(Crash, ceph_cluster, config, "crash", CrashDeployError)

        expected_racks = [
            spec["location"]["rack"] for spec in specs if spec.get("location")
        ]

        if not skip_osd:
            _refresh_devices(cephadm)
            _apply_osds(orch)
            if not _osd_ids(cephadm):
                raise OsdDeployError("OSD apply finished but osd ls is empty")
            _ensure_ssd_class(cephadm)

            if not skip_crush_rules:
                _create_mgr_rack_ssd_rule(cephadm)
                _bind_pool_rule(cephadm, ".mgr", MGR_RACK_SSD_RULE)
                _bind_pool_rule(cephadm, "device_health_metrics", MGR_RACK_SSD_RULE)
                if set_default_rule:
                    _set_default_crush_rule(cephadm)
                _verify_crush(cephadm, expected_racks)
        elif expected_racks:
            LOG.info("skip_osd set; not creating crush rules that need default~ssd")

        # odf_rook_defaults.yaml is applied at bootstrap via -c apply-odf-defaults.
        # This module does not re-check that profile (use -c verify-odf-defaults
        # on test_cephadm if you want config-dump vs YAML). It does verify crush
        # topology it created (racks, ssd class, .mgr_rack_ssd).

        if config.get("verify_cluster_health", True):
            rc = ceph_cluster.check_health(
                rhbuild=build, client=cephadm.installer, timeout=600
            )
            if rc:
                raise ClusterHealthError("Cluster health check failed")

    except DeployRookDefaultsError as exc:
        LOG.error("%s: %s", type(exc).__name__, exc, exc_info=True)
        try:
            get_cluster_state(cephadm)
        except Exception:  # noqa: BLE001
            LOG.debug("Could not gather cluster state")
        return 1
    except Exception as exc:
        LOG.error("deploy_rook_defaults_ceph failed: %s", exc, exc_info=True)
        try:
            get_cluster_state(cephadm)
        except Exception:  # noqa: BLE001
            LOG.debug("Could not gather cluster state")
        return 1

    return 0
