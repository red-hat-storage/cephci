"""SMB Cluster Functional Level readiness checks for rolling upgrades."""

from functools import partial

from smb_ceph_smb_ctl_operations import (
    _ceph_smb_ctl,
    _prepare_ceph_auth,
    _remove_ceph_auth,
    _validate_json,
    _verify_configuration,
)
from smb_operations import check_ctdb_health, check_smb_cluster, verify_smb_service

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)

COMMANDS = (
    "clusterlevel-features",
    "clusterlevel-show",
    "clusterlevel-showall",
    "clusterlevel-upgrade",
)
ALREADY_CURRENT = "CLUSTER_LEVEL_UPGRADE_STATUS_ALREADY_CURRENT"


def _level(value):
    """Validate and normalize a functional level."""
    if not isinstance(value, dict) or any(
        type(value.get(key)) is not int or value[key] < 0 for key in ("major", "minor")
    ):
        raise AssertionError(f"Invalid cluster functional level: {value}")
    return value["major"], value["minor"]


def _verify_ranges(ranges, active):
    """Each member must advertise support for the active level."""
    if not ranges or not any(
        item.get("major") == active[0]
        and type(item.get("minor_min")) is int
        and type(item.get("minor_max")) is int
        and item["minor_min"] <= active[1] <= item["minor_max"]
        for item in ranges
    ):
        raise AssertionError(f"Active level {active} is not supported: {ranges}")


def _check_operation(ctl, operation, expected, expected_nodes):
    """Validate the current-level behavior recorded in the readiness report."""
    if operation == "help":
        output = ctl(["--help"])
        for command in COMMANDS:
            if command not in output:
                raise AssertionError(f"Missing CFL command: {command}")
            help_output = ctl([command, "--help"])
            if "usage:" not in help_output:
                raise AssertionError(f"Missing usage for {command}: {help_output}")
            if command == "clusterlevel-upgrade" and "--apply" not in help_output:
                raise AssertionError("clusterlevel-upgrade help is missing --apply")
        return

    def query(command):
        return _validate_json(ctl([command]), command)

    active = _level(query("clusterlevel-show"))
    if active != expected:
        raise AssertionError(f"Expected active CFL {expected}, got {active}")
    if operation == "active_level":
        return
    if operation == "features":
        features = query("clusterlevel-features")
        if features.get("cluster_support") is not True:
            raise AssertionError(f"Cluster support is disabled: {features}")
        if not features.get("ctdb_socket") or features.get("ctdb_protocol") != 1:
            raise AssertionError(f"Unexpected CTDB feature information: {features}")
        _verify_ranges(features.get("supported_ranges"), active)
        return

    details = query("clusterlevel-showall")
    if _level(details.get("active_level")) != active:
        raise AssertionError(f"Inconsistent active CFL: {details}")
    nodes = details.get("nodes", [])
    pnns = [node.get("pnn") for node in nodes]
    if (
        len(nodes) != expected_nodes
        or any(type(pnn) is not int or pnn < 0 for pnn in pnns)
        or len(set(pnns)) != expected_nodes
    ):
        raise AssertionError(f"Expected {expected_nodes} distinct CTDB nodes: {nodes}")
    for node in nodes:
        _verify_ranges(node.get("supported_ranges"), active)
    if (
        details.get("upgrade_possible") is not False
        or details.get("highest_level") is not None
    ):
        raise AssertionError(f"Expected an already-current cluster: {details}")
    if operation == "cluster_levels":
        return

    args = ["clusterlevel-upgrade"]
    dry_run = operation == "upgrade_dry_run"
    if not dry_run:
        args.append("--apply")
    result = _validate_json(ctl(args), " ".join(args))
    if (
        result.get("dry_run") is not dry_run
        or result.get("status") != ALREADY_CURRENT
        or _level(result.get("old_level")) != active
        or result.get("new_level") is not None
        or result.get("error_vnn") != 0
        or result.get("error_status") != ""
        or result.get("unknown_status") != ""
    ):
        raise AssertionError(f"Unexpected already-current upgrade response: {result}")
    after = _level(query("clusterlevel-show"))
    if after != active:
        raise AssertionError(
            f"Already-current upgrade changed CFL: {active} -> {after}"
        )


def run(ceph_cluster, **kw):
    """Run CFL readiness from the installer and verify all CTDB members."""
    config = kw.get("config") or {}
    operation = config.get("operation")
    supported = (
        "help",
        "features",
        "active_level",
        "cluster_levels",
        "upgrade_dry_run",
        "upgrade_already_current",
    )
    if operation not in supported:
        raise ConfigError(f"Unsupported CFL readiness operation: {operation}")
    cluster_id = config.get("smb_cluster_id")
    if not cluster_id:
        raise ConfigError("Mandatory config 'smb_cluster_id' not provided")
    nodes = ceph_cluster.get_nodes("smb")
    if not nodes:
        raise ConfigError("No SMB nodes are available")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    expected = _level(config.get("expected_level", {"major": 1, "minor": 0}))
    expected_nodes = config.get("expected_nodes", len(nodes))
    if type(expected_nodes) is not int or expected_nodes < 1:
        raise ConfigError("expected_nodes must be a positive integer")
    try:
        check_smb_cluster(installer, cluster_id)
        verify_smb_service(installer, service_name="smb")
        _verify_configuration(installer, cluster_id)
        if not check_ctdb_health(nodes, cluster_id):
            raise AssertionError("CTDB health check failed")
        # cephadm is supplied on the installer, not necessarily on SMB hosts.
        # showall validates the membership and supported levels cluster-wide.
        auth = None
        try:
            auth = _prepare_ceph_auth(installer, installer, config)
            ctl = partial(_ceph_smb_ctl, installer, cluster_id=cluster_id, auth=auth)
            log.info("Checking CFL operation %s on %s", operation, installer.hostname)
            _check_operation(ctl, operation, expected, expected_nodes)
        finally:
            _remove_ceph_auth(installer, auth)
    except Exception as error:
        log.error("SMB CFL readiness operation %s failed: %s", operation, error)
        return 1
    return 0
