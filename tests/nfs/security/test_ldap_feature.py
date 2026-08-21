"""NFS-Ganesha LDAP (OpenLDAP + SSSD) tests."""

import traceback

from cli.exceptions import OperationFailedError
from tests.nfs.security.ldap_helper import DEFAULT_LDAP_MOUNT, DEFAULT_NFS_LDAP_CLUSTER
from tests.nfs.security.ldap_security_utils import (
    DEFAULT_LDAP_EXPORT,
    build_ldap_setup,
    cleanup_ldap_environment,
    configure_sssd,
    ensure_ldap_nfs_stack,
    ldap_config_get,
    ldap_node_from_cluster,
    provision_ldap_environment,
    verify_group_permissions,
    verify_ldap_outage,
    verify_mapping,
    verify_performance,
    verify_user_change,
)
from utility.log import Log

log = Log(__name__)

OP_LDAP_INFRA_BOOTSTRAP = "ldap_infra_bootstrap"
OP_LDAP_VERIFY_MAPPING = "ldap_verify_mapping"
OP_LDAP_VERIFY_GROUP_PERMISSIONS = "ldap_verify_group_permissions"
OP_LDAP_VERIFY_USER_CHANGE = "ldap_verify_user_change"
OP_LDAP_VERIFY_LDAP_OUTAGE = "ldap_verify_ldap_outage"
OP_LDAP_VERIFY_PERFORMANCE = "ldap_verify_performance"
OP_LDAP_FULL_WORKFLOW = "ldap_full_workflow"

_LDAP_ALL_OPERATIONS = [
    OP_LDAP_INFRA_BOOTSTRAP,
    OP_LDAP_VERIFY_MAPPING,
    OP_LDAP_VERIFY_GROUP_PERMISSIONS,
    OP_LDAP_VERIFY_USER_CHANGE,
    OP_LDAP_VERIFY_LDAP_OUTAGE,
    OP_LDAP_VERIFY_PERFORMANCE,
]

_TEST_CASE_TO_OP = {
    "verify_mapping": OP_LDAP_VERIFY_MAPPING,
    "verify_group_permissions": OP_LDAP_VERIFY_GROUP_PERMISSIONS,
    "verify_user_change": OP_LDAP_VERIFY_USER_CHANGE,
    "verify_ldap_outage": OP_LDAP_VERIFY_LDAP_OUTAGE,
    "verify_performance": OP_LDAP_VERIFY_PERFORMANCE,
}


def _normalize_operation(name):
    if name is None:
        return None
    return str(name).strip().lower().replace("-", "_")


def _operations_to_run(config):
    raw = config.get("operation")
    if raw is None and config.get("test_case"):
        raw = _TEST_CASE_TO_OP.get(config["test_case"])
    if raw is None:
        raise OperationFailedError(
            "config.operation is required. Use one of: {} (or ldap_full_workflow). "
            "Legacy test_case is also accepted.".format(", ".join(_LDAP_ALL_OPERATIONS))
        )
    op = _normalize_operation(raw)
    if op in (OP_LDAP_FULL_WORKFLOW, "ldap_full", "ldap_all_in_one"):
        return list(_LDAP_ALL_OPERATIONS)
    if op in _LDAP_ALL_OPERATIONS:
        return [op]
    raise OperationFailedError(
        "Unknown operation {!r}. Expected one of: {}".format(
            raw, ", ".join(_LDAP_ALL_OPERATIONS)
        )
    )


def op_ldap_infra_bootstrap(ctx):
    config = ctx["config"]
    ldap_node = ctx["ldap_node"]
    ldap_setup = build_ldap_setup(config, ldap_node)
    ctx["ldap_setup"] = ldap_setup

    _, nfs_name, nfs_export, nfs_mount = provision_ldap_environment(
        ldap_setup,
        ldap_node,
        ctx["nfs_node"],
        ctx["client_node"],
        ctx["ceph_cluster"],
        config,
    )
    ctx["nfs_name"] = nfs_name
    ctx["nfs_export"] = nfs_export
    ctx["nfs_mount"] = nfs_mount
    log.info("LDAP infrastructure bootstrap completed.")
    return ldap_setup


def _ensure_ldap_test_context(ctx):
    config = ctx["config"]
    ldap_node = ctx["ldap_node"]
    ldap_setup = build_ldap_setup(config, ldap_node)
    ctx["ldap_setup"] = ldap_setup

    if not ldap_setup.is_container_running():
        log.info(
            "OpenLDAP container not running on %s; provisioning LDAP environment",
            ldap_node.hostname,
        )
        _, nfs_name, nfs_export, nfs_mount = provision_ldap_environment(
            ldap_setup,
            ldap_node,
            ctx["nfs_node"],
            ctx["client_node"],
            ctx["ceph_cluster"],
            config,
        )
        ctx["nfs_name"] = nfs_name
        ctx["nfs_export"] = nfs_export
        ctx["nfs_mount"] = nfs_mount
    else:
        ldap_ip = ldap_node.ip_address
        configure_sssd(ctx["nfs_node"], ldap_ip, ldap_setup)
        configure_sssd(ctx["client_node"], ldap_ip, ldap_setup)
        nfs_name, nfs_export, nfs_mount, _version = ensure_ldap_nfs_stack(
            ctx["ceph_cluster"],
            ctx["client_node"],
            ctx["nfs_node"],
            config,
            ldap_setup,
        )
        ctx["nfs_name"] = nfs_name
        ctx["nfs_export"] = nfs_export
        ctx["nfs_mount"] = nfs_mount

    return ldap_setup, ctx["nfs_mount"]


def op_ldap_verify_mapping(ctx):
    ldap_setup, nfs_mount = _ensure_ldap_test_context(ctx)
    verify_mapping(
        ctx["client_node"],
        ctx["nfs_node"],
        nfs_mount,
        ctx["ceph_cluster"],
        ldap_setup,
    )
    return ldap_setup


def op_ldap_verify_group_permissions(ctx):
    ldap_setup, nfs_mount = _ensure_ldap_test_context(ctx)
    verify_group_permissions(ctx["client_node"], nfs_mount, ldap_setup)
    return ldap_setup


def op_ldap_verify_user_change(ctx):
    ldap_setup, nfs_mount = _ensure_ldap_test_context(ctx)
    try:
        verify_user_change(ctx["client_node"], ctx["nfs_node"], nfs_mount, ldap_setup)
    finally:
        ldap_setup.restore_test_users()
        ldap_ip = ctx["ldap_node"].ip_address
        configure_sssd(ctx["nfs_node"], ldap_ip, ldap_setup, force=True)
        configure_sssd(ctx["client_node"], ldap_ip, ldap_setup, force=True)
    return ldap_setup


def op_ldap_verify_ldap_outage(ctx):
    ldap_setup, nfs_mount = _ensure_ldap_test_context(ctx)
    verify_ldap_outage(ctx["client_node"], ctx["nfs_node"], nfs_mount, ldap_setup)
    return ldap_setup


def op_ldap_verify_performance(ctx):
    _, nfs_mount = _ensure_ldap_test_context(ctx)
    verify_performance(ctx["client_node"], nfs_mount)
    return ctx.get("ldap_setup")


_OP_DISPATCH = {
    OP_LDAP_INFRA_BOOTSTRAP: op_ldap_infra_bootstrap,
    OP_LDAP_VERIFY_MAPPING: op_ldap_verify_mapping,
    OP_LDAP_VERIFY_GROUP_PERMISSIONS: op_ldap_verify_group_permissions,
    OP_LDAP_VERIFY_USER_CHANGE: op_ldap_verify_user_change,
    OP_LDAP_VERIFY_LDAP_OUTAGE: op_ldap_verify_ldap_outage,
    OP_LDAP_VERIFY_PERFORMANCE: op_ldap_verify_performance,
}


def run(ceph_cluster, **kw):
    """
    OpenLDAP + SSSD NFS identity mapping tests.

    config.operation:
        ldap_infra_bootstrap | ldap_verify_mapping | ldap_verify_group_permissions |
        ldap_verify_user_change | ldap_verify_ldap_outage | ldap_verify_performance |
        ldap_full_workflow
    """
    log.info("Starting NFS LDAP feature tests")
    config = kw.get("config", {})
    steps = _operations_to_run(config)

    nfs_nodes = ceph_cluster.get_nodes(role="nfs")
    clients = ceph_cluster.get_nodes(role="client")
    if not nfs_nodes or not clients:
        raise OperationFailedError("Requires at least one NFS node and one client")

    nfs_node = nfs_nodes[0]
    client_node = clients[0]
    ldap_node = ldap_node_from_cluster(ceph_cluster, clients, config)

    nfs_name = ldap_config_get(
        config, "nfs_cluster_name", default=DEFAULT_NFS_LDAP_CLUSTER
    )
    nfs_export = ldap_config_get(
        config, "ldap_export_path", default=DEFAULT_LDAP_EXPORT
    )
    nfs_mount = ldap_config_get(
        config, "ldap_client_mount", "nfs_mount", default=DEFAULT_LDAP_MOUNT
    )

    ctx = {
        "ceph_cluster": ceph_cluster,
        "nfs_node": nfs_node,
        "client_node": client_node,
        "ldap_node": ldap_node,
        "config": config,
        "ldap_setup": None,
        "nfs_name": nfs_name,
        "nfs_export": nfs_export,
        "nfs_mount": nfs_mount,
    }

    ldap_setup = None
    try:
        for step in steps:
            ctx["ldap_setup"] = ldap_setup
            ldap_setup = _OP_DISPATCH[step](ctx)

        log.info("LDAP operations %s completed successfully.", steps)
        return 0

    except Exception as exc:
        log.error("LDAP test failed: %s", exc)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("LDAP test cleanup")
        try:
            if config.get("cleanup_ldap", False):
                cleanup_ldap_environment(
                    ldap_setup,
                    client_node,
                    nfs_mount,
                    nfs_name,
                    nfs_export,
                    nfs_nodes=[nfs_node],
                )
        except Exception as ex:
            log.error("Cleanup failed: %s", ex)
