"""NFS-Ganesha Kerberos (MIT KDC) tests."""

import traceback

from cli.exceptions import OperationFailedError
from cli.utilities.filesys import MountFailedError, Unmount
from tests.nfs.nfs_operations import mount_cleanup_retry
from tests.nfs.security.kerberos_helper import (
    DEFAULT_DOMAIN,
    DEFAULT_KDC_HOSTNAME,
    DEFAULT_REALM,
    DEFAULT_TEST_PASSWORD,
    DEFAULT_TEST_USER,
    MITKDCSetup,
)
from tests.nfs.security.krb_security_utils import (
    DEFAULT_NFS_KRB_CLUSTER,
    build_krb_mount_cmd,
    build_sys_mount_cmd,
    check_krb_mount_fails,
    check_mount_fails_without_ticket,
    cleanup_kerberos_nfs_stack,
    collect_kerberos_client_mount_paths,
    create_kerberos_export,
    delete_kerberos_export,
    deploy_kerberos_nfs_cluster,
    ensure_hosts_entries,
    isolate_client_hosts_alias,
    kdestroy_all,
    kinit_user,
    krb_config_get,
    mount_krb5,
    node_fqdn,
    prepare_client_mount_dir,
    provision_kerberos_environment,
    unmount_and_remove,
    verify_export_sectypes,
    wait_for_no_keytab_errors_in_nfs_logs,
)
from tests.nfs.security.security_utils import (
    check_mount_fails,
    wait_until_nfs_export_visible,
)
from tests.nfs.test_nfs_multiple_operations_for_upgrade import (
    create_file,
    delete_file,
    lookup_in_directory,
    read_from_file_using_dd_command,
    rename_file,
    write_to_file_using_dd_command,
)
from utility.log import Log

log = Log(__name__)

OP_KRB_INFRA_BOOTSTRAP = "krb_infra_bootstrap"
OP_KRB_DEPLOY_MOUNT_VERIFY = "krb_deploy_mount_verify"
OP_KRB_SEC_FLAVORS = "krb_sec_flavors"
OP_KRB_EXPORT_ENFORCEMENT = "krb_export_enforcement"
OP_KRB_MULTI_CLIENT = "krb_multi_client"
OP_KRB_SPN_MISMATCH = "krb_spn_mismatch"
OP_KRB_MULTI_SECTYPE = "krb_multi_sectype"
OP_KRB_FULL_WORKFLOW = "krb_full_workflow"

_KRB_ALL_OPERATIONS = [
    OP_KRB_INFRA_BOOTSTRAP,
    OP_KRB_DEPLOY_MOUNT_VERIFY,
    OP_KRB_SEC_FLAVORS,
    OP_KRB_EXPORT_ENFORCEMENT,
    OP_KRB_MULTI_CLIENT,
    OP_KRB_SPN_MISMATCH,
    OP_KRB_MULTI_SECTYPE,
]
_KRB_FULL_SEQUENCE = [OP_KRB_INFRA_BOOTSTRAP, OP_KRB_DEPLOY_MOUNT_VERIFY]

_OPS_WITH_NFS_CLUSTER = {
    OP_KRB_DEPLOY_MOUNT_VERIFY,
    OP_KRB_SEC_FLAVORS,
    OP_KRB_EXPORT_ENFORCEMENT,
    OP_KRB_MULTI_CLIENT,
    OP_KRB_SPN_MISMATCH,
    OP_KRB_MULTI_SECTYPE,
}


def _normalize_operation(name):
    if name is None:
        return None
    return str(name).strip().lower().replace("-", "_")


def _operations_to_run(config):
    raw = config.get("operation")
    if raw is None:
        raise OperationFailedError(
            "config.operation is required. Use one of: {} (or krb_full_workflow).".format(
                ", ".join(_KRB_ALL_OPERATIONS)
            )
        )
    op = _normalize_operation(raw)
    if op in (OP_KRB_FULL_WORKFLOW, "krb_full", "krb_all_in_one"):
        return list(_KRB_FULL_SEQUENCE)
    if op in _KRB_ALL_OPERATIONS:
        return [op]
    raise OperationFailedError(
        "Unknown operation {!r}. Expected one of: {}".format(
            raw, ", ".join(_KRB_ALL_OPERATIONS)
        )
    )


def _kdc_node_from_cluster(ceph_cluster, clients):
    return (
        clients[1] if len(clients) > 1 else ceph_cluster.get_nodes(role="installer")[0]
    )


def _build_kdc_setup(config, kdc_node):
    return MITKDCSetup(
        kdc_node,
        realm=krb_config_get(config, "kerberos_realm", "realm", default=DEFAULT_REALM),
        domain=krb_config_get(
            config, "kerberos_domain", "domain", default=DEFAULT_DOMAIN
        ),
        kdc_hostname=krb_config_get(
            config, "kdc_hostname", default=DEFAULT_KDC_HOSTNAME
        ),
        master_password=krb_config_get(
            config, "kdc_master_password", default="cephci-kdc-master"
        ),
        test_user=krb_config_get(config, "test_user", default=DEFAULT_TEST_USER),
        test_password=krb_config_get(
            config, "test_password", default=DEFAULT_TEST_PASSWORD
        ),
    )


def _run_basic_krb_io(client_node, mount_path, file_prefix="krb_file"):
    lookup_in_directory(client_node, mount_path)
    dd_mb = 5
    renamed = "{}_renamed".format(file_prefix)
    create_file(client_node, mount_path, file_prefix)
    write_to_file_using_dd_command(client_node, mount_path, file_prefix, dd_mb)
    read_from_file_using_dd_command(client_node, mount_path, file_prefix, dd_mb)
    rename_file(client_node, mount_path, file_prefix, renamed)
    delete_file(client_node, mount_path, renamed)


def _ensure_kerberos_environment(
    kdc_node,
    nfs_nodes,
    client_nodes,
    installer_node,
    config,
    kdc_setup=None,
):
    domain = krb_config_get(config, "kerberos_domain", "domain", default=DEFAULT_DOMAIN)
    kdc_hostname = krb_config_get(config, "kdc_hostname", default=DEFAULT_KDC_HOSTNAME)
    if kdc_setup is None:
        kdc_setup = _build_kdc_setup(config, kdc_node)
    provision_kerberos_environment(
        kdc_setup,
        kdc_node,
        nfs_nodes,
        client_nodes,
        installer_node,
        domain,
        kdc_hostname,
    )
    return kdc_setup, domain, kdc_hostname


def _deploy_kerberos_nfs_stack(
    installer_node,
    client_node,
    nfs_nodes,
    nfs_node,
    config,
    nfs_name,
):
    port = str(config.get("port", "2049"))
    deploy_kerberos_nfs_cluster(
        installer_node,
        client_node,
        nfs_nodes,
        nfs_name,
        nfs_server_hostname=nfs_node.hostname,
        port=port,
        config=config,
    )
    wait_timeout = int(config.get("post_deploy_log_wait_timeout_sec", 180))
    if not wait_for_no_keytab_errors_in_nfs_logs(
        client_node, nfs_nodes, nfs_name, timeout=wait_timeout
    ):
        if config.get("strict_krb_log_check", True):
            raise OperationFailedError(
                "NFS logs still report keytab/gssd errors after {}s".format(
                    wait_timeout
                )
            )
        log.warning("Continuing despite keytab warnings in NFS logs")


def op_krb_infra_bootstrap(
    ceph_cluster,
    kdc_node,
    nfs_nodes,
    client_nodes,
    installer_node,
    config,
):
    log.info("=== Operation: krb_infra_bootstrap ===")
    kdc_setup, _, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        client_nodes,
        installer_node,
        config,
    )
    principal = kdc_setup.test_principal
    kinit_user(client_nodes[0], principal, kdc_setup.test_password)
    log.info("Kerberos infra bootstrap OK (realm=%s)", kdc_setup.realm)
    return kdc_setup


def op_krb_deploy_mount_verify(
    ceph_cluster,
    installer_node,
    nfs_nodes,
    client_node,
    kdc_node,
    config,
    kdc_setup=None,
):
    log.info("=== Operation: krb_deploy_mount_verify ===")
    kdc_setup, domain, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        [client_node],
        installer_node,
        config,
        kdc_setup=kdc_setup,
    )

    nfs_node = nfs_nodes[0]
    nfs_fqdn = node_fqdn(nfs_node, domain)
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")
    export_path = krb_config_get(
        config, "krb_export_path", "nfs_export", default="/export_krb_0"
    )
    mount_path = krb_config_get(
        config, "krb_client_mount", "nfs_mount", default="/mnt/nfs_krb"
    )
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    sec = krb_config_get(config, "nfs_sec", "sec", default="krb5")
    sectype = krb_config_get(config, "export_sectype", "sectype", default=["krb5"])

    _deploy_kerberos_nfs_stack(
        installer_node, client_node, nfs_nodes, nfs_node, config, nfs_name
    )

    create_kerberos_export(
        client_node,
        fs_name,
        nfs_name,
        export_path,
        fs=fs_name,
        sectype=sectype,
    )

    principal = kdc_setup.test_principal
    kinit_user(client_node, principal, kdc_setup.test_password)
    prepare_client_mount_dir(client_node, mount_path)

    mount_cmd = build_krb_mount_cmd(nfs_fqdn, export_path, mount_path, version, sec=sec)
    result = check_mount_fails_without_ticket(client_node, mount_cmd)
    if result is False:
        raise OperationFailedError("Unauthenticated mount should have failed")
    elif result is None:
        raise OperationFailedError(
            "check_mount_fails_without_ticket returned None - check implementation"
        )

    kinit_user(client_node, principal, kdc_setup.test_password)
    mount_krb5(
        client_node,
        mount_path,
        version,
        port,
        nfs_fqdn,
        export_path,
        sec=sec,
    )
    _run_basic_krb_io(client_node, mount_path)

    log.info("krb_deploy_mount_verify completed successfully.")
    return kdc_setup


def op_krb_sec_flavors(
    installer_node,
    nfs_nodes,
    client_node,
    kdc_node,
    config,
    kdc_setup=None,
):
    """
    Mount and IO for each RPCSEC_GSS flavor (default krb5i, krb5p).

    Config ``sec_flavors``: list of mount/export flavors, e.g. ``["krb5i", "krb5p"]``.
    """
    log.info("=== Operation: krb_sec_flavors ===")
    kdc_setup, domain, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        [client_node],
        installer_node,
        config,
        kdc_setup=kdc_setup,
    )

    nfs_node = nfs_nodes[0]
    nfs_fqdn = node_fqdn(nfs_node, domain)
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    flavors = krb_config_get(
        config, "sec_flavors", "krb_sec_flavors", default=["krb5i", "krb5p"]
    )
    if isinstance(flavors, str):
        flavors = [flavors]

    _deploy_kerberos_nfs_stack(
        installer_node, client_node, nfs_nodes, nfs_node, config, nfs_name
    )

    principal = kdc_setup.test_principal
    export_base = krb_config_get(
        config, "krb_export_base", default="/export_krb_flavor"
    )
    mount_base = krb_config_get(
        config, "krb_client_mount_base", default="/mnt/nfs_krb_flavor"
    )

    for idx, flavor in enumerate(flavors):
        export_path = "{}_{}".format(export_base.rstrip("/"), idx)
        mount_path = "{}_{}".format(mount_base.rstrip("/"), flavor)
        log.info(
            "Testing Kerberos flavor %s export=%s mount=%s",
            flavor,
            export_path,
            mount_path,
        )

        create_kerberos_export(
            client_node,
            fs_name,
            nfs_name,
            export_path,
            fs=fs_name,
            sectype=[flavor],
        )
        wait_until_nfs_export_visible(client_node, nfs_name, export_path)

        kinit_user(client_node, principal, kdc_setup.test_password)
        prepare_client_mount_dir(client_node, mount_path)
        mount_krb5(
            client_node,
            mount_path,
            version,
            port,
            nfs_fqdn,
            export_path,
            sec=flavor,
        )
        _run_basic_krb_io(client_node, mount_path, file_prefix="krb_{}".format(flavor))
        unmount_and_remove(client_node, mount_path)
        delete_kerberos_export(client_node, nfs_name, export_path)

    log.info("krb_sec_flavors completed for %s", flavors)
    return kdc_setup


def op_krb_export_enforcement(
    installer_node,
    nfs_nodes,
    client_node,
    kdc_node,
    config,
    kdc_setup=None,
):
    """
    Verify krb-only exports reject AUTH_SYS mounts and accept ``sec=krb5*``.

    Mirrors ``op_tls_export_enforcement``: mixed export with ``sys`` + ``krb5``
    sectypes, plus krb-only export that must reject sys mounts.
    """
    log.info("=== Operation: krb_export_enforcement ===")
    kdc_setup, domain, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        [client_node],
        installer_node,
        config,
        kdc_setup=kdc_setup,
    )

    nfs_node = nfs_nodes[0]
    nfs_fqdn = node_fqdn(nfs_node, domain)
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    sec = krb_config_get(config, "nfs_sec", "sec", default="krb5")

    plain_export = krb_config_get(
        config, "plain_krb_export", "krb_plain_export", default="/export_krb_plain"
    )
    krb_only_export = krb_config_get(
        config, "krb_only_export", default="/export_krb_strict"
    )
    plain_mount = krb_config_get(
        config, "plain_krb_mount", default="/mnt/nfs_krb_plain"
    )
    krb_mount = krb_config_get(config, "krb_only_mount", default="/mnt/nfs_krb_strict")

    _deploy_kerberos_nfs_stack(
        installer_node, client_node, nfs_nodes, nfs_node, config, nfs_name
    )

    principal = kdc_setup.test_principal
    kinit_user(client_node, principal, kdc_setup.test_password)

    # Mixed export: allow AUTH_SYS and krb5 (omitting --sectype defaults to sys-only).
    create_kerberos_export(
        client_node,
        fs_name,
        nfs_name,
        plain_export,
        fs=fs_name,
        sectype=["sys", sec],
    )
    wait_until_nfs_export_visible(client_node, nfs_name, plain_export)
    prepare_client_mount_dir(client_node, plain_mount)
    mount_krb5(
        client_node,
        plain_mount,
        version,
        port,
        nfs_fqdn,
        plain_export,
        sec=sec,
    )
    lookup_in_directory(client_node, plain_mount)
    create_file(client_node, plain_mount, "plain_krb_ok")
    Unmount(client_node).unmount(plain_mount)

    # Export with krb-only sectype must reject sys mount.
    create_kerberos_export(
        client_node,
        fs_name,
        nfs_name,
        krb_only_export,
        fs=fs_name,
        sectype=[sec],
    )
    wait_until_nfs_export_visible(client_node, nfs_name, krb_only_export)
    prepare_client_mount_dir(client_node, krb_mount)

    sys_cmd = build_sys_mount_cmd(nfs_fqdn, krb_only_export, krb_mount, version)
    if not check_mount_fails(client_node, sys_cmd):
        raise MountFailedError(
            "Kerberos-only export allowed AUTH_SYS mount (expected failure)"
        )

    mount_krb5(
        client_node,
        krb_mount,
        version,
        port,
        nfs_fqdn,
        krb_only_export,
        sec=sec,
    )
    lookup_in_directory(client_node, krb_mount)
    create_file(client_node, krb_mount, "krb_strict_ok")
    unmount_and_remove(client_node, krb_mount)
    delete_kerberos_export(client_node, nfs_name, krb_only_export)
    delete_kerberos_export(client_node, nfs_name, plain_export)

    log.info("krb_export_enforcement completed successfully.")
    return kdc_setup


def op_krb_multi_client(
    installer_node,
    nfs_nodes,
    client_nodes,
    kdc_node,
    config,
    kdc_setup=None,
):
    """Two clients with user tickets mount the same krb5 export and perform IO."""
    log.info("=== Operation: krb_multi_client ===")
    if len(client_nodes) < 2:
        raise OperationFailedError(
            "krb_multi_client requires at least 2 clients (set config.clients: 2)"
        )

    kdc_setup, domain, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        client_nodes[:2],
        installer_node,
        config,
        kdc_setup=kdc_setup,
    )

    nfs_node = nfs_nodes[0]
    nfs_fqdn = node_fqdn(nfs_node, domain)
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")
    export_path = krb_config_get(
        config, "krb_export_path", "nfs_export", default="/export_krb_multi"
    )
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    sec = krb_config_get(config, "nfs_sec", "sec", default="krb5")
    mount_paths = krb_config_get(
        config,
        "krb_client_mounts",
        default=["/mnt/nfs_krb_c0", "/mnt/nfs_krb_c1"],
    )
    if len(mount_paths) < 2:
        mount_paths = ["/mnt/nfs_krb_c0", "/mnt/nfs_krb_c1"]

    _deploy_kerberos_nfs_stack(
        installer_node, client_nodes[0], nfs_nodes, nfs_node, config, nfs_name
    )

    create_kerberos_export(
        client_nodes[0],
        fs_name,
        nfs_name,
        export_path,
        fs=fs_name,
        sectype=[sec],
    )
    wait_until_nfs_export_visible(client_nodes[0], nfs_name, export_path)

    principal = kdc_setup.test_principal
    for idx, client in enumerate(client_nodes[:2]):
        mount_path = mount_paths[idx]
        kinit_user(client, principal, kdc_setup.test_password)
        prepare_client_mount_dir(client, mount_path)
        mount_krb5(
            client,
            mount_path,
            version,
            port,
            nfs_fqdn,
            export_path,
            sec=sec,
        )
        _run_basic_krb_io(client, mount_path, file_prefix="krb_mc{}".format(idx))

    for idx, client in enumerate(client_nodes[:2]):
        unmount_and_remove(client, mount_paths[idx])

    log.info("krb_multi_client completed successfully.")
    return kdc_setup


def op_krb_spn_mismatch(
    installer_node,
    nfs_nodes,
    client_node,
    kdc_node,
    config,
    kdc_setup=None,
):
    """
    Mount using a hostname alias that resolves to the NFS IP but has no ``nfs/`` SPN.

    Config ``wrong_nfs_hostname`` overrides the bogus FQDN (default ``wrongnfs.<domain>``).
    """
    log.info("=== Operation: krb_spn_mismatch ===")
    kdc_setup, domain, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        [client_node],
        installer_node,
        config,
        kdc_setup=kdc_setup,
    )

    nfs_node = nfs_nodes[0]
    nfs_fqdn = node_fqdn(nfs_node, domain)
    wrong_host = krb_config_get(
        config, "wrong_nfs_hostname", default="wrongnfs.{}".format(domain)
    )
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")
    export_path = krb_config_get(
        config, "krb_export_path", "nfs_export", default="/export_krb_spn"
    )
    wrong_mount = krb_config_get(
        config, "wrong_spn_mount", default="/mnt/nfs_krb_wrong_spn"
    )
    good_mount = krb_config_get(
        config, "krb_client_mount", "nfs_mount", default="/mnt/nfs_krb_spn_ok"
    )
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    sec = krb_config_get(config, "nfs_sec", "sec", default="krb5")

    _deploy_kerberos_nfs_stack(
        installer_node, client_node, nfs_nodes, nfs_node, config, nfs_name
    )
    create_kerberos_export(
        client_node,
        fs_name,
        nfs_name,
        export_path,
        fs=fs_name,
        sectype=[sec],
    )
    wait_until_nfs_export_visible(client_node, nfs_name, export_path)

    isolate_client_hosts_alias(
        client_node,
        nfs_node.ip_address,
        wrong_host,
        [nfs_fqdn, nfs_node.hostname],
    )
    principal = kdc_setup.test_principal
    kinit_user(client_node, principal, kdc_setup.test_password)

    wrong_cmd = build_krb_mount_cmd(
        wrong_host, export_path, wrong_mount, version, sec=sec
    )
    if not check_krb_mount_fails(client_node, wrong_cmd):
        raise MountFailedError(
            "Mount with wrong SPN hostname {} should have failed".format(wrong_host)
        )

    ensure_hosts_entries(
        client_node, nfs_node.ip_address, [nfs_fqdn, nfs_node.hostname]
    )
    prepare_client_mount_dir(client_node, good_mount)
    kinit_user(client_node, principal, kdc_setup.test_password)
    mount_krb5(
        client_node,
        good_mount,
        version,
        port,
        nfs_fqdn,
        export_path,
        sec=sec,
    )
    lookup_in_directory(client_node, good_mount)
    unmount_and_remove(client_node, good_mount)

    log.info("krb_spn_mismatch completed successfully.")
    return kdc_setup


def op_krb_multi_sectype(
    installer_node,
    nfs_nodes,
    client_node,
    kdc_node,
    config,
    kdc_setup=None,
):
    """
    Single export with multiple ``--sectype`` values; verify JSON and mount each flavor.
    """
    log.info("=== Operation: krb_multi_sectype ===")
    kdc_setup, domain, _ = _ensure_kerberos_environment(
        kdc_node,
        nfs_nodes,
        [client_node],
        installer_node,
        config,
        kdc_setup=kdc_setup,
    )

    nfs_node = nfs_nodes[0]
    nfs_fqdn = node_fqdn(nfs_node, domain)
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")
    export_path = krb_config_get(
        config, "krb_export_path", "nfs_export", default="/export_krb_multi_sec"
    )
    mount_base = krb_config_get(
        config, "krb_client_mount_base", default="/mnt/nfs_krb_msec"
    )
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    sectypes = krb_config_get(
        config,
        "export_sectypes",
        "export_sectype",
        default=["krb5", "krb5i", "krb5p"],
    )
    if isinstance(sectypes, str):
        sectypes = [sectypes]

    _deploy_kerberos_nfs_stack(
        installer_node, client_node, nfs_nodes, nfs_node, config, nfs_name
    )
    create_kerberos_export(
        client_node,
        fs_name,
        nfs_name,
        export_path,
        fs=fs_name,
        sectype=sectypes,
    )
    wait_until_nfs_export_visible(client_node, nfs_name, export_path)
    verify_export_sectypes(client_node, nfs_name, export_path, sectypes)

    principal = kdc_setup.test_principal
    prepare_client_mount_dir(client_node, mount_base)
    for flavor in sectypes:
        mount_path = "{}/{}".format(mount_base.rstrip("/"), flavor)
        kinit_user(client_node, principal, kdc_setup.test_password)
        prepare_client_mount_dir(client_node, mount_path)
        mount_krb5(
            client_node,
            mount_path,
            version,
            port,
            nfs_fqdn,
            export_path,
            sec=flavor,
        )
        _run_basic_krb_io(client_node, mount_path, file_prefix="msec_{}".format(flavor))
        unmount_and_remove(client_node, mount_path)

    log.info("krb_multi_sectype completed for %s", sectypes)
    return kdc_setup


_OP_DISPATCH = {
    OP_KRB_INFRA_BOOTSTRAP: lambda ctx: op_krb_infra_bootstrap(
        ctx["ceph_cluster"],
        ctx["kdc_node"],
        ctx["nfs_nodes"],
        ctx["clients"],
        ctx["installer"],
        ctx["config"],
    ),
    OP_KRB_DEPLOY_MOUNT_VERIFY: lambda ctx: op_krb_deploy_mount_verify(
        ctx["ceph_cluster"],
        ctx["installer"],
        ctx["nfs_nodes"],
        ctx["client_node"],
        ctx["kdc_node"],
        ctx["config"],
        kdc_setup=ctx.get("kdc_setup"),
    ),
    OP_KRB_SEC_FLAVORS: lambda ctx: op_krb_sec_flavors(
        ctx["installer"],
        ctx["nfs_nodes"],
        ctx["client_node"],
        ctx["kdc_node"],
        ctx["config"],
        kdc_setup=ctx.get("kdc_setup"),
    ),
    OP_KRB_EXPORT_ENFORCEMENT: lambda ctx: op_krb_export_enforcement(
        ctx["installer"],
        ctx["nfs_nodes"],
        ctx["client_node"],
        ctx["kdc_node"],
        ctx["config"],
        kdc_setup=ctx.get("kdc_setup"),
    ),
    OP_KRB_MULTI_CLIENT: lambda ctx: op_krb_multi_client(
        ctx["installer"],
        ctx["nfs_nodes"],
        ctx["clients"],
        ctx["kdc_node"],
        ctx["config"],
        kdc_setup=ctx.get("kdc_setup"),
    ),
    OP_KRB_SPN_MISMATCH: lambda ctx: op_krb_spn_mismatch(
        ctx["installer"],
        ctx["nfs_nodes"],
        ctx["client_node"],
        ctx["kdc_node"],
        ctx["config"],
        kdc_setup=ctx.get("kdc_setup"),
    ),
    OP_KRB_MULTI_SECTYPE: lambda ctx: op_krb_multi_sectype(
        ctx["installer"],
        ctx["nfs_nodes"],
        ctx["client_node"],
        ctx["kdc_node"],
        ctx["config"],
        kdc_setup=ctx.get("kdc_setup"),
    ),
}


def _cleanup_kerberos_test(
    client_nodes,
    mount_path,
    nfs_name,
    nfs_export_base,
    kdc_setup,
    nfs_nodes,
    config,
    extra_mounts=None,
):
    for client in client_nodes:
        kdestroy_all(client)
    for nfs_node in nfs_nodes:
        kdestroy_all(nfs_node)

    all_mounts = collect_kerberos_client_mount_paths(config)
    if extra_mounts:
        for mp in extra_mounts:
            if mp and mp not in all_mounts:
                all_mounts.append(mp)
    if mount_path and mount_path not in all_mounts:
        all_mounts.insert(0, mount_path)
    for client in client_nodes:
        for mp in sorted(all_mounts, key=len, reverse=True):
            if not mp:
                continue
            mount_cleanup_retry(client, mp)
            Unmount(client).unmount(mp)
            client.exec_command(sudo=True, cmd="rm -rf {}".format(mp), check_ec=False)

    cleanup_kerberos_nfs_stack(client_nodes[0], nfs_name)
    if kdc_setup and config.get("cleanup_kdc", False):
        kdc_setup.cleanup_kdc()


def run(ceph_cluster, **kw):
    """
    MIT Kerberos NFS tests.

    config.operation:
        krb_infra_bootstrap | krb_deploy_mount_verify | krb_sec_flavors |
        krb_export_enforcement | krb_multi_client |
        krb_spn_mismatch | krb_multi_sectype | krb_full_workflow
    """
    log.info("Starting NFS Kerberos feature tests")
    config = kw.get("config", {})
    steps = _operations_to_run(config)

    installer = ceph_cluster.get_nodes(role="installer")[0]
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")
    all_clients = ceph_cluster.get_nodes(role="client")
    no_clients = int(config.get("clients", 1))
    clients = all_clients[:no_clients]

    if not nfs_nodes or not clients:
        raise OperationFailedError("Requires at least one NFS node and one client")

    client_node = clients[0]
    kdc_node = _kdc_node_from_cluster(ceph_cluster, all_clients)
    mount_path = krb_config_get(
        config, "krb_client_mount", "nfs_mount", default="/mnt/nfs_krb"
    )
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_KRB_CLUSTER)
    nfs_export_base = krb_config_get(config, "krb_export_base", default="/export_krb")
    extra_mounts = krb_config_get(config, "krb_client_mounts", default=[])

    ctx = {
        "ceph_cluster": ceph_cluster,
        "installer": installer,
        "nfs_nodes": nfs_nodes,
        "clients": clients,
        "client_node": client_node,
        "kdc_node": kdc_node,
        "config": config,
        "kdc_setup": None,
    }

    kdc_setup = None
    try:
        for step in steps:
            ctx["kdc_setup"] = kdc_setup
            kdc_setup = _OP_DISPATCH[step](ctx)

        log.info("Kerberos operations %s completed successfully.", steps)
        return 0

    except Exception as exc:
        log.error("Kerberos test failed: %s", exc)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Kerberos test cleanup")
        try:
            if set(steps) & _OPS_WITH_NFS_CLUSTER:
                _cleanup_kerberos_test(
                    clients,
                    mount_path,
                    nfs_name,
                    nfs_export_base,
                    kdc_setup,
                    nfs_nodes,
                    config,
                    extra_mounts=extra_mounts,
                )
            elif kdc_setup and config.get("cleanup_kdc", False):
                kdc_setup.cleanup_kdc()
        except Exception as ex:
            log.error("Cleanup failed: %s", ex)
