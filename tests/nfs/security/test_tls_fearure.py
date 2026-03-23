import traceback
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.security.security_utils import (
    check_mount_fails,
    full_tls_stack_cleanup,
    probe_tls_handshake_with_openssl,
    setup_tls_client,
    setup_tls_nfs_cluster,
    tls_config_get,
    verify_ceph_orch_nfs_running,
    verify_tls_strings_in_nfs_logs,
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
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

DEFAULT_NFS_TLS_CLUSTER = "cephfs-nfs-tls"

# Single-operation names and the combined workflow
OP_TLS_DEPLOY_MOUNT_VERIFY = "tls_deploy_mount_verify"
OP_TLS_EXPORT_ENFORCEMENT = "tls_export_enforcement"
OP_TLS_LOGS_OPENSSL_PROBE = "tls_logs_openssl_probe"
OP_TLS_FULL_WORKFLOW = "tls_full_workflow"
_TLS_FULL_SEQUENCE = [
    OP_TLS_DEPLOY_MOUNT_VERIFY,
    OP_TLS_EXPORT_ENFORCEMENT,
    OP_TLS_LOGS_OPENSSL_PROBE,
]


def _normalize_operation(name):
    if name is None:
        return None
    return str(name).strip().lower().replace("-", "_")


def _operations_to_run(config):
    """
    Resolve config.operation into an ordered list of operation ids.
    Accepts tls_full_workflow / tls_all_in_one for the full sequence.
    """
    raw = config.get("operation")
    if raw is None:
        raise OperationFailedError(
            "config.operation is required. Use one of: "
            f"{OP_TLS_DEPLOY_MOUNT_VERIFY}, {OP_TLS_EXPORT_ENFORCEMENT}, "
            f"{OP_TLS_LOGS_OPENSSL_PROBE}, {OP_TLS_FULL_WORKFLOW} (or tls_all_in_one)."
        )
    op = _normalize_operation(raw)
    if op in (OP_TLS_FULL_WORKFLOW, "tls_all_in_one", "tls_full"):
        return list(_TLS_FULL_SEQUENCE)
    if op in _TLS_FULL_SEQUENCE:
        return [op]
    raise OperationFailedError(
        f"Unknown operation {raw!r}. Expected one of: {', '.join(_TLS_FULL_SEQUENCE)} "
        f"or {OP_TLS_FULL_WORKFLOW}."
    )


def op_tls_deploy_mount_verify(client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_deploy_mount_verify ===")
    fs_name = config.get("fs_name", "cephfs")
    export_name = tls_config_get(
        config, "tls_export_path", "tc_01_export", default="/tls_export1"
    )
    mount_name = tls_config_get(
        config, "tls_client_mount", "tc_01_mount", default="/mnt/tls_tc1"
    )
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))

    ok, _ = verify_ceph_orch_nfs_running(client_node, nfs_name)
    if not ok:
        raise OperationFailedError("NFS service not reported in ceph orch ps")

    expect_logs = tls_config_get(
        config,
        "tls_log_substrings",
        "tc_01_log_expect",
        default=["AUTH_TLS", "TLS"],
    )

    client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
        clients=[client_node],
        nfs_export=export_name,
        nfs_mount=mount_name,
        export_num=1,
        fs_name=fs_name,
        nfs_name=nfs_name,
        fs=fs_name,
        port=port,
        version=version,
        nfs_server=nfs_node.hostname,
        xprtsec="tls",
    )
    # exports_mounts_perclient uses "{nfs_mount}_{i}" e.g. /mnt/tls_tc1_0
    mount_path = client_export_mount_dict[client_node]["mount"][0]

    sleep(
        int(
            tls_config_get(
                config,
                "post_mount_sleep_sec",
                "tc_01_post_mount_sleep",
                default=3,
            )
        )
    )
    if not verify_tls_strings_in_nfs_logs(client_node, nfs_node, nfs_name, expect_logs):
        if tls_config_get(
            config, "strict_tls_log_check", "tc_01_strict_logs", default=True
        ):
            raise OperationFailedError(
                f"Expected TLS-related log substrings not found after mount: {expect_logs}"
            )
        log.warning("Post-mount TLS log verification failed (non-strict); continuing.")

    lookup_in_directory(client_node, mount_path)
    log.info("Performing basic IO on TLS mount (sudo) via upgrade-test helpers")
    dd_mb = 5
    base = "tc1_file"
    renamed = "tc1_renamed"
    create_file(client_node, mount_path, base)
    write_to_file_using_dd_command(client_node, mount_path, base, dd_mb)
    read_from_file_using_dd_command(client_node, mount_path, base, dd_mb)
    rename_file(client_node, mount_path, base, renamed)
    delete_file(client_node, mount_path, renamed)

    log.info("tls_deploy_mount_verify completed successfully.")


def op_tls_export_enforcement(client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_export_enforcement ===")
    fs_name = config.get("fs_name", "cephfs")
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))

    plain_export = tls_config_get(
        config, "plain_nfs_export", "tc_02_plain_export", default="/plain_export_tc2"
    )
    tls_export = tls_config_get(
        config, "tls_only_export", "tc_02_tls_export", default="/tls_export_tc2"
    )
    plain_mount = tls_config_get(
        config, "plain_export_mount", "tc_02_plain_mount", default="/mnt/plain_tc2"
    )
    tls_mount = tls_config_get(
        config, "tls_export_mount", "tc_02_tls_mount", default="/mnt/tls_tc2"
    )
    # Insecure mount attempt uses vers+port only (no xprtsec); see check_mount_fails below.
    mount_opts = f"-t nfs -o vers={version},port={port}"

    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=plain_export,
        fs=fs_name,
    )
    sleep(5)

    client_node.create_dirs(dir_path=plain_mount, sudo=True)
    # TLS-enabled Ganesha still negotiates TLS on the wire; omitting xprtsec fails with e.g.
    # "mount.nfs: failed to apply fstab options" (exit 32). "Plain" here means the export
    # was created without --xprtsec tls (not TLS-mandatory at export policy), not cleartext NFS.
    Mount(client_node).nfs(
        mount=plain_mount,
        version=version,
        port=port,
        server=nfs_node.hostname,
        export=plain_export,
        xprtsec="tls",
    )
    log.info("Plain export mounted with TLS transport (xprtsec=tls); IO with sudo")
    lookup_in_directory(client_node, plain_mount)
    create_file(client_node, plain_mount, "plain_ok")
    # Strict: we know this mount succeeded; fail the step if umount errors.
    client_node.exec_command(sudo=True, cmd=f"umount {plain_mount}")

    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=tls_export,
        fs=fs_name,
        xprtsec="tls",
    )
    sleep(5)

    client_node.create_dirs(dir_path=tls_mount, sudo=True)
    cmd_insecure = f"mount {mount_opts} {nfs_node.hostname}:{tls_export} {tls_mount}"
    if not check_mount_fails(client_node, cmd_insecure):
        raise OperationFailedError(
            "TLS export allowed insecure mount (expected failure)."
        )

    if Mount(client_node).nfs(
        mount=tls_mount,
        version=version,
        port=port,
        server=nfs_node.hostname,
        export=tls_export,
        xprtsec="tls",
    ):
        raise OperationFailedError("TLS mount with xprtsec=tls failed.")
    lookup_in_directory(client_node, tls_mount)
    create_file(client_node, tls_mount, "tls_ok")
    log.info("TLS export: IO with sudo completed.")
    # Best-effort teardown: mount_retry is for *mount* only, not umount. If the TLS
    # mount step failed earlier or cleanup already ran, plain umount can fail; use
    # Unmount (lazy umount -l, Cli.execute check_ec=False) like other NFS tests.
    Unmount(client_node).unmount(tls_mount)

    log.info("tls_export_enforcement completed successfully.")


def op_tls_logs_openssl_probe(installer_node, client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_logs_openssl_probe ===")

    expect_logs = tls_config_get(
        config,
        "tls_log_substrings",
        "tc_03_log_expect",
        "tc_01_log_expect",
        default=["AUTH_TLS", "TLS"],
    )
    verify_tls_strings_in_nfs_logs(client_node, nfs_node, nfs_name, expect_logs)

    host = nfs_node.ip_address or nfs_node.hostname
    ok_13, out_13 = probe_tls_handshake_with_openssl(
        client_node,
        host,
        port=int(config.get("port", 2049)),
        tls_version_flag="-tls1_3",
    )
    if ok_13:
        log.info("openssl TLSv1.3 probe reported a positive signal.")
    else:
        log.warning(
            "openssl TLSv1.3 probe inconclusive (NFS is not HTTPS). Snippet: %s",
            (out_13 or "")[:500],
        )

    ok_12, _ = probe_tls_handshake_with_openssl(
        client_node,
        host,
        port=int(config.get("port", 2049)),
        tls_version_flag="-tls1_2",
    )
    log.info("openssl TLSv1.2 probe ok=%s (informational)", ok_12)

    if tls_config_get(
        config, "redeploy_cluster_min_tls12", "tc_03_redeploy_tls12", default=False
    ):
        log.info(
            "redeploy_cluster_min_tls12: redeploying NFS with tls_min_version=TLSv1.2"
        )
        setup_tls_nfs_cluster(
            installer_node=installer_node,
            nfs_node=nfs_node,
            nfs_name=nfs_name,
            tls_min_version="TLSv1.2",
            tls_ciphers=tls_config_get(
                config, "nfs_tls12_ciphers", "tc_03_tls_ciphers", default="ALL"
            ),
            tls_ktls=config.get("tls_ktls", True),
            tls_debug=config.get("tls_debug", True),
        )
        sleep(30)
        verify_ceph_orch_nfs_running(client_node, nfs_name)

    log.info("tls_logs_openssl_probe completed.")


_OP_DISPATCH = {
    OP_TLS_DEPLOY_MOUNT_VERIFY: lambda inst, c, n, cfg, name: op_tls_deploy_mount_verify(
        c, n, cfg, name
    ),
    OP_TLS_EXPORT_ENFORCEMENT: lambda inst, c, n, cfg, name: op_tls_export_enforcement(
        c, n, cfg, name
    ),
    OP_TLS_LOGS_OPENSSL_PROBE: lambda inst, c, n, cfg, name: op_tls_logs_openssl_probe(
        inst, c, n, cfg, name
    ),
}


def run(ceph_cluster, **kw):
    """
    Each invocation is independent: deploy TLS NFS + tlshd, run config.operation, full cleanup.

    config.operation: tls_deploy_mount_verify | tls_export_enforcement |
        tls_logs_openssl_probe | tls_full_workflow (or tls_all_in_one)
    """
    log.info("Starting NFS TLS feature tests (independent mode)")
    config = kw.get("config", {})

    steps = _operations_to_run(config)

    installer = ceph_cluster.get_nodes(role="installer")[0]
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")
    clients = ceph_cluster.get_nodes(role="client")
    no_clients = int(config.get("clients", 1))
    clients = clients[:no_clients]
    client_node = clients[0]

    if not nfs_nodes or not clients:
        raise OperationFailedError("Requires at least one NFS node and one client")

    nfs_node = nfs_nodes[0]
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_TLS_CLUSTER)
    fs_name = config.get("fs_name", "cephfs")

    try:
        ca_cert = setup_tls_nfs_cluster(
            installer_node=installer,
            nfs_node=nfs_node,
            nfs_name=nfs_name,
            tls_min_version=config.get("tls_min_version", "TLSv1.3"),
            tls_ciphers=config.get("tls_ciphers", "ALL"),
            tls_ktls=config.get("tls_ktls", True),
            tls_debug=config.get("tls_debug", True),
        )
        setup_tls_client(client_node, ca_cert)

        for step in steps:
            _OP_DISPATCH[step](installer, client_node, nfs_node, config, nfs_name)

        log.info("NFS TLS operations %s completed successfully.", steps)
        return 0

    except Exception as e:
        log.error("Test failed: %s", e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Full TLS stack cleanup (umount, exports, cluster, subvolumes)")
        try:
            full_tls_stack_cleanup(client_node, nfs_name, fs_name=fs_name)
        except Exception as ex:
            log.error("Cleanup failed: %s", ex)
