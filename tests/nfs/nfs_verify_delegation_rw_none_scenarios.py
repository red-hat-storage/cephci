import time

from nfs_delegation_operations import (
    clamp_delegation_log_settle_seconds,
    create_delegation_exports,
    delegation_log_hits_from_lines,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_cephfs_volume_exists,
    first_nfs_container_id,
    ganesha_log_line_count,
    ganesha_log_lines_since,
    nfs4_open_delegation_types,
    redeploy_nfs_clusters,
    resolve_nfs_cmd_host,
    restore_ganesha_template_after_logging_update,
    restore_previous_template,
    run_delegation_export_client_io,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    validate_delegation_ganesha_window,
)
from nfs_operations import (
    cleanup_cluster,
    mount_retry,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from utility.log import Log

log = Log(__name__)

EXPORTS_SPEC = (
    ("/deleg_ro", "ro", "deleg_ro_mnt"),
    ("/deleg_rw", "rw", "deleg_rw_mnt"),
    ("/deleg_none", "none", "deleg_none_mnt"),
)


def run(ceph_cluster, **kw):
    """Three exports (ro/rw/none) with cluster Delegations=true; validate Ganesha delegation logs."""
    config = kw.get("config", {})
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")
    if not clients:
        raise ConfigError("This test requires at least one client node")
    if not nfs_nodes:
        raise ConfigError("This test requires at least one nfs node")
    if not installers:
        raise ConfigError("This test requires an installer node")

    installer = installers[0]
    client = clients[0]
    cephadm = CephAdm(installer).ceph
    nfs_cmd_host = resolve_nfs_cmd_host(nfs_nodes, config)

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    nfs_mount = config.get("nfs_mount", "/mnt/deleg_scen_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_deleg_scen_boot")
    subvolume_group = config.get("subvolume_group", "delegscengrp")
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    settle_seconds = clamp_delegation_log_settle_seconds(config)
    auto_create_nfs_cluster = bool(config.get("auto_create_nfs_cluster", True))
    mount_parent = config.get("deleg_mount_parent", "/mnt")
    nfs_server = nfs_nodes[0].hostname

    deleg_template_backup = False
    log_template_backup = False
    created_cluster = False
    subvol_names = []

    ensure_cephfs_volume_exists(client, fs_name)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        nfs_clusters = cephadm.nfs.cluster.ls()
        if nfs_name not in nfs_clusters:
            if not auto_create_nfs_cluster:
                raise ConfigError(
                    f"NFS cluster {nfs_name!r} missing and auto_create_nfs_cluster is false"
                )
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
            created_cluster = True
            nfs_clusters = cephadm.nfs.cluster.ls()
            if nfs_name not in nfs_clusters:
                raise OperationFailedError(
                    f"Failed to create NFS cluster {nfs_name}: {nfs_clusters}"
                )

        deleg_template_backup, log_template_backup = (
            enable_cluster_delegation_and_debug_logging(
                nfs_cmd_host,
                cephadm,
                nfs_clusters,
                installer,
                redeploy_wait,
                service_wait_timeout,
            )
        )

        subvol_names = create_delegation_exports(
            nfs_cmd_host,
            fs_name,
            nfs_name,
            subvolume_group,
            ((pseudo, deleg) for pseudo, deleg, _ in EXPORTS_SPEC),
        )

        nfs_node, container_id = first_nfs_container_id(installer, nfs_nodes, nfs_name)

        for pseudo, deleg, mnt_suffix in EXPORTS_SPEC:
            mount_point = f"{mount_parent}/{mnt_suffix}"
            client.exec_command(
                sudo=True, cmd=f"mkdir -p {mount_point}", check_ec=False
            )
            try:
                Unmount(client).unmount(mount_point)
            except Exception:
                pass
            mount_retry(client, mount_point, nfs_version, nfs_port, nfs_server, pseudo)

            io_log_start = ganesha_log_line_count(nfs_node, container_id)
            run_delegation_export_client_io(
                client, f"{mount_point}/deleg_scen_io.txt", deleg
            )

            log.info(
                "Waiting %s s after IO on %s for delegation lines in ganesha.log",
                settle_seconds,
                pseudo,
            )
            time.sleep(settle_seconds)

            window_lines = ganesha_log_lines_since(nfs_node, container_id, io_log_start)
            hits = delegation_log_hits_from_lines(window_lines)
            log.info(
                "Delegation log for %s (%s): %d new lines, %d pattern hits",
                pseudo,
                deleg,
                len(window_lines),
                len(hits),
            )
            for line in hits[-25:]:
                log.debug("%s", line)

            window_text = "\n".join(window_lines)
            ok, reason = validate_delegation_ganesha_window(deleg, window_text)
            if not ok:
                raise OperationFailedError(
                    f"Ganesha log validation failed for {pseudo} (delegations={deleg}): {reason}; "
                    f"{len(window_lines)} lines, {len(hits)} hits"
                )
            log.info(
                "Passed %s (delegations=%s); nfs4_op_open types=%s",
                pseudo,
                deleg,
                nfs4_open_delegation_types(window_text),
            )

            truncate_ganesha_container_log(nfs_node, container_id)
            try:
                Unmount(client).unmount(mount_point)
            except Exception as um_err:
                log.warning("Unmount %s: %s", mount_point, um_err)

        return 0

    except Exception as err:
        log.error("Delegation rw/ro/none scenario test failed: %s", err)
        return 1

    finally:
        for _pseudo, _deleg, mnt_suffix in EXPORTS_SPEC:
            mp = f"{mount_parent}/{mnt_suffix}"
            try:
                Unmount(client).unmount(mp)
            except Exception:
                pass
            client.exec_command(sudo=True, cmd=f"rm -rf {mp}", check_ec=False)

        teardown_delegation_exports(
            nfs_cmd_host,
            fs_name,
            nfs_name,
            subvolume_group,
            [pseudo for pseudo, _deleg, _ in EXPORTS_SPEC],
            subvol_names,
        )

        try:
            nfs_node, cid = first_nfs_container_id(installer, nfs_nodes, nfs_name)
            truncate_ganesha_container_log(nfs_node, cid)
        except Exception as trunc_err:
            log.warning("Could not truncate ganesha.log in finally: %s", trunc_err)

        try:
            restore_ganesha_template_after_logging_update(
                nfs_cmd_host, log_template_backup
            )
            restore_previous_template(nfs_cmd_host, deleg_template_backup)
            redeploy_nfs_clusters(
                cephadm,
                cephadm.nfs.cluster.ls(),
                installer,
                redeploy_wait,
                service_wait_timeout,
            )
        except Exception as restore_err:
            log.warning("Template restore failed: %s", restore_err)

        if created_cluster:
            cleanup_cluster(
                clients=[client],
                nfs_mount=nfs_mount,
                nfs_name=nfs_name,
                nfs_export=bootstrap_export,
                nfs_nodes=nfs_nodes,
            )
