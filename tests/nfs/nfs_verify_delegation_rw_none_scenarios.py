import time

from nfs_delegation_operations import (
    DELEGATION_LOG_SETTLE_SECONDS_DEFAULT,
    create_delegation_exports,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_nfs_cluster_for_delegation_test,
    mount_delegation_export,
    parse_nfs4_open_delegation_types,
    read_ganesha_delegation_tailf_capture,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_ganesha_window,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

EXPORTS_SPEC = (
    ("/deleg_ro", "ro", "deleg_ro_mnt"),
    ("/deleg_rw", "rw", "deleg_rw_mnt"),
    ("/deleg_none", "none", "deleg_none_mnt"),
)


def _run_delegation_export_client_io(client, testfile, delegation_mode):
    if delegation_mode == "none":
        client.exec_command(
            sudo=True,
            cmd="bash -c 'echo none-deleg > %s && sync'" % testfile,
        )
        client.exec_command(sudo=True, cmd="cat %s" % testfile)
    elif delegation_mode == "ro":
        client.exec_command(
            sudo=True,
            cmd="bash -c 'echo ro-deleg > %s && sync'" % testfile,
        )
        client.exec_command(sudo=True, cmd="cat %s" % testfile)
        client.exec_command(sudo=True, cmd="stat %s" % testfile)
    elif delegation_mode == "rw":
        client.exec_command(
            sudo=True,
            cmd="bash -c 'echo rw-deleg > %s && sync'" % testfile,
        )
        client.exec_command(
            sudo=True,
            cmd="bash -c 'echo append >> %s && sync'" % testfile,
        )
        client.exec_command(sudo=True, cmd="cat %s" % testfile)
    else:
        raise ConfigError(
            "Unsupported delegations mode for client IO: %r" % delegation_mode
        )


def run(ceph_cluster, **kw):
    """Three exports (ro/rw/none) with cluster Delegations=true; validate Ganesha delegation logs."""
    config = kw.get("config", {})
    if skip_delegation_tests_unless_supported(config):
        return 0
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
    nfs_cmd_host = nfs_nodes[0]

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    nfs_mount = config.get("nfs_mount", "/mnt/deleg_scen_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_deleg_scen_boot")
    subvolume_group = config.get("subvolume_group", "delegscengrp")
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    settle_seconds = int(
        config.get(
            "delegation_log_settle_seconds", DELEGATION_LOG_SETTLE_SECONDS_DEFAULT
        )
    )
    auto_create_nfs_cluster = bool(config.get("auto_create_nfs_cluster", True))
    mount_parent = config.get("deleg_mount_parent", "/mnt")
    nfs_server = nfs_nodes[0].hostname

    delegation_template_backup = False
    logging_template_backup = False
    created_cluster = False
    subvol_names = []
    nfs_node = nfs_nodes[0]
    container_id = None

    cephfs_utils = CephFSCommonUtils(ceph_cluster)
    if not cephfs_utils.get_fs_info(client, fs_name):
        cephfs_utils.create_fs(client, fs_name)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        if auto_create_nfs_cluster:
            config_with_auto = dict(config)
            config_with_auto["auto_create_nfs_cluster"] = True
        else:
            config_with_auto = config
        nfs_clusters, created_cluster = ensure_nfs_cluster_for_delegation_test(
            cephadm,
            nfs_name,
            config_with_auto,
            client=client,
            nfs_nodes=nfs_nodes,
            installer=installer,
            ceph_cluster=ceph_cluster,
        )

        delegation_template_backup, logging_template_backup = (
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
            ((pseudo, delegation_mode) for pseudo, delegation_mode, _ in EXPORTS_SPEC),
        )

        _, info = get_ganesha_info_from_container(installer, nfs_name, nfs_node)
        container_id = (info or {}).get("container_id")
        if not container_id:
            raise OperationFailedError(
                "No Ganesha container found for nfs.%s" % nfs_name
            )

        for pseudo, delegation_mode, mnt_suffix in EXPORTS_SPEC:
            mount_point = "%s/%s" % (mount_parent, mnt_suffix)
            truncate_ganesha_container_log(nfs_node, container_id)
            mount_delegation_export(
                client, mount_point, nfs_version, nfs_port, nfs_server, pseudo
            )

            start_ganesha_delegation_tailf_follow(nfs_node, container_id)
            _run_delegation_export_client_io(
                client, "%s/deleg_scen_io.txt" % mount_point, delegation_mode
            )
            log.info(
                "Waiting %s s after IO on %s for delegation lines in ganesha.log",
                settle_seconds,
                pseudo,
            )
            time.sleep(settle_seconds)
            stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
            hits = read_ganesha_delegation_tailf_capture(nfs_node, container_id)
            if not hits:
                raise OperationFailedError(
                    "Empty ganesha.log tail capture for %s (delegations=%s)"
                    % (pseudo, delegation_mode)
                )
            log.info(
                "Delegation log for %s (%s): %d tail -f capture lines",
                pseudo,
                delegation_mode,
                len(hits),
            )
            for line in hits[-25:]:
                log.debug("%s", line)

            window_text = "\n".join(hits)
            ok, reason = validate_delegation_ganesha_window(
                delegation_mode, window_text
            )
            if not ok:
                raise OperationFailedError(
                    "Ganesha log validation failed for %s (delegations=%s): %s; "
                    "%s capture lines" % (pseudo, delegation_mode, reason, len(hits))
                )
            log.info(
                "Passed %s (delegations=%s); nfs4_op_open types=%s",
                pseudo,
                delegation_mode,
                parse_nfs4_open_delegation_types(window_text),
            )

            unmount_delegation_export(client, mount_point)

        return 0

    except Exception as err:
        log.error("Delegation rw/ro/none scenario test failed: %s", err)
        return 1

    finally:
        for _pseudo, _delegation_mode, mnt_suffix in EXPORTS_SPEC:
            unmount_delegation_export(
                client, "%s/%s" % (mount_parent, mnt_suffix), remove_mount_dir=True
            )

        teardown_delegation_exports(
            nfs_cmd_host,
            fs_name,
            nfs_name,
            subvolume_group,
            [pseudo for pseudo, _delegation_mode, _ in EXPORTS_SPEC],
            subvol_names,
        )

        try:
            if container_id:
                truncate_ganesha_container_log(nfs_node, container_id)
        except Exception as trunc_err:
            log.warning("Could not truncate ganesha.log in finally: %s", trunc_err)

        try:
            restore_delegation_ganesha_templates(
                nfs_cmd_host,
                delegation_template_backup,
                logging_template_backup,
                cephadm,
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
