"""
NFSv4 WRITE delegation grant: new file creation vs existing file reopen.

Each OPEN is validated in an isolated ganesha.log capture window. On RHCS 9.1 /
Ceph 20.2.1, new-file create may grant WRITE delegation; phase 1 records observed
types and phase 2 requires WRITE delegation on reopen of the same file.
"""

import time

from nfs_delegation_operations import (
    capture_delegation_ganesha_log_phase,
    create_delegation_exports,
    delegation_timing_from_config,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_nfs_cluster_for_delegation_test,
    hold_delegation_open,
    hold_new_file_write_open,
    kill_delegation_holds,
    log_delegation_open_types,
    mount_delegation_export,
    release_delegation_hold_gracefully,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_grant_capture,
    wait_for_delegation_path,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

NEW_FILE = "deleg_write_grant_new.txt"


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    if skip_delegation_tests_unless_supported(config):
        return 0
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")
    if not clients:
        raise ConfigError("This test requires at least one client node")
    if not nfs_nodes or not installers:
        raise ConfigError("This test requires nfs and installer nodes")

    client = clients[0]
    installer, nfs_node = installers[0], nfs_nodes[0]
    cephadm = CephAdm(installer).ceph
    nfs_cmd_host = nfs_node

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    export_rw = config.get("write_grant_export", "/deleg_write_grant_rw")
    mount_point = config.get("client_mount", "/mnt/deleg_write_grant_c0")
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "delegwritegrantgrp")
    nfs_mount = config.get("nfs_mount", "/mnt/delegation_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_delegation_boot")

    timing = delegation_timing_from_config(config)
    hold_s = timing.hold_seconds
    ready_s = timing.hold_ready_seconds
    settle_s = timing.log_settle_seconds
    exit_wait = timing.exit_wait_seconds
    path_wait_retries = timing.path_wait_retries

    nfs_ver = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    server = nfs_node.hostname
    new_path = "%s/%s" % (mount_point, NEW_FILE)

    delegation_template_backup = logging_template_backup = False
    created_cluster = False
    subvol_names = []
    container_id = None

    cephfs_utils = CephFSCommonUtils(ceph_cluster)
    if not cephfs_utils.get_fs_info(client, fs_name):
        cephfs_utils.create_fs(client, fs_name)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        nfs_clusters, created_cluster = ensure_nfs_cluster_for_delegation_test(
            cephadm,
            nfs_name,
            config,
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
                service_wait,
            )
        )
        subvol_names = create_delegation_exports(
            nfs_cmd_host, fs_name, nfs_name, subvol_group, [(export_rw, "rw")]
        )
        _, info = get_ganesha_info_from_container(installer, nfs_name, nfs_node)
        container_id = (info or {}).get("container_id")
        if not container_id:
            raise OperationFailedError("No Ganesha container for nfs.%s" % nfs_name)

        mount_delegation_export(client, mount_point, nfs_ver, port, server, export_rw)
        client.exec_command(sudo=True, cmd="rm -f %s" % new_path, check_ec=False)

        hold_duration = max(int(hold_s), int(ready_s) + int(settle_s) + 30)

        log.info("Phase 1: new file create OPEN (observe delegation_type in log)")
        new_file_hits = capture_delegation_ganesha_log_phase(
            nfs_node,
            container_id,
            settle_s,
            lambda: hold_new_file_write_open(client, new_path, hold_duration),
        )
        create_types = log_delegation_open_types(
            "write_grant_new_file_create", new_file_hits
        )
        if not create_types:
            raise OperationFailedError(
                "write_grant_new_file_create: no nfs4_op_open END lines in capture"
            )

        release_delegation_hold_gracefully(client, term_wait_seconds=5)
        wait_for_delegation_path(
            client, new_path, "new file after create", retries=path_wait_retries
        )
        log.info("Waiting %ss between phases for prior delegation to exit", exit_wait)
        time.sleep(exit_wait)

        log.info("Phase 2: reopen existing file for WRITE (expect WRITE delegation)")
        existing_hits = capture_delegation_ganesha_log_phase(
            nfs_node,
            container_id,
            settle_s,
            lambda: (
                wait_for_delegation_path(
                    client,
                    new_path,
                    "pre existing write-hold",
                    retries=path_wait_retries,
                ),
                hold_delegation_open(client, new_path, "r+b", hold_duration),
                time.sleep(ready_s),
            ),
        )
        validate_delegation_grant_capture(
            "write_grant_existing_reopen", existing_hits, "write"
        )

        release_delegation_hold_gracefully(client, term_wait_seconds=3)
        return 0

    except Exception as err:
        log.error("WRITE delegation grant scenario test failed: %s", err)
        return 1

    finally:
        kill_delegation_holds(client)
        unmount_delegation_export(
            client,
            mount_point,
            remove_mount_dir=True,
            test_files=[NEW_FILE],
        )
        if subvol_names:
            teardown_delegation_exports(
                nfs_cmd_host, fs_name, nfs_name, subvol_group, [export_rw], subvol_names
            )
        try:
            if container_id:
                stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
                truncate_ganesha_container_log(nfs_node, container_id)
        except Exception as err:
            log.warning("Ganesha log cleanup failed: %s", err)
        try:
            restore_delegation_ganesha_templates(
                nfs_cmd_host,
                delegation_template_backup,
                logging_template_backup,
                cephadm,
                installer,
                redeploy_wait,
                service_wait,
            )
        except Exception as err:
            log.warning("Template restore failed: %s", err)
        if created_cluster:
            cleanup_cluster(
                clients=[client],
                nfs_mount=nfs_mount,
                nfs_name=nfs_name,
                nfs_export=bootstrap_export,
                nfs_nodes=nfs_nodes,
            )
