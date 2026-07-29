"""
NFSv4 delegation client unmount scenario (single client, rw export).

Validates DELEGRETURN when a client lazy-unmounts with an active WRITE delegation.
"""

import time

from nfs_delegation_operations import (
    create_delegation_exports,
    delegation_timing_from_config,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_nfs_cluster_for_delegation_test,
    hold_delegation_open,
    kill_delegation_holds,
    lazy_unmount_delegation_export,
    mount_delegation_export,
    read_ganesha_delegation_tailf_capture,
    release_delegation_hold_gracefully,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_close_and_return_capture,
    wait_for_delegation_path,
    write_delegation_file,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

TEST_FILE = "deleg_lifecycle_unmount.txt"


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
    export_rw = config.get("lifecycle_export", "/deleg_lifecycle_rw")
    mount_point = config.get("client_mount", "/mnt/deleg_lifecycle_c0")

    timing = delegation_timing_from_config(config)
    hold_s = timing.hold_seconds
    ready_s = timing.hold_ready_seconds
    settle_s = timing.log_settle_seconds
    path_wait_retries = timing.path_wait_retries
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "deleglifecyclegrp")
    nfs_mount = config.get("nfs_mount", "/mnt/delegation_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_delegation_boot")

    nfs_ver = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    server = nfs_node.hostname
    path = "%s/%s" % (mount_point, TEST_FILE)

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

        write_delegation_file(client, path, "seed-client_unmount\n")
        wait_for_delegation_path(
            client, path, "seed client_unmount", retries=path_wait_retries
        )

        truncate_ganesha_container_log(nfs_node, container_id)
        start_ganesha_delegation_tailf_follow(nfs_node, container_id)

        write_delegation_file(client, path, "hold-write\n")
        wait_for_delegation_path(client, path, "pre write-hold")
        hold_delegation_open(client, path, "r+b", hold_s)
        time.sleep(ready_s)
        write_delegation_file(client, path, "lifecycle-io\n", append=True)

        lazy_unmount_delegation_export(client, mount_point)
        release_delegation_hold_gracefully(client, term_wait_seconds=5)
        time.sleep(settle_s)

        stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
        validate_delegation_close_and_return_capture(
            "client_unmount",
            read_ganesha_delegation_tailf_capture(nfs_node, container_id),
        )

        return 0

    except Exception as err:
        log.error("Delegation client unmount test failed: %s", err)
        return 1

    finally:
        kill_delegation_holds(client)
        unmount_delegation_export(
            client,
            mount_point,
            remove_mount_dir=True,
            test_files=[TEST_FILE],
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
