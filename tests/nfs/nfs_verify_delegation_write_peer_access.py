"""
NFSv4 WRITE delegation peer access: recall while holder keeps FD open, grant after return.

Phase A: Client A holds WRITE delegation; Client B write triggers recall.
Phase B: Client A returns delegation; Client B obtains WRITE delegation.
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
    kill_delegation_holds,
    mount_delegation_export,
    release_delegation_hold_gracefully,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_grant_capture,
    validate_delegation_recall_ganesha_capture,
    wait_for_delegation_path,
    wait_for_delegreturn_in_ganesha_capture,
    write_delegation_file,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

TEST_FILE = "deleg_write_peer_access.txt"


def _seed_file(client, path, path_wait_retries):
    write_delegation_file(client, path, "seed-peer-access\n")
    wait_for_delegation_path(client, path, "seed", retries=path_wait_retries)


def _hold_write_on_a(client_a, path_a, hold_s, ready_s):
    write_delegation_file(client_a, path_a, "hold-write\n")
    wait_for_delegation_path(client_a, path_a, "client A pre write-hold")
    hold_delegation_open(client_a, path_a, "r+b", hold_s)
    time.sleep(ready_s)


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    if skip_delegation_tests_unless_supported(config):
        return 0
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")
    if len(clients) < int(config.get("clients", 2)):
        raise ConfigError("This test requires at least 2 client nodes")
    if not nfs_nodes or not installers:
        raise ConfigError("This test requires nfs and installer nodes")

    client_a, client_b = clients[0], clients[1]
    installer, nfs_node = installers[0], nfs_nodes[0]
    cephadm = CephAdm(installer).ceph
    nfs_cmd_host = nfs_node

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    export_rw = config.get("write_peer_export", "/deleg_write_peer_rw")
    mount_a = config.get("client_a_mount", "/mnt/deleg_write_peer_c0")
    mount_b = config.get("client_b_mount", "/mnt/deleg_write_peer_c1")
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "delegwritepeergrp")
    nfs_mount = config.get("nfs_mount", "/mnt/delegation_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_delegation_boot")

    timing = delegation_timing_from_config(config)
    hold_s = timing.hold_seconds
    ready_s = timing.hold_ready_seconds
    settle_s = timing.log_settle_seconds
    exit_wait = timing.exit_wait_seconds
    return_wait = timing.return_wait_seconds
    path_wait_retries = timing.path_wait_retries

    nfs_ver = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    server = nfs_node.hostname
    path_a = "%s/%s" % (mount_a, TEST_FILE)
    path_b = "%s/%s" % (mount_b, TEST_FILE)

    delegation_template_backup = logging_template_backup = False
    created_cluster = False
    subvol_names = []
    container_id = None

    cephfs_utils = CephFSCommonUtils(ceph_cluster)
    if not cephfs_utils.get_fs_info(client_a, fs_name):
        cephfs_utils.create_fs(client_a, fs_name)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        nfs_clusters, created_cluster = ensure_nfs_cluster_for_delegation_test(
            cephadm,
            nfs_name,
            config,
            client=client_a,
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

        for client, mp in ((client_a, mount_a), (client_b, mount_b)):
            mount_delegation_export(client, mp, nfs_ver, port, server, export_rw)

        _seed_file(client_a, path_a, path_wait_retries)
        wait_for_delegation_path(
            client_b,
            path_b,
            "client B sees shared file",
            retries=path_wait_retries,
        )
        log.info("Waiting %ss after seed for delegations to exit", exit_wait)
        time.sleep(exit_wait)

        log.info("Phase A: Client B write while Client A holds WRITE delegation")
        conflict_hits = capture_delegation_ganesha_log_phase(
            nfs_node,
            container_id,
            settle_s,
            lambda: (
                _hold_write_on_a(client_a, path_a, hold_s, ready_s),
                wait_for_delegation_path(
                    client_b,
                    path_b,
                    "client B pre-conflict write",
                    retries=path_wait_retries,
                ),
                write_delegation_file(
                    client_b, path_b, "client-b-conflict\n", append=True
                ),
            ),
        )
        validate_delegation_recall_ganesha_capture(
            "write_peer_conflict_while_a_holds", conflict_hits, True
        )

        log.info(
            "Phase B: Client A returns delegation; Client B obtains WRITE delegation"
        )
        release_delegation_hold_gracefully(client_a, term_wait_seconds=5)
        if not wait_for_delegreturn_in_ganesha_capture(
            nfs_node, container_id, timeout_seconds=return_wait + 30
        ):
            raise OperationFailedError(
                "write_peer_after_return: DELEGRETURN not seen after client A close"
            )
        time.sleep(2)

        hold_duration = max(int(hold_s), int(ready_s) + int(settle_s) + 30)
        grant_hits = capture_delegation_ganesha_log_phase(
            nfs_node,
            container_id,
            settle_s,
            lambda: (
                wait_for_delegation_path(
                    client_b,
                    path_b,
                    "client B pre write-hold",
                    retries=path_wait_retries,
                ),
                hold_delegation_open(client_b, path_b, "r+b", hold_duration),
                time.sleep(ready_s),
            ),
        )
        validate_delegation_grant_capture(
            "write_peer_grant_after_a_returns", grant_hits, "write"
        )

        release_delegation_hold_gracefully(client_b, term_wait_seconds=3)
        return 0

    except Exception as err:
        log.error("WRITE delegation peer access test failed: %s", err)
        return 1

    finally:
        for client in (client_a, client_b):
            kill_delegation_holds(client)
        for client, mp in ((client_a, mount_a), (client_b, mount_b)):
            unmount_delegation_export(
                client, mp, remove_mount_dir=True, test_files=[TEST_FILE]
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
                clients=[client_a],
                nfs_mount=nfs_mount,
                nfs_name=nfs_name,
                nfs_export=bootstrap_export,
                nfs_nodes=nfs_nodes,
            )
