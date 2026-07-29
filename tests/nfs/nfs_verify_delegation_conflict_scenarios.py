"""
NFSv4 delegation conflict recall (2 clients, shared rw export).

Validates that READ/WRITE delegations are recalled when another client performs
conflicting NFS operations (SETATTR, REMOVE, RENAME, WRITE).
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
    mount_delegation_export,
    read_ganesha_delegation_tailf_capture,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_recall_ganesha_capture,
    wait_for_delegation_path,
    write_delegation_file,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

# (scenario_name, expect_recall, test_file, hold_mode: read|write)
CONFLICT_SCENARIOS = (
    ("read_setattr_recall", True, "deleg_conflict_read_setattr.txt", "read"),
    ("read_remove_recall", True, "deleg_conflict_read_remove.txt", "read"),
    ("write_rename_recall", True, "deleg_conflict_write_rename.txt", "write"),
    ("read_write_recall", True, "deleg_conflict_read_write.txt", "read"),
    ("write_setattr_recall", True, "deleg_conflict_write_setattr.txt", "write"),
    ("read_rename_recall", True, "deleg_conflict_read_rename.txt", "read"),
)
CONFLICT_TEST_FILES = [row[2] for row in CONFLICT_SCENARIOS]


def _cleanup_paths(client_a, client_b, path_a, path_b, extra_paths=()):
    for c in (client_a, client_b):
        kill_delegation_holds(c)
    time.sleep(1)
    for c in (client_a, client_b):
        for p in (path_a, path_b, *extra_paths):
            c.exec_command(sudo=True, cmd="rm -f %s" % p, check_ec=False)


def _seed_clients(client_a, client_b, path_a, path_b, scenario, path_wait_retries):
    payload = "seed-%s\n" % scenario
    for client, path, tag in ((client_a, path_a, "A"), (client_b, path_b, "B")):
        write_delegation_file(client, path, payload)
        wait_for_delegation_path(
            client, path, "client %s after seed" % tag, retries=path_wait_retries
        )


def _establish_delegation_hold(client_a, path_a, hold_mode, hold_s, ready_s):
    if hold_mode == "read":
        client_a.exec_command(sudo=True, cmd="cat %s" % path_a)
        wait_for_delegation_path(client_a, path_a, "client A pre read-hold")
        hold_delegation_open(client_a, path_a, "rb", hold_s)
    elif hold_mode == "write":
        wait_for_delegation_path(client_a, path_a, "client A pre write-hold")
        write_delegation_file(client_a, path_a, "hold-write\n")
        hold_delegation_open(client_a, path_a, "r+b", hold_s)
    else:
        raise ConfigError("Unknown hold_mode %r" % hold_mode)
    time.sleep(ready_s)


def _run_conflict_io(name, client_b, path_b):
    wait_for_delegation_path(client_b, path_b, "client B pre-conflict")

    if name == "read_setattr_recall":
        client_b.exec_command(sudo=True, cmd="chmod 600 %s" % path_b)
        return ()
    if name == "read_remove_recall":
        client_b.exec_command(sudo=True, cmd="rm -f %s" % path_b)
        return ()
    if name == "write_rename_recall":
        renamed = "%s.renamed" % path_b
        client_b.exec_command(sudo=True, cmd="mv %s %s" % (path_b, renamed))
        return (renamed,)
    if name == "read_write_recall":
        write_delegation_file(client_b, path_b, "client-b-write\n", append=True)
        return ()
    if name == "write_setattr_recall":
        client_b.exec_command(sudo=True, cmd="chmod 644 %s" % path_b)
        return ()
    if name == "read_rename_recall":
        renamed = "%s.renamed" % path_b
        client_b.exec_command(sudo=True, cmd="mv %s %s" % (path_b, renamed))
        return (renamed,)
    raise ConfigError("Unknown conflict scenario %r" % name)


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
    export_rw = config.get("conflict_export", "/deleg_conflict_rw")
    mount_a = config.get("client_a_mount", "/mnt/deleg_conflict_c0")
    mount_b = config.get("client_b_mount", "/mnt/deleg_conflict_c1")
    timing = delegation_timing_from_config(config)
    hold_s = timing.hold_seconds
    ready_s = timing.hold_ready_seconds
    exit_wait = timing.exit_wait_seconds
    settle_s = timing.log_settle_seconds
    path_wait_retries = timing.path_wait_retries
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "delegconflictgrp")
    nfs_mount = config.get("nfs_mount", "/mnt/delegation_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_delegation_boot")

    delegation_template_backup = logging_template_backup = False
    created_cluster = False
    subvol_names, container_id = [], None

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

        nfs_ver = config.get("nfs_version", "4.2")
        port = str(config.get("port", "2049"))
        server = nfs_node.hostname
        for client, mp in ((client_a, mount_a), (client_b, mount_b)):
            mount_delegation_export(client, mp, nfs_ver, port, server, export_rw)

        for scenario, expect_recall, test_file, hold_mode in CONFLICT_SCENARIOS:
            path_a, path_b = "%s/%s" % (mount_a, test_file), "%s/%s" % (
                mount_b,
                test_file,
            )
            log.info(
                "Scenario %s (%s) hold=%s expect_recall=%s",
                scenario,
                test_file,
                hold_mode,
                expect_recall,
            )

            truncate_ganesha_container_log(nfs_node, container_id)
            _seed_clients(
                client_a, client_b, path_a, path_b, scenario, path_wait_retries
            )
            log.info("Waiting %s s after seed for delegations to exit", exit_wait)
            time.sleep(exit_wait)

            start_ganesha_delegation_tailf_follow(nfs_node, container_id)
            _establish_delegation_hold(client_a, path_a, hold_mode, hold_s, ready_s)
            extra = _run_conflict_io(scenario, client_b, path_b)
            time.sleep(settle_s)
            stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
            validate_delegation_recall_ganesha_capture(
                scenario,
                read_ganesha_delegation_tailf_capture(nfs_node, container_id),
                expect_recall,
            )
            _cleanup_paths(client_a, client_b, path_a, path_b, extra)

        return 0
    except Exception as err:
        log.error("Delegation conflict scenario test failed: %s", err)
        return 1
    finally:
        for client, mp in ((client_a, mount_a), (client_b, mount_b)):
            unmount_delegation_export(
                client, mp, remove_mount_dir=True, test_files=CONFLICT_TEST_FILES
            )
            kill_delegation_holds(client)

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
