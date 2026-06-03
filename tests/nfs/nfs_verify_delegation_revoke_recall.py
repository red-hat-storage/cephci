"""
NFSv4 delegation recall (2 clients, shared rw export).

Per scenario: seed file, wait delegation_exit_wait_seconds, tail -f, IO.
Timing defaults are defined in nfs_delegation_operations (lease + log-capture margin).
Files: deleg_recall_read_read.txt, deleg_recall_write_write.txt, deleg_recall_write_then_read.txt
"""

import time

from nfs_delegation_operations import (
    DELEGATION_EXIT_WAIT_SECONDS_DEFAULT,
    DELEGATION_HOLD_READY_SECONDS_DEFAULT,
    DELEGATION_HOLD_SECONDS_DEFAULT,
    DELEGATION_LOG_SETTLE_SECONDS_DEFAULT,
    create_delegation_exports,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_nfs_cluster_for_delegation_test,
    hold_delegation_open,
    kill_delegation_holds,
    mount_delegation_export,
    read_ganesha_delegation_tailf_capture,
    restore_delegation_ganesha_templates,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_recall_ganesha_capture,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

RECALL_SCENARIOS = (
    ("read_read_no_recall", False, "deleg_recall_read_read.txt"),
    ("write_write_recall", True, "deleg_recall_write_write.txt"),
    ("rw_read_after_write_recall", True, "deleg_recall_write_then_read.txt"),
)
RECALL_TEST_FILES = [row[2] for row in RECALL_SCENARIOS]


def _q(s):
    return s.replace("'", "'\"'\"'")


def _path_exists(client, path):
    client.exec_command(sudo=True, cmd="test -f %s" % path, check_ec=False)
    return int(getattr(client, "exit_status", 1)) == 0


def _wait_path(client, path, label, retries=20):
    parent = path.rsplit("/", 1)[0] or "/"
    for n in range(1, retries + 1):
        client.exec_command(sudo=True, cmd="ls -la %s" % parent, check_ec=False)
        if _path_exists(client, path):
            log.info("%s: %s visible (attempt %d)", label, path, n)
            return
        time.sleep(1)
    raise OperationFailedError(
        "%s: %s not visible on %s" % (label, path, client.hostname)
    )


def _cleanup_paths(client_a, client_b, path_a, path_b):
    for c in (client_a, client_b):
        kill_delegation_holds(c)
    time.sleep(1)
    client_a.exec_command(sudo=True, cmd="rm -f %s" % path_a, check_ec=False)
    client_b.exec_command(sudo=True, cmd="rm -f %s" % path_b, check_ec=False)


def _seed_clients(client_a, client_b, path_a, path_b, scenario):
    payload = "seed-%s" % scenario
    for client, path, tag in ((client_a, path_a, "A"), (client_b, path_b, "B")):
        client.exec_command(
            sudo=True,
            cmd="bash -c 'echo %s > %s && sync'" % (_q(payload), path),
        )
        _wait_path(client, path, "client %s after seed" % tag)


def _run_scenario_io(name, client_a, client_b, path_a, path_b, hold_s, ready_s):
    if name == "read_read_no_recall":
        client_a.exec_command(sudo=True, cmd="cat %s" % path_a)
        _wait_path(client_a, path_a, "client A pre read-hold")
        hold_delegation_open(client_a, path_a, "rb", hold_s)
        time.sleep(ready_s)
        _wait_path(client_b, path_b, "client B pre-IO")
        client_b.exec_command(sudo=True, cmd="cat %s && stat %s" % (path_b, path_b))
        return
    if name not in ("write_write_recall", "rw_read_after_write_recall"):
        raise ConfigError("Unknown recall scenario %r" % name)
    _wait_path(client_a, path_a, "client A pre write-hold")
    client_a.exec_command(
        sudo=True,
        cmd="bash -c 'echo hold-write > %s && sync'" % path_a,
    )
    hold_delegation_open(client_a, path_a, "r+b", hold_s)
    time.sleep(ready_s)
    _wait_path(client_b, path_b, "client B pre-IO")
    if name == "write_write_recall":
        client_b.exec_command(
            sudo=True,
            cmd="bash -c 'echo client-b-write >> %s && sync'" % path_b,
        )
    else:
        client_b.exec_command(sudo=True, cmd="cat %s" % path_b)


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
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
    export_rw = config.get("recall_export", "/deleg_recall_rw")
    mount_a = config.get("client_a_mount", "/mnt/deleg_recall_c0")
    mount_b = config.get("client_b_mount", "/mnt/deleg_recall_c1")
    hold_s = int(config.get("delegation_hold_seconds", DELEGATION_HOLD_SECONDS_DEFAULT))
    ready_s = int(
        config.get(
            "delegation_hold_ready_seconds", DELEGATION_HOLD_READY_SECONDS_DEFAULT
        )
    )
    exit_wait = int(
        config.get("delegation_exit_wait_seconds", DELEGATION_EXIT_WAIT_SECONDS_DEFAULT)
    )
    settle_s = int(
        config.get(
            "delegation_log_settle_seconds", DELEGATION_LOG_SETTLE_SECONDS_DEFAULT
        )
    )
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "delegrecallgrp")
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

        for scenario, expect_recall, test_file in RECALL_SCENARIOS:
            path_a, path_b = "%s/%s" % (mount_a, test_file), "%s/%s" % (
                mount_b,
                test_file,
            )
            log.info(
                "Scenario %s (%s) expect_recall=%s", scenario, test_file, expect_recall
            )

            truncate_ganesha_container_log(nfs_node, container_id)
            _seed_clients(client_a, client_b, path_a, path_b, scenario)
            log.info("Waiting %s s after seed for delegations to exit", exit_wait)
            time.sleep(exit_wait)

            start_ganesha_delegation_tailf_follow(nfs_node, container_id)
            _run_scenario_io(
                scenario, client_a, client_b, path_a, path_b, hold_s, ready_s
            )
            time.sleep(settle_s)
            stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
            validate_delegation_recall_ganesha_capture(
                scenario,
                read_ganesha_delegation_tailf_capture(nfs_node, container_id),
                expect_recall,
            )
            _cleanup_paths(client_a, client_b, path_a, path_b)

        return 0
    except Exception as err:
        log.error("Delegation recall test failed: %s", err)
        return 1
    finally:
        for client, mp in ((client_a, mount_a), (client_b, mount_b)):
            unmount_delegation_export(
                client, mp, remove_mount_dir=True, test_files=RECALL_TEST_FILES
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
