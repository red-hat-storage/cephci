"""
NFSv4 delegation recall and revoke (2 clients, shared rw export).

Recall: read/read (no recall), write/write, write-then-read.
Revoke: client returns delegation before peer IO (no revoke); peer write while
client A still holds an open write delegation (recall then revoke/reclaim path).

Note: ``revoke_after_recall_on_conflict`` keeps client A's hold OPEN during peer
write; it does not simulate a client that ignores DELEGRECALL until lease expiry.
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
    release_delegation_hold_gracefully,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_recall_ganesha_capture,
    validate_delegation_revoke_ganesha_capture,
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

RECALL_REVOKE_SCENARIOS = (
    ("read_read_no_recall", "recall", False, "deleg_recall_read_read.txt"),
    ("write_write_recall", "recall", True, "deleg_recall_write_write.txt"),
    ("rw_read_after_write_recall", "recall", True, "deleg_recall_write_then_read.txt"),
    (
        "revoke_io_completes_before_timeout",
        "revoke",
        False,
        "deleg_revoke_io_done.txt",
    ),
    (
        "revoke_after_recall_on_conflict",
        "revoke",
        True,
        "deleg_revoke_after_recall.txt",
    ),
)
SCENARIO_TEST_FILES = [row[3] for row in RECALL_REVOKE_SCENARIOS]


def _cleanup_paths(client_a, client_b, path_a, path_b):
    for c in (client_a, client_b):
        kill_delegation_holds(c)
    time.sleep(1)
    client_a.exec_command(sudo=True, cmd="rm -f %s" % path_a, check_ec=False)
    client_b.exec_command(sudo=True, cmd="rm -f %s" % path_b, check_ec=False)


def _seed_clients(client_a, client_b, path_a, path_b, scenario, path_wait_retries):
    payload = "seed-%s\n" % scenario
    for client, path, tag in ((client_a, path_a, "A"), (client_b, path_b, "B")):
        write_delegation_file(client, path, payload)
        wait_for_delegation_path(
            client, path, "client %s after seed" % tag, retries=path_wait_retries
        )


def _client_a_hold_write_then_return_delegation(client_a, path_a, ready_s, hold_s):
    write_delegation_file(client_a, path_a, "deleg-hold\n")
    hold_duration = max(int(hold_s), int(ready_s) + 20)
    hold_delegation_open(client_a, path_a, "r+b", hold_duration)
    time.sleep(ready_s)
    release_delegation_hold_gracefully(client_a, term_wait_seconds=5)


def _run_revoke_io_completes_before_timeout(
    client_a,
    client_b,
    path_a,
    path_b,
    nfs_node,
    container_id,
    ready_s,
    hold_s,
    return_wait,
    settle_s,
):
    wait_for_delegation_path(client_a, path_a, "client A pre write-hold")
    start_ganesha_delegation_tailf_follow(nfs_node, container_id)
    _client_a_hold_write_then_return_delegation(client_a, path_a, ready_s, hold_s)
    if not wait_for_delegreturn_in_ganesha_capture(
        nfs_node, container_id, timeout_seconds=return_wait + 30
    ):
        stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
        raise OperationFailedError(
            "revoke_io_completes_before_timeout: DELEGRETURN not seen after client A close"
        )
    time.sleep(2)
    stop_ganesha_delegation_tailf_follow(nfs_node, container_id)

    truncate_ganesha_container_log(nfs_node, container_id)
    start_ganesha_delegation_tailf_follow(nfs_node, container_id)
    time.sleep(return_wait)
    wait_for_delegation_path(client_b, path_b, "client B pre-conflict write")
    write_delegation_file(client_b, path_b, "peer-after-deleg-return\n", append=True)
    time.sleep(settle_s)
    stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
    validate_delegation_revoke_ganesha_capture(
        "revoke_io_completes_before_timeout",
        read_ganesha_delegation_tailf_capture(nfs_node, container_id),
        False,
        require_delegreturn=False,
    )


def _hold_write_delegation_on_a(client_a, path_a, hold_s, ready_s, pre_hold_label):
    wait_for_delegation_path(client_a, path_a, pre_hold_label)
    write_delegation_file(client_a, path_a, "hold-write\n")
    hold_delegation_open(client_a, path_a, "r+b", hold_s)
    time.sleep(ready_s)


def _io_read_read_no_recall(client_a, client_b, path_a, path_b, hold_s, ready_s):
    client_a.exec_command(sudo=True, cmd="cat %s" % path_a)
    wait_for_delegation_path(client_a, path_a, "client A pre read-hold")
    hold_delegation_open(client_a, path_a, "rb", hold_s)
    time.sleep(ready_s)
    wait_for_delegation_path(client_b, path_b, "client B pre-IO")
    client_b.exec_command(sudo=True, cmd="cat %s && stat %s" % (path_b, path_b))


def _io_write_write_recall(client_a, client_b, path_a, path_b, hold_s, ready_s):
    _hold_write_delegation_on_a(
        client_a, path_a, hold_s, ready_s, "client A pre write-hold"
    )
    wait_for_delegation_path(client_b, path_b, "client B pre-IO")
    write_delegation_file(client_b, path_b, "client-b-write\n", append=True)


def _io_rw_read_after_write_recall(client_a, client_b, path_a, path_b, hold_s, ready_s):
    _hold_write_delegation_on_a(
        client_a, path_a, hold_s, ready_s, "client A pre write-hold"
    )
    wait_for_delegation_path(client_b, path_b, "client B pre-IO")
    client_b.exec_command(sudo=True, cmd="cat %s" % path_b)


def _io_revoke_after_recall_on_conflict(
    client_a, client_b, path_a, path_b, hold_s, ready_s
):
    wait_for_delegation_path(
        client_a, path_a, "client A pre write-hold (recall on conflict)"
    )
    write_delegation_file(client_a, path_a, "hold-unresponsive\n")
    hold_delegation_open(client_a, path_a, "r+b", hold_s)
    time.sleep(ready_s)
    wait_for_delegation_path(client_b, path_b, "client B pre-conflict write")
    write_delegation_file(client_b, path_b, "client-b-write\n", append=True)


SCENARIO_IO_HANDLERS = {
    "read_read_no_recall": _io_read_read_no_recall,
    "write_write_recall": _io_write_write_recall,
    "rw_read_after_write_recall": _io_rw_read_after_write_recall,
    "revoke_after_recall_on_conflict": _io_revoke_after_recall_on_conflict,
}


def _run_scenario_io(name, client_a, client_b, path_a, path_b, hold_s, ready_s):
    handler = SCENARIO_IO_HANDLERS.get(name)
    if not handler:
        raise ConfigError("Unknown recall/revoke scenario %r" % name)
    handler(client_a, client_b, path_a, path_b, hold_s, ready_s)


def _validate_scenario_capture(scenario, kind, expect_positive, hits):
    if kind == "recall":
        validate_delegation_recall_ganesha_capture(scenario, hits, expect_positive)
    else:
        validate_delegation_revoke_ganesha_capture(scenario, hits, expect_positive)


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
    export_rw = config.get("recall_export", "/deleg_recall_rw")
    mount_a = config.get("client_a_mount", "/mnt/deleg_recall_c0")
    mount_b = config.get("client_b_mount", "/mnt/deleg_recall_c1")
    timing = delegation_timing_from_config(config)
    hold_s = timing.hold_seconds
    ready_s = timing.hold_ready_seconds
    return_wait = timing.return_wait_seconds
    exit_wait = timing.exit_wait_seconds
    settle_s = timing.log_settle_seconds
    path_wait_retries = timing.path_wait_retries
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

        for scenario, kind, expect_positive, test_file in RECALL_REVOKE_SCENARIOS:
            path_a, path_b = "%s/%s" % (mount_a, test_file), "%s/%s" % (
                mount_b,
                test_file,
            )
            log.info(
                "Scenario %s (%s) kind=%s expect_positive=%s",
                scenario,
                test_file,
                kind,
                expect_positive,
            )

            truncate_ganesha_container_log(nfs_node, container_id)
            _seed_clients(
                client_a, client_b, path_a, path_b, scenario, path_wait_retries
            )
            log.info("Waiting %s s after seed for delegations to exit", exit_wait)
            time.sleep(exit_wait)

            if scenario == "revoke_io_completes_before_timeout":
                _run_revoke_io_completes_before_timeout(
                    client_a,
                    client_b,
                    path_a,
                    path_b,
                    nfs_node,
                    container_id,
                    ready_s,
                    hold_s,
                    return_wait,
                    settle_s,
                )
            else:
                start_ganesha_delegation_tailf_follow(nfs_node, container_id)
                _run_scenario_io(
                    scenario, client_a, client_b, path_a, path_b, hold_s, ready_s
                )
                time.sleep(settle_s)
                stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
                _validate_scenario_capture(
                    scenario,
                    kind,
                    expect_positive,
                    read_ganesha_delegation_tailf_capture(nfs_node, container_id),
                )
            _cleanup_paths(client_a, client_b, path_a, path_b)

        return 0
    except Exception as err:
        log.error("Delegation recall/revoke test failed: %s", err)
        return 1
    finally:
        for client, mp in ((client_a, mount_a), (client_b, mount_b)):
            unmount_delegation_export(
                client, mp, remove_mount_dir=True, test_files=SCENARIO_TEST_FILES
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
