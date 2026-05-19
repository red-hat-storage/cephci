"""
NFSv4 delegation revoke and recall scenarios (multi-client).

Requires at least two client nodes on a shared rw export:

- read-read: both clients read; read delegations granted, no recall.
- write-write / write-then-read: client A holds write deleg; client B IO triggers
  Recalling delegation and successfully recalled in Ganesha logs.
"""

from nfs_delegation_operations import (
    clamp_delegation_log_settle_seconds,
    create_delegation_recall_export,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_cephfs_volume_exists,
    first_nfs_container_id,
    kill_delegation_hold_processes,
    redeploy_nfs_clusters,
    remove_shared_test_file,
    resolve_nfs_cmd_host,
    restore_ganesha_template_after_logging_update,
    restore_previous_template,
    run_recall_scenario_delegation_io,
    seed_shared_recall_file,
    start_ganesha_deleg_tailf_follow,
    stop_ganesha_deleg_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    validate_delegation_recall_capture,
    validate_no_delegation_recall_capture,
    wait_settle_then_stop_tailf_capture,
)
from nfs_operations import mount_retry

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError
from cli.utilities.filesys import Unmount
from utility.log import Log

log = Log(__name__)

RECALL_SCENARIOS = (
    (
        "read_read_no_recall",
        "/deleg_recall_rw",
        "rw",
        "Client0 read-open hold; client1 reads same file (both get read deleg, no recall)",
        False,
    ),
    (
        "write_write_recall",
        "/deleg_recall_rw",
        "rw",
        "Client0 write-open hold; client1 writes same file (write deleg recall)",
        True,
    ),
    (
        "rw_read_after_write_recall",
        "/deleg_recall_rw",
        "rw",
        "Client0 write-open hold; client1 reads same file (write deleg recalled)",
        True,
    ),
)


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")

    min_clients = int(config.get("clients", 2))
    if len(clients) < min_clients:
        raise ConfigError(
            f"This test requires at least {min_clients} client nodes, got {len(clients)}"
        )
    client_a, client_b = clients[0], clients[1]

    if not nfs_nodes:
        raise ConfigError("This test requires at least one nfs node")
    if not installers:
        raise ConfigError("This test requires an installer node")

    installer = installers[0]
    cephadm = CephAdm(installer).ceph
    nfs_cmd_host = resolve_nfs_cmd_host(nfs_nodes, config)

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    nfs_server = nfs_nodes[0].hostname
    subvolume_group = config.get("subvolume_group", "delegrecallgrp")
    export_rw = config.get("recall_export", "/deleg_recall_rw")
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    hold_seconds = int(config.get("delegation_hold_seconds", 120))
    hold_ready_wait = int(config.get("delegation_hold_ready_seconds", 8))
    settle_seconds = clamp_delegation_log_settle_seconds(config)
    mount_a = config.get("client_a_mount", "/mnt/deleg_recall_c0")
    mount_b = config.get("client_b_mount", "/mnt/deleg_recall_c1")
    test_basename = config.get("recall_test_file", "deleg_recall_shared.txt")

    deleg_template_backup = False
    log_template_backup = False
    subvol_name = None

    ensure_cephfs_volume_exists(client_a, fs_name)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        nfs_clusters = cephadm.nfs.cluster.ls()
        if nfs_name not in nfs_clusters:
            raise ConfigError(
                f"NFS cluster {nfs_name!r} must exist before this test "
                "(run cluster/delegation setup tests first or pre-create the cluster)"
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

        subvol_name = create_delegation_recall_export(
            nfs_cmd_host, fs_name, nfs_name, export_rw, subvolume_group
        )
        nfs_node, container_id = first_nfs_container_id(installer, nfs_nodes, nfs_name)

        for client, mount_point in ((client_a, mount_a), (client_b, mount_b)):
            client.exec_command(
                sudo=True, cmd=f"mkdir -p {mount_point}", check_ec=False
            )
            try:
                Unmount(client).unmount(mount_point)
            except Exception:
                pass
            mount_retry(
                client, mount_point, nfs_version, nfs_port, nfs_server, export_rw
            )

        shared_a = f"{mount_a}/{test_basename}"
        shared_b = f"{mount_b}/{test_basename}"
        first_scenario = True

        for (
            scenario_name,
            pseudo,
            _deleg,
            description,
            expect_recall,
        ) in RECALL_SCENARIOS:
            if pseudo != export_rw:
                continue
            if not first_scenario:
                remove_shared_test_file(client_a, client_b, shared_a, shared_b)
            first_scenario = False

            log.info(
                "Running recall/revoke scenario %s: %s", scenario_name, description
            )
            truncate_ganesha_container_log(nfs_node, container_id)
            seed_shared_recall_file(
                client_a, client_b, shared_a, shared_b, scenario_name
            )
            start_ganesha_deleg_tailf_follow(nfs_node, container_id)

            run_recall_scenario_delegation_io(
                scenario_name,
                client_a,
                client_b,
                shared_a,
                shared_b,
                f"seed-{scenario_name}",
                hold_seconds,
                hold_ready_wait,
            )

            deleg_hits = wait_settle_then_stop_tailf_capture(
                nfs_node, container_id, settle_seconds, scenario_name
            )
            if expect_recall:
                validate_delegation_recall_capture(scenario_name, deleg_hits)
            else:
                validate_no_delegation_recall_capture(scenario_name, deleg_hits)

            remove_shared_test_file(client_a, client_b, shared_a, shared_b)

        return 0

    except Exception as err:
        log.error("Delegation revoke/recall test failed: %s", err)
        return 1

    finally:
        for client, mount_point in ((client_a, mount_a), (client_b, mount_b)):
            try:
                Unmount(client).unmount(mount_point)
            except Exception:
                pass
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}", check_ec=False)
            kill_delegation_hold_processes(client)

        if subvol_name:
            teardown_delegation_exports(
                nfs_cmd_host,
                fs_name,
                nfs_name,
                subvolume_group,
                [export_rw],
                [subvol_name],
            )

        try:
            nfs_node, cid = first_nfs_container_id(installer, nfs_nodes, nfs_name)
            stop_ganesha_deleg_tailf_follow(nfs_node, cid)
            truncate_ganesha_container_log(nfs_node, cid)
        except Exception as trunc_err:
            log.warning("Could not stop tail -f or truncate ganesha.log: %s", trunc_err)

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
