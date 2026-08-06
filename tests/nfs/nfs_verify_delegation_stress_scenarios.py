"""
NFSv4 delegation stress: multi-client, multi-mount mixed IO.

Mounts the export on up to 4 clients (5 mounts per client), runs background read/write
workloads, triggers occasional cross-client writes on a shared file, then checks the
filtered Ganesha tail -f capture for delegation activity after IO stops.

Set ``strict_log_validation: true`` in suite config for grant-type and recall validation;
default is a loose grant/recall activity check only.
"""

import time

from nfs_delegation_operations import (
    create_delegation_exports,
    delegation_timing_from_config,
    enable_cluster_delegation_and_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    ensure_nfs_cluster_for_delegation_test,
    mount_delegation_export,
    q_delegation_shell,
    read_ganesha_delegation_tailf_capture,
    restore_delegation_ganesha_templates,
    skip_delegation_tests_unless_supported,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    teardown_delegation_exports,
    truncate_ganesha_container_log,
    unmount_delegation_export,
    validate_delegation_stress_loose_capture,
    validate_delegation_stress_strict_capture,
    wait_for_delegation_path,
    write_delegation_file,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

CONFLICT_FILE = "deleg_stress_conflict.dat"
WORKLOAD_KINDS = ("seq_read", "rand_read", "seq_write", "rand_write")
MOUNTS_PER_CLIENT_MAX = 5


def _workload_pidfile(client):
    host = str(getattr(client, "hostname", "client")).replace("/", "_")
    return "/tmp/ganesha_delegation_workload_%s.pids" % host


def _kill_workloads(client):
    pidfile = _workload_pidfile(client)
    client.exec_command(
        sudo=True,
        cmd=(
            "if [ -f %s ]; then while read -r pid; do "
            '[ -n "$pid" ] && kill "$pid" 2>/dev/null || true; '
            "done < %s; rm -f %s; fi"
        )
        % (pidfile, pidfile, pidfile),
        check_ec=False,
    )


def _resolve_container_id(installer, nfs_name, nfs_node):
    _, info = get_ganesha_info_from_container(installer, nfs_name, nfs_node)
    container_id = (info or {}).get("container_id")
    if not container_id:
        raise OperationFailedError("No Ganesha container for nfs.%s" % nfs_name)
    return container_id


def _mount_point(client_idx, mount_idx):
    return "/mnt/deleg_stress_c%s_m%s" % (client_idx, mount_idx)


def _setup_client_mounts(clients, mounts_per_client, nfs_ver, port, server, export_rw):
    specs = []
    for ci, client in enumerate(clients):
        for mi in range(mounts_per_client):
            mp = _mount_point(ci, mi)
            mount_delegation_export(client, mp, nfs_ver, port, server, export_rw)
            specs.append((client, mp, WORKLOAD_KINDS[(ci + mi) % len(WORKLOAD_KINDS)]))
    return specs


def _teardown_client_mounts(clients, mounts_per_client):
    for ci, client in enumerate(clients):
        for mi in range(mounts_per_client):
            unmount_delegation_export(
                client, _mount_point(ci, mi), remove_mount_dir=True
            )


def _first_mount_per_client(mount_roots):
    first = {}
    for client, mp, _kind in mount_roots:
        if client not in first:
            first[client] = mp
    return first


def _seed_workload_files(mount_roots, path_wait_retries):
    seeded = []
    for client, mp, kind in mount_roots:
        host = getattr(client, "hostname", "client").replace(".", "_")
        path = "%s/stress_%s.dat" % (mp, host)
        write_delegation_file(client, path, "seed-%s\n" % host)
        wait_for_delegation_path(
            client, path, "seed %s" % path, retries=path_wait_retries
        )
        seeded.append((client, path, kind))
    return seeded


def _seed_conflict_file(clients, mount_roots, path_wait_retries):
    mounts = _first_mount_per_client(mount_roots)
    seed_client = clients[0]
    seed_path = "%s/%s" % (mounts[seed_client], CONFLICT_FILE)
    write_delegation_file(seed_client, seed_path, "conflict-seed\n")
    paths = {}
    for client in clients:
        path = "%s/%s" % (mounts[client], CONFLICT_FILE)
        wait_for_delegation_path(
            client,
            path,
            "conflict seed on %s" % client.hostname,
            retries=path_wait_retries,
        )
        paths[client] = path
    return paths


def _start_mixed_workload(client, path, kind, duration_s):
    safe = q_delegation_shell(path)
    pidfile = _workload_pidfile(client)
    if kind == "seq_read":
        body = "while [ $(date +%%s) -lt $end ]; do cat '%s' >/dev/null; done" % safe
    elif kind == "rand_read":
        body = (
            "while [ $(date +%%s) -lt $end ]; do "
            "dd if='%s' of=/dev/null bs=4096 count=1 skip=$((RANDOM%%32)) 2>/dev/null; "
            "done"
        ) % safe
    elif kind == "seq_write":
        body = (
            "while [ $(date +%%s) -lt $end ]; do "
            "echo seq-write-$(date +%%s) >> '%s'; sync; done"
        ) % safe
    else:
        body = (
            "while [ $(date +%%s) -lt $end ]; do "
            "echo rand-$RANDOM >> '%s'; sync; done"
        ) % safe
    client.exec_command(
        sudo=True,
        cmd=(
            "bash -c 'end=$(( $(date +%%s) + %s )); %s' >/dev/null 2>&1 & echo $! >> %s"
            % (int(duration_s), body, pidfile)
        ),
        check_ec=False,
    )


def _cross_client_conflict_write(writer, conflict_path, tag):
    content = "conflict-%s-%s\n" % (tag, getattr(writer, "hostname", "w"))
    write_delegation_file(writer, conflict_path, content, append=True)


def _validate_stress_capture(hits, strict_validation, hold_mode):
    if strict_validation:
        validate_delegation_stress_strict_capture(hits, hold_mode)
    else:
        validate_delegation_stress_loose_capture(hits)


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

    num_clients = min(int(config.get("clients", 4)), len(clients), 4)
    clients = clients[:num_clients]
    mounts_per_client = min(
        int(config.get("mounts_per_client", 3)), MOUNTS_PER_CLIENT_MAX
    )

    installer, nfs_node = installers[0], nfs_nodes[0]
    cephadm = CephAdm(installer).ceph
    nfs_cmd_host = nfs_node

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    export_rw = config.get("stress_export", "/deleg_stress_rw")
    timing = delegation_timing_from_config(config)
    strict_validation = bool(config.get("strict_log_validation", False))
    hold_mode = str(config.get("hold_mode", "write")).strip().lower()
    if hold_mode not in ("read", "write"):
        raise ConfigError("hold_mode accepts only read or write")

    workload_duration = int(config.get("workload_duration_seconds", 180))
    conflict_interval = int(config.get("conflict_interval_seconds", 45))
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "delegstressgrp")
    nfs_mount = config.get("nfs_mount", "/mnt/delegation_bootstrap")
    bootstrap_export = config.get("bootstrap_export", "/export_delegation_boot")

    delegation_template_backup = logging_template_backup = False
    created_cluster = False
    subvol_names, container_id = [], None
    mount_roots = []

    log.info(
        "Stress config: clients=%d mounts_per_client=%d strict_validation=%s hold_mode=%s",
        num_clients,
        mounts_per_client,
        strict_validation,
        hold_mode,
    )

    cephfs_utils = CephFSCommonUtils(ceph_cluster)
    if not cephfs_utils.get_fs_info(clients[0], fs_name):
        cephfs_utils.create_fs(clients[0], fs_name)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        nfs_clusters, created_cluster = ensure_nfs_cluster_for_delegation_test(
            cephadm,
            nfs_name,
            config,
            client=clients[0],
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
        container_id = _resolve_container_id(installer, nfs_name, nfs_node)

        nfs_ver = config.get("nfs_version", "4.2")
        port = str(config.get("port", "2049"))
        server = nfs_node.hostname
        mount_roots = _setup_client_mounts(
            clients, mounts_per_client, nfs_ver, port, server, export_rw
        )
        workload_specs = _seed_workload_files(mount_roots, timing.path_wait_retries)
        conflict_paths = _seed_conflict_file(
            clients, mount_roots, timing.path_wait_retries
        )

        log.info(
            "Waiting %s s after seed for delegations to exit", timing.exit_wait_seconds
        )
        time.sleep(timing.exit_wait_seconds)

        truncate_ganesha_container_log(nfs_node, container_id)
        start_ganesha_delegation_tailf_follow(nfs_node, container_id)

        for client, path, kind in workload_specs:
            log.info(
                "Starting %s workload on %s path=%s duration=%ss",
                kind,
                getattr(client, "hostname", client),
                path,
                workload_duration,
            )
            _start_mixed_workload(client, path, kind, workload_duration)

        end_at = time.time() + workload_duration
        next_conflict = time.time() + conflict_interval
        round_num = 0
        while time.time() < end_at:
            if conflict_interval > 0 and time.time() >= next_conflict:
                writer = clients[round_num % len(clients)]
                log.info(
                    "Cross-client conflict write round %d from %s",
                    round_num,
                    getattr(writer, "hostname", writer),
                )
                _cross_client_conflict_write(
                    writer, conflict_paths[writer], "r%d" % round_num
                )
                round_num += 1
                next_conflict = time.time() + conflict_interval
            time.sleep(5)

        for client in clients:
            _kill_workloads(client)

        log.info(
            "Waiting %s s for delegation log lines to settle",
            timing.log_settle_seconds,
        )
        time.sleep(timing.log_settle_seconds)
        stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
        hits = read_ganesha_delegation_tailf_capture(nfs_node, container_id)
        _validate_stress_capture(hits, strict_validation, hold_mode)

        return 0
    except Exception as err:
        log.error("Delegation stress IO test failed: %s", err)
        return 1
    finally:
        for client in clients:
            _kill_workloads(client)
        if mount_roots:
            _teardown_client_mounts(clients, mounts_per_client)
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
                clients=clients[:1],
                nfs_mount=nfs_mount,
                nfs_name=nfs_name,
                nfs_export=bootstrap_export,
                nfs_nodes=nfs_nodes,
            )
