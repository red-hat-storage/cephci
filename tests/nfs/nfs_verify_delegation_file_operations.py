"""
NFSv4 delegation file operations (CEPH-83632379).

Scenarios: open/close cycles, read-write IO with recall, lock/unlock, setattr/truncate.
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
    validate_delegation_lock_capture,
    validate_delegation_open_close_cycles_capture,
    validate_delegation_rw_io_recall_capture,
    validate_delegation_setattr_capture,
    wait_for_delegation_path,
    write_delegation_file,
)
from nfs_operations import cleanup_cluster, get_ganesha_info_from_container

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

# (scenario_name, min_clients, test_file, validator)
FILE_OP_SCENARIOS = (
    ("open_close_cycles", 1, "deleg_fops_open_close.txt", "open_close"),
    ("read_write_io_recall", 2, "deleg_fops_rw_recall.txt", "rw_recall"),
    ("lock_unlock", 2, "deleg_fops_lock.txt", "lock"),
    ("setattr_truncate", 1, "deleg_fops_setattr.txt", "setattr"),
)
SCENARIO_TEST_FILES = [row[2] for row in FILE_OP_SCENARIOS]


def _seed_file(client, path, scenario, path_wait_retries):
    write_delegation_file(client, path, "seed-%s\n" % scenario)
    wait_for_delegation_path(client, path, "seed", retries=path_wait_retries)


def _prime_delegation_hold(client, path, hold_mode):
    if hold_mode == "read":
        client.exec_command(sudo=True, cmd="cat %s" % path)
    else:
        write_delegation_file(client, path, "hold-prime\n", append=True)


def _run_open_close_cycles(client, path, cycles):
    """Local OPEN/read/CLOSE loops while a background hold keeps the delegation active."""
    client.exec_command(
        sudo=True,
        cmd=(
            "python3 <<'PY'\n"
            "path = %r\n"
            "for _ in range(%d):\n"
            "    f = open(path, 'rb')\n"
            "    f.read(1)\n"
            "    f.close()\n"
            "PY" % (path, int(cycles))
        ),
    )


def _run_sequential_random_rw(client, path, rounds):
    client.exec_command(
        sudo=True,
        cmd=(
            "bash -c 'for i in $(seq 1 %d); do "
            "echo rw-$i >> %s; head -c 32 %s >/dev/null; "
            "dd if=%s of=/dev/null bs=1 skip=$((i%%8)) count=1 2>/dev/null; "
            "done && sync'" % (int(rounds), path, path, path)
        ),
    )
    out, _ = client.exec_command(sudo=True, cmd="cat %s" % path)
    if not (out or "").strip():
        raise OperationFailedError("read/write IO: empty readback from %s" % path)


def _run_lock_holder(client, path, hold_s):
    client.exec_command(
        sudo=True,
        cmd=(
            "nohup python3 <<'PY' >/dev/null 2>&1 &\n"
            "import fcntl, time\n"
            "path = %r\n"
            "f = open(path, 'r+b')\n"
            "fcntl.lockf(f, fcntl.LOCK_EX)\n"
            "time.sleep(%d)\n"
            "fcntl.lockf(f, fcntl.LOCK_UN)\n"
            "f.close()\n"
            "PY" % (path, int(hold_s))
        ),
        check_ec=False,
    )


def _expect_peer_lock_blocked(client, path, wait_s):
    out, err = client.exec_command(
        sudo=True,
        cmd=(
            "python3 <<'PY'\n"
            "import fcntl, sys, time\n"
            "path = %r\n"
            "deadline = time.time() + %d\n"
            "blocked = False\n"
            "with open(path, 'r+b') as f:\n"
            "    while time.time() < deadline:\n"
            "        try:\n"
            "            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
            "            print('UNEXPECTED_OK')\n"
            "            sys.exit(2)\n"
            "        except BlockingIOError:\n"
            "            blocked = True\n"
            "            time.sleep(1)\n"
            "print('LOCK_BLOCKED' if blocked else 'NO_BLOCK')\n"
            "PY" % (path, int(wait_s))
        ),
        check_ec=False,
    )
    if "LOCK_BLOCKED" not in (out or ""):
        host = client.hostname
        exit_code = getattr(client, "exit_status", "?")
        raise OperationFailedError(
            "lock_unlock: client %s did not observe blocking lock on %s "
            "(out=%r, err=%r, exit=%s)" % (host, path, out, err, exit_code)
        )
    log.info("lock_unlock: peer lock attempt blocked on %s", path)


def _run_setattr_truncate_ops(client, path):
    client.exec_command(sudo=True, cmd="chmod 640 %s" % path, check_ec=False)
    client.exec_command(sudo=True, cmd="chown root:root %s" % path, check_ec=False)
    client.exec_command(sudo=True, cmd="truncate -s 4096 %s" % path, check_ec=False)
    client.exec_command(sudo=True, cmd="touch %s" % path, check_ec=False)
    client.exec_command(
        sudo=True,
        cmd="setfattr -n user.deleg_test -v fops %s" % path,
        check_ec=False,
    )
    out, _ = client.exec_command(
        sudo=True,
        cmd="getfattr -n user.deleg_test --only-values %s" % path,
        check_ec=False,
    )
    if "fops" not in (out or ""):
        log.warning(
            "setfattr/getfattr not confirmed on %s (xattr may be unsupported)", path
        )
    size_out, _ = client.exec_command(
        sudo=True, cmd="stat -c %%s %s" % path, check_ec=False
    )
    try:
        if int((size_out or "0").strip()) < 4096:
            raise OperationFailedError(
                "setattr_truncate: truncate did not reach 4096 bytes on %s" % path
            )
    except ValueError:
        log.warning("setattr_truncate: could not parse stat size: %r", size_out)
    log.info("setattr_truncate: metadata and size checks completed on %s", path)


def _validate_capture(scenario, validator, hits, hold_mode, open_close_cycles):
    if validator == "open_close":
        validate_delegation_open_close_cycles_capture(
            scenario, hits, open_close_cycles, hold_mode
        )
    elif validator == "rw_recall":
        validate_delegation_rw_io_recall_capture(scenario, hits, hold_mode)
    elif validator == "lock":
        validate_delegation_lock_capture(scenario, hits, hold_mode)
    elif validator == "setattr":
        validate_delegation_setattr_capture(scenario, hits, hold_mode)
    else:
        raise ConfigError("Unknown file-op validator %r" % validator)


def _run_scenario_io(
    name,
    client_a,
    client_b,
    path_a,
    path_b,
    hold_mode,
    hold_s,
    ready_s,
    open_close_cycles,
    lock_wait_s,
    path_wait_retries=40,
):
    open_mode = "r+b" if hold_mode == "write" else "rb"

    _prime_delegation_hold(client_a, path_a, hold_mode)

    if name == "open_close_cycles":
        hold_delegation_open(client_a, path_a, open_mode, hold_s)
        time.sleep(ready_s)
        _run_open_close_cycles(client_a, path_a, open_close_cycles)
        return

    if name == "read_write_io_recall":
        hold_delegation_open(client_a, path_a, open_mode, hold_s)
        time.sleep(ready_s)
        _run_sequential_random_rw(client_a, path_a, open_close_cycles)
        wait_for_delegation_path(
            client_b, path_b, "client B pre-conflict", retries=path_wait_retries
        )
        write_delegation_file(client_b, path_b, "peer-recall\n", append=True)
        _run_sequential_random_rw(client_a, path_a, 4)
        return

    if name == "lock_unlock":
        # Delegation hold OPEN, then a second OPEN with an exclusive lock on client A.
        hold_delegation_open(client_a, path_a, open_mode, hold_s)
        time.sleep(ready_s)
        _run_lock_holder(client_a, path_a, hold_s)
        time.sleep(5)
        _expect_peer_lock_blocked(client_b, path_b, lock_wait_s)
        write_delegation_file(client_b, path_b, "peer-lock-io\n", append=True)
        return

    if name == "setattr_truncate":
        hold_delegation_open(client_a, path_a, open_mode, hold_s)
        time.sleep(ready_s)
        _run_setattr_truncate_ops(client_a, path_a)
        return

    raise ConfigError("Unknown file operations scenario %r" % name)


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

    client_a = clients[0]
    client_b = clients[1] if len(clients) > 1 else None
    installer, nfs_node = installers[0], nfs_nodes[0]
    cephadm = CephAdm(installer).ceph
    nfs_cmd_host = nfs_node

    fs_name = config.get("fs_name", "cephfs")
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    export_rw = config.get("fops_export", "/deleg_fops_rw")
    mount_a = config.get("client_a_mount", "/mnt/deleg_fops_c0")
    mount_b = config.get("client_b_mount", "/mnt/deleg_fops_c1")
    hold_mode = str(config.get("hold_mode", "write")).strip().lower()
    if hold_mode not in ("read", "write"):
        raise ConfigError("hold_mode accepts only read or write")

    timing = delegation_timing_from_config(config)
    hold_s = timing.hold_seconds
    ready_s = timing.hold_ready_seconds
    exit_wait = timing.exit_wait_seconds
    settle_s = timing.log_settle_seconds
    path_wait_retries = timing.path_wait_retries
    open_close_cycles = int(config.get("open_close_cycles", 5))
    lock_wait_s = int(config.get("lock_wait_seconds", 15))
    redeploy_wait = int(config.get("redeploy_wait", 30))
    service_wait = int(config.get("service_wait_timeout", 300))
    subvol_group = config.get("subvolume_group", "delegfopsgrp")
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
        mount_delegation_export(client_a, mount_a, nfs_ver, port, server, export_rw)
        if client_b:
            mount_delegation_export(client_b, mount_b, nfs_ver, port, server, export_rw)

        for scenario, min_clients, test_file, validator in FILE_OP_SCENARIOS:
            if len(clients) < min_clients:
                raise ConfigError(
                    "Scenario %s requires %d clients" % (scenario, min_clients)
                )
            if min_clients > 1 and client_b is None:
                raise ConfigError("Scenario %s requires a second client" % scenario)

            path_a = "%s/%s" % (mount_a, test_file)
            path_b = "%s/%s" % (mount_b, test_file) if min_clients > 1 else path_a
            log.info("Scenario %s (%s) validator=%s", scenario, test_file, validator)

            truncate_ganesha_container_log(nfs_node, container_id)
            _seed_file(client_a, path_a, scenario, path_wait_retries)
            if min_clients > 1:
                _seed_file(client_b, path_b, scenario, path_wait_retries)
            log.info("Waiting %s s after seed for delegations to exit", exit_wait)
            time.sleep(exit_wait)

            start_ganesha_delegation_tailf_follow(nfs_node, container_id)
            _run_scenario_io(
                scenario,
                client_a,
                client_b,
                path_a,
                path_b,
                hold_mode,
                hold_s,
                ready_s,
                open_close_cycles,
                lock_wait_s,
                path_wait_retries,
            )
            time.sleep(settle_s)
            stop_ganesha_delegation_tailf_follow(nfs_node, container_id)
            _validate_capture(
                scenario,
                validator,
                read_ganesha_delegation_tailf_capture(nfs_node, container_id),
                hold_mode,
                open_close_cycles,
            )
            kill_delegation_holds(client_a)
            if client_b:
                kill_delegation_holds(client_b)

        return 0
    except Exception as err:
        log.error("Delegation file operations test failed: %s", err)
        return 1
    finally:
        kill_delegation_holds(client_a)
        if client_b:
            kill_delegation_holds(client_b)
        unmount_delegation_export(
            client_a,
            mount_a,
            remove_mount_dir=True,
            test_files=SCENARIO_TEST_FILES,
        )
        if client_b:
            unmount_delegation_export(
                client_b,
                mount_b,
                remove_mount_dir=True,
                test_files=SCENARIO_TEST_FILES,
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
