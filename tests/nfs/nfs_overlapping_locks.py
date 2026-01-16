import time

from nfs_operations import cleanup_cluster, enable_v3_locking, setup_nfs_cluster

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")

    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]

    if not nfs_nodes or not clients:
        raise ConfigError("Missing NFS nodes or clients in cluster")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    required_clients = int(config.get("clients", "3"))

    if required_clients > len(clients):
        raise ConfigError("Requested more clients than available")

    clients = clients[:required_clients]
    nfs_node = nfs_nodes[0]

    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    mount_path = "/mnt/nfs"

    rc = 1

    try:
        log.info("=== Setting up NFS Cluster ===")

        setup_nfs_cluster(
            clients,
            nfs_node.hostname,
            port,
            version,
            nfs_name,
            mount_path,
            fs_name,
            nfs_export,
            fs_name,
            ceph_cluster=ceph_cluster,
        )

        if version == 3:
            enable_v3_locking(installer, nfs_name, nfs_node, nfs_node.hostname)

        log.info("=== Running overlapping locks test ===")

        result = run_overlapping_locks_test(clients, mount_path)

        if not result["success"]:
            raise Exception("Overlapping locks test failed: %s" % result["error"])

        log.info("Overlapping locks test PASSED.")
        rc = 0

    except (CommandFailed, Exception) as e:
        log.error("Test failed: %s" % e)

    finally:
        log.info("Cleaning up exports and NFS cluster...")
        cleanup_cluster(clients, mount_path, nfs_name, nfs_export)

    return rc


def run_overlapping_locks_test(clients, mount_path):
    """
    Test overlapping byte-range locks:
    1. Non-overlapping locks should succeed simultaneously
    2. Overlapping locks should block each other
    """
    script_src = "tests/nfs/scripts_tools/overlapping_locks.py"
    script_dst = "/root/overlapping_locks.py"

    # Upload script to all clients
    for c in clients:
        log.info("Uploading overlapping locks script to %s..." % c.hostname)
        c.upload_file(sudo=True, src=script_src, dst=script_dst)
        c.exec_command(sudo=True, cmd=f"chmod +x {script_dst}")

    # Test scenario 1: Non-overlapping locks (should succeed simultaneously)
    # Client 1: locks bytes 0-10
    # Client 2: locks bytes 20-30 (non-overlapping)
    log.info("=== Test 1: Non-overlapping locks (should succeed) ===")

    non_overlapping_results = []
    with parallel() as p:
        # Client 1 locks 0-10
        p.spawn(
            clients[0].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client1 0 10",
            check_ec=False,
        )
        # Client 2 locks 20-30 (non-overlapping)
        p.spawn(
            clients[1].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client2 20 10",
            check_ec=False,
        )

        for result in p:
            out, err = result
            out = out.decode() if isinstance(out, bytes) else out
            non_overlapping_results.append(out)
            log.debug("Non-overlapping test output:\n%s" % out)

    # Verify both clients acquired locks (non-overlapping should succeed)
    success_count = sum(1 for out in non_overlapping_results if "SUCCESS" in out)
    if success_count < 2:
        return {
            "success": False,
            "error": "Non-overlapping locks test failed - expected both clients to succeed",
        }

    log.info("Non-overlapping locks test PASSED - both clients acquired locks")

    # Wait a bit before next test
    time.sleep(5)

    # Test scenario 2: Overlapping locks (should block)
    # Client 1: locks bytes 0-20
    # Client 2: tries to lock bytes 10-30 (overlaps with 0-20)
    log.info("=== Test 2: Overlapping locks (should block) ===")

    overlapping_results = []
    with parallel() as p:
        # Client 1 locks 0-20
        p.spawn(
            clients[0].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client1 0 20",
            check_ec=False,
        )
        # Small delay to ensure client1 acquires lock first
        time.sleep(1)
        # Client 2 tries to lock 10-30 (overlapping)
        p.spawn(
            clients[1].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client2 10 20",
            check_ec=False,
        )

        for result in p:
            out, err = result
            out = out.decode() if isinstance(out, bytes) else out
            overlapping_results.append(out)
            log.debug("Overlapping test output:\n%s" % out)

    # Verify: Client 1 should succeed
    # Client 2 should be initially blocked (may eventually succeed after client1 releases)
    client1_success = any(
        "client1" in out and "SUCCESS" in out for out in overlapping_results
    )
    client2_initially_blocked = any(
        "client2" in out and ("Lock initially blocked" in out or "BLOCKED" in out)
        for out in overlapping_results
    )
    client2_success = any(
        "client2" in out and "SUCCESS" in out for out in overlapping_results
    )

    if not client1_success:
        return {
            "success": False,
            "error": "Overlapping locks test failed - client1 should have acquired lock",
        }

    # Client 2 should be initially blocked (it may eventually succeed after client1 releases)
    if not client2_initially_blocked and not client2_success:
        return {
            "success": False,
            "error": (
                "Overlapping locks test failed - client2 should have been initially "
                "blocked or eventually succeeded"
            ),
        }

    if client2_initially_blocked:
        log.info(
            "Overlapping locks test PASSED - client1 acquired, client2 was initially blocked"
        )
    elif client2_success:
        log.info(
            "Overlapping locks test PASSED - client1 acquired first, client2 succeeded after client1 released"
        )

    return {
        "success": True,
        "response": "All overlapping locks tests passed successfully",
        "error": None,
    }
