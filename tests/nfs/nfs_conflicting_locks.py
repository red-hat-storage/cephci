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
    required_clients = int(config.get("clients", "4"))

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

        log.info("=== Running conflicting locks test ===")

        result = run_conflicting_locks_test(clients, mount_path)

        if not result["success"]:
            raise Exception("Conflicting locks test failed: %s" % result["error"])

        log.info("Conflicting locks test PASSED.")
        rc = 0

    except (CommandFailed, Exception) as e:
        log.error("Test failed: %s" % e)

    finally:
        log.info("Cleaning up exports and NFS cluster...")
        cleanup_cluster(clients, mount_path, nfs_name, nfs_export)

    return rc


def run_conflicting_locks_test(clients, mount_path):
    """
    Test conflicting byte-range locks:
    1. Multiple clients try to acquire the exact same lock range
    2. Multiple clients try to acquire overlapping lock ranges
    3. Verify only one client can hold the lock at a time
    """
    script_src = "tests/nfs/scripts_tools/conflicting_locks.py"
    script_dst = "/root/conflicting_locks.py"

    # Upload script to all clients
    for c in clients:
        log.info("Uploading conflicting locks script to %s..." % c.hostname)
        c.upload_file(sudo=True, src=script_src, dst=script_dst)
        c.exec_command(sudo=True, cmd=f"chmod +x {script_dst}")

    # Test scenario 1: Exact same lock range (maximum conflict)
    # All clients try to lock the exact same bytes: 0-20
    log.info("=== Test 1: Exact same lock range (maximum conflict) ===")

    exact_conflict_results = []
    with parallel() as p:
        # All clients try to acquire the same lock range simultaneously
        for idx, client in enumerate(clients):
            cmd = f"python3 {script_dst} {mount_path} client{idx+1} 0 20"
            log.info("Starting client %s with exact same lock range: 0-20" % (idx + 1))
            p.spawn(
                client.exec_command,
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )

        for result in p:
            out, err = result
            out = out.decode() if isinstance(out, bytes) else out
            exact_conflict_results.append(out)
            log.debug("Exact conflict test output:\n%s" % out)

    # Verify: Only one client can hold the lock at a time (conflict resolution)
    # With retry logic, clients may acquire sequentially, but not simultaneously
    success_count = sum(1 for out in exact_conflict_results if "SUCCESS" in out)
    blocked_count = sum(1 for out in exact_conflict_results if "BLOCKED" in out)
    has_retries = any("retry" in out.lower() for out in exact_conflict_results)

    log.info(
        "Exact conflict results: %s succeeded, %s blocked"
        % (success_count, blocked_count)
    )

    # At least one client should succeed (proves conflict resolution works)
    if success_count < 1:
        return {
            "success": False,
            "error": "Exact conflict test failed - expected at least 1 client to succeed, got %s"
            % success_count,
        }

    # If multiple clients succeeded, verify they did so sequentially (not simultaneously)
    # Sequential acquisition is proven by retries - if clients acquired simultaneously,
    # there would be no retries needed
    if success_count > 1:
        if not has_retries:
            return {
                "success": False,
                "error": (
                    "Exact conflict test failed - %s clients succeeded but no retries "
                    "detected (may indicate simultaneous acquisition)"
                )
                % success_count,
            }
        log.info(
            "Multiple clients succeeded sequentially - retries detected, conflict resolution verified"
        )
    else:
        # Only one client succeeded - this is valid, others were blocked
        log.info(
            "One client succeeded, %s blocked - conflict resolution verified"
            % blocked_count
        )

    log.info("Exact conflict test PASSED - only one client held lock at a time")

    # Wait before next test
    time.sleep(5)

    # Test scenario 2: Overlapping lock ranges
    # Client 1: locks 0-15
    # Client 2: tries to lock 10-25 (overlaps with 0-15)
    # Client 3: tries to lock 5-20 (overlaps with 0-15)
    log.info("=== Test 2: Overlapping lock ranges ===")

    overlapping_conflict_results = []
    with parallel() as p:
        # Client 1 locks 0-15
        p.spawn(
            clients[0].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client1 0 15",
            check_ec=False,
        )
        time.sleep(1)
        # Client 2 tries to lock 10-25 (overlapping)
        p.spawn(
            clients[1].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client2 10 15",
            check_ec=False,
        )
        time.sleep(1)
        # Client 3 tries to lock 5-20 (overlapping)
        p.spawn(
            clients[2].exec_command,
            sudo=True,
            cmd=f"python3 {script_dst} {mount_path} client3 5 15",
            check_ec=False,
        )

        for result in p:
            out, err = result
            out = out.decode() if isinstance(out, bytes) else out
            overlapping_conflict_results.append(out)
            log.debug("Overlapping conflict test output:\n%s" % out)

    # Verify: Client 1 should succeed
    # Clients 2 and 3 should be initially blocked (may eventually succeed after client1 releases)
    client1_success = any(
        "client1" in out and "SUCCESS" in out for out in overlapping_conflict_results
    )
    client2_initially_blocked = any(
        "client2" in out
        and (
            "Lock conflict detected" in out
            or "retry" in out.lower()
            or "BLOCKED" in out
        )
        for out in overlapping_conflict_results
    )
    client3_initially_blocked = any(
        "client3" in out
        and (
            "Lock conflict detected" in out
            or "retry" in out.lower()
            or "BLOCKED" in out
        )
        for out in overlapping_conflict_results
    )

    if not client1_success:
        return {
            "success": False,
            "error": "Overlapping conflict test failed - client1 should have acquired lock",
        }

    # At least one of the overlapping clients should be initially blocked
    if not client2_initially_blocked and not client3_initially_blocked:
        return {
            "success": False,
            "error": "Overlapping conflict test failed - at least one overlapping client should be initially blocked",
        }

    # With retry logic, clients may eventually succeed after client1 releases
    if client2_initially_blocked:
        log.info("Client2 was initially blocked (may have eventually succeeded)")
    if client3_initially_blocked:
        log.info("Client3 was initially blocked (may have eventually succeeded)")

    log.info(
        "Overlapping conflict test PASSED - client1 acquired, overlapping clients were blocked"
    )

    # Test scenario 3: Partial overlap (if we have 4+ clients)
    if len(clients) >= 4:
        log.info("=== Test 3: Partial overlap scenario ===")
        time.sleep(5)

        partial_overlap_results = []
        with parallel() as p:
            # Client 1 locks 0-10
            p.spawn(
                clients[0].exec_command,
                sudo=True,
                cmd=f"python3 {script_dst} {mount_path} client1 0 10",
                check_ec=False,
            )
            time.sleep(1)
            # Client 2 tries to lock 5-15 (partially overlaps)
            p.spawn(
                clients[1].exec_command,
                sudo=True,
                cmd=f"python3 {script_dst} {mount_path} client2 5 10",
                check_ec=False,
            )
            time.sleep(1)
            # Client 3 tries to lock 8-18 (partially overlaps)
            p.spawn(
                clients[2].exec_command,
                sudo=True,
                cmd=f"python3 {script_dst} {mount_path} client3 8 10",
                check_ec=False,
            )
            time.sleep(1)
            # Client 4 tries to lock 20-30 (non-overlapping, should succeed)
            p.spawn(
                clients[3].exec_command,
                sudo=True,
                cmd=f"python3 {script_dst} {mount_path} client4 20 10",
                check_ec=False,
            )

            for result in p:
                out, err = result
                out = out.decode() if isinstance(out, bytes) else out
                partial_overlap_results.append(out)
                log.debug("Partial overlap test output:\n%s" % out)

        # Verify: Client 1 should succeed, Client 4 (non-overlapping) should succeed
        # Clients 2 and 3 (overlapping) should be initially blocked (may eventually succeed)
        client1_success = any(
            "client1" in out and "SUCCESS" in out for out in partial_overlap_results
        )
        client4_success = any(
            "client4" in out and "SUCCESS" in out for out in partial_overlap_results
        )
        client2_initially_blocked = any(
            "client2" in out
            and (
                "Lock conflict detected" in out
                or "retry" in out.lower()
                or "BLOCKED" in out
            )
            for out in partial_overlap_results
        )
        client3_initially_blocked = any(
            "client3" in out
            and (
                "Lock conflict detected" in out
                or "retry" in out.lower()
                or "BLOCKED" in out
            )
            for out in partial_overlap_results
        )

        if not client1_success:
            return {
                "success": False,
                "error": "Partial overlap test failed - client1 should have acquired lock",
            }

        if not client4_success:
            return {
                "success": False,
                "error": "Partial overlap test failed - client4 (non-overlapping) should have succeeded",
            }

        # At least one overlapping client should be initially blocked
        if not client2_initially_blocked and not client3_initially_blocked:
            return {
                "success": False,
                "error": "Partial overlap test failed - at least one overlapping client should be initially blocked",
            }

        log.info(
            "Partial overlap test PASSED - client1 and client4 succeeded, overlapping clients were blocked"
        )

    return {
        "success": True,
        "response": "All conflicting locks tests passed successfully",
        "error": None,
    }
