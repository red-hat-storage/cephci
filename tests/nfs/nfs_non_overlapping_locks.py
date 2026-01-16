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

        log.info("=== Running non-overlapping locks test ===")

        result = run_non_overlapping_locks_test(clients, mount_path)

        if not result["success"]:
            raise Exception("Non-overlapping locks test failed: %s" % result["error"])

        log.info("Non-overlapping locks test PASSED.")
        rc = 0

    except (CommandFailed, Exception) as e:
        log.error("Test failed: %s" % e)

    finally:
        log.info("Cleaning up exports and NFS cluster...")
        cleanup_cluster(clients, mount_path, nfs_name, nfs_export)

    return rc


def run_non_overlapping_locks_test(clients, mount_path):
    """
    Test non-overlapping byte-range locks:
    Multiple clients should be able to hold non-overlapping locks simultaneously.
    """
    script_src = "tests/nfs/scripts_tools/non_overlapping_locks.py"
    script_dst = "/root/non_overlapping_locks.py"

    # Upload script to all clients
    for c in clients:
        log.info("Uploading non-overlapping locks script to %s..." % c.hostname)
        c.upload_file(sudo=True, src=script_src, dst=script_dst)
        c.exec_command(sudo=True, cmd=f"chmod +x {script_dst}")

    # Test scenario: Multiple clients with non-overlapping locks
    # Client 1: locks bytes 0-10
    # Client 2: locks bytes 20-30 (non-overlapping with client1)
    # Client 3: locks bytes 40-50 (non-overlapping with client1 and client2)
    log.info(
        "=== Testing non-overlapping locks - all should succeed simultaneously ==="
    )

    # Define non-overlapping lock ranges
    lock_ranges = [
        (0, 10),  # Client 1: bytes 0-10
        (20, 10),  # Client 2: bytes 20-30
        (40, 10),  # Client 3: bytes 40-50
    ]

    all_results = []

    # Execute all clients simultaneously
    with parallel() as p:
        for idx, client in enumerate(clients[: len(lock_ranges)]):
            start, length = lock_ranges[idx]
            cmd = f"python3 {script_dst} {mount_path} client{idx+1} {start} {length}"
            log.info(
                "Starting client %s with lock range: start=%s, len=%s"
                % (idx + 1, start, length)
            )
            p.spawn(
                client.exec_command,
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )

        for result in p:
            out, err = result
            out = out.decode() if isinstance(out, bytes) else out
            all_results.append(out)
            log.debug("Client output:\n%s" % out)

    # Verify all clients successfully acquired their locks
    success_count = sum(1 for out in all_results if "SUCCESS" in out)
    failed_count = sum(1 for out in all_results if "FAILED" in out)

    log.info(
        "Results: %s clients succeeded, %s clients failed"
        % (success_count, failed_count)
    )

    if failed_count > 0:
        return {
            "success": False,
            "error": "Non-overlapping locks test failed - %s client(s) failed to acquire lock"
            % failed_count,
        }

    if success_count < len(clients[: len(lock_ranges)]):
        return {
            "success": False,
            "error": "Non-overlapping locks test failed - expected %s clients to succeed, got %s"
            % (len(clients[: len(lock_ranges)]), success_count),
        }

    log.info(
        "All %s clients successfully acquired non-overlapping locks simultaneously"
        % success_count
    )

    # Additional test: Test with more clients if available
    if len(clients) >= 4:
        log.info("=== Additional test: 4 clients with non-overlapping locks ===")
        time.sleep(3)

        # Extended lock ranges for 4 clients
        extended_ranges = [
            (0, 10),  # Client 1: bytes 0-10
            (20, 10),  # Client 2: bytes 20-30
            (40, 10),  # Client 3: bytes 40-50
            (60, 10),  # Client 4: bytes 60-70
        ]

        extended_results = []
        with parallel() as p:
            for idx, client in enumerate(clients[:4]):
                start, length = extended_ranges[idx]
                cmd = (
                    f"python3 {script_dst} {mount_path} client{idx+1} {start} {length}"
                )
                log.info(
                    "Starting client %s with lock range: start=%s, len=%s"
                    % (idx + 1, start, length)
                )
                p.spawn(
                    client.exec_command,
                    sudo=True,
                    cmd=cmd,
                    check_ec=False,
                )

            for result in p:
                out, err = result
                out = out.decode() if isinstance(out, bytes) else out
                extended_results.append(out)
                log.debug("Extended test output:\n%s" % out)

        extended_success = sum(1 for out in extended_results if "SUCCESS" in out)
        if extended_success < 4:
            return {
                "success": False,
                "error": "Extended non-overlapping locks test failed - expected 4 clients to succeed",
            }

        log.info("Extended test PASSED - all 4 clients acquired non-overlapping locks")

    return {
        "success": True,
        "response": "All non-overlapping locks tests passed - multiple clients held locks simultaneously",
        "error": None,
    }
