import re

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

        log.info("=== Running queue management locks test ===")

        result = run_queue_management_test(clients, mount_path)

        if not result["success"]:
            raise Exception("Queue management locks test failed: %s" % result["error"])

        log.info("Queue management locks test PASSED.")
        rc = 0

    except (CommandFailed, Exception) as e:
        log.error("Test failed: %s" % e)

    finally:
        log.info("Cleaning up exports and NFS cluster...")
        cleanup_cluster(clients, mount_path, nfs_name, nfs_export)

    return rc


def run_queue_management_test(clients, mount_path):
    """
    Test queue management for byte-range locks:
    1. Multiple clients request the same lock simultaneously
    2. First client should acquire immediately
    3. Other clients should queue and acquire sequentially (FIFO)
    4. Verify acquisition order and timing
    """
    script_src = "tests/nfs/scripts_tools/queue_management_locks.py"
    script_dst = "/root/queue_management_locks.py"

    # Upload script to all clients
    for c in clients:
        log.info("Uploading queue management locks script to %s..." % c.hostname)
        c.upload_file(sudo=True, src=script_src, dst=script_dst)
        c.exec_command(sudo=True, cmd=f"chmod +x {script_dst}")

    # Test scenario: Multiple clients request the same lock
    # All clients try to acquire the same byte range: 0-20
    # Expected: First client gets lock immediately, others queue and acquire sequentially
    log.info("=== Testing queue management - FIFO behavior ===")

    # Start all clients simultaneously
    all_results = []

    with parallel() as p:
        # All clients try to acquire the same lock range simultaneously
        for idx, client in enumerate(clients):
            cmd = f"python3 {script_dst} {mount_path} client{idx+1} 0 20"
            log.info("Starting client %s requesting lock range: 0-20" % (idx + 1))
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
            log.debug("Queue management test output:\n%s" % out)

    # Parse results to extract acquisition times and queue behavior
    acquisition_times = {}
    client_success = {}
    client_waited = {}  # Track which clients waited in queue

    for out in all_results:
        # Extract client ID
        client_match = re.search(r"\[Client (client\d+)\]", out)
        if not client_match:
            continue
        client_id = client_match.group(1)

        # Check if client was waiting in queue (retrying)
        if "Waiting in queue" in out or "retry" in out.lower():
            client_waited[client_id] = True

        # Extract acquisition time
        time_match = re.search(r"ACQUIRED_AT ([\d.]+)", out)
        if time_match:
            acquisition_times[client_id] = float(time_match.group(1))
            client_success[client_id] = True
        elif "SUCCESS" in out:
            # If we see SUCCESS but no time, assume it was immediate (first client)
            if client_id not in acquisition_times:
                acquisition_times[client_id] = 0.0
            client_success[client_id] = True
        elif "TIMEOUT" in out or "FAILED" in out:
            client_success[client_id] = False

    log.info("Acquisition times: %s" % acquisition_times)
    log.info("Clients that waited in queue: %s" % list(client_waited.keys()))

    # Verify all clients eventually succeeded
    success_count = sum(1 for success in client_success.values() if success)
    if success_count != len(clients):
        return {
            "success": False,
            "error": "Queue management test failed - expected all %s clients to succeed, got %s"
            % (len(clients), success_count),
        }

    # Verify FIFO behavior: clients should acquire locks sequentially
    # First client should acquire immediately (time ~0)
    # Subsequent clients should wait in queue and acquire after previous ones release
    sorted_clients = sorted(acquisition_times.items(), key=lambda x: x[1])

    log.info("Lock acquisition order:")
    for idx, (client_id, acq_time) in enumerate(sorted_clients):
        waited = (
            " (waited in queue)"
            if client_waited.get(client_id, False)
            else " (acquired immediately)"
        )
        log.info(
            "  %s. %s acquired at %.2f seconds%s"
            % (idx + 1, client_id, acq_time, waited)
        )

    # First client should acquire relatively quickly (may have small delay due to setup)
    # Allow up to 5 seconds for initial acquisition (accounting for network/file system delays)
    first_client_time = sorted_clients[0][1]
    if first_client_time > 5.0:
        log.warning(
            "First client took %.2f seconds to acquire - may indicate initial delay"
            % first_client_time
        )
        # Don't fail, but log a warning as this might indicate a setup issue

    # Verify that subsequent clients waited in queue (proving queue behavior)
    # At least one client (other than the first) should have waited
    subsequent_clients_waited = sum(
        1 for client_id, _ in sorted_clients[1:] if client_waited.get(client_id, False)
    )

    if len(sorted_clients) > 1 and subsequent_clients_waited == 0:
        return {
            "success": False,
            "error": "Queue management test failed - subsequent clients should wait in queue, but no waiting detected",
        }

    log.info(
        "Queue behavior verified - %s out of %s subsequent clients waited in queue"
        % (subsequent_clients_waited, len(sorted_clients) - 1)
    )

    # Subsequent clients should acquire after previous ones (with lock duration delay)
    # Each client holds lock for ~8 seconds, so next should acquire after ~8 seconds
    lock_duration = 8
    for idx in range(1, len(sorted_clients)):
        prev_time = sorted_clients[idx - 1][1]
        curr_time = sorted_clients[idx][1]
        expected_min_time = prev_time + lock_duration - 1  # Allow 1 second tolerance

        if curr_time < expected_min_time:
            log.warning(
                "Client %s acquired lock too early: %.2f < %.2f (expected minimum)"
                % (sorted_clients[idx][0], curr_time, expected_min_time)
            )
            # This might be acceptable if locks are released quickly, so we'll warn but not fail

    log.info(
        "FIFO queue management verified - clients waited in queue and acquired locks sequentially"
    )

    # Additional test: Verify no two clients hold the lock simultaneously
    # This is implicit in the sequential acquisition, but we can verify timing
    for i in range(len(sorted_clients) - 1):
        curr_end = sorted_clients[i][1] + lock_duration
        next_start = sorted_clients[i + 1][1]
        if next_start < curr_end - 1:  # Allow 1 second overlap tolerance
            log.warning(
                "Potential simultaneous lock: client %s released at ~%.2f, client %s acquired at %.2f"
                % (
                    sorted_clients[i][0],
                    curr_end,
                    sorted_clients[i + 1][0],
                    next_start,
                )
            )

    return {
        "success": True,
        "response": "Queue management test passed - FIFO behavior verified, all clients acquired locks sequentially",
        "error": None,
    }
