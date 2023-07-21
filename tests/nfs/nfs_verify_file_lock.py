from threading import Thread

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


CMD = """python3 -c 'from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN;from time import sleep;f = open(
"/mnt/nfs/sample_file", "w");flock(f.fileno(), LOCK_EX | LOCK_NB);sleep(30);flock(f.fileno(), LOCK_UN)'"""


def get_file_lock(client):
    try:
        client.exec_command(cmd=CMD, sudo=True)
        log.info("Executed successfully")
    except Exception:
        log.info(f"Exception happened while running on {client.hostname}")


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    no_clients = int(config.get("clients", "2"))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    # Setup nfs cluster
    setup_nfs_cluster(
        clients,
        nfs_server_name,
        port,
        version,
        nfs_name,
        nfs_mount,
        fs_name,
        nfs_export,
        fs,
    )

    # Create a file on Client 1
    file_path = "/mnt/nfs/sample_file"
    clients[0].exec_command(cmd=f"touch {file_path}", sudo=True)

    # Perform File Lock from client 1
    c1 = Thread(target=get_file_lock, args=(clients[0],))
    c1.start()
    log.info("Acquired lock from client 1")

    # At the same time, try locking file from client 2
    try:
        clients[1].exec_command(cmd=CMD, sudo=True)
        raise OperationFailedError(
            "Unexpected: Client 2 was able to access file lock while client 1 lock was active"
        )
    except Exception:
        log.info(
            "Expected: Failed to acquire lock from client 2 while client 1 lock is in on"
        )
        return 1

    # Wait for the lock to release
    c1.join()

    # Try again when the lock is released
    try:
        clients[1].exec_command(cmd=CMD, sudo=True)
        log.info(
            "Expected: Successfully acquired lock from client 2 while client 1 lock is released"
        )
    except Exception:
        log.error(
            "Unexpected: Failed to acquire lock from client 2 while client 1 lock is in removed"
        )
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
