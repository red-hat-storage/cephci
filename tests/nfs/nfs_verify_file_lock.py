from threading import Thread
from time import sleep

from nfs_operations import cleanup_cluster, enable_v3_locking, setup_nfs_cluster

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def get_file_lock(client):
    """
    Gets the file lock on the file
    Args:
        client (ceph): Ceph client node
    """
    cmd = """python3 -c 'from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN;from time import sleep;f = open(
"/mnt/nfs/sample_file", "w");flock(f.fileno(), LOCK_EX | LOCK_NB);sleep(30);flock(f.fileno(), LOCK_UN)'"""
    client.exec_command(cmd=cmd, sudo=True)


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
    installer = ceph_cluster.get_nodes("installer")[0]

    try:
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
            ceph_cluster=ceph_cluster,
        )
    except Exception as e:
        log.error(f"Failed to setup nfs cluster {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1

    # Check the mount protocol
    if version == 3:
        enable_v3_locking(installer, nfs_name, nfs_node, nfs_server_name)

    # Create a file on Client 1
    file_path = f"{nfs_mount}/sample_file"
    clients[0].exec_command(cmd=f"touch {file_path}", sudo=True)

    # Perform File Lock from client 1
    c1 = Thread(target=get_file_lock, args=(clients[0],))
    c1.start()

    # Adding a constant sleep as its required for the thread call to start the lock process
    sleep(2)
    try:
        get_file_lock(clients[1])
        log.error(
            "Unexpected: Client 2 was able to access file lock while client 1 lock was active"
        )
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1
    except Exception as e:
        log.info(
            f"Expected: Failed to acquire lock from client 2 while client 1 lock is in on {e}"
        )

    c1.join()

    try:
        get_file_lock(clients[1])
        log.info(
            "Expected: Successfully acquired lock from client 2 while client 1 lock is released"
        )
    except Exception as e:
        log.error(
            f"Unexpected: Failed to acquire lock from client 2 while client 1 lock is in removed {e}"
        )
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1

    cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
