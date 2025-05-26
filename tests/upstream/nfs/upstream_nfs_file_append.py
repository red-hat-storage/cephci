from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
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

        # Create files from client 1
        for i in range(10):
            cmd = f"touch {nfs_mount}/test_file_{i}"
            clients[0].exec_command(cmd=cmd, sudo=True)

        # From client 2 append to the file created
        thread_pool = []
        for i in range(10):
            cmd = f"dd if=/dev/urandom of={nfs_mount}/test_file_{i} bs=10M count=1"
            th = Thread(
                target=lambda: clients[1].exec_command(cmd=cmd, sudo=True), args=()
            )
            th.start()
            # Adding an explicit time for the append operation to start
            sleep(3)
            thread_pool.append(th)

        # While the file append is in progress (latest running thread), perform the read ops
        while thread_pool[-1].is_alive():
            for op in ["ls -lart", "du -sh", "find"]:
                cmd = f"{op} {nfs_mount}"
                clients[2].exec_command(cmd=cmd, sudo=True)

        # Wait for io to complete on all clients
        for th in thread_pool:
            th.join()

    except Exception as e:
        log.error(f"Failed to validate file append operation : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
