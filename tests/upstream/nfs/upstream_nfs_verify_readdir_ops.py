from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.io.io import linux_untar
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

        # Linux untar on client 1
        io = linux_untar(clients[0], nfs_mount)

        # Test 1: Perform Linux untar from 1 client and do readir operation from other client (ls -lart)
        cmd = f"ls -lart {nfs_mount}"
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Test 2: Perform Linux untar from 1 client and do readir operation from other client (du -sh)
        cmd = f"du -sh {nfs_mount}"
        clients[2].exec_command(cmd=cmd, sudo=True)

        # Perform Linux untar from 1 client and do readir operation from other client (finds)
        cmd = f"find {nfs_mount} -name *.txt"
        clients[3].exec_command(cmd=cmd, sudo=True)

        # Wait for io to complete on all clients
        for th in io:
            th.join()

        # Repeat the tests post untar completes
        # Test 1: Perform Linux untar from 1 client and do readir operation from other client (ls -lart)
        cmd = f"ls -lart {nfs_mount}"
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Test 2: Perform Linux untar from 1 client and do readir operation from other client (du -sh)
        cmd = f"du -sh {nfs_mount}"
        clients[2].exec_command(cmd=cmd, sudo=True)

        # Perform Linux untar from 1 client and do readir operation from other client (finds)
        cmd = f"find {nfs_mount} -name *.txt"
        clients[3].exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        log.error(f"Failed to validate read dir operations : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        sleep(30)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
