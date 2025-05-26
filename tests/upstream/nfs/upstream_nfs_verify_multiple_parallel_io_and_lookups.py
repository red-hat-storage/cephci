from threading import Thread

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.io.io import linux_untar
from cli.utilities.utils import create_files
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
    setup = config.get("setup", True)
    io = config.get("io", True)
    cleanup = config.get("io", True)

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
        if setup:
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

        if io:
            # Linux untar on client 1
            th1 = Thread(
                target=linux_untar,
                args=(
                    clients[0],
                    nfs_mount,
                ),
            )

            # Do file creation from client 2
            th2 = Thread(
                target=create_files,
                args=(clients[1], nfs_mount, 100),
            )

            th1.start()
            th2.start()

            # While the IO's are in progress, do the look ups in parallel
            while th1.is_alive() or th2.is_alive():
                # Test 1: Perform Linux untar from 1 client and do readir operation from other client (ls -lart)
                cmd = f"ls -lart {nfs_mount}"
                clients[1].exec_command(cmd=cmd, sudo=True)

                # Test 2: Perform Linux untar from 1 client and do readir operation from other client (du -sh)
                cmd = f"du -sh {nfs_mount}"
                clients[2].exec_command(cmd=cmd, sudo=True)

            th1.join()
            th2.join()

    except Exception as e:
        log.error(f"Failed to validate multiple ios and lookups : {e}")
        return 1
    finally:
        if cleanup:
            log.info("Cleaning up")
            import time

            time.sleep(20)
            cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
            log.info("Cleaning up successful")
    return 0
