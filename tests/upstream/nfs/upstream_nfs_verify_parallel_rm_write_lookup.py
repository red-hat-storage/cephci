from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, perform_lookups
from utility.log import Log

log = Log(__name__)


def perform_rm(client, nfs_mount, file_count):
    # Rm to be performed on files created else it fails.
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf {nfs_mount}/file{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to Remove file 'file{i}'")


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
    file_count = 100
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

        operations = []
        # Create large files from client 1
        th = Thread(
            target=create_files,
            args=(clients[1], nfs_mount, file_count),
        )
        operations.append(th)

        # Perform lookups from client 2
        th = Thread(target=perform_lookups, args=(clients[2], nfs_mount, file_count))
        operations.append(th)

        # Start the operations
        for op in operations:
            op.start()
            # Adding an explicit sleep for the file create operation to start before the rm begins
            sleep(1)

        # log.info("Performing rm while file creation and lookup is in progress")
        while operations[0].is_alive() or operations[1].is_alive():
            perform_rm(clients[0], nfs_mount, file_count)

        # Wait for the ops to complete
        for op in operations:
            op.join()

    except Exception as e:
        log.error(f"Failed to validate parallel write, lookup and rm : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
