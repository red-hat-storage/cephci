from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, perform_lookups
from utility.log import Log

log = Log(__name__)


def create_hard_link(client, mount_point, file_count):
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True, cmd=f"ln {mount_point}/file{i} {mount_point}/link_file{i}"
            )
        except Exception:
            raise OperationFailedError(f"failed to create softlink file{i}")


def run(ceph_cluster, **kw):
    """Verify create file, create soflink and lookups from nfs clients
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    file_count = int(config.get("file_count", "10"))

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

        # Create oprtaions on each client
        operations = [
            Thread(target=create_files, args=(clients[0], nfs_mount, file_count)),
            Thread(target=create_hard_link, args=(clients[1], nfs_mount, file_count)),
            Thread(target=perform_lookups, args=(clients[2], nfs_mount, file_count)),
        ]

        # start opertaion on each client
        for operation in operations:
            operation.start()
            # Adding sleep of 0.5 sleep, hardlink and lookup happen after file creation
            sleep(0.5)

        # Wait for all operation to finish
        for operation in operations:
            operation.join()

    except Exception as e:
        log.error(f"Fail to perform file operation in nfs mount : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
