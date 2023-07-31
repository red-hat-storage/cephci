from threading import Thread
from time import sleep

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.utilities.utils import (
    change_ownership,
    change_permission,
    create_files,
    perform_lookups,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify create file, create soflink and lookups from nfs clients
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    file_count = int(config.get("file_count", "10"))
    operations = config.get("operations")
    Thread_operations = []
    permissions = "+rwx"
    user = "cephuser"

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
        )

        # Create oprtaions on each client
        for client, operation in operations.items():
            if operation == "create_files":
                Thread_operations.append(
                    Thread(
                        target=create_files,
                        args=(clients[int(client[-2:]) - 1], nfs_mount, file_count),
                    )
                )
            elif operation == "change_ownership":
                Thread_operations.append(
                    Thread(
                        target=change_ownership,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_mount,
                            file_count,
                            user,
                        ),
                    )
                )
            elif operation == "perform_lookups":
                Thread_operations.append(
                    Thread(
                        target=perform_lookups,
                        args=(clients[int(client[-2:]) - 1], nfs_mount, file_count),
                    )
                )
            elif operation == "change_permission":
                Thread_operations.append(
                    Thread(
                        target=change_permission,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_mount,
                            file_count,
                            permissions,
                        ),
                    )
                )

        # start opertaion on each client
        for Thread_operation in Thread_operations:
            Thread_operation.start()
            sleep(0.5)

        # Wait to complete operations
        for Thread_operation in Thread_operations:
            Thread_operation.join()

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
