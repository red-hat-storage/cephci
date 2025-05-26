from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, remove_files
from utility.log import Log

log = Log(__name__)


def modify_files(client, mount_point, file_count):
    """
    Modify files
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        file_count (int): total file count
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"echo 'test' >> {mount_point}/file{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to modify file file{i}")


def run(ceph_cluster, **kw):
    """Verify NFS HA cluster creation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "2"))
    no_servers = int(config.get("servers", "2"))
    file_count = int(config.get("file_count", "100"))
    operations = config.get("operations")
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    Thread_operations = []

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    # If the setup doesn't have required number of nfs server, exit.
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]

    try:
        # Setup nfs cluster with HA
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
            ha,
            vip,
            ceph_cluster=ceph_cluster,
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
            elif operation == "remove_files":
                Thread_operations.append(
                    Thread(
                        target=remove_files,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_mount,
                            file_count,
                        ),
                    )
                )
            elif operation == "modify_files":
                Thread_operations.append(
                    Thread(
                        target=modify_files,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_mount,
                            file_count,
                        ),
                    )
                )

        # start opertaion on each client
        for Thread_operation in Thread_operations:
            Thread_operation.start()
            sleep(3)

        # Perfrom and verify failover
        failover_node = nfs_nodes[0]
        perform_failover(nfs_nodes, failover_node, vip)

        # Wait to complete operations
        for Thread_operation in Thread_operations:
            Thread_operation.join()

    except Exception as e:
        log.error(
            f"Failed to perform faiolver while delete in progress from client2. Error: {e}"
        )
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
