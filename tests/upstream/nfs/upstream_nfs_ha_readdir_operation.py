from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.io import linux_untar
from cli.utilities.utils import create_files, perform_lookups, reboot_node, remove_files
from utility.log import Log

log = Log(__name__)


def perfrom_linux_untar(client, nfs_mount):
    """Perfrom linux untar
    Args:
        clients (ceph): Client nodes
        nfs_mount (str): mount path
    """
    try:
        linux_untar(client, nfs_mount)
    except Exception:
        raise OperationFailedError("failed to perform linux_untar")


def perfrom_du_sh(client, nfs_mount):
    """Perfrom du sh operation
    Args:
        clients (ceph): Client nodes
        nfs_mount (str): mount path
    """
    try:
        client.exec_command(cmd=f"du -sh {nfs_mount}", sudo=True)
    except Exception:
        raise OperationFailedError("failed to perform linux_untar")


def run(ceph_cluster, **kw):
    """Verify create file, create soflink and lookups from nfs clients
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "3"))
    no_servers = int(config.get("servers", "2"))
    file_count = int(config.get("file_count", "100"))
    operations = config.get("operations")
    Thread_operations = []
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    # If the setup doesn't have required number of nfs server, exit.
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]

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
            elif operation == "perfrom_linux_untar":
                Thread_operations.append(
                    Thread(
                        target=perfrom_linux_untar,
                        args=(clients[int(client[-2:]) - 1], nfs_mount),
                    )
                )
            elif operation == "perform_lookups":
                Thread_operations.append(
                    Thread(
                        target=perform_lookups,
                        args=(clients[int(client[-2:]) - 1], nfs_mount, file_count),
                    )
                )
            elif operation == "perfrom_du_sh":
                Thread_operations.append(
                    Thread(
                        target=perfrom_du_sh,
                        args=(clients[int(client[-2:]) - 1], nfs_mount),
                    )
                )

        # start opertaion on each client
        for Thread_operation in Thread_operations:
            Thread_operation.start()
            sleep(3)

        # Reboot VIP NFS-Ganesha server
        if not reboot_node(servers[0]):
            raise OperationFailedError(f"Node {servers[0].hostname} failed to reboot.")
            return 1

        # Wait to complete operations
        for Thread_operation in Thread_operations:
            Thread_operation.join()

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        sleep(100)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
