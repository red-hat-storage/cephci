from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, perform_lookups
from utility.log import Log

log = Log(__name__)


def modify_file(client, mount_point, file_count, file_size, modified_time):
    for i in range(1, file_count + 1):
        # Modify and check file size
        client.exec_command(
            sudo=True, cmd=f"truncate -s {file_size} {mount_point}/file{i}"
        )
        out = client.exec_command(cmd=f"du -h {mount_point}/file{i}", sudo=True)
        if file_size not in out[0]:
            raise OperationFailedError(f"failed to modify file{i} size")

        # Modify and check access and modification time
        client.exec_command(
            sudo=True, cmd=f"touch -t {modified_time} {mount_point}/file{i}"
        )
        out = client.exec_command(
            cmd=f'ls -l --time-style="+%Y%m%d%H%M.%S" {mount_point}/file{i}', sudo=True
        )
        if modified_time not in out[0]:
            raise OperationFailedError(f"failed to modify file{i} access time")


def run(ceph_cluster, **kw):
    """Modifying file attributes such as size, modification time, and access time
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
    file_size = config.get("file_size", "100K")
    modified_time = config.get("modified_time", "202308100000.00")

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
            Thread(
                target=modify_file,
                args=(clients[1], nfs_mount, file_count, file_size, modified_time),
            ),
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
