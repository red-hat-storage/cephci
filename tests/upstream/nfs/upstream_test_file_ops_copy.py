from threading import Thread

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def create_copy_files(mount_point, num_files, client1, client2):
    for i in range(1, num_files + 1):
        try:
            client1.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={mount_point}/file{i} bs=1 count=1",
            )
            client2.exec_command(
                sudo=True,
                cmd=f"cp {mount_point}/file{i} {mount_point}/copyfile{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to perform operation on file{i}")


def create_copy_dirs(mount_point, num_dirs, client1, client2):
    for i in range(1, num_dirs + 1):
        try:
            client1.exec_command(
                sudo=True,
                cmd=f"mkdir {mount_point}/dir{i}",
            )
            client2.exec_command(
                sudo=True,
                cmd=f"cp -r {mount_point}/dir{i} {mount_point}/copydir{i}",
            )
        except Exception:
            raise OperationFailedError(
                f"failed to perform operation on directory dir{i}"
            )


def perform_lookups(client, mount_point, num_files, num_dirs):
    for _ in range(1, num_files + num_dirs):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"ls -laRt {mount_point}/",
            )
        except FileNotFoundError as e:
            error_message = str(e)
            if "No such file or directory" not in error_message:
                raise OperationFailedError("failed to perform lookups")
            log.warning(f"Ignoring error: {error_message}")
        except Exception:
            raise OperationFailedError("failed to perform lookups")


def run(ceph_cluster, **kw):
    """Test copy of files and directories on NFS mount
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version")
    num_files = config.get("num_files")
    num_dirs = config.get("num_dirs")
    no_clients = int(config.get("clients", "3"))
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    nfs_server_name = nfs_nodes[0].hostname
    fs_name = "cephfs"

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients

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
            fs_name,
            ceph_cluster=ceph_cluster,
        )
        # Create files and dirs from client 1 and copy files and dirs from client 2
        client1 = clients[0]
        client2 = clients[1]
        operations = [
            Thread(
                target=create_copy_files,
                args=(nfs_mount, num_files, client1, client2),
            ),
            Thread(
                target=create_copy_dirs,
                args=(nfs_mount, num_dirs, client1, client2),
            ),
            Thread(
                target=perform_lookups,
                args=(clients[2], nfs_mount, num_files, num_dirs),
            ),
        ]

        # start opertaion on each client
        for operation in operations:
            operation.start()

        # Wait for all operation to finish
        for operation in operations:
            operation.join()

        log.info("Successfully completed the copy tests for files and dirs")

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
