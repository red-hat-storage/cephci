import concurrent.futures

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def create_rename_files(mount_point, num_files, client1, client2):
    for i in range(1, num_files + 1):
        try:
            client1.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={mount_point}/file{i} bs=1 count=1",
            )
            client2.exec_command(
                sudo=True,
                cmd=f"mv {mount_point}/file{i} {mount_point}/renamefile{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to create file file{i}")


def create_rename_dirs(mount_point, num_dirs, client1, client2):
    for i in range(1, num_dirs + 1):
        try:
            client1.exec_command(
                sudo=True,
                cmd=f"mkdir {mount_point}/dir{i}",
            )
            client2.exec_command(
                sudo=True,
                cmd=f"mv {mount_point}/dir{i} {mount_point}/renamedir{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to create directory dir{i}")


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
    """Test rename of files and directories on NFS mount
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

        # Create files from Client 1 and perform lookups and rename from client 2 and client 3
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []

            create_files_future = executor.submit(
                create_rename_files,
                nfs_mount,
                num_files,
                client1=clients[0],
                client2=clients[1],
            )
            create_dirs_future = executor.submit(
                create_rename_dirs,
                nfs_mount,
                num_dirs,
                client1=clients[0],
                client2=clients[1],
            )
            perform_lookups_future = executor.submit(
                perform_lookups, clients[2], nfs_mount, num_files, num_dirs
            )

            futures.extend(
                [
                    create_files_future,
                    create_dirs_future,
                    perform_lookups_future,
                ]
            )

            # Wait for creates and renames to complete
            concurrent.futures.wait(
                [
                    create_files_future,
                    create_dirs_future,
                    perform_lookups_future,
                ]
            )

        log.info("Successfully completed the rename tests for files and dirs")

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
