from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def create_files(client, mount_point, file_count):
    """
    Create files
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        file_count (int): total file count
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"su cephuser -c 'dd if=/dev/urandom of={mount_point}/file{i} bs=1 count=1'",
            )
        except Exception:
            raise OperationFailedError(f"failed to create file file{i}")


def perform_lookups(client, mount_point, file_count):
    """
    Perform lookups
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        file_count (int): total file count
    """
    for i in range(1, file_count + 1):
        out = client.exec_command(
            sudo=True,
            cmd=f"su cephuser -c 'ls -laRt {mount_point}/file{i}'",
        )
        if "S" not in out[0]:
            raise OperationFailedError(f"failed to setuid bit file{i}")


def setuid_bit(client, mount_point, file_count):
    """
    setuid bit
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        file_count (int): total file count
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"su cephuser -c 'chmod u+s {mount_point}/file{i}'",
            )
        except Exception:
            raise OperationFailedError(f"failed to create file file{i}")


def run(ceph_cluster, **kw):
    """Verify setuid bit set on a file
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "3"))
    file_count = int(config.get("file_count", "10"))
    operations = config.get("operations")
    Thread_operations = []
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    nfs_server_name = installer.ip_address
    fs_name = "cephfs"

    # Squashed export parameters
    nfs_export_squash = "/export_squash"
    nfs_squash_mount = "/mnt/nfs_squash"

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

        # Create export with squash permission
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_squash,
            fs=fs_name,
            squash="rootsquash",
            installer=installer
        )
        sleep(9)

        # Mount the volume with rootsquash enable on clients
        clients[0].create_dirs(dir_path=nfs_squash_mount, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_squash_mount,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export_squash,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {clients[0].hostname}")
        log.info("Mount succeeded on client")

        # Create oprtaions on each client
        for client, operation in operations.items():
            if operation == "create_files":
                Thread_operations.append(
                    Thread(
                        target=create_files,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_squash_mount,
                            file_count,
                        ),
                    )
                )
            elif operation == "setuid_bit":
                Thread_operations.append(
                    Thread(
                        target=setuid_bit,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_squash_mount,
                            file_count,
                        ),
                    )
                )
            elif operation == "perform_lookups":
                Thread_operations.append(
                    Thread(
                        target=perform_lookups,
                        args=(
                            clients[int(client[-2:]) - 1],
                            nfs_squash_mount,
                            file_count,
                        ),
                    )
                )

        # start opertaion on each client
        for Thread_operation in Thread_operations:
            Thread_operation.start()
            sleep(3)

        # Wait to complete operations
        for Thread_operation in Thread_operations:
            Thread_operation.join()

    except Exception as e:
        log.error(
            f"Failed to perform faiolver while delete in progress from client2. Error: {e}"
        )
        return 1
    finally:
        sleep(10)
        # Cleaning up the squash export and mount dir
        for client in clients[:2]:
            log.info("Unmounting nfs-ganesha squash mount on client:")
            if Unmount(client).unmount(nfs_squash_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {clients[0].hostname}"
                )
            log.info("Removing nfs-ganesha squash mount dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_squash_mount}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
