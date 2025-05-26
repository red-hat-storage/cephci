from threading import Thread

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def create_files(client, mount_point, file_count):
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={mount_point}/file{i} bs=1 count=1",
            )
        except Exception:
            raise OperationFailedError(f"failed to create file file{i}")


def create_soft_link(client, mount_point, file_count):
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True, cmd=f"ln -s {mount_point}/file{i} {mount_point}/link_file{i}"
            )
        except Exception:
            raise OperationFailedError(f"failed to create softlink file{i}")


def perform_lookups(client, mount_point, num_files):
    for _ in range(1, num_files):
        try:
            log.info(
                client.exec_command(
                    sudo=True,
                    cmd=f"ls -laRt {mount_point}/",
                )
            )
        except FileNotFoundError as e:
            error_message = str(e)
            if "No such file or directory" not in error_message:
                raise OperationFailedError("failed to perform lookups")
            log.warning(f"Ignoring error: {error_message}")
        except Exception:
            raise OperationFailedError("failed to perform lookups")


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
            Thread(target=create_soft_link, args=(clients[1], nfs_mount, file_count)),
            Thread(target=perform_lookups, args=(clients[1], nfs_mount, file_count)),
        ]

        # start opertaion on each client
        for operation in operations:
            operation.start()

        # Wait for all operation to finish
        for operation in operations:
            operation.join()

    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
