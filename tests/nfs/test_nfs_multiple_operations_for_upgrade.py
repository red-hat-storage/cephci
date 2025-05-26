from concurrent.futures import ThreadPoolExecutor

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def create_file(client, nfs_mount, file_name):
    """Create a file in the NFS mount point"""
    try:
        cmd = f"touch {nfs_mount}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)
        log.info("File - {0} created successfully".format(file_name))
    except Exception as e:
        log.error(f"Failed to create file {file_name}: {e}")
        raise OperationFailedError(f"Failed to create file {file_name}: {e}")


def delete_file(client, nfs_mount, file_name):
    """Delete a file in the NFS mount point"""
    try:
        cmd = f"rm -rf {nfs_mount}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)
        log.info("File - {0} deleted successfully".format(file_name))
    except Exception as e:
        log.error(f"Failed to delete file {file_name}: {e}")
        raise OperationFailedError(f"Failed to delete file {file_name}: {e}")


def rename_file(client, nfs_mount, old_name, new_name):
    """Rename a file in the NFS mount point"""
    try:
        cmd = f"mv {nfs_mount}/{old_name} {nfs_mount}/{new_name}"
        client.exec_command(cmd=cmd, sudo=True)
        log.info("File renamed successfully")
    except Exception as e:
        log.error(f"Failed to rename file {old_name} to {new_name}: {e}")
        raise OperationFailedError(
            f"Failed to rename file {old_name} to {new_name}: {e}"
        )


def write_to_file_using_dd_command(client, nfs_mount, file_name, size):
    """Write to a file in the NFS mount point using dd command"""
    try:
        cmd = f"dd if=/dev/zero of={nfs_mount}/{file_name} bs={size}M count=5"
        client.exec_command(cmd=cmd, sudo=True)
        log.info("File written successfully")
    except Exception as e:
        log.error(f"Failed to write to file {file_name}: {e}")
        raise OperationFailedError(f"Failed to write to file {file_name}: {e}")


def read_from_file_using_dd_command(client, nfs_mount, file_name, size):
    """Read from a file in the NFS mount point using dd command"""
    try:
        cmd = f"dd if={nfs_mount}/{file_name} of=/dev/null bs={size}M count=5"
        client.exec_command(cmd=cmd, sudo=True)
        log.info("File read successfully")
    except Exception as e:
        log.error(f"Failed to read from file {file_name}: {e}")
        raise OperationFailedError(f"Failed to read from file {file_name}: {e}")


def permission_to_directory(client, nfs_mount):
    """Provide permission to directory"""
    try:
        cmd = f"chmod 777 {nfs_mount}"
        client.exec_command(cmd=cmd, sudo=True)
        log.info("Permission provided successfully")
    except Exception as e:
        log.error(f"Failed to provide permission to directory {nfs_mount}: {e}")
        raise OperationFailedError(
            f"Failed to provide permission to directory {nfs_mount}: {e}"
        )


def create_nfs_cluster(
    clients,
    nfs_server_name,
    port,
    version,
    nfs_name,
    nfs_mount,
    fs_name,
    nfs_export,
    fs,
    ceph_cluster=None,
):
    """Create NFS cluster"""
    try:

        # Setup NFS cluster
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
    except Exception as e:
        raise ConfigError(f"Failed to create NFS cluster: {e}")


def run(ceph_cluster, **kw):
    """nfs multiple operations for upgrade test
    Test will have multiple operations like create, delete, rename, remove
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    operation = config.get("operation", None)
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    file_count = int(config.get("file_count", "100"))
    dd_command_size_in_M = config.get("dd_command_size_in_M", "100")

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
    old_file_name = "old_file"
    new_file_name = "new_file"
    nfs_server_name = nfs_node.hostname

    try:
        if operation == "before_upgrade":
            # Create NFS Cluster
            create_nfs_cluster(
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
            # Create file in parellel using ThreadPoolExecutor
            log.info("Creating files in parallel using ThreadPoolExecutor")
            with ThreadPoolExecutor(max_workers=None) as executor:
                for client in clients:
                    futures = [
                        executor.submit(
                            create_file,
                            client,
                            nfs_mount,
                            old_file_name + "_{0}".format(i),
                        )
                        for i in range(file_count)
                    ]
                for future in futures:
                    future.result()
            log.info("All files created successfully")

        elif operation == "after_upgrade":
            old_file_name, new_file_name = new_file_name, old_file_name
            # Create file in parellel using ThreadPoolExecutor
            log.info(
                "Creating files in parallel using ThreadPoolExecutor after upgrade"
            )
            with ThreadPoolExecutor(max_workers=None) as executor:
                for client in clients:
                    futures = [
                        executor.submit(
                            create_file,
                            client,
                            nfs_mount,
                            old_file_name + "_{0}".format(i),
                        )
                        for i in range(file_count // 2)
                    ]
                for future in futures:
                    future.result()
            log.info(
                "All delete files during before upgrade are created successfully after upgrade"
            )

        # provide permission to directory
        for client in clients:
            permission_to_directory(client, nfs_mount)
        log.info("Permission provided to directory successfully")

        # write to file using dd command parellel using ThreadPoolExecutor
        log.info("Writing to files in parallel using ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=None) as executor:
            for client in clients:
                futures = [
                    executor.submit(
                        write_to_file_using_dd_command,
                        client,
                        nfs_mount,
                        old_file_name + "_{0}".format(i),
                        dd_command_size_in_M,
                    )
                    for i in range(file_count)
                ]
            for future in futures:
                future.result()
        log.info("All files written successfully")

        # read from file using dd command parellel using ThreadPoolExecutor
        log.info("Reading from files in parallel using ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=None) as executor:
            for client in clients:
                futures = [
                    executor.submit(
                        read_from_file_using_dd_command,
                        client,
                        nfs_mount,
                        old_file_name + "_{0}".format(i),
                        dd_command_size_in_M,
                    )
                    for i in range(file_count)
                ]
            for future in futures:
                future.result()
        log.info("All files read successfully")

        # rename file in parellel using ThreadPoolExecutor
        log.info("Renaming files in parallel using ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=None) as executor:
            for client in clients:
                futures = [
                    executor.submit(
                        rename_file,
                        client,
                        nfs_mount,
                        old_file_name + "_{0}".format(i),
                        new_file_name + "_{0}".format(i),
                    )
                    for i in range(file_count)
                ]
            for future in futures:
                future.result()
        log.info("All files renamed successfully")

        # delete file in parellel using ThreadPoolExecutor
        if operation == "before_upgrade":  # reduce the file count to half
            log.info("delete files in parallel using ThreadPoolExecutor before upgrade")
            with ThreadPoolExecutor(max_workers=None) as executor:
                for client in clients:
                    futures = [
                        executor.submit(
                            delete_file,
                            client,
                            nfs_mount,
                            new_file_name + "_{0}".format(i),
                        )
                        for i in range(file_count // 2)
                    ]
                for future in futures:
                    future.result()
            log.info("half of the files deleted successfully before upgrade")
            return 0

        elif operation == "after_upgrade":
            log.info("delete files in parallel using ThreadPoolExecutor after upgrade")
            with ThreadPoolExecutor(max_workers=None) as executor:
                for client in clients:
                    futures = [
                        executor.submit(
                            delete_file,
                            client,
                            nfs_mount,
                            new_file_name + "_{0}".format(i),
                        )
                        for i in range(file_count)
                    ]
                for future in futures:
                    future.result()
            log.info("All files deleted successfully after upgrade")
            return 0
    except Exception as e:
        raise ConfigError(f"test_nfs_multiple_operations_for_upgrade: {e} failed")
    finally:
        if operation == "after_upgrade":
            # Cleanup NFS Cluster
            cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
