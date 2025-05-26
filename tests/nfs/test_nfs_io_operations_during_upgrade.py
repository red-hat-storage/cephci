from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.nfs_operations import (
    NfsCleanupFailed,
    _get_client_specific_mount_versions,
    exports_mounts_perclient,
)
from tests.nfs.test_nfs_multiple_operations_for_upgrade import (
    create_file,
    delete_file,
    read_from_file_using_dd_command,
    rename_file,
    write_to_file_using_dd_command,
)
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


@retry(OperationFailedError, tries=3, delay=5, backoff=2)
def mount_retry(
    clients, client_num, mount_name, version, port, nfs_server, export_name
):
    if Mount(clients[client_num]).nfs(
        mount=mount_name,
        version=version,
        port=port,
        server=nfs_server,
        export=export_name,
    ):
        return True
    else:
        raise OperationFailedError(
            "Failed to mount nfs on %s" % clients[client_num].hostname
        )


def create_export_and_mount_for_existing_nfs_cluster(
    clients,
    nfs_export,
    nfs_mount,
    export_num,
    fs_name,
    nfs_name,
    fs,
    port,
    version="4.0",
    ha=False,
    vip=None,
    nfs_server=None,
):

    client_export_mount_dict = exports_mounts_perclient(
        clients, nfs_export, nfs_mount, export_num
    )
    for client_num in range(len(clients)):
        for export_num in range(
            len(client_export_mount_dict[clients[client_num]]["export"])
        ):
            export_name = client_export_mount_dict[clients[client_num]]["export"][
                export_num
            ]
            mount_name = client_export_mount_dict[clients[client_num]]["mount"][
                export_num
            ]
            Ceph(clients[client_num]).nfs.export.create(
                fs_name=fs_name,
                nfs_name=nfs_name,
                nfs_export=export_name,
                fs=fs,
            )
            sleep(1)
            # Get the mount versions specific to clients
            mount_versions = _get_client_specific_mount_versions(version, clients)
            # Step 4: Perform nfs mount
            # If there are multiple nfs servers provided, only one is required for mounting
            if isinstance(nfs_server, list):
                nfs_server = nfs_server[0]
            if ha:
                nfs_server = vip.split("/")[0]  # Remove the port
            for version, clients in mount_versions.items():
                clients[client_num].create_dirs(dir_path=mount_name, sudo=True)

                if Mount(clients[client_num]).nfs(
                    mount=mount_name,
                    version=version,
                    port=port,
                    server=nfs_server,
                    export=export_name,
                ):
                    raise OperationFailedError(
                        "Failed to mount nfs on %s" % clients[client_num].hostname
                    )
                sleep(1)
        log.info("Mount succeeded on all clients")

    return client_export_mount_dict


def perform_io_operations_in_loop(
    client_export_mount_dict, clients, file_count, dd_command_size_in_M
):
    """
    Perform IO operations on the mounted NFS exports.

    Args:
        client_export_mount_dict (dict): Dictionary containing client export and mount information.
        clients (list): List of client nodes.
        file_count (int): Number of files to create for each operation.
        dd_command_size_in_M (str): Size in MB for dd command operations.
    """
    file_name = "created_during_upgrade_file"
    renamed_file_name = "re_renamed_during_upgrade_file"
    log.info("Creating files in parallel using ThreadPoolExecutor")
    with ThreadPoolExecutor(max_workers=None) as executor:
        for client in clients:
            for mount in client_export_mount_dict[client]["mount"]:
                futures = [
                    executor.submit(
                        create_file,
                        client,
                        mount,
                        file_name + "_{0}".format(i),
                    )
                    for i in range(file_count)
                ]
        for future in futures:
            future.result()
    log.info("All files created successfully")

    # write to file using dd command parellel using ThreadPoolExecutor
    log.info("Writing to files in parallel using ThreadPoolExecutor")
    with ThreadPoolExecutor(max_workers=None) as executor:
        for client in clients:
            for mount in client_export_mount_dict[client]["mount"]:
                futures = [
                    executor.submit(
                        write_to_file_using_dd_command,
                        client,
                        mount,
                        file_name + "_{0}".format(i),
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
            for mount in client_export_mount_dict[client]["mount"]:
                futures = [
                    executor.submit(
                        read_from_file_using_dd_command,
                        client,
                        mount,
                        file_name + "_{0}".format(i),
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
            for mount in client_export_mount_dict[client]["mount"]:
                futures = [
                    executor.submit(
                        rename_file,
                        client,
                        mount,
                        file_name + "_{0}".format(i),
                        renamed_file_name + "_{0}".format(i),
                    )
                    for i in range(file_count)
                ]
            for future in futures:
                future.result()
    log.info("All files renamed successfully")

    log.info("delete files in parallel using ThreadPoolExecutor during upgrade")
    with ThreadPoolExecutor(max_workers=None) as executor:
        for client in clients:
            for mount in client_export_mount_dict[client]["mount"]:
                futures = [
                    executor.submit(
                        delete_file,
                        client,
                        mount,
                        renamed_file_name + "_{0}".format(i),
                    )
                    for i in range(file_count)
                ]
            for future in futures:
                future.result()


def remove_exports_and_unmount(client_export_mount_dict, clients, nfs_name):
    """
    Unmounts the NFS exports and removes them from the NFS cluster.

    Args:
        client_export_mount_dict (dict): Dictionary containing client export and mount information.
        clients (list): List of client nodes.
    """
    timeout, interval = 600, 10
    for client_num in range(len(clients)):
        for export_num in range(
            len(client_export_mount_dict[clients[client_num]]["export"])
        ):
            export_name = client_export_mount_dict[clients[client_num]]["export"][
                export_num
            ]
            mount_name = client_export_mount_dict[clients[client_num]]["mount"][
                export_num
            ]
            client = list(client_export_mount_dict.keys())[client_num]
            # Clear the nfs_mount, at times rm operation can fail
            # as the dir is not empty, this being an expected behaviour,
            # the solution is to repeat the rm operation.
            for w in WaitUntil(timeout=timeout, interval=interval):
                try:
                    client.exec_command(
                        sudo=True, cmd=f"rm -rf {mount_name}/*", long_running=True
                    )
                    break
                except Exception as e:
                    log.warning(f"rm operation failed, repeating!. Error {e}")
            if w.expired:
                raise NfsCleanupFailed(
                    "Failed to cleanup nfs mount dir even after multiple iterations. Timed out!"
                )
            log.info("Unmounting nfs-ganesha mount on client:")
            sleep(3)
            if Unmount(client).unmount(mount_name):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {client.hostname}"
                )
            log.info("Removing nfs-ganesha mount dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {mount_name}")
            sleep(3)

            # Delete all exports
            Ceph(clients[0]).nfs.export.delete(nfs_name, export_name)
            log.info(
                f"Deleted export {export_name} from NFS cluster {nfs_name} on client {client.hostname}"
            )


def run(ceph_cluster, **kw):
    """
    1, pick the existing NFS cluster
    2. create multiple exports
    3. mount to clients
    4. create a folder so all the oparation from this case will run in that folder
    5. perform Create,read,rename,copy, read/write using DD command and deletion in a loop may be 10 or 20 times
    6. unmount + delete exports
    7.reapeat 1,2,3,4,5, and 6th steps in a loop - 10 or 20 times
    8.donnot delete the cluster
    """
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    loop_count = config.get("loop_count", None)
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    file_count = int(config.get("file_count", "100"))
    dd_command_size_in_M = config.get("dd_command_size_in_M", "100")
    export_num = config.get("exports_number", 1)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    client = clients[0]  # Select only the required number of clients

    # 1. pick the existing NFS cluster
    nfs_cluster_name = Ceph(client).nfs.cluster.ls()[0]
    nfs_hostname = Ceph(client).nfs.cluster.info(nfs_cluster_name)[nfs_cluster_name][
        "backend"
    ][0]["hostname"]
    try:
        for l in range(loop_count):
            log.info(
                f"Starting NFS IO operations loop \n "
                f"Loop - {l + 1}/{loop_count} \n" + "=" * 30 + "\n"
            )

            # 2. create multiple exports
            # 3. mount to clients
            client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
                clients,
                f"/export/nfs_{nfs_cluster_name}",
                f"/mnt/nfs_{nfs_cluster_name}",
                export_num,
                fs_name="cephfs",
                nfs_name=nfs_cluster_name,
                fs="cephfs",
                port=port,
                version=version,
                ha=False,
                nfs_server=nfs_hostname,
            )

            # 4. perform Create, read, rename, copy, read/write using DD command and deletion
            perform_io_operations_in_loop(
                client_export_mount_dict,
                clients,
                file_count,
                dd_command_size_in_M,
            )

            # 5. unmount + delete exports
            log.info("Unmounting and deleting exports")
            remove_exports_and_unmount(
                client_export_mount_dict, clients, nfs_cluster_name
            )
        return 0
    except Exception as e:
        log.error(f"An error occurred during NFS IO operations: {e}")
        raise OperationFailedError(f"NFS IO operations failed: {e}")
