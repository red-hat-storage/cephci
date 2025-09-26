from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.nfs_operations import (
    NfsCleanupFailed,
    _get_client_specific_mount_versions,
    exports_mounts_perclient,
    mount_retry,
)
from tests.nfs.test_nfs_multiple_operations_for_upgrade import (
    create_file,
    delete_file,
    read_from_file_using_dd_command,
    rename_file,
    write_to_file_using_dd_command,
)
from utility.log import Log

log = Log(__name__)


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
    **kwargs,
):
    """
    Create exports and mount them from existing NFS clusters.

    Args:
        clients: List of client nodes
        nfs_export: Export path(s) for single/multi cluster
        nfs_mount: Mount path(s)
        export_num: Number of exports per cluster
        fs_name: Filesystem name(s)
        nfs_name: NFS cluster name(s)
        fs: Filesystem(s)
        port: NFS server port(s)
        version: NFS version(s)
        ha: High availability flag
        vip: Virtual IP for HA
        nfs_server: NFS server address(es)
        kwargs: Additional arguments for export creation
    """
    # Detect multi-cluster mode
    if isinstance(nfs_name, list):
        num_clusters = len(nfs_name)
        log.info(f"Multi-cluster mode: {num_clusters} clusters")

        # Ensure nfs_server list matches number of clusters if provided
        if isinstance(nfs_server, list):
            if len(nfs_server) < num_clusters:
                log.warning(
                    f"nfs_server list ({len(nfs_server)}) shorter than number of clusters ({num_clusters})"
                )
                # Extend nfs_server list by repeating the last server
                nfs_server.extend([nfs_server[-1]] * (num_clusters - len(nfs_server)))

        all_clusters_export_mounts = {}
        for i in range(num_clusters):
            _nfs_export = nfs_export[i] if isinstance(nfs_export, list) else nfs_export
            _nfs_mount = nfs_mount[i] if isinstance(nfs_mount, list) else nfs_mount
            _nfs_name = nfs_name[i]
            _fs = fs[i] if isinstance(fs, list) else fs
            _port = port[i] if isinstance(port, list) else port
            _nfs_server = nfs_server[i] if isinstance(nfs_server, list) else nfs_server

            log.info(
                f"Creating/mounting for cluster {i + 1}/{num_clusters}:"
                f"\n - Name: {_nfs_name}"
                f"\n - Export: {_nfs_export}"
                f"\n - Mount: {_nfs_mount}"
                f"\n - Server: {_nfs_server}"
                f"\n - Port: {_port}"
            )

            # Use the rest of the logic as for single cluster
            client_export_mount_dict = exports_mounts_perclient(
                clients, _nfs_export, _nfs_mount, export_num
            )
            for client_num in range(len(clients)):
                for export_idx in range(
                    len(client_export_mount_dict[clients[client_num]]["export"])
                ):
                    export_name = client_export_mount_dict[clients[client_num]][
                        "export"
                    ][export_idx]
                    mount_name = client_export_mount_dict[clients[client_num]]["mount"][
                        export_idx
                    ]

                    if kwargs:
                        Ceph(clients[client_num]).nfs.export.create(
                            fs_name=fs_name,
                            nfs_name=_nfs_name,
                            nfs_export=export_name,
                            fs=_fs,
                            **kwargs,
                        )
                    else:
                        Ceph(clients[client_num]).nfs.export.create(
                            fs_name=fs_name,
                            nfs_name=_nfs_name,
                            nfs_export=export_name,
                            fs=_fs,
                        )
                    all_exports = Ceph(clients[0]).nfs.export.ls(nfs_name)
                    if export_name not in all_exports:
                        raise OperationFailedError(
                            f"Export {export_name} not found in the list of exports {all_exports}"
                        )
                    sleep(1)

                    # Get the mount versions specific to clients
                    mount_versions = _get_client_specific_mount_versions(
                        version, clients
                    )
                    # Use only the i-th nfs_server if passed as a list
                    _cluster_nfs_server = _nfs_server
                    if isinstance(_nfs_server, list):
                        _cluster_nfs_server = _nfs_server[i]
                    if ha:
                        _cluster_nfs_server = vip.split("/")[0]  # Remove the port
                    for mount_version, _clients in mount_versions.items():
                        _clients[client_num].create_dirs(dir_path=mount_name, sudo=True)
                        if not mount_retry(
                            client=_clients[client_num],
                            mount_name=mount_name,
                            version=mount_version,
                            port=_port,
                            nfs_server=_cluster_nfs_server,
                            export_name=export_name,
                        ):
                            log.info(f"Mount failed, {mount_name}")
                            raise OperationFailedError(
                                f"Failed to mount nfs on {_clients[client_num].hostname}"
                            )
                        sleep(1)
                    log.info("Mount succeeded on all clients.")

            all_clusters_export_mounts[_nfs_name] = client_export_mount_dict
        return all_clusters_export_mounts

    # ---------- single-cluster logic ----------
    client_export_mount_dict = exports_mounts_perclient(
        clients, nfs_export, nfs_mount, export_num
    )
    for client_num in range(len(clients)):
        for export_idx in range(
            len(client_export_mount_dict[clients[client_num]]["export"])
        ):
            export_name = client_export_mount_dict[clients[client_num]]["export"][
                export_idx
            ]
            mount_name = client_export_mount_dict[clients[client_num]]["mount"][
                export_idx
            ]
            if kwargs:
                Ceph(clients[client_num]).nfs.export.create(
                    fs_name=fs_name,
                    nfs_name=nfs_name,
                    nfs_export=export_name,
                    fs=fs,
                    **kwargs,
                )
            else:
                Ceph(clients[client_num]).nfs.export.create(
                    fs_name=fs_name, nfs_name=nfs_name, nfs_export=export_name, fs=fs
                )
            sleep(1)
            mount_versions = _get_client_specific_mount_versions(version, clients)
            # If list, only mount on first nfs_server for single cluster for backward compatibility
            _nfs_server = nfs_server[0] if isinstance(nfs_server, list) else nfs_server
            if ha:
                _nfs_server = vip.split("/")
            for mount_version, _clients in mount_versions.items():
                _clients[client_num].create_dirs(dir_path=mount_name, sudo=True)
                if not mount_retry(
                    client=_clients[client_num],
                    mount_name=mount_name,
                    version=mount_version,
                    port=port,
                    nfs_server=_nfs_server,
                    export_name=export_name,
                ):
                    log.info(f"Mount failed, {mount_name}")
                    raise OperationFailedError(
                        f"Failed to mount nfs on {_clients[client_num].hostname}"
                    )
                sleep(1)
            log.info("Mount succeeded on all clients.")
    return client_export_mount_dict


def perform_io_operations_in_loop(
    client_export_mount_dict,
    clients,
    file_count,
    dd_command_size_in_M,
    multicluster=False,
):
    """
    Perform IO operations on mounted NFS exports for single or multiple clusters.

    Args:
        client_export_mount_dict (dict): For single cluster: {client: {'mount': [...], 'export': [...]}},
                                       For multi cluster: {cluster_name: {client: {'mount': [...], 'export': [...]}}
        clients (list): List of client nodes
        file_count (int): Number of files to create for each operation
        dd_command_size_in_M (int): Size in MB for dd command operations
        multicluster (bool): Whether this is a multi-cluster operation
    """
    file_name = "created_during_upgrade_file"
    renamed_file_name = "re_renamed_during_upgrade_file"

    def _process_single_cluster(mount_dict):
        """Helper function to process IO for a single cluster's mounts"""
        # Create files
        log.info(f"Creating {file_count} files on each mount point")
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = []
            for client in clients:
                for mount in mount_dict[client]["mount"]:
                    for i in range(file_count):
                        futures.append(
                            executor.submit(
                                create_file,
                                client,
                                mount,
                                f"{file_name}_{i}",
                            )
                        )
            for future in futures:
                future.result()
        log.info("File creation completed")

        # Write to files using dd
        log.info(f"Writing {dd_command_size_in_M}M to each file")
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = []
            for client in clients:
                for mount in mount_dict[client]["mount"]:
                    for i in range(file_count):
                        futures.append(
                            executor.submit(
                                write_to_file_using_dd_command,
                                client,
                                mount,
                                f"{file_name}_{i}",
                                dd_command_size_in_M,
                            )
                        )
            for future in futures:
                future.result()
        log.info("Write operations completed")

        # Read from files using dd
        log.info("Reading back written files")
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = []
            for client in clients:
                for mount in mount_dict[client]["mount"]:
                    for i in range(file_count):
                        futures.append(
                            executor.submit(
                                read_from_file_using_dd_command,
                                client,
                                mount,
                                f"{file_name}_{i}",
                                dd_command_size_in_M,
                            )
                        )
            for future in futures:
                future.result()
        log.info("Read operations completed")

        # Rename files
        log.info("Renaming all files")
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = []
            for client in clients:
                for mount in mount_dict[client]["mount"]:
                    for i in range(file_count):
                        futures.append(
                            executor.submit(
                                rename_file,
                                client,
                                mount,
                                f"{file_name}_{i}",
                                f"{renamed_file_name}_{i}",
                            )
                        )
            for future in futures:
                future.result()
        log.info("Rename operations completed")

        # Delete files
        log.info("Deleting all files")
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = []
            for client in clients:
                for mount in mount_dict[client]["mount"]:
                    for i in range(file_count):
                        futures.append(
                            executor.submit(
                                delete_file,
                                client,
                                mount,
                                f"{renamed_file_name}_{i}",
                            )
                        )
            for future in futures:
                future.result()
        log.info("Delete operations completed")

    if multicluster:
        log.info(f"Running IO operations on {len(client_export_mount_dict)} clusters")
        for cluster_name, mount_dict in client_export_mount_dict.items():
            log.info(f"Processing IO operations for cluster: {cluster_name}")
            _process_single_cluster(mount_dict)
            log.info(f"Completed IO operations for cluster: {cluster_name}")
    else:
        log.info("Running IO operations on single cluster")
        _process_single_cluster(client_export_mount_dict)
        log.info("Completed all IO operations")


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
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

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
                ha=ha,
                nfs_server=nfs_hostname,
                vip=vip,
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
