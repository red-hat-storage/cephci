from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def setup_nfs_cluster(
    clients, nfs_server, port, version, nfs_name, nfs_mount, fs_name, export, fs
):
    # Step 1: Enable nfs
    Ceph(clients[0]).mgr.module(action="enable", module="nfs", force=True)

    # Step 2: Create an NFS cluster
    Ceph(clients[0]).nfs.cluster.create(name=nfs_name, nfs_server=nfs_server)

    # Step 3: Perform Export on clients
    for client in clients:
        Ceph(client).nfs.export.create(
            fs_name=fs_name, nfs_name=nfs_name, nfs_export=export, fs=fs
        )

    # Step 4: Perform nfs mount
    for client in clients:
        client.create_dirs(dir_path=nfs_mount, sudo=True)
        if Mount(client).nfs(
            mount=nfs_mount,
            version=version,
            port=port,
            server=nfs_server,
            export=export,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
    log.info("Mount succeeded on all clients")


def cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export):
    """
    Clean up the cluster post nfs operation
    Steps:
        1. rm -rf of the content inside the mount folder --> rm -rf /mnt/nfs/*
        2. Unmount the volume
        3. rm -rf of the mount point
        4. delete export
        5. delete cluster
    Args:
        clients (ceph): Client nodes
        nfs_mount (str, dict): nfs mount path(s)
        nfs_name (str, list): nfs cluster name(s)
        nfs_export (str, dict): nfs export path(s)
    """
    if not isinstance(clients, list):
        clients = [clients]
    if not isinstance(nfs_export, dict):
        nfs_export = {nfs_export: nfs_name}

    for client in clients:
        if isinstance(nfs_mount, dict):
            # To handle the corner scenario where the process failed at one client and other clients were skipped.
            # In those case, we don't have to perform cleanup on unused clients
            if client.hostname not in nfs_mount:
                continue
            mount_path = nfs_mount[client.hostname]
        else:
            mount_path = nfs_mount

        client.exec_command(sudo=True, cmd=f"rm -rf {mount_path}/*")
        log.info("Unmounting nfs-ganesha mount on client:")
        if Unmount(client).unmount(mount_path):
            raise OperationFailedError(f"Failed to unmount nfs on {client.hostname}")
        log.info("Removing nfs-ganesha mount dir on client:")
        client.exec_command(sudo=True, cmd=f"rm -rf  {mount_path}")

    # Delete all exports
    for export, nfs_server in nfs_export.items():
        Ceph(clients[0]).nfs.export.delete(nfs_server, export)
        Ceph(clients[0]).nfs.cluster.delete(nfs_server)
