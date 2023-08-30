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
    # If there are multiple nfs servers provided, only one is required for mounting
    if isinstance(nfs_server, list):
        nfs_server = nfs_server[0]
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
        nfs_mount (str): nfs mount path
        nfs_name (str): nfs cluster name
        nfs_export (str): nfs export path
    """
    if not isinstance(clients, list):
        clients = [clients]

    for client in clients:
        client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}/*")
        log.info("Unmounting nfs-ganesha mount on client:")
        if Unmount(client).unmount(nfs_mount):
            raise OperationFailedError(f"Failed to unmount nfs on {client.hostname}")
        log.info("Removing nfs-ganesha mount dir on client:")
        client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}")

    Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export)
    Ceph(clients[0]).nfs.cluster.delete(nfs_name)
