from threading import Thread

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import get_ip_from_node, reboot_node
from utility.log import Log

log = Log(__name__)


class NfsCleanupFailed(Exception):
    pass


def setup_nfs_cluster(
    clients,
    nfs_server,
    port,
    version,
    nfs_name,
    nfs_mount,
    fs_name,
    export,
    fs,
    ha=False,
    vip=None,
):
    # Step 1: Enable nfs
    Ceph(clients[0]).mgr.module(action="enable", module="nfs", force=True)

    # Step 2: Create an NFS cluster
    Ceph(clients[0]).nfs.cluster.create(
        name=nfs_name, nfs_server=nfs_server, ha=ha, vip=vip
    )

    # Step 3: Perform Export on clients
    i = 0
    for client in clients:
        Ceph(client).nfs.export.create(
            fs_name=fs_name, nfs_name=nfs_name, nfs_export=f"{export}_{i}", fs=fs
        )
        i += 1

    # Step 4: Perform nfs mount
    # If there are multiple nfs servers provided, only one is required for mounting
    if isinstance(nfs_server, list):
        nfs_server = nfs_server[0]
    if ha:
        nfs_server = vip.split("/")[0]  # Remove the port

    i = 0
    for client in clients:
        client.create_dirs(dir_path=nfs_mount, sudo=True)
        if Mount(client).nfs(
            mount=nfs_mount,
            version=version,
            port=port,
            server=nfs_server,
            export=f"{export}_{i}",
        ):
            raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
        i += 1
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

    # Wait until the rm operation is complete
    timeout, interval = 600, 10
    for client in clients:
        # Clear the nfs_mount, at times rm operation can fail
        # as the dir is not empty, this being an expected behaviour,
        # the solution is to repeat the rm operation.
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                client.exec_command(
                    sudo=True, cmd=f"rm -rf {nfs_mount}/*", long_running=True
                )
                break
            except Exception as e:
                log.warning(f"rm operation failed, repeating!. Error {e}")
        if w.expired:
            raise NfsCleanupFailed(
                "Failed to cleanup nfs mount dir even after multiple iterations. Timed out!"
            )

        log.info("Unmounting nfs-ganesha mount on client:")
        if Unmount(client).unmount(nfs_mount):
            raise OperationFailedError(f"Failed to unmount nfs on {client.hostname}")
        log.info("Removing nfs-ganesha mount dir on client:")
        client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}")

    # Delete all exports
    for i in range(len(clients)):
        Ceph(clients[0]).nfs.export.delete(nfs_name, f"{nfs_export}_{i}")
    Ceph(clients[0]).nfs.cluster.delete(nfs_name)


def perform_failover(nfs_nodes, failover_node, vip):
    # Trigger reboot on the failover node
    th = Thread(target=reboot_node, args=(failover_node,))
    th.start()

    # Validate any of the other nodes has got the VIP
    flag = False

    # Remove the port from vip
    if "/" in vip:
        vip = vip.split("/")[0]

    # Perform the check with a timeout of 60 seconds
    for w in WaitUntil(timeout=60, interval=5):
        for node in nfs_nodes:
            if node != failover_node:
                assigned_ips = get_ip_from_node(node)
                log.info(f"IP addrs assigned to node : {assigned_ips}")
                # If vip is assigned, set the flag and exit
                if vip in assigned_ips:
                    flag = True
                    log.info(f"Failover success, VIP reassigned to {node.hostname}")
        if flag:
            break
    if w.expired:
        raise OperationFailedError(
            "The failover process failed and vip is not assigned to the available nodes"
        )
    # Wait for the node to complete reboot
    th.join()
