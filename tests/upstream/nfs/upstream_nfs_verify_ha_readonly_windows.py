from threading import Thread
from time import sleep

from upstream_nfs_operations import (
    cleanup_cluster,
    perform_failover,
    permission,
    setup_nfs_cluster,
)

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify HA with RO volume set on windows client"""
    config = kw.get("config")
    # nfs cluster details
    nfs_nodes = ceph_cluster.get_nodes("installer")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    servers = nfs_nodes[:no_servers]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "3")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    window_nfs_mount = "Z:"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    port = "12049"

    # Linux clients
    linux_clients = ceph_cluster.get_nodes("client")
    no_linux_clients = int(config.get("linux_clients", "1"))
    linux_clients = linux_clients[:no_linux_clients]
    if no_linux_clients > len(linux_clients):
        raise ConfigError("The test requires more linux clients than available")
    client = linux_clients[0]

    # Windows clients
    windows_clients = []
    for windows_client_obj in setup_windows_clients(config.get("windows_clients")):
        windows_clients.append(windows_client_obj)

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            linux_clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )

        # Fetch the VIP
        if "/" in vip:
            vip = vip.split("/")[0]

        # Mount NFS-Ganesha V3 to window
        cmd = f"mount {vip}:/export_0 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(5)

        # Unmount the share from windows client
        cmd = f"umount {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(5)

        failover_node = nfs_nodes[0]

        operations = []

        # Perform nide reboot operation
        th = Thread(
            target=perform_failover,
            args=(nfs_nodes, failover_node, vip),
        )
        operations.append(th)

        # Change the export permission to RO
        th = Thread(
            target=permission,
            args=(client, nfs_name, nfs_export),
            kwargs={"old_permission": "RW", "new_permission": "RO"},
        )
        operations.append(th)

        # Start the operations
        for op in operations:
            op.start()
            # Adding an explicit sleep for the file create operation to start before the rm begins
            sleep(1)

        # Wait for the ops to complete
        for op in operations:
            op.join()
        sleep(5)

        # Remount the nfs share on same windows client
        cmd = f"mount {vip}:{nfs_export}_0 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(5)

        # Try creating files on windows mount point
        cmd = f"type nul > {window_nfs_mount}\\file_ro"
        _, rc = windows_clients[0].exec_command(cmd=cmd, check_ec=False)
        if "The media is write protected" in str(rc):
            log.info("Export is readonly")
        else:
            raise OperationFailedError("File created on Readonly export")

    except Exception as e:
        log.error(f"Failed to validate readonly with failover on a ha cluster: {e}")
        # Cleanup
        for windows_client in windows_clients:
            cmd = f"del /q /f {window_nfs_mount}\\*.*"
            windows_client.exec_command(cmd=cmd)
            cmd = f"umount {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        # Cleanup
        log.info("Cleanup")
        for windows_client in windows_clients:
            cmd = f"del /q /f {window_nfs_mount}\\*.*"
            windows_client.exec_command(cmd=cmd)
            cmd = f"umount {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
    return 0
