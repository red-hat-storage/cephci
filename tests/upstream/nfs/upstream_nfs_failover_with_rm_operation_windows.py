from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def delete_files(client, window_nfs_mount, file_count):
    """
    Delete files
    """
    for i in range(1, file_count + 1):
        try:
            cmd = f" del {window_nfs_mount}\\win_file{i}"
            client.exec_command(cmd=cmd)
        except Exception:
            raise OperationFailedError(f"failed to delete file win_file{i}")


def run(ceph_cluster, **kw):
    """Verify HA with node reboot and file remove operation in parallel on windows client"""
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
    file_count = 10

    # Linux clients
    linux_clients = ceph_cluster.get_nodes("client")
    no_linux_clients = int(config.get("linux_clients", "1"))
    linux_clients = linux_clients[:no_linux_clients]
    if no_linux_clients > len(linux_clients):
        raise ConfigError("The test requires more linux clients than available")

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

        operations = []

        # Run IO's from windows client
        create_files(windows_clients[0], window_nfs_mount, file_count, True)

        failover_node = nfs_nodes[0]

        # Perform node reboot operation
        th = Thread(
            target=perform_failover,
            args=(nfs_nodes, failover_node, vip),
        )
        operations.append(th)

        # Delete the file from windows mount
        th = Thread(
            target=delete_files,
            args=(windows_clients[0], window_nfs_mount, file_count),
        )
        operations.append(th)

        # Start the operations
        for op in operations:
            op.start()
            sleep(1)

        # Wait for the ops to complete
        for op in operations:
            op.join()

        # Check if the files are delete from mount point
        for i in range(1, file_count + 1):
            cmd = f" dir {window_nfs_mount}\\win_file{i}"
            _, rc = windows_clients[0].exec_command(cmd=cmd, check_ec=False)
            if "File Not Found" in str(rc):
                log.info("File does not exist on mount point")
            else:
                raise OperationFailedError(f"Deleted file still exist- win_file{i}")

    except Exception as e:
        log.error(f"Failed to validate file delete with failover on a ha cluster: {e}")
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
