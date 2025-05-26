from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.utilities.utils import create_files
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Test simultaneous mount of same or different export on both linux (v4.1) and windows (v3)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")

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

        # Mount NFS-Ganesha V3 to window
        cmd = f"mount {nfs_nodes[0].ip_address}:/export_0 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(15)

        # Run parallel IO in linux and window mount point
        linux_io = Thread(
            target=create_files,
            args=(linux_clients[0], nfs_mount, 10),
        )

        windows_io = Thread(
            target=create_files,
            args=(windows_clients[0], window_nfs_mount, 10, True),
        )
        linux_io.start()
        windows_io.start()

        linux_io.join()
        windows_io.join()
    except Exception as e:
        log.error(f"Failed to setup nfs-ganesha cluster {e}")
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
        for windows_client in windows_clients:
            cmd = f"del /q /f {window_nfs_mount}\\*.*"
            windows_client.exec_command(cmd=cmd)
            cmd = f"umount {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
    return 0
