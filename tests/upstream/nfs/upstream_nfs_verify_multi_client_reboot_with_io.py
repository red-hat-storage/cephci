from threading import Thread

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, perform_lookups, reboot_node
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def dir_s(client, mount_point, file_count):
    for i in range(1, file_count + 1):
        try:
            cmd = f"dir /s {mount_point}win_file{i}"
            client.exec_command(
                cmd=cmd,
            )
        except Exception:
            raise OperationFailedError(f"failed to dir /s win_file{i}")


def run(ceph_cluster, **kw):
    """NFS-Ganesha test parallel readdir operations (dir, findstr, dir /s) from multiple windows Mount
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
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

        # Mount NFS-Ganesha V3 to window clients
        for windows_client in windows_clients:
            cmd = f"mount {nfs_nodes[0].ip_address}:/export_0 {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)

        # Create files from window client1
        create_files(windows_clients[0], window_nfs_mount, 5, True)

        # Run parallel readdir operations (dir, dir /s)
        readdir1 = Thread(
            target=perform_lookups,
            args=(windows_clients[0], window_nfs_mount, 5, True),
        )
        readdir2 = Thread(
            target=dir_s,
            args=(windows_clients[0], window_nfs_mount, 5),
        )

        readdir1.start()
        readdir2.start()

        # Reboot VIP NFS-Ganesha server
        if not reboot_node(servers[0]):
            raise OperationFailedError(f"Node {servers[0].hostname} failed to reboot.")

        readdir1.join()
        readdir2.join()

    except Exception as e:
        log.error(f"Failed to setup nfs-ganesha cluster {e}")
        # Cleanup
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}/*")
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        # Cleanup
        log.info("Cleanup")
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}/*")
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
    return 0
