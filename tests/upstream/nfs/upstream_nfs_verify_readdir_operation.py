from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, perform_lookups
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def create_dirs(client, window_nfs_mount):
    try:
        for i in range(1, 11):
            cmd = f"mkdir {window_nfs_mount}\\squashed_dir{i}"
            client.exec_command(cmd=cmd)
    except Exception:
        raise OperationFailedError(f"failed to create directory squashed_dir{i}")


def run(ceph_cluster, **kw):
    """Verify the rootsquash functionality on windows client with readdir operations in parallel"""
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
    nfs_export1 = "/squashed_export"
    nfs_mount = "/mnt/squashed_nfs"
    window_nfs_mount = "Y:"
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

    # Squashed export parameters
    nfs_export_squash = "/squashed_export_0"
    original_squash_value = '"squash": "none"'
    new_squash_value = '"squash": "rootsquash"'

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
            nfs_export1,
            fs,
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )

        # Change the permission of mount dir from Linux client
        linux_clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}/")

        # Enable rootsquash on the export by editing the conf file
        out = Ceph(linux_clients[0]).nfs.export.get(nfs_name, nfs_export_squash)
        linux_clients[0].exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        linux_clients[0].exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_squash_value}/{new_squash_value}/g' export.conf",
        )
        Ceph(linux_clients[0]).nfs.export.apply(nfs_name, "export.conf")

        # Wait till the NFS daemons are up
        sleep(10)

        # Mount NFS-Ganesha V3 to window clients
        for windows_client in windows_clients:
            cmd = f"mount {nfs_nodes[0].ip_address}:{nfs_export_squash} {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)

        # Run parallel readdir operations with creation of files and dirs in loop
        readdir1 = Thread(
            target=perform_lookups,
            args=(windows_clients[1], window_nfs_mount, 5, True),
        )
        readdir2 = Thread(
            target=create_dirs,
            args=(windows_clients[0], window_nfs_mount),
        )

        readdir3 = Thread(
            target=create_files,
            args=(windows_clients[0], window_nfs_mount, 10, True),
        )

        readdir1.start()
        readdir2.start()
        readdir3.start()

        readdir1.join()
        readdir2.join()
        readdir3.join()

    except Exception as e:
        log.error(f"Failed to validate export rootsquash: {e}")
        # Cleanup
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}/*")
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export1)
        return 1
    finally:
        # Cleanup
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}/*")
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export1)
    return 0
