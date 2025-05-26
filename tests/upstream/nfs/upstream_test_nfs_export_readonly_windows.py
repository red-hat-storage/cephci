from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def permission(client, nfs_name, nfs_export, old_permission, new_permission):
    # Change export permissions to RO
    out = Ceph(client).nfs.export.get(nfs_name, f"{nfs_export}_0")
    client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
    client.exec_command(
        sudo=True,
        cmd=f'sed -i \'s/"access_type": "{old_permission}"/"access_type": "{new_permission}"/\' export.conf',
    )
    Ceph(client).nfs.export.apply(nfs_name, "export.conf")

    # Wait till the NFS daemons are up
    sleep(10)


def run(ceph_cluster, **kw):
    """Verify readonly permission of NFS share on windows client"""
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

        # Mount NFS-Ganesha V3 to window
        cmd = f"mount {nfs_nodes[0].ip_address}:/export_0 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(5)

        # Run IO's from window client
        create_files(windows_clients[0], window_nfs_mount, 20, True)

        # Unmount the share from windows client
        cmd = f"umount {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(5)

        # Change export permissions to RO
        permission(
            client, nfs_name, nfs_export, old_permission="RW", new_permission="RO"
        )

        # Remount the nfs share on same windows client
        cmd = f"mount {nfs_nodes[0].ip_address}:{nfs_export}_0 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(5)

        # Try creating files on windows mount point
        cmd = f"type nul > {window_nfs_mount}\\file_ro"
        _, rc = windows_clients[0].exec_command(cmd=cmd, check_ec=False)
        if "The media is write protected" in str(rc):
            log.info("Export is readonly")
        else:
            raise OperationFailedError("File created on Readonly export")

        # Change export permissions to RW
        permission(
            client, nfs_name, nfs_export, old_permission="RO", new_permission="RW"
        )

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
