from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify the rootsquash functionality on windows client"""
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

    # Squashed export parameters
    nfs_export_squash = "/export_1"
    nfs_squash_mount = "/mnt/nfs_squash"

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

        # Create export with squash permission
        Ceph(linux_clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_squash,
            fs=fs_name,
            squash="rootsquash",
            installer=nfs_nodes[0]
        )

        # Mount the volume with rootsquash enable on client
        linux_clients[0].create_dirs(dir_path=nfs_squash_mount, sudo=True)
        if Mount(linux_clients[0]).nfs(
            mount=nfs_squash_mount,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export_squash,
        ):
            raise OperationFailedError(
                f"Failed to mount nfs on {linux_clients[0].hostname}"
            )
        log.info("Mount succeeded on client")

        # Mount NFS-Ganesha V3 to window
        cmd = f"mount {nfs_nodes[0].ip_address}:/export_1 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)
        sleep(15)

        # Try Creating directory from windows mount point with rootsquash enabled
        cmd = f"mkdir {window_nfs_mount}\\squashed_dir"
        _, rc = windows_clients[0].exec_command(cmd=cmd, check_ec=False)
        if "Access is denied" in str(rc):
            log.info("Rootsquash is enabled and root access is denied")
        else:
            log.error("Failed to validate export rootsquash")
            return 1

        # Try Creating file from window mount point with rootsquash enabled
        cmd = f"type nul > {window_nfs_mount}\\squashed_file"
        _, rc = windows_clients[0].exec_command(cmd=cmd, check_ec=False)
        if "Access is denied" in str(rc):
            log.info("Rootsquash is enabled and root access is denied")
        else:
            log.error("Failed to validate export rootsquash")
            return 1

    except Exception as e:
        log.error(f"Failed to validate export rootsquash: {e}")
        return 1
    finally:
        # Cleanup
        log.info("Cleanup")
        log.info("Unmounting nfs-ganesha squash mount on client:")
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}/*")
        if Unmount(linux_clients[0]).unmount(nfs_squash_mount):
            raise OperationFailedError(
                f"Failed to unmount nfs on {linux_clients[0].hostname}"
            )
        log.info("Removing nfs-ganesha squash mount dir on client:")
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_squash_mount}")
        Ceph(linux_clients[0]).nfs.export.delete(nfs_name, nfs_export_squash)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(linux_clients[0], nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
