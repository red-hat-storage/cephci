from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, get_file_owner
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify the rootsquash post mounting NFS share on windows client"""
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
    nfs_export_squash = "/export_0"
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

        # Run IO's from window client
        create_files(windows_clients[0], window_nfs_mount, 10, True)

        # Change the permission of mount dir from Linux client
        linux_clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}/")

        # Enable rootsquash through export file by editing the conf file
        out = Ceph(linux_clients[0]).nfs.export.get(nfs_name, nfs_export_squash)
        linux_clients[0].exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        linux_clients[0].exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_squash_value}/{new_squash_value}/g' export.conf",
        )
        Ceph(linux_clients[0]).nfs.export.apply(nfs_name, "export.conf")

        # Add wait till the NFS daemons are up
        sleep(10)

        # Rcreate some more files from window mount point post enabling rootsquash
        for i in range(1, 11):
            cmd = f"type nul > {window_nfs_mount}\\squashed_file{i}"
            windows_clients[0].exec_command(cmd=cmd)

        # Check if the files are created by "root" user before enabling the rootsquash
        for i in range(1, 11):
            out = get_file_owner(f"{nfs_mount}/win_file{i}", linux_clients)
            if "root" not in out:
                raise OperationFailedError("File is not created by root user")
        log.info("File created by root user")

        # Check if the files are created by "squashed" user post enabling the rootsquash
        for i in range(1, 11):
            out = get_file_owner(f"{nfs_mount}/squashed_file{i}", linux_clients)
            if "squashuser" not in out:
                raise OperationFailedError("File is not created by squashed user")
        log.info("File created by squashed user")

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
