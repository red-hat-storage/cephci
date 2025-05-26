from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import create_files, get_file_owner
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def validate_file_owner(nfs_mount, linux_clients, owner):
    for i in range(1, 11):
        out = get_file_owner(f"{nfs_mount}/win_file{i}", linux_clients)
        if owner not in out:
            raise OperationFailedError(f"File is not created by {owner} user")
    log.info(f"File created by {owner} user")


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
    nfs_export1 = "/squashed_export"
    nfs_mount1 = "/mnt/squashed_nfs"
    window_nfs_mount1 = "Y:"
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
    owner = ["squashuser", "rootuser"]

    # Second export parameters
    nfs_export2 = "/default_export"
    nfs_mount2 = "/mnt/default_nfs"
    window_nfs_mount2 = "Z:"

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            linux_clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount1,
            fs_name,
            nfs_export1,
            fs,
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )

        # Change the permission of mount dir from Linux client
        linux_clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount1}/")

        # Enable rootsquash on first export by editing the conf file
        out = Ceph(linux_clients[0]).nfs.export.get(nfs_name, nfs_export_squash)
        linux_clients[0].exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        linux_clients[0].exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_squash_value}/{new_squash_value}/g' export.conf",
        )
        Ceph(linux_clients[0]).nfs.export.apply(nfs_name, "export.conf")

        # Wait till the NFS daemons are up
        sleep(10)

        # Mount first export via V3 to windows client
        cmd = f"mount {nfs_nodes[0].ip_address}:{nfs_export_squash} {window_nfs_mount1}"
        windows_clients[0].exec_command(cmd=cmd)

        # Create files on first export from window client
        create_files(windows_clients[0], window_nfs_mount1, 10, True)

        # Validate if the files are created by squashed user
        validate_file_owner(nfs_mount1, linux_clients, owner=owner[0])

        # Create second export with default values
        Ceph(linux_clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export2,
            fs=fs_name,
            installer=nfs_nodes[0]
        )

        # Mount the second export via v3 to window clients
        cmd = f"mount {nfs_nodes[0].ip_address}:/default_export {window_nfs_mount2}"
        windows_clients[0].exec_command(cmd=cmd)

        # Create a directory on mount point
        cmd = f"mkdir {window_nfs_mount2}\\root_dir"
        windows_clients[0].exec_command(cmd=cmd)

        # Directory path for windows and linux for second export
        mount_dir_win = f"{window_nfs_mount2}\\root_dir"
        mount_dir_lin = f"{nfs_mount2}/root_dir"

        # Create files on second export from window client
        create_files(windows_clients[0], mount_dir_win, 10, True)

        # Mount the second export on linux client
        linux_clients[0].create_dirs(dir_path=nfs_export2, sudo=True)
        if Mount(linux_clients[0]).nfs(
            mount=nfs_mount2,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export2,
        ):
            raise OperationFailedError(
                f"Failed to mount nfs on {linux_clients[0].hostname}"
            )
        log.info("Mount succeeded on client")

        # Validate if the files are created by root user
        validate_file_owner(mount_dir_lin, linux_clients, owner=owner[1])

    except Exception as e:
        log.error(f"Failed to validate export rootsquash: {e}")
        return 1
    finally:
        # Cleanup
        log.info("Cleanup")
        log.info("Unmounting nfs-ganesha mounts from client:")
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount2}/*")
        if Unmount(linux_clients[0]).unmount(nfs_mount2):
            raise OperationFailedError(
                f"Failed to unmount nfs on {linux_clients[0].hostname}"
            )
        log.info("Removing nfs-ganesha mount dir on client:")
        linux_clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount2}")
        Ceph(linux_clients[0]).nfs.export.delete(nfs_name, nfs_export2)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(linux_clients[0], nfs_mount1, nfs_name, nfs_export1)
        log.info("Cleaning up successfull")
    return 0
