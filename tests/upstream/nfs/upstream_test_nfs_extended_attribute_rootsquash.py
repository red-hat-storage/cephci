from time import sleep

from upstream_nfs_operations import cleanup_cluster, getfattr, setfattr, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import get_file_owner
from utility.log import Log

log = Log(__name__)


def validate_file_owner(nfs_mount, clients, owner):
    for i in range(1, 11):
        out = get_file_owner(f"{nfs_mount}/win_file{i}", clients)
        if owner not in out:
            raise OperationFailedError(f"File is not created by {owner} user")
    log.info(f"File created by {owner} user")


def run(ceph_cluster, **kw):
    """Verify the rootsquash functionality with extended attributes on v4,2"""
    config = kw.get("config")
    # nfs cluster details
    nfs_nodes = ceph_cluster.get_nodes("installer")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "3")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export1 = "/squashed_export"
    nfs_mount1 = "/mnt/squashed_nfs"
    fs = "cephfs"
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    nfs_node = nfs_nodes[0]
    nfs_server_name = nfs_node.hostname

    # Linux clients
    clients = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "2"))
    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients

    # Squashed export parameters
    nfs_export_squash = "/squashed_export_0"
    original_squash_value = '"squash": "none"'
    new_squash_value = '"squash": "rootsquash"'
    owner = "squashuser"

    filename = "Testfile"

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            clients,
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
        clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount1}/")

        # Enable rootsquash on first export by editing the conf file
        out = Ceph(clients[0]).nfs.export.get(nfs_name, nfs_export_squash)
        clients[0].exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        clients[0].exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_squash_value}/{new_squash_value}/g' export.conf",
        )
        Ceph(clients[0]).nfs.export.apply(nfs_name, "export.conf")

        # Wait till the NFS daemons are up
        sleep(10)

        # Mount the export via v4.2 on client
        clients[0].create_dirs(dir_path=nfs_export_squash, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_mount1,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export_squash,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {clients[0].hostname}")
        log.info("Mount succeeded on client")

        # Create a file on Mount point
        cmd = f"touch {nfs_mount1}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Validate if the files are created by squashed user
        out = get_file_owner(f"{nfs_mount1}/{filename}", clients)
        if owner not in out:
            raise OperationFailedError(f"File is not created by {owner} user")
        log.info(f"File created by {owner} user")

        # Set multiple extended attribute of the file
        for i in range(1, 11):
            setfattr(
                client=clients[0],
                file_path=f"{nfs_mount1}/{filename}",
                attribute_name=f"myattr{i}",
                attribute_value=f"value{i}",
            )
            log.info(f"Successfully set the attribute 'myattr{i}' on file - {filename}")

        # Fetch the extended attribute of the file
        out = getfattr(client=clients[0], file_path=f"{nfs_mount1}/{filename}")

    except Exception as e:
        log.error(f"Failed to validate export rootsquash: {e}")
        return 1
    finally:
        # Cleanup
        log.info("Cleanup")
        log.info("Unmounting nfs-ganesha mounts from client:")
        clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount1}/*")
        if Unmount(clients[0]).unmount(nfs_mount1):
            raise OperationFailedError(
                f"Failed to unmount nfs on {clients[0].hostname}"
            )

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients[0], nfs_mount1, nfs_name, nfs_export1)
        log.info("Cleaning up successfull")
    return 0
