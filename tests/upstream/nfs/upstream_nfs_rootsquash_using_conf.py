from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import get_file_owner
from utility.log import Log

log = Log(__name__)


def rootsquash_using_conf(
    client, nfs_name, nfs_export_squash, original_squash_value, new_squash_value
):
    """Create rootsquash using conf file
    Args:
        client(obj): client object
        nfs_name(str): nfs server name
        nfs_export_squash(str): nfs squash export name
        original_squash_value(str): original squash value in conf file
        new_squash_value(str): new squash value in conf file
    """
    try:
        out = Ceph(client).nfs.export.get(nfs_name, nfs_export_squash)
        client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        client.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_squash_value}/{new_squash_value}/g' export.conf",
        )
        Ceph(client).nfs.export.apply(nfs_name, "export.conf")
    except Exception:
        raise OperationFailedError("failed to enable rootsquash using conf file")


def run(ceph_cluster, **kw):
    """Test squash export option on NFS mount using conf file
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version")
    no_clients = int(config.get("clients", "3"))
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    nfs_server_name = nfs_nodes[0].hostname
    fs_name = "cephfs"

    # Squashed export parameters
    nfs_export_squash = "/export_squash"
    nfs_squash_mount = "/mnt/nfs_squash"
    original_squash_value = '"squash": "none"'
    new_squash_value = '"squash": "rootsquash"'

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs_name,
            ceph_cluster=ceph_cluster,
        )

        # Create export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_squash,
            fs=fs_name,
            installer=nfs_nodes[0]
        )

        # Enable rootsquash using conf file
        rootsquash_using_conf(
            clients[0],
            nfs_name,
            nfs_export_squash,
            original_squash_value,
            new_squash_value,
        )

        # Change the permission of mount dir
        clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}/")

        # Mount the volume
        clients[0].create_dirs(dir_path=nfs_squash_mount, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_squash_mount,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export_squash,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {clients[0].hostname}")
        log.info("Mount succeeded on client")

        # Create file on non squashed dir and check permission
        clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_mount}/file_nosquash",
        )
        out = get_file_owner(f"{nfs_mount}/file_nosquash", clients)
        if "root" not in out:
            raise OperationFailedError("File is not created by root user")
        log.info("File created by root user")

        # Create file on squashed dir and check permission
        clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_squash_mount}/file_squashed",
        )
        out = get_file_owner(f"{nfs_squash_mount}/file_squashed", clients)
        if "squashuser" not in out:
            raise OperationFailedError("File is not created by squashed user")
        log.info("File created by squashed user")

    except Exception as e:
        log.error(f"Failed to validate export rootsquash: {e}")
        return 1

    finally:
        log.info("Cleaning up")
        # Cleaning up the squash export and mount dir
        log.info("Unmounting nfs-ganesha squash mount on client:")
        if Unmount(clients[0]).unmount(nfs_squash_mount):
            raise OperationFailedError(
                f"Failed to unmount nfs on {clients[0].hostname}"
            )
        log.info("Removing nfs-ganesha squash mount dir on client:")
        clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_squash_mount}")
        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export_squash)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients[0], nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
