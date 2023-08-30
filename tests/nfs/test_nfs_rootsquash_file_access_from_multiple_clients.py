from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import perform_lookups
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Test squash export option on NFS mount using conf file
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
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
        )

        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_squash,
            fs=fs_name,
            squash="rootsquash",
        )

        # Change the permission of mount dir
        clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}/")

        # Mount the volume
        for client in clients[:2]:
            client.create_dirs(dir_path=nfs_squash_mount, sudo=True)
            if Mount(client).nfs(
                mount=nfs_squash_mount,
                version=version,
                port=port,
                server=nfs_server_name,
                export=nfs_export_squash,
            ):
                raise OperationFailedError(
                    f"Failed to mount nfs on {clients[0].hostname}"
                )
            log.info("Mount succeeded on client")

        # Create file on squashed dir
        clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_squash_mount}/file_nosquash",
        )

        # Try accessing the file from client 2
        perform_lookups(clients[1], nfs_squash_mount, 1)

    except Exception as e:
        log.error(f"Failed to validate export rootsquash: {e}")
        return 1

    finally:
        # Cleaning up the squash export and mount dir
        log.info("Unmounting nfs-ganesha squash mount on client:")
        for client in clients[:2]:
            if Unmount(client).unmount(nfs_squash_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {clients[0].hostname}"
                )
            log.info("Removing nfs-ganesha squash mount dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_squash_mount}")
            Ceph(client).nfs.export.delete(nfs_name, nfs_export_squash)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
