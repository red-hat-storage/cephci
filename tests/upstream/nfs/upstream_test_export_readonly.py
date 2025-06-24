from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Test readonly export option on NFS mount
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

    # RO export parameters
    nfs_export_readonly = "/exportRO"
    nfs_readonly_mount = "/mnt/nfs_readonly"

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

        # Create export with RO permission
        installer = ceph_cluster.get_nodes("installer")[0]
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_readonly,
            fs=fs_name,
            readonly=True,
            installer=installer
        )

        # Mount the readonlyvolume on client
        clients[0].create_dirs(dir_path=nfs_readonly_mount, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_readonly_mount,
            version=version,
            port=port,
            server=installer.ip_address,
            export=nfs_export_readonly,
        ):
            log.error(f"Failed to mount nfs on {clients[0].hostname}")
            return 1
        log.info("Mount succeeded on client")

        # Test writes on Readonly export
        _, rc = clients[0].exec_command(
            sudo=True, cmd=f"touch {nfs_readonly_mount}/file_ro", check_ec=False
        )
        # Ignore the "Read-only file system" error and consider it as a successful execution
        if "touch: cannot touch" in str(rc) and "Read-only file system" in str(rc):
            log.info("creation of file on RO export failed with expected error")
            pass
        else:
            log.error(f"failed to create file on {clients[0].hostname}")

        # Test writes on RW export
        clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_mount}/file_rw",
        )
    except Exception as e:
        log.error(f"Failed to validate export readonly: {e}")
        return 1

    finally:
        log.info("Cleaning up")
        # Cleaning up the Readonly export and mount dir
        log.info("Unmounting nfs-ganesha readonly mount on client:")
        if Unmount(clients[0]).unmount(nfs_readonly_mount):
            raise OperationFailedError(
                f"Failed to unmount nfs on {clients[0].hostname}"
            )
        log.info("Removing nfs-ganesha readonly mount dir on client:")
        clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_readonly_mount}")
        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export_readonly)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients[0], nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
