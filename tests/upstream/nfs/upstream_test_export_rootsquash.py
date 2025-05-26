from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster, create_export

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def get_file_owner(filepath, clients):
    out = clients[0].exec_command(sudo=True, cmd=f"ls -n {filepath}")
    uid = int(out[0].split()[2])
    gid = int(out[0].split()[3])
    if uid == 0 and gid == 0:
        log.info(f"The file '{filepath}' is created by the root user.")
        return "rootuser"
    elif uid == 4294967294 and gid == 4294967294:
        log.info(f"The file '{filepath}' is created by the squashed user.")
        return "squashuser"
    else:
        log.info(
            f"The file '{filepath}' is created by an unknown user with UID: {uid} and GID: {gid}."
        )
        return None


def run(ceph_cluster, **kw):
    """Test squash export option on NFS mount
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    installer = ceph_cluster.get_nodes("installer")[0]
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

        # Create export with squash permission
        create_export(installer, nfs_export_squash,squash="rootsquash")
        # Ceph(clients[0]).nfs.export.create(
        #     fs_name=fs_name,
        #     nfs_name=nfs_name,
        #     nfs_export=nfs_export_squash,
        #     fs=fs_name,
        #     squash="rootsquash",
        # )

        # Mount the volume with rootsquash enable on client
        sleep(9)
        clients[0].create_dirs(dir_path=nfs_squash_mount, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_squash_mount,
            version=version,
            port=port,
            server=installer.ip_address,
            export=nfs_export_squash,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {clients[0].hostname}")
        log.info("Mount succeeded on client")

        # Create file on non squashed dir
        clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_mount}/file_nosquash",
        )

        # Check permission of file created by root user
        clients[0].exec_command(sudo=True, cmd=f"ls -n {nfs_mount}/file_nosquash")
        out = get_file_owner(f"{nfs_mount}/file_nosquash", clients)
        if "root" not in out:
            log.error("File is not created by root user")
            return 1
        log.info("File created by root user")

        # Try creating file on squashed dir
        _, rc = clients[0].exec_command(
            sudo=True, cmd=f"touch {nfs_squash_mount}/file_squashed", check_ec=False
        )
        if "Permission denied" in str(rc):
            log.info("Rootsquash is enabled and root access is denied")
        else:
            log.error("Failed to validate export rootsquash")
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
        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
