from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify symbolic links scenarios
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

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
            fs,
            ceph_cluster=ceph_cluster,
        )

        # Create file in local file system
        cmd = f"touch {nfs_mount}/test_file"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Create hard links
        cmd = f"ln {nfs_mount}/test_file {nfs_mount}/hard_link_file1"
        clients[0].exec_command(cmd=cmd, sudo=True)
        cmd = f"ln {nfs_mount}/test_file {nfs_mount}/hard_link_file2"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Delete file with multiple hard links
        cmd = f"rm -rf {nfs_mount}/test_file"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Verify hardlink remaning hardlinks files
        hardlink_file1_inode = (
            clients[0]
            .exec_command(
                cmd="ls -i /mnt/nfs/hard_link_file1 | awk '{print $1}'", sudo=True
            )[0]
            .strip()
        )
        hardlink_file2_inode = (
            clients[0]
            .exec_command(
                cmd="ls -i /mnt/nfs/hard_link_file2 | awk '{print $1}'", sudo=True
            )[0]
            .strip()
        )
        if hardlink_file1_inode != hardlink_file2_inode:
            raise OperationFailedError(
                "hard link file not have same inode as original file"
            )
            return 1
        else:
            log.info("iNode match for original and hard link file")
    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        log.info("Cleaning up")
        sleep(3)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
