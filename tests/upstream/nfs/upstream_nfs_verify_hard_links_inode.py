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
        cmd = f"ln {nfs_mount}/test_file {nfs_mount}/link_file"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Check inode for original and hard link file
        original_file_inode = (
            clients[0]
            .exec_command(cmd="ls -i /mnt/nfs/test_file | awk '{print $1}'", sudo=True)[
                0
            ]
            .strip()
        )
        hard_link_file_inode = (
            clients[0]
            .exec_command(cmd="ls -i /mnt/nfs/link_file | awk '{print $1}'", sudo=True)[
                0
            ]
            .strip()
        )
        if original_file_inode != hard_link_file_inode:
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
