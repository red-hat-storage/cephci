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
        cmd = "touch /tmp/test_file"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Create symbolic links to files residing on local file systems with NFS
        cmd = f"ln -s /tmp/test_file {nfs_mount}/link_file"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Check symbolic links created successfully with local file system
        out = (
            clients[0]
            .exec_command(
                cmd="ls -l /mnt/nfs/link_file | awk '{print $10}'", sudo=True
            )[0]
            .strip()
        )
        if "->" not in out:
            raise OperationFailedError(
                "Failed to created symbolic links to files from local file systems to NFS"
            )
            return 1
        else:
            log.info(
                "Successfully created symbolic links to files from local file systems to NFS"
            )

    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        log.info("Cleaning up")
        sleep(3)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
