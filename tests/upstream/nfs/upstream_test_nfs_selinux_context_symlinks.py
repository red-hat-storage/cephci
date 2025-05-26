from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify selinux context label with hardlink and symlink
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
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
    filename = "Testfile"

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

        # Create a file on mount point
        cmd = f"touch {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Set the selinux label for file
        chcon_cmd = f"chcon -t public_content_t {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=chcon_cmd, sudo=True)

        # Create a file with soft link
        cmd = f"ln -s {nfs_mount}/{filename} {nfs_mount}/{filename}_soft"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Check the selinux label for the softlink file
        cmd = f"ls -Z {nfs_mount}/{filename}_soft"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        if "public_content_t" not in out[0]:
            log.info(f"selinux lable is set correctly for softlink file: {out[0]}")
        else:
            raise OperationFailedError(
                "Selinux label is not set correctly.It should be defaut."
            )

        # Create a file with hard link
        cmd = f"ln {nfs_mount}/{filename} {nfs_mount}/{filename}_hard"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Check the selinux label for the hardlink file
        cmd = f"ls -Z {nfs_mount}/{filename}_hard"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        if "public_content_t" in out[0]:
            log.info(f"selinux lable is set correctly for hardlink file: {out[0]}")
        else:
            raise OperationFailedError("Selinux label is not the same as parent file")

    except Exception as e:
        log.error(
            f"Failed to verify the selinux label for hard link and soft link files on NFS v4.2 : {e}"
        )
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
