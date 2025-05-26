from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def create_file(count, mount_point, filename, client):
    log.info(f"Creating file of : {count}G")
    cmd = f"dd if=/dev/urandom of={mount_point}/{filename} bs=1G count={count}"
    client.exec_command(cmd=cmd, sudo=True)


def verify_disk_usage(client, mount_point, size, filename=None):
    if filename:
        cmd = f"du -sh {mount_point}/{filename}"
    else:
        cmd = f"du -sh {mount_point}"
    out = client.exec_command(cmd=cmd, sudo=True)
    size_str = out[0].strip().split()[0]
    numeric_part = size_str.rstrip("G")
    size_rounded = f"{int(float(numeric_part))}G"
    if size_rounded == size:
        log.info(f"File created with correct space: {size_rounded}")
    else:
        raise OperationFailedError(
            f"File '{filename}' took incorrect space utilization. Expected: {size}, Actual: {size_rounded}"
        )


def run(ceph_cluster, **kw):
    """Verify the space allocation with different file sizes
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

        # List of files and sizes to create and verify
        files_and_sizes = [("file1", "1G"), ("file2", "2G"), ("file3", "3G")]

        # Create files and verify disk usage for each file
        for filename, size in files_and_sizes:
            create_file(
                count=int(size[:-1]),
                mount_point=nfs_mount,
                filename=filename,
                client=clients[0],
            )
            verify_disk_usage(
                client=clients[0], mount_point=nfs_mount, filename=filename, size=size
            )

        # Validate the total disk usage of mount point
        verify_disk_usage(client=clients[0], mount_point=nfs_mount, size="6G")

    except Exception as e:
        log.error(f"Failed to  verify the space allocation test on NFS v4.2 : {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
