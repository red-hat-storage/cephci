from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify the partial space deallocation using truncate operation
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

        # Create a file
        for i in range(1, 6):
            cmd = f"dd if=/dev/urandom of={nfs_mount}/{filename}{i} bs=1G count=3"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Verify the file's size are allocated correctly
            cmd = f"du -sh {nfs_mount}/{filename}{i}"
            out = clients[0].exec_command(cmd=cmd, sudo=True)
            print(out)
            file_size = out[0].strip().split()[0]
            print(file_size)
            if file_size == "3.0G":
                log.info(f"File created with correct space: {file_size}")
            else:
                raise OperationFailedError(
                    f"File {filename}{i} took incorrect space utilization. Expected: 3.0G , Actual: {file_size}"
                )

        # Truncate all the files to 1GB
        for i in range(1, 6):
            cmd = f"truncate -s 1G {nfs_mount}/{filename}{i}"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Verify all files are truncated  correctly
            cmd = f"du -sh {nfs_mount}/{filename}{i}"
            out = clients[0].exec_command(cmd=cmd, sudo=True)
            print(out)
            file_size = out[0].strip().split()[0]
            print(file_size)
            if file_size == "1.0G":
                log.info(f"File created with correct space: {file_size}")
            else:
                raise OperationFailedError(
                    f"File {filename}{i} took incorrect space utilization. Expected: 1.0G , Actual: {file_size}"
                )

    except Exception as e:
        log.error(
            f"Failed to  verify the partial space deallocation test on NFS v4.2 : {e}"
        )
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
