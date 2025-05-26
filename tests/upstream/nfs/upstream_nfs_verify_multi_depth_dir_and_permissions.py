from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    operation = config.get("operation")
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

        # Create multi depth dirs under nfs share
        depth = int(config.get("dir_depth", 5))
        dirs_per_depth = int(config.get("dirs_per_depth", 2))
        dir = nfs_mount
        try:
            for i in range(depth):
                dir = f"{dir}/test_dir{i}"
                cmd = f"mkdir -p {dir}"
                clients[0].exec_command(cmd=cmd, sudo=True)
                # Make a file inside the dir
                cmd = f"touch {dir}/test_file{i}"
                clients[0].exec_command(cmd=cmd, sudo=True)
                for j in range(dirs_per_depth):
                    cmd = f"mkdir -p {dir}/test_dir{j}"
                    clients[0].exec_command(cmd=cmd, sudo=True)
        except Exception as e:
            raise OperationFailedError(f"Failed to create multi depth dirs {str(e)}")

        if operation == "default_permissions":
            # Check 1: Try reading a non-existing file
            default_dir_permission = "drwxr-xr-x"
            default_file_permission = "-rw-r--r--"
            dir = nfs_mount
            for i in range(depth):
                # Check default dir permission
                dir = f"{dir}/test_dir{i}"
                cmd = f"ls -lrt {dir}"
                out, _ = clients[0].exec_command(cmd=cmd, sudo=True)
                if default_dir_permission not in out:
                    raise OperationFailedError(
                        f"Dir permission is different than default {out}"
                    )

                # Check default file permission
                cmd = f"ls -lrt {dir}/test_file{i}"
                out, _ = clients[0].exec_command(cmd=cmd, sudo=True)
                if default_file_permission not in out:
                    raise OperationFailedError(
                        f"File permission is different than default {out}"
                    )

    except Exception as e:
        log.error(f"Failed to validate nfs dir operations and permission checks : {e}")
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
