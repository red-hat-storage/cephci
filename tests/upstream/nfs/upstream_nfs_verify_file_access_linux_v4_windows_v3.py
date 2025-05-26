from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify access file from both v3 (windows) mount and linux v 4.1 mount
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    # Get config
    config = kw.get("config")

    # get file access scenario
    operations = config.get("operations")

    # nfs cluster details
    nfs_nodes = ceph_cluster.get_nodes("installer")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    servers = nfs_nodes[:no_servers]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    window_nfs_mount = "Z:"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # Linux clients
    linux_clients = ceph_cluster.get_nodes("client")
    no_linux_clients = int(config.get("linux_clients", "1"))
    linux_clients = linux_clients[:no_linux_clients]
    if no_linux_clients > len(linux_clients):
        raise ConfigError("The test requires more linux clients than available")

    # Windows clients
    windows_clients = []
    for windows_client_obj in setup_windows_clients(config.get("windows_clients")):
        windows_clients.append(windows_client_obj)

    try:
        # Setup nfs cluster with v4.1 and v3
        setup_nfs_cluster(
            linux_clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )

        # Mount NFS-Ganesha V3 to window
        cmd = f"mount {nfs_nodes[0].ip_address}:/export_0 {window_nfs_mount}"
        windows_clients[0].exec_command(cmd=cmd)

        for operation in operations:
            if operation == "create_file":
                # Create file with content from linux client v4.1
                linux_clients[0].exec_command(
                    cmd=f'echo "test" > {nfs_mount}/test.txt', sudo=True
                )

                # Check same file content from windows client v3
                out = windows_clients[0].exec_command(
                    cmd=f"type {window_nfs_mount}\\test.txt"
                )
                if "test" not in out[0]:
                    raise OperationFailedError(
                        "Fail to access file from windows client v3"
                    )
                    return 1
            elif operation == "delete_file":
                # Delete file with content from linux client v4.1
                linux_clients[0].exec_command(
                    cmd=f"rm -rf {nfs_mount}/test.txt", sudo=True
                )
                # Check same file deleted from windows client v3
                out = windows_clients[0].exec_command(cmd=f"dir {window_nfs_mount}")
                if "test" in out[0]:
                    raise OperationFailedError(
                        "Deleted file present in windows client v3"
                    )
                    return 1

    except Exception as e:
        log.error(f"Failed to setup nfs-ganesha cluster {e}")
        # Cleanup
        for windows_client in windows_clients:
            cmd = f"del /q /f {window_nfs_mount}\\*.*"
            windows_client.exec_command(cmd=cmd)
            cmd = f"umount {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        # Cleanup
        for windows_client in windows_clients:
            cmd = f"del /q /f {window_nfs_mount}\\*.*"
            windows_client.exec_command(cmd=cmd)
            cmd = f"umount {window_nfs_mount}"
            windows_client.exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
    return 0
