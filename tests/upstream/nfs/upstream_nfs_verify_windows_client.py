from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.utilities.windows_utils import (
    establish_windows_client_conn,
    execute_command_on_windows_node,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify NFS HA cluster creation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    no_clients = int(config.get("clients", "2"))
    no_servers = int(config.get("servers", "1"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    windows_clients = config.get("win-clients", None)
    win_clients = []

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    if windows_clients:
        for wn in windows_clients:
            win_nodes = {
                wn.get("ip"): {
                    "user": wn.get("user"),
                    "password": wn.get("password"),
                }
            }
            win_clients = establish_windows_client_conn(windows_clients=win_nodes)

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
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )
        # If windows clients are present, perform mount
        if win_clients:
            cmd = f"mount {nfs_nodes[0].ip_address}:/export_0 Z:"
            execute_command_on_windows_node(win_clients[0], cmd)

        log.info("Successfully setup NFS HA cluster")

        # Mount windows client
    except Exception as e:
        log.error(f"Failed to setup nfs ha cluster {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        # Unmount windows mount
        cmd = r"umount Z:"
        execute_command_on_windows_node(win_clients[0], cmd)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
