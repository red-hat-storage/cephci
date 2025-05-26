from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.io.small_file import SmallFile
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
    no_servers = int(config.get("servers", "2"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

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

    try:
        # Setup nfs cluster with HA
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

        # Trigger smallfile IO
        th = Thread(
            target=SmallFile(clients[0]).run,
            args=(nfs_mount, ["create"]),
        )
        th.start()

        # Keeping a static sleep for the IO to start before performing failover
        sleep(3)
        # Perform and verify failover while IO in progress
        failover_node = nfs_nodes[0]
        perform_failover(nfs_nodes, failover_node, vip)

        # Wait for IO to complete
        th.join()
    except Exception as e:
        log.error(f"Failed to perform faiolver while IO in progress. Error: {e}")
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
