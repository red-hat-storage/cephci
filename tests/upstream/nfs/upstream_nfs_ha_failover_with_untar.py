from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.io.io import linux_untar
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Perform failover when linux untar is running
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "1"))
    no_servers = int(config.get("servers", "3"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    # If the setup doesn't have required number of nfs server, exit.
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]

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

        sleep(3)

        # Linux untar on client 1
        th = Thread(
            target=linux_untar,
            args=(
                clients[0],
                nfs_mount,
            ),
        )
        th.start()

        # Perfrom and verify failover
        failover_node = nfs_nodes[0]
        perform_failover(nfs_nodes, failover_node, vip)

        # Wait to complete linux untar
        th.join()

    except Exception as e:
        log.error(f"Error : {e}")
        log.info("Cleaning up")
        sleep(100)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
        return 1
    finally:
        log.info("Cleaning up")
        sleep(100)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
