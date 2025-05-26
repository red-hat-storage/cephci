from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import get_ip_from_node
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
        log.info("Successfully setup NFS HA cluster")

        # Identify the VIP node
        if "/" in vip:
            vip = vip.split("/")[0]

        vip_node = None
        for node in nfs_nodes:
            assigned_ips = get_ip_from_node(node)
            if vip in assigned_ips:
                vip_node = node
                break
        if vip_node is None:
            raise OperationFailedError("VIP not assigned to any of the nfs nodes")

        # Kill Ganesha process on one nfs server
        cmd = "pkill ganesha"
        vip_node.exec_command(cmd=cmd, sudo=True)

        # Now check if the VIP is assigned to the other nfs nodes
        # Perform the check with a timeout of 60 seconds
        flag = False
        for w in WaitUntil(timeout=60, interval=5):
            for node in nfs_nodes:
                if node != vip_node:
                    assigned_ips = get_ip_from_node(node)
                    log.info(f"IP addrs assigned to node : {assigned_ips}")
                    # If vip is assigned, set the flag and exit
                    if vip in assigned_ips:
                        flag = True
                        log.info(f"Failover success, VIP reassigned to {node.hostname}")
            if flag:
                break
        if w.expired:
            raise OperationFailedError(
                "The failover process failed and vip is not assigned to the available nodes"
            )
        log.info("VIP assigned to other nfs node successfully")
    except Exception as e:
        log.error(
            f"Failed to validate nfs process kill and failover on a ha cluster: {e}"
        )
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
