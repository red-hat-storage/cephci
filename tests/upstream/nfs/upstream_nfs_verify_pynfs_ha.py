from threading import Thread

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import get_ip_from_node
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
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

        # Create a file on Client 1
        cmd = (
            f"python3 -m pip install ply;cd {nfs_mount};git clone git://linux-nfs.org/~bfields/pynfs.git;cd pynfs;-- "
            f"yes |"
            f"python setup.py build;cd nfs{version};./testserver.py {nfs_server_name}:{nfs_export} -v --outfile "
            f"~/pynfs.run --maketree --showomit --rundep all"
        )
        pynfs = Thread(
            target=lambda: clients[0].exec_command(
                cmd=cmd, sudo=True, long_running=True
            ),
            args=(),
        )
        pynfs.start()

        # Identify the VIP node
        if "/" in vip:
            vip = vip.split("/")[0]

        failover_node = None
        for node in nfs_nodes:
            assigned_ips = get_ip_from_node(node)
            if vip in assigned_ips:
                failover_node = node
                break
        if failover_node is None:
            raise OperationFailedError("VIP not assigned to any of the nfs nodes")
        perform_failover(nfs_nodes, failover_node, vip)

        # Wait for pynfs to complete
        pynfs.join()
    except Exception as e:
        log.error(f"Failed to run pynfs with HA on {clients[0].hostname}, Error: {e}")
        return 1

    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
