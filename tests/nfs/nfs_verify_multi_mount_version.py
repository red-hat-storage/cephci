from threading import Thread

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from cli.utilities.utils import create_files
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Mount the export with mixed NFS versions across Linux clients and run IO in parallel.

    ``nfs_version`` may be a scalar or a per-client list such as ``[{4.2: 2}, {3: 1}]``.
    If any client uses v3, ``setup_nfs_cluster`` enables NFSv3 on the cluster (multi-protocol).
    """
    config = kw.get("config")
    # nfs cluster details
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    servers = nfs_nodes[:no_servers]
    no_clients = int(config.get("clients", "2"))
    port = config.get("port", "2049")
    version = config.get("nfs_version", "3")
    log.info("nfs_version config (mount mix / scalar): %s", version)
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    clients = ceph_cluster.get_nodes("client")[:no_clients]

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

        # Run parallel IO on each client's mount
        threads = []
        for client in clients:
            io = Thread(
                target=create_files,
                args=(client, nfs_mount, 10),
            )
            io.start()
            threads.append(io)
        for th in threads:
            th.join()
        return 0
    except Exception as e:
        log.error(f"Failed to setup nfs-ganesha cluster {e}")
    finally:
        log.info("Cleaning up the NFS cluster and unmounting the mounts")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=servers)
