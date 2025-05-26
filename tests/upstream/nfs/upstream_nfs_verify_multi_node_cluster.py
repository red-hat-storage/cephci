from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
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
    no_cluster_nodes = int(config.get("nfs_cluster_nodes", "1"))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    if no_cluster_nodes > len(nfs_nodes):
        raise ConfigError("The test requires more nfs server nodes than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = []
    for nfs_node in nfs_nodes[:no_cluster_nodes]:
        nfs_server_name.append(nfs_node.hostname)

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
        # Perform file creation on client 1
        for i in range(100):
            clients[0].exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={nfs_mount}/file{i} bs=1 count=1",
            )

        # Test 1: Perform readir operation from other client (ls -lart)
        cmd = f"ls -lart {nfs_mount}"
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Test 2: Perform readir operation from other client (du -sh)
        cmd = f"du -sh {nfs_mount}"
        clients[2].exec_command(cmd=cmd, sudo=True)

    except Exception as e:
        log.error(f"Failed to validate multi node deployment operations : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
