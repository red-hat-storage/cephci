from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from ceph.waiter import WaitUntil
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

        # Create files from dd on client 1
        cmd = f"for i in $(seq 1 15);do dd if=/dev/urandom of={nfs_mount}/file$i bs=1G count=1;done"
        clients[1].exec_command(cmd=cmd, sudo=True, timeout=600)

        for i in range(1, 15):
            # Validate the size of file is 1GB each
            cmd = f"du -h {nfs_mount}/file{i}"
            out = clients[1].exec_command(cmd=cmd, sudo=True)
            size_str = out[0].split("\t")[0].strip()
            if size_str != "1.0G":
                raise ValueError("The file created is not of correct size")

            # Truncate the file to 1M
            cmd = f"truncate -s 1M {nfs_mount}/file{i}"
            out = clients[1].exec_command(cmd=cmd, sudo=True)

            # Wait until the file truncate operation is complete
            timeout, interval = 60, 2
            for w in WaitUntil(timeout=timeout, interval=interval):
                cmd = f"du -h {nfs_mount}/file{i}"
                out = clients[1].exec_command(cmd=cmd, sudo=True)
                size_str = out[0].split("\t")[0].strip()
                if size_str == "1.0M":
                    log.info(f"File{i} is truncated to 1M")
                    break
                if w.expired:
                    raise ValueError("The file created is not truncated")

    except Exception as e:
        log.error(f"Failed to perform truncate operations : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
