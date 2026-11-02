from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Export one volume per client and mount each export on that client.

    setup_nfs_cluster creates /export_0, /export_1, ... and mounts each on
    a different client at the same local path. Each client writes a file
    on its own export.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "3"))
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")
    clients = clients[:no_clients]
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    fs_name = "cephfs"
    nfs_server = nfs_nodes[0].hostname

    try:
        setup_nfs_cluster(
            clients,
            nfs_server,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs_name,
            ceph_cluster=ceph_cluster,
            single_export=False,
        )
        for index, client in enumerate(clients):
            marker = f"{nfs_mount}/client_{index}"
            client.exec_command(sudo=True, cmd=f"echo {client.hostname} > {marker}")
            content, _ = client.exec_command(sudo=True, cmd=f"cat {marker}")
            if client.hostname not in content:
                raise OperationFailedError(
                    f"Client {client.hostname} did not read its file on /export_{index}"
                )
            log.info("Client %s wrote and read %s", client.hostname, marker)
        log.info("Mounted %s exports on %s clients", no_clients, no_clients)
        return 0
    except Exception as exc:
        log.error(f"Failed multi-export multi-client mount: {exc}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(
            clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes[0]
        )
