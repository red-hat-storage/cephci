from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.cthon import Cthon
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
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

        # mimic the export name as in setup_nfs_cluster
        export_list = ["/export_{x}".format(x=x) for x in range(0, len(clients))]

        # Run cthon tests on all exports
        for i in range(0, len(clients)):
            cthon = Cthon(clients[i])
            out, err = cthon.execute_cthon(
                export_psudo_path=export_list[i],
                mount_dir=nfs_mount,
                server_node_ip=nfs_node.ip_address,
            )
            if "All tests completed" in out:
                log.info(
                    "output: {0} \n Cthon tests passed for export: {1} for client {2} \n \n".format(
                        out, export_list[i], clients[i].hostname
                    )
                )
                cthon.cleanup()
            else:
                raise OperationFailedError(
                    "Cthon test failed with error: {0} \n output: {1}".format(err, out)
                )
        return 0
    finally:
        log.info("Cleanup in progress")
        # Cleanup nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
