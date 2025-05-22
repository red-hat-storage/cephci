from concurrent.futures import ThreadPoolExecutor

from nfs_operations import (
    cleanup_custom_nfs_cluster_multi_export_client,
    exports_mounts_perclient,
    setup_custom_nfs_cluster_multi_export_client,
)

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.cthon import Cthon
from utility.log import Log

log = Log(__name__)


def run_cthon_tests(client, exports, mounts, nfs_node_ip, cthon_obj):
    """
    Run Cthon tests for a client on assigned exports.
    """
    try:
        for i in range(len(exports)):
            out, err = cthon_obj.execute_cthon(
                export_psudo_path=exports[i],
                mount_dir=mounts[i],
                server_node_ip=nfs_node_ip,
            )
            if "All tests completed" in out:
                log.info(
                    f"{out} \n Cthon tests passed for export: {exports[i]} on client: {client.hostname}"
                )
    except Exception as e:
        log.error(
            "Command failed for export: {0} on client: {1} with error: {2}".format(
                exports[i], client.hostname, e
            )
        )
        raise OperationFailedError(
            f"Cthon tests failed for export: {exports[i]} on client: {client.hostname}"
        )


def run(ceph_cluster, **kw):
    """
    Run Cthon tests across clients by dividing exports evenly.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes(role="client")

    nfs_node = nfs_nodes[0]
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    num_exports = int(config.get("total_num_exports", 1))
    num_clients = int(config.get("clients", 1))
    # batch_size = int(config.get("max_batch_size", 10))

    # Validate client count
    if num_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:num_clients]  # Select only the required number of clients
    client_export_mount_dict = exports_mounts_perclient(
        clients, nfs_export, nfs_mount, num_exports
    )
    try:
        log.info("Starting NFS cluster setup")
        setup_custom_nfs_cluster_multi_export_client(
            clients,
            nfs_node.hostname,
            config.get("port", "2049"),
            config.get("nfs_version", "4.0"),
            nfs_name="cephfs-nfs",
            fs="cephfs",
            fs_name="cephfs",
            ceph_cluster=ceph_cluster,
            export_num=num_exports,
            nfs_export=nfs_export,
            nfs_mount=nfs_mount,
        )
        log.info("NFS cluster setup completed successfully.")
        log.info("Starting Cthon tests")
        # Install dependencies for all clients in parallel
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = [
                executor.submit(Cthon(client).install_dependencies)
                for client in clients
            ]
            for future in futures:
                future.result()  # Wait for all installations to complete
        log.info("Dependencies installed successfully.")

        # Run Cthon tests in parallel
        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = [
                executor.submit(
                    run_cthon_tests,
                    clients[i],
                    client_export_mount_dict[clients[i]]["export"],
                    client_export_mount_dict[clients[i]]["mount"],
                    nfs_node.ip_address,
                    Cthon(clients[i]),
                )
                for i in range(num_clients)
            ]
            for future in futures:
                future.result()  # Wait for all tests to complete

        log.info("All Cthon tests completed successfully.")

        return 0
    except Exception as e:
        log.error(f"Error during Cthon tests: {e}")
        raise
    finally:
        log.info("Cleanup in progress")
        for client in clients:
            cthon_obj = Cthon(client)
            cthon_obj.cleanup()
        cleanup_custom_nfs_cluster_multi_export_client(
            clients, nfs_mount, "cephfs-nfs", nfs_export, num_exports
        )
        log.info("Cleanup completed.")
