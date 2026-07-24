import time
from concurrent.futures import ThreadPoolExecutor

from nfs_operations import (
    cleanup_custom_nfs_cluster_multi_export_client,
    exports_mounts_perclient,
    setup_custom_nfs_cluster_multi_export_client,
)

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.cthon import Cthon
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


@retry(OperationFailedError, tries=5, delay=15, backoff=2)
def run_single_export(client, export, mount, nfs_node_ip, cthon_obj):
    """
    Run Cthon on a single export/mount for a client.
    Retries on failure.
    """
    try:
        out, err = cthon_obj.execute_cthon(
            export_psudo_path=export,
            mount_dir=mount,
            server_node_ip=nfs_node_ip,
        )
        if "All tests completed" in out:
            log.info(
                f"{out} \nCthon test passed for export: {export} on client: {client.hostname}"
            )
    except Exception as e:
        log.error(
            f"Cthon test failed for export: {export} on client: {client.hostname} - Error: {e}"
        )
        raise OperationFailedError(f"Cthon test failed on {client.hostname}")


def run_cthon_tests(client, exports, mounts, nfs_node_ip, cthon_obj):
    """
    Run Cthon tests concurrently for all exports of a single client.
    """
    with ThreadPoolExecutor(max_workers=len(exports)) as executor:
        futures = [
            executor.submit(
                run_single_export, client, exports[i], mounts[i], nfs_node_ip, cthon_obj
            )
            for i in range(len(exports))
        ]
        for future in futures:
            future.result()


def run(ceph_cluster, **kw):
    """
    Main entry point for running Cthon NFS tests.

    - Sets up NFS cluster and exports.
    - Installs dependencies on all clients.
    - Runs Cthon tests in parallel on all clients and exports.
    - Supports both loop-based and time-based longevity testing.
    - Cleans up all resources at the end.
    """
    config = kw.get("config")
    cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes(role="client")

    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_export = config.get("nfs_export", "/export")
    num_exports = int(config.get("total_num_exports", 1))
    num_clients = int(config.get("clients", 1))
    longevity = config.get("longevity", False)
    longevity_loop = int(config.get("longevity_loop", 1))
    longevity_duration = float(config.get("longevity_duration", 0))  # in hours
    nfs_name = config.get("cluster_name", "cephfs-nfs")
    nfs_node = nfs_nodes[2] if longevity else nfs_nodes[0]

    # Validate client count
    if num_clients > len(clients):
        raise ConfigError("More clients requested than available")

    clients = clients[:num_clients]
    # Map each client to its exports and mount points
    client_export_mount_dict = exports_mounts_perclient(
        clients, nfs_export, nfs_mount, num_exports
    )

    try:
        log.info("Setting up NFS cluster...")
        # Deploy NFS cluster and configure exports/mounts for all clients
        setup_custom_nfs_cluster_multi_export_client(
            clients,
            nfs_node.hostname,
            config.get("port", "2049"),
            config.get("nfs_version", "4.0"),
            nfs_name=nfs_name,
            fs="cephfs",
            fs_name="cephfs",
            ceph_cluster=ceph_cluster,
            export_num=num_exports,
            nfs_export=nfs_export,
            nfs_mount=nfs_mount,
        )
        log.info("NFS cluster setup complete.")

        log.info("Installing dependencies on all clients...")
        # Install Cthon dependencies in parallel on all clients
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(Cthon(client).install_dependencies)
                for client in clients
            ]
            for f in futures:
                f.result()
        log.info("Dependencies installed.")

        log.info("Cluster health before running Cthon tests...")
        cephfs_common_utils.wait_for_healthy_ceph(clients[0], 60)

        def execute_all_clients():
            """
            Run Cthon tests on all clients in parallel.
            Each client runs Cthon on all its exports concurrently.
            """
            with ThreadPoolExecutor(max_workers=len(clients)) as executor:
                futures = [
                    executor.submit(
                        run_cthon_tests,
                        client,
                        client_export_mount_dict[client]["export"],
                        client_export_mount_dict[client]["mount"],
                        nfs_node.ip_address,
                        Cthon(client),
                    )
                    for client in clients
                ]
                for f in futures:
                    f.result()

        # Longevity mode: run for a specified duration (in hours)
        if longevity and longevity_duration > 0:
            log.info(
                "\n \n"
                + "->" * 30
                + f"Running longevity for {longevity_duration} hours "
                + "<-" * 30
                + "\n"
            )
            start = time.time()
            loop = 0
            while (time.time() - start) < (longevity_duration * 3600):
                log.info(
                    "\n \n"
                    + "=" * 30
                    + f"\n Running longevity loop {loop} (time-based) \n "
                    + "=" * 30
                )
                loop = loop + 1
                execute_all_clients()
        else:
            # Loop mode: run for a fixed number of iterations
            for i in range(longevity_loop if longevity else 1):
                log.info(f"Loop {i + 1} of Cthon tests")
                execute_all_clients()

        log.info("All Cthon tests completed successfully.")
        return 0

    except Exception as e:
        log.error(f"Cthon test error: {e}")
        raise

    finally:
        log.info("Cleanup started...")
        # Cleanup Cthon artifacts and NFS cluster configuration
        for client in clients:
            Cthon(client).cleanup()
        cleanup_custom_nfs_cluster_multi_export_client(
            clients, nfs_mount, nfs_name, nfs_export, num_exports
        )
        log.info("Cleanup complete.")
