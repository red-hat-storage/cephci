from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError
from tests.nfs.nfs_operations import (
    create_nfs_via_file_and_verify,
    delete_nfs_clusters_in_parallel,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Deploy multiple NFS Ganesha instances concurrently and verify their creation and deletion.
    This function creates NFS Ganesha instances based on the provided configuration,
    performs cleanup, and optionally runs longevity tests.
    This test won't run well on Mac devices
    """

    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")

    no_clients = int(config.get("clients", "2"))
    nfs_instance_number = int(config.get("nfs_instance_number", "1"))
    installer = ceph_cluster.get_nodes(role="installer")[0]
    original_config = config.get("spec", None)
    timeout = int(config.get("timeout", 300))
    longevity = config.get("longevity", False)
    longevity_loop = int(config.get("longevity_loop", 1))
    nfs_instances_name_prefix = original_config["service_id"]
    # Allow some time for the cluster to stabilize

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    clean_up_happened = None
    try:

        new_objects = []
        for i in range(nfs_instance_number):
            new_object = {
                "service_type": original_config["service_type"],
                "service_id": f"{original_config['service_id']}{i if i != 0 else ''}",
                "placement": {
                    "host_pattern": original_config["placement"]["host_pattern"]
                },
                "spec": {
                    "port": original_config["spec"]["port"] + i,
                    "monitoring_port": original_config["spec"]["monitoring_port"] + i,
                },
            }
            new_objects.append(new_object)
        log.info(f"New NFS Ganesha objects to be created: {new_objects}")

        # Create a nfs instance using the provided configuration
        if not create_nfs_via_file_and_verify(installer, new_objects, timeout):
            return 1
        log.info("NFS Ganesha instances created successfully")
        clean_up_happened = False

        clusters = CephAdm(installer).ceph.nfs.cluster.ls()
        clusters = [x for x in clusters if x.startswith(nfs_instances_name_prefix)]
        if not delete_nfs_clusters_in_parallel(installer, timeout, clusters):
            log.error("Failed to delete NFS Ganesha instances")
            clean_up_happened = False
            return 1
        else:
            clean_up_happened = True
            log.info("NFS Ganesha instances deleted successfully")

        for i in range(longevity_loop):
            log.info("\n \n" + '='*30  + f"Running longevity loop {i + 1}/{longevity_loop} \n " + '='*30)
            # Create a nfs instance using the provided configuration
            if not create_nfs_via_file_and_verify(installer, new_objects, timeout):
                return 1
            log.info("NFS Ganesha instances created successfully")
            clean_up_happened = False
            clusters = CephAdm(installer).ceph.nfs.cluster.ls()
            clusters = [x for x in clusters if x.startswith(nfs_instances_name_prefix)]
            if not delete_nfs_clusters_in_parallel(installer, timeout, clusters):
                log.error("Failed to delete NFS Ganesha instances")
                clean_up_happened = False
                return 1
            else:
                clean_up_happened = True
                log.info("NFS Ganesha instances deleted successfully")

        return 0
    except Exception as e:
        log.error(f"An error occurred during NFS Ganesha deployment: {e}")
        return 1
    finally:
        log.info("Cleanup in progress...")
        # Ensure cleanup of any created NFS instances
        if not clean_up_happened:
            log.info("Cleaning up any created NFS Ganesha instances")
            clusters = CephAdm(installer).ceph.nfs.cluster.ls()
            delete_nfs_clusters_in_parallel(installer, timeout, clusters)
        log.info("Cleanup completed")
