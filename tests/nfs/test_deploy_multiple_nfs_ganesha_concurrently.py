from cli.exceptions import ConfigError
from tests.nfs.nfs_operations import (
    create_nfs_via_file_and_verify,
    delete_nfs_clusters_in_parallel,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify create file, create soflink and lookups from nfs clients
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")

    no_clients = int(config.get("clients", "2"))
    nfs_instance_number = int(config.get("nfs_instance_number", "1"))
    installer = ceph_cluster.get_nodes(role="installer")[0]
    original_config = config.get("spec", None)
    timeout = int(config.get("timeout", 300))
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

        if not delete_nfs_clusters_in_parallel(installer, timeout):
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
            delete_nfs_clusters_in_parallel(installer, timeout)
        log.info("Cleanup completed")
