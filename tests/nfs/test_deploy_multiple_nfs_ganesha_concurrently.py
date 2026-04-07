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
    nfs_nodes = ceph_cluster.get_nodes("nfs")
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

    def get_free_ports(nodes, count, start_port):
        """Scan for free ports on all nodes."""
        free_ports = []
        port = start_port
        while len(free_ports) < count and port < 65535:
            is_used = False
            for node in nodes:
                # Check if port is in use using ss
                cmd = f"ss -H -tulpn sport = :{port}"
                out = node.exec_command(cmd=cmd, sudo=True)
                if out[0].strip():
                    is_used = True
                    break
            if not is_used:
                free_ports.append(port)
            port += 1
        return free_ports

    try:
        # Find 12 free ports (6 services * 2 ports each)
        all_nfs_nodes = ceph_cluster.get_nodes("nfs")
        needed_ports = nfs_instance_number * 2
        free_ports = get_free_ports(all_nfs_nodes, needed_ports, 50200)

        if len(free_ports) < needed_ports:
            raise ConfigError(f"Could not find {needed_ports} free ports on NFS nodes")

        new_objects = []
        for i in range(nfs_instance_number):
            nfs_port = free_ports[i * 2]
            mon_port = free_ports[i * 2 + 1]

            new_object = {
                "service_type": original_config["service_type"],
                "service_id": f"concurrent-nfs-{i}",
                "placement": {"label": original_config["placement"]["label"]},
                "spec": {
                    "port": nfs_port,
                    "monitoring_port": mon_port,
                },
            }
            new_objects.append(new_object)
        log.info(
            f"New NFS Ganesha objects to be created with dynamic ports: {new_objects}"
        )

        # Create a nfs instance using the provided configuration
        if not create_nfs_via_file_and_verify(
            installer, new_objects, timeout, nfs_nodes
        ):
            return 1
        log.info("NFS Ganesha instances created successfully")
        clean_up_happened = False

        try:
            delete_nfs_clusters_in_parallel(installer, timeout)
            log.info("NFS Ganesha instances deleted successfully")
            clean_up_happened = True
        except Exception as deletion_error:
            log.error(f"Failed to delete NFS Ganesha instances: {deletion_error}")
            clean_up_happened = False
            return 1

        return 0
    except Exception as e:
        log.error(f"An error occurred during NFS Ganesha deployment: {e}")
        return 1
    finally:
        log.info("Cleanup in progress...")
        # Ensure cleanup of any created NFS instances
        if not clean_up_happened:
            log.info("Cleaning up any created NFS Ganesha instances")
            try:
                delete_nfs_clusters_in_parallel(installer, timeout)
                log.info("Cleanup completed successfully")
            except Exception as cleanup_error:
                log.warning(f"Cleanup failed: {cleanup_error}")
        else:
            log.info("No additional cleanup needed")
