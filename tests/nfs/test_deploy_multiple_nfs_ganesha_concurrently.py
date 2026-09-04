from cli.exceptions import ConfigError
from tests.nfs.nfs_operations import (
    create_nfs_via_file_and_verify,
    delete_nfs_clusters_in_parallel,
)
from utility.log import Log

log = Log(__name__)


def _port_available_on_node(node, port):
    """Return True when TCP port is free on node (ss + bind probe).

    Cephadm verifies NFS ports with a bind attempt; ``ss`` alone can miss
    listeners that appear while parallel orch applies are in flight.
    """
    out, _ = node.exec_command(
        sudo=True,
        cmd=f"ss -H -tln sport = :{port}",
        check_ec=False,
        timeout=30,
    )
    if (out or "").strip():
        log.debug("Port %s in use on %s (ss listener)", port, node.hostname)
        return False
    bind_cmd = (
        'python3 -c "import socket; s=socket.socket();'
        "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1);"
        "s.bind(('0.0.0.0', %d)); s.close()\"" % port
    )
    node.exec_command(sudo=True, cmd=bind_cmd, check_ec=False, timeout=30)
    if int(getattr(node, "exit_status", 1)) != 0:
        log.debug("Port %s bind probe failed on %s", port, node.hostname)
        return False
    return True


def _find_free_ports(nodes, count, start_port, reserved=None):
    """Return ``count`` ports free on every NFS host."""
    reserved = set(reserved or ())
    free_ports = []
    port = start_port
    while len(free_ports) < count and port < 65535:
        if port in reserved:
            port += 1
            continue
        if all(_port_available_on_node(node, port) for node in nodes):
            free_ports.append(port)
            reserved.add(port)
        port += 1
    return free_ports


def _verify_ports_free_on_all_nodes(nodes, ports):
    """Raise when any assigned port is busy on any nfs host."""
    for port in ports:
        for node in nodes:
            if not _port_available_on_node(node, port):
                raise ConfigError(
                    "Port %s not available on %s before NFS deploy"
                    % (port, node.hostname)
                )


def run(ceph_cluster, **kw):
    """Deploy multiple NFS-Ganesha services concurrently and verify cleanup.

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
    port_scan_start = int(config.get("port_scan_start", 52000))
    # Cephadm reserves cluster_qos_port (default 31311) for every NFS service.
    # Concurrent instances sharing nfs hosts collide on that default unless each
    # gets a unique qos port. Enable via suite config on Tentacle+ (field is
    # Tentacle+ only); Squid suites leave this unset.
    use_cluster_qos = bool(
        config.get("enable_cluster_qos_port")
        or (original_config or {}).get("spec", {}).get("cluster_qos_port") is not None
    )
    ports_per_instance = 3 if use_cluster_qos else 2

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    clean_up_happened = None

    try:
        all_nfs_nodes = ceph_cluster.get_nodes("nfs")
        needed_ports = nfs_instance_number * ports_per_instance
        free_ports = _find_free_ports(all_nfs_nodes, needed_ports, port_scan_start)

        if len(free_ports) < needed_ports:
            raise ConfigError(
                "Could not find %s free ports on all NFS nodes (found %s)"
                % (needed_ports, len(free_ports))
            )

        new_objects = []
        for i in range(nfs_instance_number):
            base = i * ports_per_instance
            nfs_port = free_ports[base]
            mon_port = free_ports[base + 1]

            new_object = {
                "service_type": original_config["service_type"],
                "service_id": "concurrent-nfs-%s" % i,
                "placement": {"label": original_config["placement"]["label"]},
                "spec": {
                    "port": nfs_port,
                    "monitoring_port": mon_port,
                },
            }
            if use_cluster_qos:
                new_object["spec"]["cluster_qos_port"] = free_ports[base + 2]
            new_objects.append(new_object)
        log.info(
            "New NFS Ganesha objects to be created with dynamic ports: %s",
            new_objects,
        )

        # Re-probe immediately before apply; parallel suite tests may have
        # bound ports between the initial scan and orch apply.
        _verify_ports_free_on_all_nodes(all_nfs_nodes, free_ports)

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
            log.error("Failed to delete NFS Ganesha instances: %s", deletion_error)
            clean_up_happened = False
            return 1

        log.info(
            "TEST PASSED - deployed %s concurrent NFS clusters (CEPH-83621553)",
            nfs_instance_number,
        )
        return 0
    except Exception as e:
        log.error("An error occurred during NFS Ganesha deployment: %s", e)
        return 1
    finally:
        log.info("Cleanup in progress...")
        if not clean_up_happened:
            log.info("Cleaning up any created NFS Ganesha instances")
            try:
                delete_nfs_clusters_in_parallel(installer, timeout)
                log.info("Cleanup completed successfully")
            except Exception as cleanup_error:
                log.warning("Cleanup failed: %s", cleanup_error)
        else:
            log.info("No additional cleanup needed")
