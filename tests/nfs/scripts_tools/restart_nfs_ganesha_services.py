import time

from ceph.ceph import CommandFailed
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


@retry(CommandFailed, tries=4, delay=5, backoff=2)
def restart_nfs_ganesha_services(client, nfs_instance):
    try:
        # Restart the specified NFS instance
        Ceph(client).orch.restart(f"nfs.{nfs_instance}")
        log.info(f"Successfully restarted NFS cluster {nfs_instance}")
    except CommandFailed:
        log.error(f"Failed to restart NFS cluster {nfs_instance}")


def run(ceph_cluster, **kw):
    """
    Restart NFS cluster instances based on the provided configuration.

    This function supports both single and longevity-based restarts of NFS clusters.
    It allows specifying the number of clients, restart intervals, and the duration
    for longevity-based restarts.

    Args:
        ceph_cluster (CephCluster): The Ceph cluster object.
        **kw: Arbitrary keyword arguments containing the configuration.

    Keyword Args:
        config (dict): Configuration dictionary with the following keys:
            - clients (int): Number of clients to use (default: 1).
            - longevity (bool): Whether to run in longevity mode (default: False).
            - longevity_loop (int): Number of loops for longevity (default: 1).
            - longevity_duration (float): Duration for longevity in hours (default: 0).
            - restart_interval (int): Interval between restarts in minutes (default: 0).
            - instances_to_restart (list): Specific NFS instances to restart (default: None).
    """
    config = kw.get("config")
    clients = ceph_cluster.get_nodes(role="client")

    # Extract configuration values with defaults
    num_clients = int(config.get("clients", 1))
    longevity = config.get("longevity", False)
    longevity_loop = int(config.get("longevity_loop", 1))
    restart_duration = config.get("restart_duration", 0)  # in minutes
    longevity_duration = float(config.get("longevity_duration", 0))  # in hours
    restart_interval = config.get("restart_interval", 0)  # in minutes
    instances_to_restart = config.get("instances_to_restart", None)
    longevity_duration = restart_duration if restart_duration else longevity_duration

    # Validate the number of clients
    if num_clients > len(clients):
        raise ConfigError("More clients requested than available")

    # Limit the clients to the requested number
    clients = clients[:num_clients]

    # Determine the NFS instances to restart
    if instances_to_restart is None:
        nfs_to_restart = []
        for attempt in range(30):
            nfs_to_restart = Ceph(clients[0]).nfs.cluster.ls()
            if nfs_to_restart:
                log.info(f"Found NFS instances to restart: {nfs_to_restart}")
                break
            log.info(
                f"No NFS instances found, retrying ({attempt + 1}/10) after 10 seconds..."
            )
            time.sleep(10)
    else:
        nfs_to_restart = instances_to_restart

    if not nfs_to_restart:
        raise ConfigError(
            "No NFS instances found to restart. Please check the configuration."
        )

    if longevity and longevity_duration > 0:
        # Longevity mode: Restart NFS instances for a specified duration
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
            loop += 1
            for instances in nfs_to_restart:
                restart_nfs_ganesha_services(clients[0], instances)
            if restart_interval > 0:
                # Wait for the specified interval before the next restart
                log.info(
                    f"Waiting for {restart_interval} minutes before the next restart..."
                )
                time.sleep(restart_interval * 60)
    else:
        # Non-longevity mode: Perform a fixed number of restart loops
        for i in range(longevity_loop if longevity else 1):
            log.info(f"Loop {i + 1} of NFS cluster restarts")
            for instances in nfs_to_restart:
                restart_nfs_ganesha_services(clients[0], instances)
            if restart_interval > 0 and i < (longevity_loop - 1):
                # Wait for the specified interval before the next restart
                log.info(
                    f"Waiting for {restart_interval} minutes before the next restart..."
                )
                time.sleep(restart_interval * 60)
    return 0
