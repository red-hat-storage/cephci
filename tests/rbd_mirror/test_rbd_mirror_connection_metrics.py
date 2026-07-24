"""Test case covered -
CEPH-83575566 - Performance counter for rbd mirror network
connection health metrics.

Pre-requisites :
1. Two Clusters must be up and running to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files on each node.

Test Case Flow:
    1. Create a pool on both clusters.
    2. Create an Image on primary mirror cluster in same pool.
    3. Configure mirroring (peer bootstrap) between two clusters.
    4. Enable image mode snapshot based mirroring on the pool respectively.
    5. Start running IOs on the primary image.
    6. Verify ceph exporter from prometheus for network connection
       health metrics.
"""

from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def validate_connection_metrics(admin_node, port):
    """Method to validate network connection health metrics.
    Args:
        admin_node: cephadm node with ceph-exporter deployed.
        port: ceph-exporter port number to get metrics

    Example:
        # TYPE ceph_AsyncMessenger_Worker_msgr_connection_idle_timeouts counter
        ceph_AsyncMessenger_Worker_msgr_connection_idle_timeouts{id="0"} 128

        # TYPE ceph_AsyncMessenger_Worker_msgr_connection_ready_timeouts counter
        ceph_AsyncMessenger_Worker_msgr_connection_ready_timeouts{id="0"} 0

    Returns:
        0 - if test case pass
        1 - if test case fails
    """
    # connection metrics to validate
    connection_metrics = [
        "ceph_AsyncMessenger_Worker_msgr_connection_idle_timeouts",
        "ceph_AsyncMessenger_Worker_msgr_connection_ready_timeouts",
    ]

    node_ip = admin_node.ip_address
    cmd = f"curl http://{node_ip}:{port}/metrics | grep ceph_AsyncMessenger_Worker_msgr"

    out = admin_node.exec_command(cmd=cmd, output=True, sudo=True)
    lines = out[0].split("\n")

    # Dictionary to store parsed metrics
    metrics = {}

    # Iterate over lines and extract metrics
    for line in lines:
        if line.startswith("#") or not line.strip():
            # Skip comments and empty lines
            continue

        # Split the line into parts using whitespace
        metric_name, metric_value = line.split(None, 2)[:2]
        metrics[metric_name] = int(metric_value)

    # Check if metrics are available in the response
    if not metrics:
        log.error("Error: No metrics found in the response.")
        return 1

    # Validate each connection metric
    for counter in connection_metrics:
        if not any(counter in metric_name for metric_name in metrics):
            log.error(f"Error: Metric {counter} not found in the response {metrics}")
            return 1

    log.info(f"Network connection Metrics avialble are {metrics}")
    return 0


def run(**kw):
    """Verification of Performance counter for rbd mirror network
    connection health metrics.

    Args:
        **kw: test data

    Returns:
        0 - if test case pass
        1 - if test case fails
    """

    try:
        log.info(
            "Starting test CEPH-83575566 - Performance counter for rbd mirror\
             network connection health metrics."
        )
        config = kw.get("config")

        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd1"), config
        )
        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )

        poolname = mirror1.random_string() + "_test_pool"
        imagename = mirror1.random_string() + "_test_image"
        metrics_port = config.get("ceph_exporter_port", "9926")

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "10G"),
            io_total=config.get("io_total", "4G"),
            mode=config.get("mode", "pool"),
            mirrormode=config.get("mirrormode", "journal"),
            peer_mode=config.get("peer_mode", "bootstrap"),
            rbd_client=config.get("rbd_client", "client.admin"),
            build=config.get("build"),
            **kw,
        )

        mirror1.mirror_snapshot_schedule_add(poolname=poolname, interval="3m")

        # To get cephadm admin node from ceph cluster
        admin_node = kw["ceph_cluster"].get_nodes(role="_admin")[0]

        if validate_connection_metrics(admin_node, metrics_port):
            log.error("Network connection metrics not available")
            return 1

        log.info("Required Network connection metrics are available")
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
