"""Test case covered -
CEPH-83575565 - Performance counter metrics for snapshot based
mirroring.

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
    6. Verify snapshot mirror based performance conter metrics.
"""

from ceph.rbd.workflows.rbd_mirror_metrics import create_symlink_and_get_metrics
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def validate_snap_metrics(mirror_node):
    """Method to validate snapshot based mirroring metrics.
    Args:
        mirror_node: rbd-mirror daemon node.

    Example:
        "rbd_mirror_snapshot": {
            "labels": {},
            "counters": {
                "snapshots": 13404,
                "sync_time": {
                    "avgcount": 13404,
                    "sum": 42.406661993,
                    "avgtime": 0.003163731
                },
                "sync_bytes": 1693450240
            }

        "rbd_mirror_snapshot_image": {
            "labels": {
                "image": "io_set_1",
                "namespace": "",
                "pool": "test_pool"
            },
            "counters": {
                "snapshots": 49,
                "sync_time": {
                    "avgcount": 49,
                    "sum": 6.935865147,
                    "avgtime": 0.141548268
                },
                "sync_bytes": 1588592640,
                "remote_timestamp": 1682935620.226228164,
                "local_timestamp": 1682935620.226228164,
                "last_sync_time": 0.004110399,
                "last_sync_bytes": 0
            }
        }
    Returns:
        0 - if test case pass
        1 - if test case fails
    """
    metrics = create_symlink_and_get_metrics(mirror_node)

    # Validate snapshot metrics
    snapshot_metrics = metrics.get("rbd_mirror_snapshot", [])
    if snapshot_metrics:
        snapshot_counters = snapshot_metrics[0].get("counters", {})
        if all(
            key in snapshot_counters for key in ["snapshots", "sync_time", "sync_bytes"]
        ):
            log.info("All required snapshot metrics are present as below.")
            log.info(snapshot_metrics)
        else:
            log.error("One or more required snapshot metrics are missing.")
            return 1
    else:
        log.error("rbd mirror snapshot metrics are missing.")
        return 1

    # Validate snapshot based mirroring image metrics
    image_metrics = metrics.get("rbd_mirror_snapshot_image", [])
    if image_metrics:
        image_labels = image_metrics[0].get("labels", {})
        image_counters = image_metrics[0].get("counters", {})
        required_labels = ["image", "namespace", "pool"]
        if all(key in image_labels for key in required_labels):
            log.info("All required image snapshot labels are present.")
        else:
            log.error("One or more image snapshot labels are missing.")
            return 1

        required_counters = [
            "snapshots",
            "sync_time",
            "sync_bytes",
            "remote_timestamp",
            "local_timestamp",
            "last_sync_time",
            "last_sync_bytes",
        ]
        if all(key in image_counters for key in required_counters):
            log.info("All required image snapshot counters are present as below.")
            log.info(image_metrics)
            return 0
        else:
            log.error("One or more image snapshot counters are missing.")
            return 1
    else:
        log.error("rbd mirror snapshot image metrics are missing.")
        return 1


def run(**kw):
    """Verification of Performance counter metrics for snapshot
    based mirroring.

    Args:
        **kw: test data

    Returns:
        0 - if test case pass
        1 - if test case fails
    """

    try:
        log.info(
            "Starting CEPH-83575565 - Performance counter metrics for snapshot "
            "based mirroring"
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

        # To get rbd-mirror daemon node from ceph cluster
        mirror_node = kw["ceph_cluster"].get_nodes(role="rbd-mirror")[0]

        if validate_snap_metrics(mirror_node):
            log.error("Snapshot based mirroring metrics not available")
            return 1

        log.info("Required snapshot based mirroring metrics are available")
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
