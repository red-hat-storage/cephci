"""Test case covered -
CEPH-83575564 - Performance counter metrics for journal based
mirroring.

Pre-requisites :
1. Two Clusters must be up and running to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files on each node.

Test Case Flow:
    1. Create a pool on both clusters.
    2. Create an Image on primary mirror cluster in same pool.
    3. Configure mirroring (peer bootstrap) between two clusters.
    4. Enable pool journal based mirroring on the pool respectively.
    5. Start running IOs on the primary image.
    6. Verify journal mirror based performance counter metrics.
"""

from ceph.rbd.workflows.rbd_mirror_metrics import create_symlink_and_get_metrics
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def validate_journal_metrics(mirror_node):
    """Method to validate journal based mirroring metrics.
    Args:
        mirror_node: rbd-mirror daemon node.

    Example:
        "rbd_mirror_journal_image": {
        "labels": {
            "image": "j_image1",
            "namespace": "",
            "pool": "test_pool"
        },
        "counters": {
            "entries": 38406,
            "replay_bytes": 158976398,
            "replay_latency": {
                "avgcount": 38406,
                "sum": 2978.156400591,
                "avgtime": 0.077544040
            }
        }
    Returns:
        0 - if test case pass
        1 - if test case fails
    """
    metrics = create_symlink_and_get_metrics(mirror_node)

    # Validate journal based mirroring image metrics
    journal_metrics = metrics.get("rbd_mirror_journal_image", [])
    if journal_metrics:
        journal_labels = journal_metrics[0].get("labels", {})
        journal_counters = journal_metrics[0].get("counters", {})

        # Check for required labels
        required_labels = ["image", "namespace", "pool"]
        if all(key in journal_labels for key in required_labels):
            log.info("All required journal image labels are present.")
        else:
            log.error("One or more journal image labels are missing.")
            return 1

        # Check for required counters
        required_counters = [
            "entries",
            "replay_bytes",
            "replay_latency",
        ]
        if all(key in journal_counters for key in required_counters):
            log.info("All required journal image counters are present as below.")
            log.info(journal_metrics)
            return 0
        else:
            log.error("One or more journal image counters are missing.")
            return 1
    else:
        log.error("rbd mirror journal image metrics are missing.")
        return 1


def run(**kw):
    """Verification of Performance counter metrics for journal
    based mirroring.

    Args:
        **kw: test data

    Returns:
        0 - if test case pass
        1 - if test case fails
    """

    try:
        log.info(
            "Starting CEPH-83575564 - Performance counter metrics for journal "
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

        # To get rbd-mirror daemon node from ceph cluster
        mirror_node = kw["ceph_cluster"].get_nodes(role="rbd-mirror")[0]

        if validate_journal_metrics(mirror_node):
            log.error("Journal based mirroring metrics not available")
            return 1

        log.info("Required journal based mirroring metrics are available")
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
