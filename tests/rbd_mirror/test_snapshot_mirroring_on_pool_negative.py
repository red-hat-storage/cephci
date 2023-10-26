from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.workflows.cleanup import cleanup
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verify that snapshot based mirroring cannot be enabled at pool level
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    Test cases covered -
    1) CEPH-83573617 - [negative scenario] Verify snap mirroring on pool level
    kw:
        config:
            rep_pool_config:
              num_pools: 1
              num_images: 1 # one each of below encryptions will be applied on each image
              mode: pool
              mirrormode: snapshot
            ec_pool_config:
              num_pools: 1
              num_images: 1 # one each of below encryptions will be applied on each image
              mode: pool
              mirrormode: snapshot

    Test Case Flow
    1. Bootstrap two CEPH clusters and create a pool and an image each for replicated and ec pools
    2. Setup snapshot based mirroring in between these clusters at pool level
    3. Verify that enabling snapshot based mirroring at pool level errors out appropriately
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info(
        "Running negative scenario of enabling snapshot based mirroring at pool level"
    )
    try:
        ret_val = 0
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        if not mirror_obj.get("output"):
            log.error("Snapshot based mirroring did not fail in pool mode")
            ret_val = 1
        for output in mirror_obj.get("output"):
            if output:
                log.info(output)
            else:
                log.error("Snapshot based mirroring did not fail in pool mode")
                ret_val = 1
    except Exception as e:
        log.error(
            f"Testing snapshot mirroring at pool level failed with error {str(e)}"
        )
        ret_val = 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return ret_val
