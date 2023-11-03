import json
from time import sleep

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd_mirror import wait_for_replay_complete
from utility.log import Log

log = Log(__name__)


def create_delete_snapshot(rbd, pool, image, operation, count):
    """Create or delete snapshots based on the operation specified.
    Args:
        rbd: rbd object
        pool: pool name
        image: image name
        operation: create/delete
        count: number of snapshots to be created or deleted.
    """
    for i in range(0, count):
        snap_spec = {"snap-spec": f"{pool}/{image}@{image}_snap_{i}"}
        if operation == "create":
            out, err = rbd.snap.create(**snap_spec)
        else:
            out, err = rbd.snap.rm(**snap_spec)
        if out or err and "100% complete" not in err:
            raise Exception(f"Snapshot {operation} failed for {snap_spec}")
    return 0


def toggle_journaling_and_verify(rbd1, rbd2, pool, image):
    """Disable and enable journaling feature with a delay and verify image
    deletes when feature is disabled on secondary.
    Args:
        rbd1: rbd object for primary
        rbd2: rbd object for secondary
        pool: pool name
        image: image name
    """
    sleep(15)
    feature_spec = {"image-spec": f"{pool}/{image}", "features": "journaling"}
    out, err = rbd1.feature.disable(**feature_spec)
    if out or err:
        raise Exception(f"Journaling feature disable failed for {image}")

    pool_spec = {"pool": pool, "format": "json"}

    sleep(15)

    out, err = rbd2.ls(**pool_spec)

    if err:
        raise Exception(f"Fetching images in pool {pool} failed with err {err}")
    if out:
        out_json = json.loads(out)
        if image not in out_json:
            log.info(
                "Image not present in secondary when journaling is disabled in primary"
            )
        else:
            raise Exception(
                "Image present in secondary even though journaling is disabled in primary"
            )

    sleep(15)

    out, err = rbd1.feature.enable(**feature_spec)
    if out or err:
        raise Exception(f"Journaling feature enable failed for {image}")

    sleep(15)

    return 0


def test_toggle_journaling_feature(**kw):
    """
    Toggles journaling feature while snapshots are created and deleted on an image
    """
    for pool_type in kw.get("pool_types"):
        config = kw.get("config", {}).get(pool_type)
        multi_pool_config = getdict(config)
        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            for image in multi_image_config.keys():
                try:
                    test_config = [
                        {"operation": "create", "count": 25},
                        {"operation": "delete", "count": 15, "verify_count": 10},
                    ]
                    for test in test_config:
                        with parallel() as p:
                            p.spawn(
                                create_delete_snapshot,
                                rbd=kw.get("rbd_obj"),
                                pool=pool,
                                image=image,
                                operation=test.get("operation"),
                                count=test.get("count"),
                            )
                            p.spawn(
                                toggle_journaling_and_verify,
                                rbd1=kw.get("rbd_obj"),
                                rbd2=kw.get("sec_obj"),
                                pool=pool,
                                image=image,
                            )

                        rbd2 = kw.get("sec_obj")
                        wait_for_replay_complete(
                            rbd=rbd2,
                            cluster_name=kw.get("ceph_cluster").name,
                            imagespec=f"{pool}/{image}",
                        )

                        image_spec = {"image-spec": f"{pool}/{image}", "format": "json"}
                        out, err = rbd2.snap.ls(**image_spec)
                        if err:
                            log.error("Fetch snapshots for secondary failed")
                            return 1
                        if out:
                            out_json = json.loads(out)
                            snap_json = [k for k in out_json if "snap" in k["name"]]
                            count = test.get("verify_count", test.get("count"))
                            if len(snap_json) == count:
                                log.info(
                                    f"All snapshots {test.get('operation')}d at primary are synced to secondary"
                                )
                            else:
                                log.error(
                                    "There is discrepancy between primary and secondary snapshots"
                                )
                                return 1
                except Exception as e:
                    log.error(f"Test failed for {image} with error {e}")
                    return 1
    return 0


def run(**kw):
    """Disable and enable Journalling while snapshots are being created/deleted
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    Test cases covered -
    1) CEPH-10469 - Disable and enable Journalling while snapshots are being created/deleted
    kw:
        config:
            rep_pool_config:
              num_pools: 1
              num_images: 1 # one each of below encryptions will be applied on each image
              mode: pool
              mirrormode: journal
            ec_pool_config:
              num_pools: 1
              num_images: 1 # one each of below encryptions will be applied on each image
              mode: pool
              mirrormode: journal

    Test Case Flow
    1. Bootstrap two CEPH clusters and create a pool and an image each for replicated and ec pools
    2. Setup journal based mirroring in between these clusters at pool level
    3. Create 25 snapshots for the image and disable journaling feature while snapshots are getting
    created.
    4. The image in secondary should be deleted as soon as journaling feature is disabled.
    5. After a few seconds, enable the journaling feature and verify that all snapshots which were
    created are mirrored in secondary.
    6. Delete 15 of the 25 snapshots created for the image and disable journaling feature while
    snapshots are getting deleted.
    7. The image in secondary should be deleted as soon as journaling feature is disabled.
    8. After a few seconds, enable the journaling feature and verify that all snapshots which were
    deleted in primary are not present in secondary.
    """
    pool_types = []
    log.info(
        "Running test CEPH-10469 disable and enable journaling while snapshots are being created/deleted"
    )
    try:
        ret_val = 0
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        cluster_name = kw["ceph_cluster"].name
        sec_cluster_name = [
            k for k in kw.get("ceph_cluster_dict").keys() if k != cluster_name
        ][0]
        pool_types = mirror_obj.get(cluster_name).get("pool_types")
        rbd_obj = mirror_obj.get(cluster_name).get("rbd")
        sec_obj = mirror_obj.get(sec_cluster_name).get("rbd")
        ret_val = test_toggle_journaling_feature(
            rbd_obj=rbd_obj, sec_obj=sec_obj, pool_types=pool_types, **kw
        )
        if ret_val:
            log.error("Testing toggle journaling feature failed")
    except Exception as e:
        log.error(
            f"Testing snapshot mirroring at pool level failed with error {str(e)}"
        )
        ret_val = 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return ret_val
