"""This modules verify parent snapshot deletion of a clone having a snapshot on mirrored image
Test case covered -
CEPH-9515 - Delete the parent snapshot of a clone having a snapshot (deep flatten),
on the image that is getting mirrored to remote cluster

Pre-requisites :
1. At least two clusters must be up and running with enough number of OSDs to create pools
2. We need atleast one client node with ceph-common package,
    conf and keyring files

Test Case Flow:
1. Create Replication pool
2. Create an image with deep flatten feature in primary and perform IOs.
3. enable pool mirroring on the image.
4. verify for the pool mirror mode enabled on secondary.
5. Create and list the snapshot on primary.
6. Protect the snapshot and create clone of snapshot.
7. verify that clone of a image within the Pool also get mirrored.
8. while writing some IOS, flatten clone, unprotect and remove the parent snapshot
9. verify the changes in secondary rbd cluster.
10. try doing same operation on secondary image, it should not allow as primary image holds lock.
11. Repeat step 2 to 10 for EC pool.
"""

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def flatten_unprotect_delete_snap(rbd, pool, image, clone, snap, snap_name):
    if rbd.flatten_clone(pool, clone):
        log.error(f"Flatten clone failed for {clone}")
        return 1
    # verify parent snap remove
    if rbd.unprotect_snapshot(snap_name):
        log.error(f"Unprotect snapshot is failed for {snap}")
        return 1
    if rbd.snap_remove(pool, image, snap):
        log.error(f"Snapshot remove is failed for {image}")
        return 1


def test_mirror_delete_parent_snap(rbd_mirror, pool_type, **kw):
    """
    Test to verify parent snapshot deletion of a clone having a snapshot on the mirrored image

    Args:
        rbd_mirror: rbd mirror object
        pool_type: ec pool or rep pool
        **kw: test data containing pool name, image name and other details
    Returns:
        0 if test pass, else 1
    """
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image
        snap_1 = kw["config"][pool_type].get("snap", f"{image}_snap")
        clone_1 = kw["config"][pool_type].get("clone", f"{image}_clone")

        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]
        mirror2.wait_for_replay_complete(imagespec)
        if rbd1.snap_create(pool, image, snap_1):
            log.error(f"Snapshot with name {snap_1} creation failed for {image}")
            return 1
        snap_name_1 = f"{pool}/{image}@{snap_1}"
        if rbd1.protect_snapshot(snap_name_1):
            log.error(f"Snapshot protect failed for {pool}/{image}")
            return 1
        if rbd1.create_clone(snap_name_1, pool, clone_1):
            log.error(f"Clone creation failed for {pool}/{clone_1}")
            return 1

        # verify in secondary for clone mirror
        imagespec2 = pool + "/" + clone_1
        mirror1.wait_for_status(imagespec=imagespec2, state_pattern="up+stopped")
        mirror2.wait_for_replay_complete(imagespec2)
        if mirror2.image_exists(imagespec2):
            log.error(f"No such image info found for {imagespec2}")
            return 1
        mirror2.wait_for_status(imagespec=imagespec2, state_pattern="up+replaying")

        # delete parent snap while running io's in image
        with parallel() as p:
            p.spawn(
                mirror1.benchwrite, imagespec=imagespec, io=config.get("io_total", "1G")
            )
            p.spawn(
                flatten_unprotect_delete_snap,
                rbd1,
                pool,
                image,
                clone_1,
                snap_1,
                snap_name_1,
            )

        # Verify for parent snapshot deletion
        if snap_1 in rbd1.snap_ls(pool_name=pool, image_name=image, snap_name=snap_1):
            log.info(f"parent snapshot {snap_1} is not removed as expected")
            return 1
        log.info(f"parent snapshot {snap_1} is removed successfully")

        # verify creating snapshot in secondary cluster
        snap_2 = kw["config"][pool_type].get("snap", f"{image}_snap2")
        if rbd2.snap_create(pool, image, snap_2):
            log.info(
                "As expected Snapshot creation failed due to image lock in primary cluster"
            )
            return 0

    except Exception as e:
        log.exception(e)
        return 1

    # Cleans up the configuration
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])


def run(**kw):
    log.info("Starting RBD mirroring test case - CEPH-9515")
    """verify for parent snapshot deletion of a clone having a snapshot (deep flatten)
    on the image that is getting mirrored to remote cluster.
    Args:

        **kw: test data
        Example::
            config:
                ec_pool_config:
                    imagesize: 2G
                    io_total: 200M
                rep_pool_config:
                    imagesize: 2G
                    io_total: 200M
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        if "rep_rbdmirror" in mirror_obj:
            log.info("Executing test on replicated pool")
            if test_mirror_delete_parent_snap(
                mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
            ):
                return 1
        if "ec_rbdmirror" in mirror_obj:
            log.info("Executing test on ec pool")
            if test_mirror_delete_parent_snap(
                mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
            ):
                return 1
    return 0
