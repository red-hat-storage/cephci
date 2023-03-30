"""Module to execute image shrink on primary cluster

Test case covered -
CEPH-9499 - Shrink images on local/primary cluster while mirroring is happening

Pre-requisites :
1. At least two clusters must be up and running with enough number of OSDs to create pools
2. We need atleast one client node with ceph-common package,
    conf and keyring files

Test Case Flow:
1. Bring up 2 cluster and configure the mirroring.
2. run the IO on images.
3. Shrink the image on primary cluster by using --allow-shrink flag.
4. verify for the image status in secondary and verify for the size is updated.
"""

from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_image_shrink_primary(rbd_mirror, pool_type, **kw):
    """
    Test to verify image size in secondary when shrink in primary

    Args:
        rbd_mirror: rbd mirror object
        pool_type: ec pool or rep pool
        **kw: test data
        Example::
            ec_pool_config:
                mode: image
                mirrormode: snapshot
                size: 2G
                io_total: 10

    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image
        resize = "1G"
        mirror1.resize_image(imagespec=imagespec, size=resize)

        if config[pool_type]["mode"] == "image":
            mirror1.create_mirror_snapshot(imagespec)
            mirror2.wait_for_snapshot_complete(imagespec)

        else:
            mirror2.wait_for_replay_complete(imagespec)

        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]
        out1 = rbd1.image_info(pool, image)
        out2 = rbd2.image_info(pool, image)
        if out1["size"] == out2["size"]:
            log.info(f"New image size {resize} is updated in secondary")
            return 0
        log.error(f"New image size {resize} is not updated in secondary")
        return 1

    except Exception as e:
        log.error(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])


def run(**kw):
    """Shrink images on local/primary cluster while mirroring is happening.

    Args:
        **kw: test data
        Example::
            ec_pool_config:
                mode: image
                mirrormode: snapshot
                size: 2G
                io_total: 10
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    log.info("Starting RBD mirroring test case - CEPH-9499")
    for key, value in kw["config"].items():
        if key == "journal":
            kw["config"].update(value)
            mirror_obj = rbd_mirror_config(**kw)
        elif key == "snapshot":
            kw["config"].update(value)
            mirror_obj = rbd_mirror_config(**kw)
        else:
            break
        if mirror_obj:
            log.info("Executing test on replicated pool")
            if test_image_shrink_primary(
                mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
            ):
                return 1
            log.info("Executing test on ec pool")
            if test_image_shrink_primary(
                mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
            ):
                return 1

        if key == "journal":
            for key in kw["config"]["journal"]:
                kw["config"].pop(key)
        elif key == "snapshot":
            for key in kw["config"]["snapshot"]:
                kw["config"].pop(key)
        kw["config"].pop("ec-pool-k-m")
    return 0
