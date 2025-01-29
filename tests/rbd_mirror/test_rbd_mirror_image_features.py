"""Test case covered -
CEPH-9520 - When a both Pool and image based mirroring is established,
verify if change of any image features in primary site should reflect
on remote site upon modification.

Test Case Flow:
1. Create a pool on both clusters.
2. Configure mirroring (peer bootstrap) between two clusters.
3. Enable pool mode journal based mirroring on the pool respectively.
4. Verify if change of any image features in primary site gets mirrored in secondary site.
5. Enable image mode snapshot based mirroring on the pool respectively.
6. Verify if change of any image features in primary site gets mirrored in secondary site.
7. perform test on both replicated and EC pool
"""

import datetime
import time

from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_image_feature(rbd_mirror, pool_type, **kw):
    """Method to validate change of image feature in primary
    got reflected onto secondary in Journal and snapshot based RBD mirroring.
    Args:
        rbd_mirror: Object
        pool_type: Replicated or EC pool
        **kw: test data
        Example::
            config:
                imagesize: 2G
    Returns:
        int: The return value: 0 for success, 1 for failure
    """
    try:
        log.info("Running RBD mirroring test case for image features reflection")
        config = kw.get("config")
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw["ceph_cluster_dict"].keys()
        ]
        poolname = config[pool_type]["pool"]
        imagename = config[pool_type]["image"]
        imagespec = poolname + "/" + imagename
        image_feature = "object-map"
        features = list(image_feature.split(","))

        # Disable image features in primary cluster
        mirror1.image_feature_disable(imagespec=imagespec, image_feature=image_feature)

        mirror_mode = mirror1.get_mirror_mode(imagespec)
        if mirror_mode == "snapshot":
            mirror1.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename)

        # wait till image gets reflected to secondary
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
        while end_time > datetime.datetime.now():
            # Verify image feature reflection in secondary cluster
            out = rbd2.image_info(poolname, imagename)
            if not [word for word in features if word in out["features"]]:
                log.info(
                    f"Image features got disabled successfully in secondary for {mirror_mode} based"
                )
                break
            time.sleep(30)
        else:
            log.error(
                f"Image features not disabled in secondary for {mirror_mode} based"
            )
            return 1

        # Enable image features in primary cluster
        mirror1.image_feature_enable(imagespec=imagespec, image_feature=image_feature)

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
        while end_time > datetime.datetime.now():
            # Verify image feature reflection in secondary cluster
            out = rbd2.image_info(poolname, imagename)
            result = [word for word in features if word in out["features"]]
            if set(result) == set(features):
                log.info(
                    f"Image features got enabled successfully in secondary for {mirror_mode} based"
                )
                break
            time.sleep(30)
        else:
            log.error(
                f"Image features not enabled in secondary for {mirror_mode} based"
            )
            return 1
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])


def run(**kw):
    """Verification of change in properties of image in Primary Pool
    gets reflected on remote site upon modification.

    Args:
        **kw: test data
        Example::
            config:
                journal:
                  ec_pool_config:
                    size: 2G
                  rep_pool_config:
                    size: 2G
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    log.info(
        "Starting CEPH-9520 [Rbd Mirror] - Verify if change in properties of"
        "image in Primary Pool gets reflected on remote site upon modification"
    )

    # To test against journal based mirroring
    config = kw["config"]["journal"]
    kw["config"].update(config)
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_image_feature(mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw):
            return 1

        log.info("Executing test on ec pool")
        if test_image_feature(mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw):
            return 1

    # To test against snapshot based mirroring
    config = kw["config"]["snapshot"]
    kw["config"].update(config)
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_image_feature(mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw):
            return 1

        log.info("Executing test on ec pool")
        if test_image_feature(mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw):
            return 1
    return 0
