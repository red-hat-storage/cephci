import json
import re
from datetime import datetime

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.krbd_io_handler import krbd_io_handler
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def test_rbd(rbd, pool_type, **kw):
    """Verify tier-0 RBD operations.

    This module verifies tier-0 RBD operations

    Args:
        kw: test data

    Examples:
        kw:
            config:
                ec-pool-k-m: 2,1
                ec-pool-only: False
                ec_pool_config:
                    pool: rbd_pool
                    data_pool: rbd_ec_pool
                    ec_profile: rbd_ec_profile
                    image: rbd_image
                    image_thick_provision: rbd_thick_image
                    thick_size: 2G
                    size: 10G
                    snap: rbd_ec_pool_snap
                    clone: rbd_ec_pool_clone
                rep_pool_config:
                    pool: rbd_rep_pool
                    image: rbd_rep_image
                    image_thick_provision: rbd_rep_thick_image
                    thick_size: 2G
                    size: 10G
                    snap: rbd_rep_pool_snap
                    clone: rbd_rep_pool_clone
                operations:
                    map: true
                    io: true
                    nounmap: false

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        pool = kw["config"][pool_type]["pool"]
        image = kw["config"][pool_type]["image"]
        size = kw["config"][pool_type]["size"]
        size_int = int(re.findall(r"\d+", size)[0])
        snap = kw["config"][pool_type].get("snap", f"{image}_snap")
        clone = kw["config"][pool_type].get("clone", f"{image}_clone")
        image_spec = f"{pool}/{image}"
        snap_name = f"{pool}/{image}@{snap}"

        # Run fio and krbd map operations on image
        kw.get("config")["image_spec"] = [image_spec]
        kw.get("config")["size"] = size
        kw["rbd_obj"] = rbd
        krbd_io_handler(**kw)

        # List images in a pool and verify
        image_list = rbd.list_images(pool)
        if image not in image_list:
            log.error(f"Image {image} is not present in images listed for pool {pool}")
            return 1

        # Display image info and verify
        image_info = rbd.image_info(pool, image)
        log.info(f"Image info : {image_info}")
        if image not in image_info["name"]:
            log.error(
                f"Image {image} is not listed in 'rbd image info' for pool {pool}"
            )
            return 1

        # List image metadata, set last_update metadata and remove it and verify
        image_meta = rbd.image_meta(action="list", image_spec=image_spec)
        try:
            image_meta = json.loads(image_meta)
            log.info(f"Image meta : {image_meta}")
        except ValueError as e:
            log.error(f"Image meta fetch failed : {image_meta} with error {e}")

        # verify image info and log any errors

        # Set last_update metadata and remove it and verify both
        date = datetime.today().strftime("%Y-%m-%d")
        rbd.image_meta(
            action="set",
            image_spec=image_spec,
            key="last_update",
            value=date,
        )

        update_date = rbd.image_meta(
            action="get", image_spec=image_spec, key="last_update"
        )
        log.info(f"Updated date : {update_date}")
        if update_date.strip() != date:
            log.error("Setting image metadata last_update failed")
            return 1

        rbd.image_meta(
            action="remove",
            image_spec=image_spec,
            key="last_update",
        )
        update_date = rbd.image_meta(
            action="get", image_spec=image_spec, key="last_update"
        )
        log.info(f"Updated date : {update_date}")
        if update_date != 1:
            log.error("Removing image metadata last_update failed")
            return 1

        # Create snapshot, protect, clone, flatten, resize, remove, rollback
        if rbd.snap_create(pool, image, snap):
            log.error(f"Snapshot create failed for {snap_name}")
            return 1

        if rbd.protect_snapshot(snap_name):
            log.error(f"Snapshot protect failed for {snap_name}")
            return 1

        if rbd.create_clone(snap_name, pool, clone):
            log.error(f"Clone create failed for {clone}")
            return 1

        if rbd.flatten_clone(pool, clone):
            log.error(f"Clone flatten failed for {clone}")
            return 1

        resize_int = size_int + 2
        resize = f"{resize_int}{size.replace(str(size_int),'')}"
        if rbd.image_resize(pool, clone, resize):
            log.error(f"Clone resize failed for {clone}")
            return 1

        rbd_info = rbd.image_info(pool_name=pool, image_name=clone)
        if int(rbd_info["size"] / (1024 * 1024 * 1024)) != resize_int:
            log.error(f"Image resize failed on clone {clone}")
            return 1

        # verify rollback functionality, resize original image, rollback to the snap
        # verify that original image size is preserved and resize is reversed
        # the steps given below perform this verification

        if rbd.image_resize(pool, image, resize):
            log.error(f"Image resize failed for {image}")
            return 1

        rbd_info = rbd.image_info(pool_name=pool, image_name=image)
        if int(rbd_info["size"] / (1024 * 1024 * 1024)) != resize_int:
            log.error(f"Image resize failed on {image}")
            return 1

        if rbd.snap_rollback(snap_name):
            log.error(f"Snap roll back command failed for {snap_name}")

        rbd_info = rbd.image_info(pool_name=pool, image_name=image)
        if rbd_info["size"] / (1024 * 1024 * 1024) != size_int:
            log.error(f"Snap roll back failed on {image}")
            return 1

        # Add verification for snap rollback
        if rbd.unprotect_snapshot(snap_name):
            log.error(f"Snapshot unprotect failed for {snap_name}")
            return 1

        if rbd.snap_remove(pool, image, snap):
            log.error(f"Snapshot remove failed for {snap_name}")
            return 1

        # Rename (move) an image to a new name and verify
        image_spec_new = f"{pool}/{image}_new"
        rbd.move_image(image_spec, image_spec_new)
        if not rbd.image_exists(
            pool_name=pool, image_name=f"{image}_new"
        ) or rbd.image_exists(pool_name=pool, image_name=image):
            log.error(f"Image rename failed from {image_spec} to {image_spec_new}")
            return 1

        rbd.remove_image(pool_name=pool, image_name=f"{image}_new")
        if rbd.image_exists(pool_name=pool, image_name=f"{image}_new"):
            log.error(f"Image {image}_new not deleted successfully")
            return 1
        return 0
    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(pools=[pool])


def run(**kw):
    """Verify image creation and deletion on ecpools.

    This module verifies image operations on ecpools

    Args:
        kw: test data

    Examples:
        kw:
            config:
                ec-pool-k-m: 2,1
                ec-pool-only: False
                ec_pool_config:
                    pool: rbd_pool
                    data_pool: rbd_ec_pool
                    ec_profile: rbd_ec_profile
                    image: rbd_image
                    image_thick_provision: rbd_thick_image
                    thick_size: 2G
                    size: 10G
                    snap: rbd_ec_pool_snap
                    clone: rbd_ec_pool_clone
                rep_pool_config:
                    pool: rbd_rep_pool
                    image: rbd_rep_image
                    image_thick_provision: rbd_rep_thick_image
                    thick_size: 2G
                    size: 10G
                    snap: rbd_rep_pool_snap
                    clone: rbd_rep_pool_clone
                operations:
                    map: true
                    io: true
                    nounmap: false

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test case covered -
    1) CEPH-83574241 - RBD tier-0 operations
    Test Case Flow
    1. Create RBD images with thick and thin provisioning on replicated pools
    2. Create RBD images with thick and thin provisioning on ec pools
    with overwrites enabled for data pool and metadata on replicated
    3. Verify map operations using krbd client and run fio
    4. Rename, list, move the above images and verify
    5. Fetch image info, image-meta list, remove image-meta and set image-meta
    6. Create snapshots of above images, protect, clone, flatten, resize, remove and rollback the snapshots
    7. Delete images and pools and verify.
    """
    log.info("Running rbd tier-0 tests on replicated and ecpools")
    if not kw.get("config"):
        kw["config"] = {"do_not_create_image": True}
    else:
        kw.get("config")["do_not_create_image"] = True
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        # Run tests on thin provisioned image on replicated pool
        kw.get("config")["do_not_cleanup_pool"] = True
        rc = test_rbd(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw)

        if rc:
            return rc

        # Run tests on thin provisioned image on ec pool
        rc = test_rbd(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)

        if rc:
            return rc

        # Run tests on thick provisioned image on replicated pool
        kw.get("config")["do_not_cleanup_pool"] = False
        kw.get("config")["thick_provision"] = True
        kw["config"]["rep_pool_config"]["image"] = kw["config"]["rep_pool_config"].get(
            "image_thick_provision",
            f"rbd_thick_{kw['config']['rep_pool_config']['image']}",
        )
        kw["config"]["rep_pool_config"]["snap"] = kw["config"]["rep_pool_config"].get(
            "snap_thick_provision",
            f"rbd_thick_{kw['config']['rep_pool_config']['snap']}",
        )
        kw["config"]["rep_pool_config"]["clone"] = kw["config"]["rep_pool_config"].get(
            "clone_thick_provision",
            f"rbd_thick_{kw['config']['rep_pool_config']['clone']}",
        )
        kw["config"]["rep_pool_config"]["size"] = kw["config"]["rep_pool_config"].get(
            "thick_size", "2G"
        )
        rc = test_rbd(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw)

        if rc:
            return rc

        # Run tests on thick provisioned image on ec pool
        kw["config"]["ec_pool_config"]["image"] = kw["config"]["ec_pool_config"].get(
            "image_thick_provision",
            f"rbd_thick_{kw['config']['ec_pool_config']['image']}",
        )
        kw["config"]["ec_pool_config"]["snap"] = kw["config"]["ec_pool_config"].get(
            "snap_thick_provision",
            f"rbd_thick_{kw['config']['ec_pool_config']['snap']}",
        )
        kw["config"]["ec_pool_config"]["clone"] = kw["config"]["ec_pool_config"].get(
            "clone_thick_provision",
            f"rbd_thick_{kw['config']['ec_pool_config']['clone']}",
        )
        kw["config"]["ec_pool_config"]["size"] = kw["config"]["ec_pool_config"].get(
            "thick_size", "2G"
        )
        rc = test_rbd(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)

    return rc
