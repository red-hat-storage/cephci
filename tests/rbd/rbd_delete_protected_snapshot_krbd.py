from tests.rbd.exceptions import ProtectSnapError, RbdBaseException
from tests.rbd.krbd_io_handler import krbd_io_handler
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def rbd_delete_protect_snap(rbd, pool_type, **kw):
    """
        Deletion of protected snapshot on  RBD image should fail
    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """

    try:
        pool = kw["config"][pool_type]["pool"]
        image = kw["config"][pool_type]["image"]
        snap = kw["config"][pool_type].get("snap", f"{pool}_{image}_snap")
        clone_prefix = kw["config"][pool_type].get("clone", f"{pool}_{image}_clone")
        size = kw["config"][pool_type]["size"]
        snap_name = f"{pool}/{image}@{snap}"
        image_spec = f"{pool}/{image}"

        # Run fio and krbd map operations on image
        kw.get("config")["image_spec"] = [image_spec]
        kw.get("config")["size"] = size
        kw["rbd_obj"] = rbd
        krbd_io_handler(**kw)

        if rbd.snap_create(pool, image, snap):
            log.error(f"Creation of snapshot {snap} failed")
            return 1

        if rbd.protect_snapshot(snap_name):
            log.error(f"Snapshot protect failed for {snap_name}")
            return 1

        log.info("Create multiple clones from the snap image")
        for i in range(1, 11):
            clone_name = f"{clone_prefix}_{i}"
            if rbd.create_clone(snap_name, pool, clone_name):
                log.error(f"Creation of clone {clone_name} failed")
                return 1
            else:
                log.debug(f"Clone {clone_name} created successfully")

        out, err = rbd.snap_remove(pool, image, snap, all=True, check_ec=False)
        if "Removing snap: 0% complete...failed" not in err:
            log.error(f"{out}")
            raise ProtectSnapError(f" Removal of protected Snap failed: {out}")
        log.debug(
            f"Snapshot '{snap_name}' for image '{image}' in pool '{pool}' failed to remove as it is protected: {out}"
        )
        log.info(f"{err}")
        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Delete the protected snapshot and it should fail.
    This test verifies removal of protected snapshot.
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    Test case covered -
    CEPH-9224 - Trying to delete a protected snapshot will fail.
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
    2. We need atleast one client node with ceph-common package,
       conf and keyring files
    Test Case Flow:
    1. Create a pool
    2. Create an image
    3. Run Ios on an Image using krbd client
    4. Take a snapshot
    5. Protect a snapshot
    6. clone a snapshot
    7. try to delete the snap and it should fail
    8. Repeat steps 1 to 7 for ecpool
    """
    log.info("Running Test for deletion of protected snapshot")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if rbd_delete_protect_snap(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw):
            return 1
        log.info("Executing test on EC pool")
        if rbd_delete_protect_snap(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw):
            return 1
    return 0
