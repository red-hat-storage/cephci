"""Test case covered - CEPH-83576099

Test Case Flow:
1. Configure snapshot based mirroring between two clusters
2. create snapshots of an image
3. protect the snapshot of an image
4. clone the snapshot to new image
5. Tried to enable cloned images for snapshot-based mirroring it should not allow
6. Flatten the cloned image
7. Enable snapshot based mirroring for the flattened child image
8. Perform test steps for both Replicated and EC pool
"""

from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_snapshot_mirror_clone(rbd_mirror, pool_type, **kw):
    """Method to validate snapshot based mirroring of cloned images
    and images which are flattened.

    Args:
        rbd_mirror: Object
        pool_type: Replicated or EC pool
        **kw: test data
            Example::
            config:
                ec_pool_config:
                  mirrormode: snapshot
                  mode: image
                rep_pool_config:
                  mirrormode: snapshot
                  mode: image
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        log.info("Starting snapshot based RBD mirroring test case")
        config = kw.get("config")
        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        poolname = config[pool_type]["pool"]
        imagename = config[pool_type]["image"]
        snap = "snap1"
        clone = "clone1"
        snap_name = f"{poolname}/{imagename}@{snap}"

        # creating snapshot of an image
        rbd1.snap_create(poolname, imagename, snap)

        # protect the snapshot of an image
        rbd1.protect_snapshot(snap_name)

        # clone the snapshot to new image
        rbd1.create_clone(
            snap_name=snap_name,
            pool_name=poolname,
            image_name=clone,
        )

        # Tried to enable cloned images for snapshot-based mirroring it should not allow
        command = f"rbd mirror image enable {poolname}/{clone} snapshot"
        out, err = rbd1.exec_cmd(cmd=command, all=True, check_ec=False)
        if "not supported" not in err:
            log.error(
                "cloned image has parent and it's snapshot-based mirroring enabled test failed"
            )
            log.debug(err)
            return 1
        log.info(
            "cloned image has parent, hence it's snapshot-based mirroring is not supported"
        )

        # Flatten the cloned image
        rbd1.flatten_clone(poolname, clone)

        # Enable snapshot based mirroring for the flattened child image
        out, err = rbd1.exec_cmd(cmd=command, all=True, check_ec=False)
        if "Mirroring enabled" not in out:
            log.error(
                "cloned image which flattened also not able to enable snapshot-based mirroring"
            )
            log.debug(out)
            return 1
        log.info(
            "cloned image which flattened, able to enable snapshot-based mirroring"
        )

        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])


def run(**kw):
    """
    Enabling snapshot based mirroring is not supported for cloned images
    which is not yet flattened i.e the cloned images which still hold
    the parent and child relationship.

    Args:
        **kw: test data
            Example::
            config:
                ec_pool_config:
                  mirrormode: snapshot
                  mode: image
                rep_pool_config:
                  mirrormode: snapshot
                  mode: image

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info(
        "Starting test case CEPH-83576099 "
        "Enable snapshot based mirroring of cloned images which are not flattened"
    )

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_snapshot_mirror_clone(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_snapshot_mirror_clone(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1

    return 0
