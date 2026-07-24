from ceph.parallel import parallel
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_write_from_secondary(self, mirrormode, imagespec):
    """Method to verify write operation should not be performed from secondary site,
    as mirrored image got locked from primary site.

    Args:
        self: mirror2 object
        mirrormode: type of mirroring journal or snapshot
        imagespec: poolname + "/" + imagename
    Returns:
        0 - if test case pass
        1 - if test case fails
    """
    out, err = self.benchwrite(
        imagespec=imagespec, long_running=False, all=True, check_ec=False
    )
    log.debug(err)

    if "Read-only file system" in err:
        log.info(
            "As Expected writing data from secondary got failed, "
            f"mirrored image got locked from primary in {mirrormode} based mirroring"
        )
        return 0

    log.error(
        f"Able to write data into secondary image for {mirrormode} based mirroring,"
        " This is invalid since image should be locked at primary."
    )
    return 1


def test_journal_to_snapshot(rbd_mirror, pool_type, **kw):
    """verification Snap mirroring on journaling based image after disable journaling on image.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - if test case fails

    Test case flow:
    1. Create image on cluster-1.
    2. Enable journal mirroring for created image on cluster-1 and run IOs.
    3. Ensure mirror image created on cluster-2.
    4. Disable journal mirroring on image.
    5. enable snapshot mirroring on same image.
    6. Verify for snapshot mirroring and run IOs
    7. cleanup
    """
    try:
        log.info("Starting RBD mirroring test case")
        config = kw.get("config", {}).get(pool_type)
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        poolname = config.get("pool")
        imagename = config.get("image")
        imagespec = poolname + "/" + imagename

        mirrormode = mirror1.get_mirror_mode(imagespec)
        # Verification of Negative test as image should not allow write operation from secondary
        if test_write_from_secondary(mirror2, mirrormode, imagespec):
            return 1

        # Disabling journal mirror on image
        mirror1.disable_mirroring("image", imagespec)

        # enable snapshot mirroring on image
        mirror1.enable_mirror_image(poolname, imagename, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)

        # Verification of mirrored image on cluster
        if mirror2.image_exists(imagespec):
            return 1

        mirrormode = mirror1.get_mirror_mode(imagespec)
        if mirrormode == "snapshot":
            log.info("snapshot mirroring is enabled")
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))
        with parallel() as p:
            p.spawn(
                mirror1.wait_for_status, imagespec=imagespec, state_pattern="up+stopped"
            )
            p.spawn(
                mirror2.wait_for_status,
                imagespec=imagespec,
                state_pattern="up+replaying",
            )
        # Verification of Negative test as image should not allow write operation from secondary
        if test_write_from_secondary(mirror2, mirrormode, imagespec):
            return 1
        return 0

    except Exception as e:
        log.error(e)
        return 1

    # Cleans up the configuration
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])


def run(**kw):
    """
    Journal based mirroring to snapshot based mirroring conversion.

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
                snapshot_schedule_level: "cluster"
                imagesize: 2G

    Returns:
        int: The return value - 0 for success, 1 for failure
    """
    log.info(
        "Starting CEPH-83573618, "
        "Test to verify snapshot based mirroring on journaling based mirrored images."
    )

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_journal_to_snapshot(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_journal_to_snapshot(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1
    return 0
