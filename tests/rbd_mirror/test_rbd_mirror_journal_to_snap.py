from ceph.parallel import parallel
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
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
        1 - it test case fails
    """
    out, err = self.exec_cmd(
        cmd=(
            "rbd bench --io-type write --io-threads 16 "
            f"--io-total '500M' {imagespec}"
        ),
        all=True,
        check_ec=False,
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


def run(**kw):
    """verification Snap mirroring on journaling based image after disable journaling on image.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails

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
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw["ceph_cluster_dict"].values()
        ]
        poolname = mirror1.random_string() + "_tier_2_rbd_mirror_pool"
        imagename = mirror1.random_string() + "_tier_2_rbd_mirror_image"
        imagespec = poolname + "/" + imagename

        # initial mirror configuration
        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="image",
            mirrormode="journal",
            **kw,
        )

        mirrormode = mirror1.get_mirror_mode(imagespec)
        # Verification of Negative test as image should not allow write operation from secondary
        test_write_from_secondary(mirror2, mirrormode, imagespec)

        # Disabling journal mirror on image
        mirror1.disable_mirroring("image", imagespec)

        # enable snapshot mirroring on image
        mirror1.enable_mirror_image(poolname, imagename, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)

        # Verification of mirrored image on cluster
        mirror2.image_exists(imagespec)

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
        test_write_from_secondary(mirror2, mirrormode, imagespec)
        return 0

    except Exception as e:
        log.error(e)
        return 1

    # Cleans up the configuration
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
