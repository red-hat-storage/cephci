from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    --> Configures RBD Mirroring on cephadm
    --> Creates Pool Image and enables Mirroring
    --> Runs IO using rbd bench
    --> Resize the image
    --> Allow changes to the image to mirror
    --> Check for data consistency
    --> Cleanup
    Args:
        **kw:
            peer_mode:
                manual if mirroring daemon was added using ceph-ansible for 4x
                manual by default for all versions below 4x
                bootstrap by default for all versions starting from 4x
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    try:
        log.info("Starting RBD mirroring test case")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        poolname = mirror1.random_string() + "_tier_1_rbd_mirror_pool"
        imagename = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec = poolname + "/" + imagename

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="pool",
            mirrormode="journal",
            peer_mode=config.get("peer_mode", "bootstrap"),
            rbd_client=config.get("rbd_client", "client.admin"),
            build=config.get("build"),
            **kw,
        )

        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        mirror1.resize_image(imagespec=imagespec, size=config.get("resize_to"))
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.exception(ve)
    except Exception as e:
        log.exception(e)
        return 1
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
