from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from tests.rbd_mirror.rbd_mirror_utils import create_mirrored_images_with_user_specified
from utility.log import Log

log = Log(__name__)


def configure_snapshot_mirroring(**kw):
    """
    Method to create snapshot based mirroring,
    Args:
        **kw: test data
        Example::
            config:
                imagesize: 2G
    Returns:
        0 - if test case pass
        1 - if test case fails
    """

    try:
        log.info("Configuring RBD mirroring with snapshot based")
        config = kw.get("config")
        poolname = config.get("poolname")
        mode = config.get("mode")
        mirrormode = config.get("mirrormode")
        imagesize = config.get("imagesize", "1G")

        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd1"), config
        )
        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )

        imagename = "test_mirror_image" + mirror1.random_string()

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=imagesize,
            io_total=config.get("io-total", "1G"),
            mode=mode,
            mirrormode=mirrormode,
            peer_mode=config.get("peer_mode", "bootstrap"),
            rbd_client=config.get("rbd_client", "client.admin"),
            build=config.get("build"),
            **kw
        )

        return 0

    except Exception as e:
        log.exception(e)
        return 1


def run(**kw):
    # To configure snapshot based mirroring
    configure_snapshot_mirroring(**kw)

    # To create user specified count mirrored images
    create_mirrored_images_with_user_specified(**kw)

    return 0
