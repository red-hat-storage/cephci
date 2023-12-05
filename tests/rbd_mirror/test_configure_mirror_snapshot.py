from time import sleep

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

        imagename = "test_mirror_image" + mirror1.random_string(len=5)

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
            **kw,
        )
        mirror1.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename)
        mirror1.verify_snapshot_schedule(imagespec=f"{poolname}/{imagename}")

        return 0

    except Exception as e:
        log.exception(e)
        return 1


def run(**kw):
    # To configure snapshot based mirroring
    configure_snapshot_mirroring(**kw)

    # To create user specified count mirrored images
    create_mirrored_images_with_user_specified(**kw)

    # Stop and start rbd-mirror is a workaround suggested by dev
    # in the bug https://bugzilla.redhat.com/show_bug.cgi?id=2009735
    # to avoid failures during forced failover.
    if kw["config"].get("stop_rbd_mirror_on_primary"):
        sleep(120)
        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd1"), kw["config"]
        )
        mirror1.change_service_state(None, "stop")

    return 0
