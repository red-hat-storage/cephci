from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    --> Deploy two clusters using Cephadm
    --> Configures RBD Mirroring
    --> Check the mirror status on primary and secoundary clusters
    --> Check the rbd-daemons on cluster
    Args:
        **kw:
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
        # Check the mirror status
        poolname = mirror1.random_string() + "_tier_1_rbd_mirror_pool"
        imagename = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec = poolname + "/" + imagename
        mirror1.create_pool(poolname=poolname)
        mirror2.create_pool(poolname=poolname)
        mirror1.create_image(imagespec=imagespec, size=config.get("imagesize"))
        mirror1.config_mirror(mirror2, poolname=poolname, mode="pool")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        # Check rbd-daemon runing status
        out1 = mirror1.mirror_daemon_status("rbd-mirror")
        out2 = mirror1.mirror_daemon_status("rbd-mirror")
        log.info(f"RBD Daemon Status of cluster rbd1 : {out1}")
        log.info(f"RBD Daemon Status of cluster rbd2 : {out2}")
        if out1 == 1 or out2 == 1:
            return 1
        return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.exception(ve)
    except Exception as e:
        log.exception(e)
        return 1
