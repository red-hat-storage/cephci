from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    --> Creates user specified Pool and enables snapshot based Mirroring on the pool
    --> Runs IO using rbd bench
    --> Creates images on the pool and verifies snapshots are created for each image
    --> Cleanup
    Args:
        **kw: test data
        Example::
            config:
                imagesize: 2G
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    try:
        log.info("Configuring RBD mirroring with snapshot based")
        config = kw.get("config")
        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd1"), config
        )
        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )
        for i in range(1, 20):
            imagename = "tier_2_rbd_mirror_image" + mirror1.random_string()
            mirror1.initial_mirror_config(
                mirror2,
                poolname=config.get("poolname"),
                imagename=imagename,
                imagesize=config.get("imagesize", "1G"),
                io_total=config.get("io-total", "1G"),
                mode="image",
                mirrormode="snapshot",
                **kw,
            )
        return 0
    except Exception as e:
        log.exception(e)
        return 1
