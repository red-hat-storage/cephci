from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """To promote secondary cluster as primary after primary cluster fails,
    which could be used for Regional DR scenario.

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
        log.info("Running test CEPH-9476, Promoting secondary cluster as primary")
        config = kw.get("config")
        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )
        rbd1, rbd2, rbd3 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw["ceph_cluster_dict"].keys()
        ]
        poolname = config.get("poolname")
        mirror2_image_list = rbd2.list_images(poolname)
        # To run I/O on only first image
        is_first_image = True
        for imagename in mirror2_image_list:
            imagespec = poolname + "/" + imagename
            mirror2.promote(imagespec=imagespec, force=True)
            mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
            if is_first_image:
                mirror2.benchwrite(imagespec=imagespec, io=config.get("io-total"))
                is_first_image = False
        log.info("Successfully promoted secondary cluster as primary")
        return 0

    except Exception as e:
        log.exception(e)
        return 1
