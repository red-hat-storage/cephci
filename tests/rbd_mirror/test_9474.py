import time

from ceph.parallel import parallel
from ceph.utils import hard_reboot
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_9474(rbd_mirror, pool_type, **kw):
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image
        osd_cred = config.get("osp_cred")

        with parallel() as p:
            p.spawn(
                mirror1.benchwrite,
                imagespec=imagespec,
                io=config[pool_type].get("io_total"),
            )
            p.spawn(hard_reboot, osd_cred, name="ceph-rbd2")
        time.sleep(60)
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])


def run(**kw):
    """
    Secondary cluster failure - Abrupt Failure - Recovery of failed Cluster
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9474 - Secondary cluster failure - Abrupt Failure - Recovery of failed Cluster
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with:
        1. 3 monitors
        2. Atleast 9 osds
        3. Atleast 1 Client

    Test Case Flow:
    1. Follow the latest official Block device Doc to configure RBD Mirroring -
        For Both Pool and Image Based Mirroring:-
    2. Start the IOs on the primary image. Fail the secondary cluster machines abruptly (power supply failure)
    3. Bring back the secondary machines.
    4. Check the data integrity and size of the primary and secondary images.
    """
    log.info("Starting CEPH-9474")

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_9474(mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw):
            return 1

        log.info("Executing test on ec pool")
        if test_9474(mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw):
            return 1

    return 0
