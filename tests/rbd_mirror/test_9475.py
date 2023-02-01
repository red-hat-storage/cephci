import time

from ceph.parallel import parallel
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_9475(rbd_mirror, pool_type, **kw):
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image

        with parallel() as p:
            for node in mirror2.ceph_nodes:
                p.spawn(
                    mirror2.exec_cmd,
                    ceph_args=False,
                    cmd="reboot",
                    node=node,
                    check_ec=False,
                )
            p.spawn(
                mirror1.benchwrite,
                imagespec=imagespec,
                io=config[pool_type].get("io_total"),
            )
        time.sleep(30)
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[pool])
        return 0

    except Exception as e:
        log.exception(e)
        return 1


def run(**kw):
    """
    Secondary cluster failure - Ordered Shutdown - Recovery of shutdown cluster.
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9475 - Secondary cluster failure - Ordered Shutdown - Recovery of shutdown cluster.
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with:
        1. 3 monitors
        2. Atleast 9 osds
        3. Atleast 1 Client

    Test Case Flow:
    1. Do an orderly shutdown of all the machines on the slave cluster.
    2. Write some IOs on the primary image.
    3. Bring back the secondary cluster.
    4. Verify that both primary and secondary image has same data
    """
    log.info("Starting CEPH-9475")

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_9475(mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw):
            return 1

        log.info("Executing test on ec pool")
        if test_9475(mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw):
            return 1

    return 0
