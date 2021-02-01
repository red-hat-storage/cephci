import logging
import time

from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    try:
        log.info("Starting CEPH-9475")
        mirror1 = kw.get("test_data")["mirror1"]
        mirror2 = kw.get("test_data")["mirror2"]
        config = kw.get("config")
        poolname = mirror1.random_string() + "9475pool"
        imagename = mirror1.random_string() + "9475image"
        imagespec = poolname + "/" + imagename

        mirror1.create_pool(poolname=poolname)
        mirror2.create_pool(poolname=poolname)
        mirror1.create_image(imagespec=imagespec, size=config.get("imagesize"))
        mirror1.config_mirror(mirror2, poolname=poolname, mode="pool")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

        with parallel() as p:
            for node in mirror2.ceph_nodes:
                p.spawn(
                    mirror2.exec_cmd,
                    ceph_args=False,
                    cmd="reboot",
                    node=node,
                    check_ec=False,
                )
            p.spawn(mirror1.benchwrite, imagespec=imagespec, io=config.get("io-total"))
        time.sleep(30)
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0

    except Exception as e:
        log.exception(e)
        return 1
