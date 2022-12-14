import time

from ceph.parallel import parallel
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    try:
        log.info("Starting CEPH-9475")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        poolname = mirror1.random_string() + "9475pool"
        imagename = mirror1.random_string() + "9475image"
        imagespec = poolname + "/" + imagename

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            mode="pool",
        )

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
