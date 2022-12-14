import time

from ceph.parallel import parallel
from ceph.utils import hard_reboot
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    try:
        log.info("Starting CEPH-9474")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        osd_cred = config.get("osp_cred")
        poolname = mirror1.random_string() + "9474pool"
        imagename = mirror1.random_string() + "9474image"
        imagespec = poolname + "/" + imagename

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            mode="pool",
        )

        with parallel() as p:
            p.spawn(mirror1.benchwrite, imagespec=imagespec, io=config.get("io-total"))
            p.spawn(hard_reboot, osd_cred, name="ceph-rbd2")
        time.sleep(60)
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0

    except Exception as e:
        log.exception(e)
        return 1
