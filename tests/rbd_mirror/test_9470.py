import time

from ceph.utils import hard_reboot
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    try:
        log.info("Starting CEPH-9470")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        osd_cred = config.get("osp_cred")
        poolname = mirror1.random_string() + "9470pool"
        imagename = mirror1.random_string() + "9470image"
        imagespec = poolname + "/" + imagename
        state_after_demote = "up+stopped" if mirror1.ceph_version < 3 else "up+unknown"

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="pool",
        )

        hard_reboot(osd_cred, name="ceph-rbd1")

        mirror2.promote(imagespec=imagespec, force=True)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        time.sleep(60)
        mirror1.demote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+error")
        mirror1.resync(imagespec=imagespec)
        time.sleep(100)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.wait_for_replay_complete(imagespec=imagespec)
        mirror2.demote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror1.promote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0

    except Exception as e:
        log.exception(e)
        return 1
