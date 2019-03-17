import logging
import time

from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    try:
        log.info("Starting CEPH-9471")
        mirror1 = kw.get('test_data')['mirror1']
        mirror2 = kw.get('test_data')['mirror2']
        config = kw.get('config')
        poolname = mirror1.random_string() + '9471pool'
        imagename = mirror1.random_string() + '9471image'
        imagespec = poolname + '/' + imagename
        state_after_demote = 'up+stopped' if mirror1.ceph_version < 3 else 'up+unknown'

        mirror1.create_pool(poolname=poolname)
        mirror2.create_pool(poolname=poolname)
        mirror1.create_image(imagespec=imagespec, size=config.get('imagesize'))
        mirror1.config_mirror(mirror2, poolname=poolname, mode='pool')
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.benchwrite(imagespec=imagespec, io=config.get('io-total'))
        mirror2.wait_for_replay_complete(imagespec=imagespec)
        mirror1.demote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        with parallel() as p:
            for node in mirror1.ceph_nodes:
                p.spawn(mirror1.exec_cmd, ceph_args=False, cmd='reboot',
                        node=node, check_ec=False)
        mirror2.promote(imagespec=imagespec)
        mirror2.benchwrite(imagespec=imagespec, io=config.get('io-total'))
        time.sleep(30)
        mirror2.check_data(peercluster=mirror1, imagespec=imagespec)
        mirror2.demote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror1.promote(imagespec=imagespec)
        mirror1.benchwrite(imagespec=imagespec, io=config.get('io-total'))
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0

    except Exception as e:
        log.exception(e)
        return 1
