import logging
import time
logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    log.info("Bug 1834974")
    ceph_nodes = kw.get('ceph_nodes')
    time.sleep(180)
    for cnode in ceph_nodes:
        out, err = cnode.exec_command(cmd='sudo df -h | grep -v shm | grep -i containers | wc -l', long_running=True)
        if int(out) == 0:
            log.info("No old container directories found")
        else:
            log.error("Old container directories found which are consuming space")
            return 1
    return 0
