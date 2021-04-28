import logging
import time

LOG = logging.getLogger(__name__)


def run(**kw):
    LOG.info("Executing test case: Bug 1834974 verification")
    ceph_nodes = kw.get("ceph_nodes")

    time.sleep(180)

    for cnode in ceph_nodes:
        out, err = cnode.exec_command(
            sudo=True,
            cmd="df -h | grep -v shm | grep -i containers | wc -l",
        )
        out = out.read().decode()
        err = err.read().decode()

        if int(out) != 0:
            LOG.debug(err)
            LOG.error("Old container directories found which are consuming space")
            return 1

        LOG.info("No old container directories found.")

    return 0
