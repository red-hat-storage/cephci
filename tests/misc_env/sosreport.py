"""Collecting logs using sosreport from all nodes in cluster Except Client Node."""

import re

from ceph.parallel import parallel
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Entry point for execution used by cephci framework.

    :param kw:
       - ceph_nodes: ceph node list representing a cluster

    :return: 0 on success, 1 for failures
    """
    log.info(f"MetaData Information {log.metadata} in {__name__}")
    ceph_nodes = kw.get("ceph_nodes")
    results = dict()

    log.info("Running sosreport test")
    with parallel() as p:
        for cnode in ceph_nodes:
            if cnode.role != "client":
                p.spawn(generate_sosreport, cnode, results)

    log.info(results)
    if all(results.values()):
        return 0

    return 1


def generate_sosreport(cnode, results):
    """
    Generate sosreport to the given node and validate for the ceph
    :param:
       - ceph_nodes: ceph node list representing a cluster
       - results: Dictionary to store cnode and result of this function as boolean.
                  True for Success False otherwise
    """
    node = cnode.hostname
    try:
        results.update({node: True})
        cnode.exec_command(cmd="sudo yum -y install sos", check_ec=False)

        out, err = cnode.exec_command(
            sudo=True, cmd="sosreport -a --all-logs --batch", timeout=1200
        )
        sosreport = re.search(r"/var/tmp/sosreport-.*.tar.xz", out)
        if err and not sosreport:
            log.error(err)
            results[node] = False
            return

        log.info(f"Generated sosreport is :{sosreport.group()}")
        cnode.exec_command(
            sudo=True, cmd=f"tar -tvf {sosreport.group()} '*ceph*' | wc -l"
        )
    except BaseException as be:  # noqa
        log.exception(be)
        results[node] = False
