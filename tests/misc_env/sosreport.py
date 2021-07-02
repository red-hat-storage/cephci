""" Collecting logs using sosreport from all nodes in cluster Except Client Node"""

import logging
import re

from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    """
    :param kw:
       - ceph_nodes: ceph node list representing a cluster
    :return: 0 on success, 1 for failures
    """
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
       - results: Dictionary to store cnode and result of this function as boolean. True for Success False otherwise
    """
    node = cnode.hostname
    try:
        results.update({node: True})
        out, err = cnode.exec_command(
            sudo=True, cmd="yum -y install sos", long_running=True
        )
        if err:
            results[node] = False
            return
        out, err = cnode.exec_command(
            sudo=True, cmd="sosreport -a --all-logs -e ceph --batch", long_running=True
        )
        sosreport = re.search(r"/var/tmp/sosreport-.*.tar.xz", out)
        if err and not sosreport:
            results[node] = False
            return
        log.info(f"Generated sosreport is :{sosreport.group()}")
        out, err = cnode.exec_command(
            sudo=True,
            cmd=f"tar -tvf {sosreport.group()} '*ceph*' | wc -l",
            long_running=True,
        )
        if err or int(out) < 1:
            results[node] = False
            return
    except Exception:
        results[node] = False
