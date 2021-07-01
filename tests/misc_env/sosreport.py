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
    if False in results.values():
        return 1
    else:
        return 0


def generate_sosreport(cnode, results):
    """
    :param:
       - ceph_nodes: ceph node list representing a cluster
       - results: Dictionary to store cnode and result of this function as boolean. True for Success False otherwise
    """
    node = cnode.hostname
    results.update({node: True})
    out, err = cnode.exec_command(cmd="sudo yum -y install sos", long_running=True)
    if not err:
        out, err = cnode.exec_command(cmd="sudo sosreport -a --all-logs -e ceph --batch", long_running=True)
        sosreport = re.search(r"sosreport-.*.tar.xz", out)
        if not err and sosreport:
            log.info("Generated sosreport is : {}".format(sosreport.group()))
            command = "mkdir -p sos_report;sudo mv /var/tmp/" + sosreport.group() + " sos_report"
            out, err = cnode.exec_command(cmd=command, long_running=True)
            if not err:
                cmd = "sudo tar -tvf sos_report/" + sosreport.group() + " '*ceph*' | wc -l"
                out, err = cnode.exec_command(cmd=cmd, long_running=True)
                if err or int(out) < 1:
                    results[node] = False
            else:
                results[node] = False
        else:
            results[node] = False
    else:
        results[node] = False
