import json
import time

from ceph.parallel import parallel
from ceph.rados_utils import RadosHelper
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    BZ https://bugzilla.redhat.com/show_bug.cgi?id=1754078 :
    Run scrub/deep scrub and check osd memory usage
    1. Run deep scrub on a cluster parallely when IOs are running
    2. check memory usage of osd daemons is not crossing 'osd memory target' value in ceph.conf on each osd node
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running bz-1754078")
    log.info(run.__doc__)

    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    pg_count = config.get("pg_count", 8)
    timeout = config.get("timeout", 10)
    mons = []
    role = "mon"
    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(cmon=ctrlr.hostname))
    helper = RadosHelper(ctrlr, config, log)
    with parallel() as p:
        helper = RadosHelper(mons[0], config, log)
        p.spawn(helper.run_radosbench, pg_count, timeout)
        helper = RadosHelper(mons[1], config, log)
        p.spawn(helper.run_scrub)
        helper = RadosHelper(mons[2], config, log)
        p.spawn(helper.run_deep_scrub)

        time.sleep(10)
        osd_nodes = []
        role = "osd"
        with parallel() as p:
            for ceph in ceph_nodes:
                if ceph.role == role:
                    osd_nodes.append(ceph)
                    out, err = ceph.exec_command(
                        cmd="sudo ceph osd ls-tree {host}".format(host=ceph.hostname)
                    )
                    osd_id_list_on_node = out.split()
                    log.info("osds on node {}".format(ceph.hostname))
                    log.info(osd_id_list_on_node)
                    osd_mem_target = check_osd_memory_target_of_node(
                        ceph, osd_id_list_on_node
                    )
                    log.info(
                        "Node {a} osd_memory_target in bytes: {b}".format(
                            a=ceph.hostname, b=osd_mem_target
                        )
                    )
                    p.spawn(
                        check_osd_daemon_memory_usage,
                        ceph,
                        osd_id_list_on_node,
                        osd_mem_target,
                    )
                    time.sleep(1)

    return 0


def check_osd_daemon_memory_usage(osd_node, osd_id_list_on_node, osd_mem_target):
    """
    Checks if on a host all osd daemons memory usage is under memory target limit for 3min
    Args:
        node (ceph.ceph.CephNode): ceph node
    """
    log.info(
        "checking memory usage of osds for 180 seconds on {}".format(osd_node.hostname)
    )
    timeout = 180
    while timeout:
        for osd_id in range(len(osd_id_list_on_node)):
            cmd = 'ps aux | grep "/usr/bin/ceph-osd -f --cluster ceph --id {} "'.format(
                osd_id_list_on_node[osd_id]
            )
            cmd_with_mem = "{} | grep -v grep | awk {{'print $6'}}".format(cmd)
            out, err = osd_node.exec_command(cmd=cmd_with_mem)
            out = int(out)
            current_mem_usage = out * 1000
            log.info(
                "osd {a} current memory usage:{b}".format(
                    a=osd_id_list_on_node[osd_id], b=current_mem_usage
                )
            )
            if current_mem_usage > osd_mem_target:
                raise Exception(
                    "osd {} daemon not under memory target,exiting!".format(
                        osd_id_list_on_node[osd_id]
                    )
                )
            log.info(
                "osd memory usage under limit for osd {}".format(
                    osd_id_list_on_node[osd_id]
                )
            )
        timeout = timeout - 1


def check_osd_memory_target_of_node(osd_node, osd_id_list_on_node):
    """
    returns the osd node memory target as per conf
    """
    cmd = "sudo ceph daemon osd.{id} config show -f=json-pretty".format(
        id=osd_id_list_on_node[0]
    )
    out_json, err = osd_node.exec_command(cmd=cmd)
    target_mem = json.loads(out_json)
    osd_memory_target = target_mem["osd_memory_target"]
    return int(osd_memory_target)
