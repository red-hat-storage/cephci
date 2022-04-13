import json
import random
import time

from ceph.rados_utils import RadosHelper
from utility.log import Log

log = Log(__name__)


def get_ms_type(osd, osds, ceph_cluster):
    """
    check what's the default messenger
    """
    target_osd_hostname = ceph_cluster.get_osd_metadata(osd).get("hostname")
    tosd = ceph_cluster.get_node_by_hostname(target_osd_hostname)
    probe_ms = "sudo ceph --admin-daemon /var/run/ceph/ceph-osd.{oid}.asok \
                config show --format json".format(
        oid=osd
    )
    (outbuf, err) = tosd.exec_command(cmd=probe_ms)
    log.info(outbuf)
    jconfig = json.loads(outbuf)
    return jconfig["ms_type"]


def run(ceph_cluster, **kw):
    """
    CEPH-11538:
    Check for default messenger i.e. async messenger
    swith b/w simple and async messenger

    1. By default 3.x wil have async messenger, anything below
    will have simple messenger
    2. add ms_type = async for enabling async and check io
    3. add ms_type=simple for enabling simple and check io

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running CEPH-11538")
    log.info(run.__doc__)
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    mons = []
    osds = []

    role = "client"
    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    role = "osd"
    for osd in ceph_nodes:
        if osd.role == role:
            osds.append(osd)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(cmon=ctrlr.hostname))
    helper = RadosHelper(ctrlr, config, log)

    """crete a pool for io"""
    pname = "mscheck_{rand}".format(rand=random.randint(0, 10000))
    helper.create_pool(pname, 1)
    log.info("pool {pname} create".format(pname=pname))
    time.sleep(5)
    cmd = "osd map {pname} {obj} --format json".format(pname=pname, obj="obj1")
    (outbuf, err) = helper.raw_cluster_cmd(cmd)
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    targt_osd = cmdout["up"][0]

    """check what's the default messenger"""
    mstype = get_ms_type(targt_osd, osds, ceph_cluster)
    if mstype != "async+posix":
        log.error(
            "default on luminous should be async but\
                   we have {mstype}".format(
                mstype=mstype
            )
        )
        return 1

    """switch to simple and do IO"""
    inject_osd = "tell osd.* injectargs --ms_type simple"
    (out, err) = helper.raw_cluster_cmd(inject_osd)
    log.info(out)

    time.sleep(4)
    """check whether ms_type changed"""
    mstype = get_ms_type(targt_osd, osds, ceph_cluster)
    if "simple" == mstype:
        log.info("successfull changed to simple")
    else:
        log.error("failed to change the ms_type to simple")
        return 1

    """change ms_type back to async"""
    inject_osd = "tell osd.* injectargs --ms_type async+posix"
    (out, err) = helper.raw_cluster_cmd(inject_osd)
    log.info(out)
    time.sleep(4)
    """check whether ms_type changed"""
    mstype = get_ms_type(targt_osd, osds, ceph_cluster)
    if "async+posix" == mstype:
        log.info("successfull changed to async+posix")
    else:
        log.error("failed to change the ms_type to async")
        return 1
    return 0
