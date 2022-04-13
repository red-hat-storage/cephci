import json
import random
import time
import traceback

from ceph.rados_utils import RadosHelper
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
     CEPH-9311 - RADOS: Pyramid erasure codes (Local Repai     rable erasure codes):
     Bring down 2 osds (in case of k=4) from 2 localities      so that recovery happens from local repair code

     1. Create a LRC profile and then create a ec pool
     #ceph osd erasure-code-profile set $profile \
        plugin=lrc \
        k=4 m=2 l=3 \
        ruleset-failure-domain=osd
     # ceph osd pool create $poolname 1 1  erasure $profile

    2. start writing objects to the pool

    # rados -p poolname bench 1000 write --no-cleanup

    3. Bring down 2 osds from 2 different localities which    contains data chunk:(for this we need to figure out
    mapping) for ex: with k=4, m=2, l=3 mapping looks like
    chunk nr    01234567
    step 1      _cDD_cDD    (Here DD are data chunks )
    step 2      cDDD____
    step 3      ____cDDD

    from "step 1" in the above mapping we can see that
    data chunk is divided into 2 localities which is
    anlogous to 2 data center. so in our case for ex
    we have to bring down (3,7) OR (2,7) OR (2,6) OR (3,6)    .

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info("Running test ceph-9311")
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))

    mons = []
    role = "client"

    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(cmon=ctrlr.hostname))

    helper = RadosHelper(ctrlr, config, log)

    """Create an LRC profile"""
    sufix = random.randint(0, 10000)
    prof_name = "LRCprofile{suf}".format(suf=sufix)
    if build.startswith("4"):
        profile = "osd erasure-code-profile set {LRCprofile} plugin=lrc k=4 m=2 l=3 \
            crush-failure-domain=osd".format(
            LRCprofile=prof_name
        )
    else:
        profile = "osd erasure-code-profile set {LRCprofile} plugin=lrc k=4 m=2 l=3 \
            ruleset-failure-domain=osd crush-failure-domain=osd".format(
            LRCprofile=prof_name
        )
    try:
        (outbuf, err) = helper.raw_cluster_cmd(profile)
        log.info(outbuf)
        log.info("created profile {LRCprofile}".format(LRCprofile=prof_name))
    except Exception:
        log.error("LRC profile creation failed")
        log.error(traceback.format_exc())
        return 1
    """create LRC ec pool"""
    pool_name = "lrcpool{suf}".format(suf=sufix)
    try:
        helper.create_pool(pool_name, 1, prof_name)
        log.info("Pool {pname} created".format(pname=pool_name))
    except Exception:
        log.error("lrcpool create failed")
        log.error(traceback.format_exc())
        return 1

    """ Bringdown 2 osds which contains a 'D' from both localities
        we will be chosing osd at 2 and 7 from the given active set list
    """
    oname = "UNIQUEOBJECT{i}".format(i=random.randint(0, 10000))
    cmd = "osd map {pname} {obj} --format json".format(pname=pool_name, obj=oname)
    (outbuf, err) = helper.raw_cluster_cmd(cmd)
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    # targt_pg = cmdout['pgid']
    target_osds_ids = []
    for i in [2, 7]:
        target_osds_ids.append(cmdout["up"][i])

    # putobj = "sudo rados -p {pool} put {obj} {path}".format(
    #     pool=pool_name, obj=oname, path="/etc/hosts"
    # )
    for i in range(10):
        putobj = "sudo rados -p {pool} put {obj} {path}".format(
            pool=pool_name, obj="{oname}{i}".format(oname=oname, i=i), path="/etc/hosts"
        )
        (out, err) = ctrlr.exec_command(cmd=putobj)
    """Bringdown tosds"""
    osd_service_map_list = []
    for osd_id in target_osds_ids:
        target_osd_hostname = ceph_cluster.get_osd_metadata(osd_id).get("hostname")
        target_osd_node = ceph_cluster.get_node_by_hostname(target_osd_hostname)
        osd_service = ceph_cluster.get_osd_service_name(osd_id)
        osd_service_map_list.append(
            {"osd_node": target_osd_node, "osd_service": osd_service}
        )
        helper.kill_osd(target_osd_node, osd_service)
        time.sleep(5)

        outbuf = "degrade"
        timeout = 10
        found = 0
        status = "-s --format json"
        while timeout:
            if "active" not in outbuf:
                (outbuf, err) = helper.raw_cluster_cmd(status)
                time.sleep(1)
                timeout = timeout - 1
            else:
                found = 1
                break
        if timeout == 0 and found == 0:
            log.error("cluster didn't become active+clean..timeout")
            return 1

    """check whether read/write can be done on the pool"""
    for i in range(10):
        putobj = "sudo rados -p {pool} put {obj} {path}".format(
            pool=pool_name, obj="{oname}{i}".format(oname=oname, i=i), path="/etc/hosts"
        )
        (out, err) = ctrlr.exec_command(cmd=putobj)
        log.info(out)
    for i in range(10):
        putobj = "sudo rados -p {pool} get {obj} {path}".format(
            pool=pool_name,
            obj="{oname}{i}".format(oname=oname, i=i),
            path="/tmp/{obj}{i}".format(obj=oname, i=i),
        )
        (out, err) = ctrlr.exec_command(cmd=putobj)
        log.info(out)
    """donewith the test ,revive osds"""
    for osd_service_map in osd_service_map_list:
        helper.revive_osd(
            osd_service_map.get("osd_node"), osd_service_map.get("osd_service")
        )

    return 0
