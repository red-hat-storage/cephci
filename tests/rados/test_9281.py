import hashlib
import random
import time
import traceback

from ceph.parallel import parallel
from ceph.rados_utils import RadosHelper
from utility.log import Log

log = Log(__name__)

fcsum = "" """checksum to verify in read"""
objlist = []


def prepare_sdata(mon):
    """
    create a 4MB obj, same obj will be put nobj times
    because in do_rados_get we have to verify checksum
    """
    global fcsum
    sdata = "/tmp/sdata.txt"
    DSTR = "hello world"
    dbuf = DSTR * 4
    sfd = None

    try:

        sfd = mon.remote_file(file_name=sdata, file_mode="w+")
        sfd.write(dbuf)
        sfd.flush()
    except Exception:
        log.error("file creation failed")
        log.error(traceback.format_exc())

    sfd.seek(0)
    fcsum = hashlib.md5(sfd.read()).hexdigest()
    log.info("md5 digest = {fcsum}".format(fcsum=fcsum))
    sfd.close()

    return sdata


def do_rados_put(mon, pool, nobj):
    """
    write nobjs to cluster with sdata as source
    """
    src = prepare_sdata(mon)
    log.debug("src file is {src}".format(src=src))

    for i in range(nobj):
        print("running command on {mon}".format(mon=mon.hostname))
        put_cmd = "sudo rados put -p {pname} obj{i} {src}".format(
            pname=pool, i=i, src=src
        )
        log.debug("cmd is {pcmd}".format(pcmd=put_cmd))
        try:
            (out, err) = mon.exec_command(cmd=put_cmd)
        except Exception:
            log.error(traceback.format_exc)
            return 1
        objlist.append("obj{i}".format(i=i))

    return 0


def do_rados_get(mon, pool, niter):
    """
    scan the pool and get all objs verify checksum with
    fcsum
    """
    global fcsum
    for i in range(niter):
        pool_ls = "sudo rados -p {pool} ls".format(pool=pool)
        (out, err) = mon.exec_command(cmd=pool_ls)

        while not fcsum:
            pass
            """
            read objects one by one from the previous list
            and compare checksum of each object
            """
        for obj in objlist:
            file_name = "/tmp/{obj}".format(obj=obj)
            get_cmd = "sudo rados -p {pool} get  {obj} {file_name}".format(
                pool=pool, obj=obj, file_name=file_name
            )
            try:
                mon.exec_command(cmd=get_cmd, check_ec=True, timeout=1000)
                outbuf = out.splitlines()
                log.info(outbuf)
            except Exception:
                log.error("rados get failed for {obj}".format(obj=obj))
                log.error(traceback.format_exc)
            dfd = mon.remote_file(file_name=file_name, file_mode="r")
            dcsum = hashlib.md5(dfd.read()).hexdigest()
            log.debug("csum of obj {objname}={dcsum}".format(objname=obj, dcsum=dcsum))
            print(type(fcsum))
            print("fcsum=", fcsum)
            print(type(dcsum))
            if fcsum != dcsum:
                log.error("checksum mismatch for obj {obj}".format(obj=obj))
                dfd.close()
                return 1
            dfd.close()


def run(ceph_cluster, **kw):
    """
     1. Create a LRC profile and then create a ec pool
            #ceph osd erasure-code-profile set $profile \
            plugin=lrc \
            k=4 m=2 l=3 \
            ruleset-failure-domain=osd
             # ceph osd pool create $poolname 1 1  erasure $profile

    2. start writing a large object so that we will get \
            sometime to fail the osd while the reads and writes are
            in progress on an object

    # rados put -p lrcpool obj1 /src/path
    #rados get -p lrcpool obj1 /tmp/obj1

    while above command is in progress kill primary
    osd responsible for the PG.
    primary can be found from
    # ceph pg dump

    3. Bring back primary

    4. Repeat the step 2 but this time kill some secondary osds

    Args:
        ceph_cluster (ceph.ceph.Ceph):
    """

    log.info("Running test CEPH-9281")
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

    """ create LRC profile """
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

    """rados put and get in a parallel task"""
    with parallel() as p:
        p.spawn(do_rados_put, ctrlr, pool_name, 20)
        p.spawn(do_rados_get, ctrlr, pool_name, 10)

        for res in p:
            log.info(res)

    try:
        pri_osd_id = helper.get_pg_primary(pool_name, 0)
        log.info("PRIMARY={pri}".format(pri=pri_osd_id))
    except Exception:
        log.error("getting primary failed")
        log.error(traceback.format_exc())
        return 1

    log.info("SIGTERM osd")
    target_osd_hostname = ceph_cluster.get_osd_metadata(pri_osd_id).get("hostname")
    pri_osd_node = ceph_cluster.get_node_by_hostname(target_osd_hostname)
    pri_osd_service = ceph_cluster.get_osd_service_name(pri_osd_id)
    try:
        helper.kill_osd(pri_osd_node, pri_osd_service)
        log.info("osd killed")
    except Exception:
        log.error("killing osd failed")
        log.error(traceback.format_exc())
    if not helper.wait_until_osd_state(osd_id=pri_osd_id, down=True):
        log.error("unexpected! osd is still up")
        return 1
    time.sleep(5)
    log.info("Reviving osd {osd}".format(osd=pri_osd_id))

    try:
        if helper.revive_osd(pri_osd_node, pri_osd_service):
            log.error("revive failed")
            return 1
    except Exception:
        log.error("revive failed")
        log.error(traceback.format_exc())
        return 1
    if not helper.wait_until_osd_state(pri_osd_id):
        log.error("osd is DOWN")
        return 1
    log.info(f"Revival of Primary OSD : {pri_osd_id} is complete\n Killing random OSD")

    time.sleep(10)
    try:
        rand_osd_id = helper.get_pg_random(pool_name, 0)
        log.info("RANDOM OSD={rosd}".format(rosd=rand_osd_id))
    except Exception:
        log.error("getting  random osd failed")
        log.error(traceback.format_exc())
        return 1
    log.info("SIGTERM osd")
    target_osd_hostname = ceph_cluster.get_osd_metadata(rand_osd_id).get("hostname")
    rand_osd_node = ceph_cluster.get_node_by_hostname(target_osd_hostname)
    rand_osd_service = ceph_cluster.get_osd_service_name(rand_osd_id)
    try:
        helper.kill_osd(rand_osd_node, rand_osd_service)
        log.info("osd killed")
    except Exception:
        log.error("killing osd failed")
        log.error(traceback.format_exc())
    if not helper.wait_until_osd_state(osd_id=rand_osd_id, down=True):
        log.error("unexpected! osd is still up")
        return 1
    time.sleep(5)
    log.info("Reviving osd {osd}".format(osd=rand_osd_id))
    try:
        if helper.revive_osd(rand_osd_node, rand_osd_service):
            log.error("revive failed")
            return 1
    except Exception:
        log.error("revive failed")
        log.error(traceback.format_exc())
        return 1
    if not helper.wait_until_osd_state(rand_osd_id):
        log.error("osd is DOWN")
        return 1
    log.info(f"Revival of Random OSD : {rand_osd_id} is complete")
    return 0
