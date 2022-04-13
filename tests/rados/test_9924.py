import json
import random
import time
import traceback

from ceph.rados_utils import RadosHelper
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-9925 - [RADOS]:
    Rewrite a known omap item of a replica and list-inconsistent-obj
    Steps:
        1. create an object in a replica pool
        2. add some omap keys and corresponding values to the object
        3. chose one of the replica and using ceph-objectstore-rool corrupt omap key or
           value
        4. Run deep-scrub - scrub should report inconsistency
        5. run rados list-inconsistent-pg <pool> - should list the pg in
           which object is inconsistent
        6. Run rados list-inconsistent-obj <pg> should report omap digest mismatch error
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running CEPH-9924")
    log.info(run.__doc__)
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    mons = []
    role = "client"
    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(cmon=ctrlr.hostname))
    helper = RadosHelper(ctrlr, config, log)

    """create an replica pool"""
    pname0 = "replica_pool_{rand}".format(rand=random.randint(0, 10000))
    pname = pname0
    try:
        helper.create_pool(pname, 128)
        log.info("Pool {pname} is create".format(pname=pname))
    except Exception:
        log.error("failed to create pool")
        log.error(traceback.format_exc())
        return 1
    """check whether pool exists"""
    try:
        helper.get_pool_num(pname)
    except Exception:
        log.error("Unable to find pool")
        log.error(traceback.format_exc())
        return 1

    time.sleep(10)

    oname = "OBJ_{pname}".format(pname=pname)

    putobj = "sudo rados -p {pool} put {obj} {path}".format(
        pool=pname, obj=oname, path="/etc/hosts"
    )
    (out, err) = ctrlr.exec_command(cmd=putobj)
    """ creating omap key/value pairs for an object"""

    for i in range(4):
        omapcmd = "sudo rados -p {pool} setomapval {obj} {keey} {valu}".format(
            pool=pname, obj=oname, keey="key" + str(i), valu="value" + str(i)
        )
        (out, err) = ctrlr.exec_command(cmd=omapcmd)
        log.info(
            "put {obj}, omap key {keey} value {valu}".format(
                obj=oname, keey="key" + str(i), valu="value" + str(i)
            )
        )

    """
    Goto destination osd, stop the osd service to
    use ceph-objectstore-tool to corrupt
    omap keys
    """

    cmd = "osd map {pname} {obj} --format json".format(pname=pname, obj=oname)
    (outbuf, err) = helper.raw_cluster_cmd(cmd)
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    targt_pg = cmdout["pgid"]
    """Considering non primary osd"""
    targt_osd_id = cmdout["up"][1]
    #    target_osd = ceph_cluster.get_osd_by_id(targt_osd_id)
    #    target_osd_node = target_osd.node
    target_osd_hostname = ceph_cluster.get_osd_metadata(targt_osd_id).get("hostname")
    log.info(target_osd_hostname)
    target_osd_node = ceph_cluster.get_node_by_hostname(target_osd_hostname)
    cot_environment = target_osd_node
    osd_service = ceph_cluster.get_osd_service_name(targt_osd_id)
    partition_path = ceph_cluster.get_osd_metadata(targt_osd_id).get("osd_data")
    helper.kill_osd(target_osd_node, osd_service)
    time.sleep(10)
    osd_metadata = ceph_cluster.get_osd_metadata(targt_osd_id)
    osd_data = osd_metadata.get("osd_data")
    osd_journal = osd_metadata.get("osd_journal")

    if ceph_cluster.containerized:
        # target_osd_node.exec_command(cmd='sudo yum install -y ceph-osd', check_ec=False)
        docker_image_string = "{docker_registry}/{docker_image}:{docker_tag}".format(
            docker_registry=ceph_cluster.ansible_config.get("ceph_docker_registry"),
            docker_image=ceph_cluster.ansible_config.get("ceph_docker_image"),
            docker_tag=ceph_cluster.ansible_config.get("ceph_docker_image_tag"),
        )
        cot_environment = helper.get_mgr_proxy_container(
            target_osd_node, docker_image_string
        )
        device_mount_data, err = cot_environment.exec_command(
            cmd='mount | grep "{partition_path} "'.format(
                partition_path=partition_path
            ),
            check_ec=False,
        )
        if not device_mount_data:
            cot_environment.exec_command(
                cmd="sudo mount {partition_path} {directory}".format(
                    partition_path=partition_path, directory=osd_data
                )
            )

    # docker_image_string = '{docker_registry}/{docker_image}:{docker_tag}'.format(
    #     docker_registry=ceph_cluster.ansible_config.get('ceph_docker_registry'),
    #     docker_image=ceph_cluster.ansible_config.get('ceph_docker_image'),
    #     docker_tag=ceph_cluster.ansible_config.get('ceph_docker_image_tag'))
    # mgr_proxy = helper.get_mgr_container_proxy(target_osd_node, docker_image_string)

    slist_cmd = "sudo ceph-objectstore-tool --data-path \
            {osd_data} --journal-path \
            {osd_journal} \
            --pgid {pgid} {obj} list-omap".format(
        osd_data=osd_data, osd_journal=osd_journal, obj=oname, pgid=targt_pg
    )
    (outbuf, err) = cot_environment.exec_command(cmd=slist_cmd)
    keylist = outbuf.split()
    log.info(outbuf)
    """corrupting an omap key by rewriting the omap key with different value"""
    corrupt_cmd = "sudo ceph-objectstore-tool --data-path \
            {osd_data} --journal-path \
            {osd_journal} \
                   --pgid {pgid} {obj} set-omap \
                   {outbuf} {path}".format(
        osd_data=osd_data,
        osd_journal=osd_journal,
        obj=oname,
        pgid=targt_pg,
        outbuf=keylist[0],
        path="/etc/hosts",
    )
    (outbuf, err) = cot_environment.exec_command(cmd=corrupt_cmd)
    log.info(outbuf)

    helper.revive_osd(target_osd_node, osd_service)
    time.sleep(10)
    run_scrub = "pg deep-scrub {pgid}".format(pgid=targt_pg)
    (outbuf, err) = helper.raw_cluster_cmd(run_scrub)
    log.info(outbuf)

    while "HEALTH_ERR" and "active+clean+inconsistent" not in outbuf:
        status = "-s --format json"
        (outbuf, err) = helper.raw_cluster_cmd(status)
    log.info("HEALTH_ERR found as expected")
    log.info("inconsistent found as expected")

    timeout = 100
    found = 0
    while timeout:
        incon_pg = "sudo rados list-inconsistent-pg {pname}".format(pname=pname)
        (outbuf, err) = ctrlr.exec_command(cmd=incon_pg)
        log.info(outbuf)
        if targt_pg not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("pg not listed as inconsistent")
        return 1

    timeout = 100
    found = 0
    while timeout:
        incon_obj = "sudo rados list-inconsistent-obj {pg}".format(pg=targt_pg)
        (outbuf, err) = ctrlr.exec_command(cmd=incon_obj)
        log.info(outbuf)
        if oname not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("object is not listed in inconsistent obj")
        return 1

    return 0
