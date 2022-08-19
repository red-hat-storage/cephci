"""
Module to Verify if PG dup entries are trimmed successfully.
"""
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Test to verify if PG dup entries are trimmed successfully.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    pool_configs = config["pool_configs"]
    pool_configs_path = config["pool_configs_path"]
    test_image = config.get("container_image")
    log.debug(f"Verifying pglog dups trimming on OSDs, test img : {test_image}")

    with open(pool_configs_path, "r") as fd:
        pool_conf_file = yaml.safe_load(fd)

    pools = []
    acting_sets = {}
    osds = []
    for i in pool_configs:
        pool = pool_conf_file[i["type"]][i["conf"]]
        if pool["pool_type"] == "replicated":
            pool.update({"size": "3"})
        create_given_pool(rados_obj, pool)
        pools.append(pool["pool_name"])

    log.info(f"Created 3 pools for testing. pools : {pools}")

    log.debug(
        "Writing test data to write objects into PG log, before injecting corrupt dups"
    )
    with parallel() as p:
        for pool in pools:
            p.spawn(rwrite_nosave, obj=rados_obj, dur=50, count=2, pool=pool)
            time.sleep(2)

    # Identifying 1 PG from each pool to inject dups
    for pool in pools:
        pool_id = pool_obj.get_pool_id(pool_name=pool)
        pgid = f"{pool_id}.0"
        pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
        acting_sets[pgid] = pg_set
        [osds.append(osd) for osd in pg_set]

    log.info(f"Identified Acting set of OSDs for the Pools. {acting_sets}")

    # Collect num pglog objects from dump_mempools
    pre_pglog_items = {}
    for osd in osds:
        pre_pglog_items[osd] = get_pglog_items(obj=rados_obj, osd=osd).get("items")
        log.debug(
            f"Number of pglog items collected from mempools :\n {osd} -> {pre_pglog_items[osd]}"
        )

    log.debug("Setting noout and pause flags")
    cmd = " ceph osd set noout && ceph osd set pause"
    rados_obj.node.shell([cmd])

    # sleeping for 10 seconds for pause flag to take effect
    time.sleep(10)

    # Proceeding to stop OSDs from one acting set at a time, injecting dups
    for pgid in acting_sets.keys():
        log.debug(f"Stopping OSDs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        for osd in acting_sets[pgid]:
            rados_obj.change_osd_state(action="stop", target=osd)

        # Starting to use COT from the 1st OSD in acting set
        for osd in acting_sets[pgid]:
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd)
            method_should_succeed(copy_cot_script, host)
            log.debug(f"Copied the COT script on to host : {host.hostname}")
            # Collecting the num of dups present on the PG - OSD
            method_should_succeed(
                run_cot_command, host=host, osd=osd, task="log", pgid=pgid, startosd=0
            )
            # todo: check the logs generated and fetch dups count

            # injecting dup entries
            method_should_succeed(
                run_cot_command,
                host=host,
                osd=osd,
                task="pg-log-inject-dups",
                pgid=pgid,
                startosd=0,
                image=test_image,
            )

            # Collecting the num of dups present on the PG - OSD
            method_should_succeed(
                run_cot_command, host=host, osd=osd, task="log", pgid=pgid, startosd=1
            )
            # todo: check the logs generated and fetch dups count, should be 100 more than previous

            log.info(
                f"Finished injecting corrupt dups into OSD : {osd} , part of pg : {pgid}\n"
            )
            rados_obj.change_osd_state(action="stop", target=osd)
        log.info(
            f"Completed injecting dups into all the OSDs for pg : {pgid}\n OSDs: {acting_sets[pgid]}\n"
        )

        log.debug(f"Starting OSDs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        for osd in acting_sets[pgid]:
            rados_obj.change_osd_state(action="restart", target=osd)

    log.info(
        f"Completed injecting dups into all the OSDs for pg : {acting_sets.keys()}"
    )

    log.debug("Un-Setting noout and pause flags")
    cmd = " ceph osd unset noout && ceph osd unset pause"
    rados_obj.node.shell([cmd])

    return 0

    # todo: inflate the dup count to desired levels
    # todo: Check the memory usage of the affected OSDs
    # todo: Upgrade to the latest builds
    # todo: Check the boot-up times after upgrade
    # todo: Post upgrade checks: logging, RES memory release, dups auto trimmed, No crashes, errors


def run_cot_command(**kwargs) -> bool:
    """
    Runs the shell script to trigger COT command on the OSD
    Args:
        **kwargs:
            host: host object to run the operation
            osd: OSD ID on which cot should be run
            task: Operation to be run, one of the below
                1. log , 2. pg-log-inject-dups , 3. trim-pg-log-dups
            pgid: PGID on which cot should be run
            image: Ceph image to be used to create shell
            startosd: param to specify if the OSD should be started after COT operation

    Returns: Pass -> True, Fail -> False

    """
    host = kwargs["host"]
    osd = kwargs["osd"]
    task = kwargs["task"]
    pgid = kwargs["pgid"]
    image = kwargs.get("image")
    startosd = kwargs.get("startosd", 1)

    cmd_options = f"-o {osd} -p {pgid} -t {task} -s {startosd}"
    if image:
        cmd_options += f" -i {image}"
    cmd = f"sh run_cot.sh {cmd_options}"
    try:
        host.exec_command(sudo=True, cmd=cmd, long_running=True)
        return True
    except Exception as err:
        log.error(
            f"Failed to run the COT tool onto host : {host.hostname}\n\n error: {err}"
        )
        return False


def copy_cot_script(host) -> bool:
    """
    Copies the shell script to run COT commands
    Args:
        host: Host node to copy script into

    Returns: Pass -> True, Fail -> False

    """
    script_loc = "https://raw.githubusercontent.com/red-hat-storage/cephci/master/utility/run_cot.sh"
    try:
        host.exec_command(
            sudo=True,
            cmd=f"curl -k {script_loc} -O",
        )
        # providing execute permissions
        host.exec_command(sudo=True, cmd="chmod 755 run_cot.sh")
        return True
    except Exception as err:
        log.error(
            f"Failed to copy the COT script onto host : {host.hostname}\n\n error: {err}"
        )
        return False


def rwrite_nosave(obj, pool, dur, count) -> bool:
    """
    Method to write rados objects to pools using radosbench utility, without saving the data.
    Args:
        obj: Class object to connect to cluster
        pool: Name of the pool to write data
        dur: duration for which the IO should be written
        count: Number of times the bench should be initiated

    Returns: Pass -> True, Fail -> False

    """
    for _ in range(count):
        cmd = f"sudo rados --no-log-to-stderr -b 2Kb -p {pool} bench {dur} write"
        try:
            obj.node.shell([cmd])
            return True
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool}, \n\n {err}")
            return False


def get_pglog_items(obj, osd) -> dict:
    """
    Get the pglog items and bytes used by the provided OSDs
    Args:
        obj: Cephadm object
        osd: OSD ID

    Returns: Dict with items and bytes used by OSD. Eg: {'items': 635, 'bytes': 313144}

    """
    cmd = f"ceph tell osd.{osd} dump_mempools"
    out = obj.run_ceph_command(cmd)
    return out["mempool"]["by_pool"]["osd_pglog"]
