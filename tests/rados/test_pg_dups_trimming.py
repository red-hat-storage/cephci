"""
Module to Verify if PG dup entries are trimmed successfully.
"""
import datetime
import json
import random
import re
import time
from threading import Thread

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

CONTINEOUS_IO = False


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
    installer = ceph_cluster.get_nodes(role="installer")[0]
    log.debug(f"Verifying pglog dups trimming on OSDs, test img : {test_image}")

    try:
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

        log.info(f"Created {len(pools)} pools for testing. pools : {pools}")
        log.debug(
            "Writing test data to write objects into PG log, before injecting corrupt dups"
        )

        # Having continuous IOs throughout the duration of testing
        def write_continuous_io():
            with parallel() as p:
                for pool in pools:
                    p.spawn(
                        rwrite_nosave, obj=rados_obj, dur=50, count=999999, pool=pool
                    )
                    time.sleep(1)
                    if not CONTINEOUS_IO:
                        break

        CONTINEOUS_IO = True
        contineous_io_thread = Thread(target=write_continuous_io, daemon=True)
        contineous_io_thread.start()

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
        flag_set_cmds = ["ceph osd set noout", "ceph osd set pause"]
        flag_unset_cmds = ["ceph osd unset noout", "ceph osd unset pause"]
        [rados_obj.node.shell([cmd]) for cmd in flag_set_cmds]

        # sleeping for 10 seconds for pause flag to take effect
        time.sleep(10)
        log.debug("Injecting the corrupted dups on all the  OSDs")
        if not inject_dups(
            rados_obj=rados_obj, acting_sets=acting_sets, test_image=test_image
        ):
            log.error("Could not inject corrupt dups into the cluster")
            return 1

        log.info(
            f"Completed injecting dups into all the OSDs for pg : {acting_sets.keys()}"
        )

        log.debug("Un-Setting noout and pause flags")
        [rados_obj.node.shell([cmd]) for cmd in flag_unset_cmds]

        # Sleeping for 10 seconds for removed flags to take effect
        time.sleep(10)

        log.debug("Inflating the dup counts on all the corrupted OSDs")
        new_acting_set = inflate_dups(
            rados_obj=rados_obj,
            acting_sets=acting_sets,
            pools=pools,
            test_image=test_image,
        )

        if not new_acting_set:
            log.error("Could not inflate the dups to the desired levels")
            return 1

        # Check the memory usage of the affected OSDs
        log.debug(
            "Completed inflating the dup conunts on the cluster.\n"
            "collecting memory utilization by OSDs before upgrade."
        )

        mem_util_pre_up = get_osd_memory_usages(rados_obj=rados_obj)
        for osd in osds:
            log.debug(
                f"Memory utilization for {osd} before upgrade is : {mem_util_pre_up[osd]}"
            )

        start_time, _ = installer.exec_command(cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'")

        log.debug("Proceeding to upgrade cluster after inflating dup counts")
        if not upgrade_test_cluster(ceph_cluster=ceph_cluster, **config):
            log.error("Upgrade failed")
            return 1

        # todo: Check the boot-up times after upgrade

        # Post upgrade checks: logging, RES memory release, dups auto trimmed, No crashes, errors
        time.sleep(120)
        log.info("Upgrade completed on the cluster")
        end_time, _ = installer.exec_command(cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'")

        # Checking the logs generated post upgrade
        log.debug("Proceeding to check logging post upgrade")
        if not verify_trim_dups_warn_log(
            rados_obj=rados_obj,
            acting_sets=new_acting_set,
            start_time=start_time.strip(),
            end_time=end_time.strip(),
        ):
            log.error("Warning logs not generated post upgrade.")
            return 1
        log.debug("Verified logging of warning messages post upgrade")

        for pool in pools:
            method_should_succeed(
                rwrite_nosave, obj=rados_obj, dur=50, count=2, pool=pool
            )

        # Check the memory usage of the affected OSDs
        log.debug("collecting memory utilization by OSDs")
        mem_util_post_up = get_osd_memory_usages(rados_obj=rados_obj)
        for osd in osds:
            log.debug(
                f"Memory utilization for {osd} After upgrade is : {mem_util_post_up[osd]}"
            )
            if mem_util_post_up[osd]["RES"] >= mem_util_pre_up[osd]["RES"]:
                log.error(f"RES Memory not released for OSD : {osd}")
                return 1
        log.debug("RES Memory released after Upgrade")

        # Checking the dup count on all the OSDs:
        log.debug("Proceeding to check dup counts post upgrade")
        if not verify_post_upgrade_dups(rados_obj=rados_obj, acting_sets=acting_sets):
            log.error("there are more than 3000 dups post upgrade on one or more OSDs")
            return 1
        log.debug("The dup count on all the OSDs is as expected.")
        log.info("Completed the workflow")
        return 0

    except Exception as err:
        log.error(f"Could not run the workflow Err: {err}")
        return 1

    finally:
        CONTINEOUS_IO = False
        # contineous_io_thread.join()


def upgrade_test_cluster(ceph_cluster, **kwargs) -> bool:
    """
    Performs upgrade of the test cluster
    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns: Pass -> True, Fail -> False
    """
    log.debug("Starting upgrade")
    try:
        cephadm = Orch(cluster=ceph_cluster, **kwargs)
        cephadm.set_tool_repo()
        # Install cephadm
        cephadm.install()

        # Check service versions vs available and target containers
        cephadm.upgrade_check(image=kwargs.get("container_image"))
        # work around for upgrading from 5.1 and 5.2 to 5.1 and 5.2 latest
        installer = ceph_cluster.get_nodes(role="installer")[0]
        base_cmd = "sudo cephadm shell -- ceph"
        ceph_version, err = installer.exec_command(cmd=f"{base_cmd} version")
        if ceph_version.startswith("ceph version 16.2."):
            installer.exec_command(
                cmd=f"{base_cmd} config set mgr mgr/cephadm/no_five_one_rgw true --force"
            )
            installer.exec_command(cmd=f"{base_cmd} orch upgrade stop")

        # Start Upgrade
        kwargs.update({"args": {"image": "latest"}})
        cephadm.start_upgrade(kwargs)

        # Monitor upgrade status, till completion
        cephadm.monitor_upgrade_status()
        log.info("Completed upgrade on the cluster")
        return True
    except Exception:
        log.error("Could not upgrade the cluster")
        return False


def verify_offline_trimming(rados_obj, osd, pgid, image) -> bool:
    """
    Tests if are able to use the offline trimming tool to trim the dup entries generated
    Args:
        rados_obj: Rados object to perform operations
        osd: osd ID on which to trim dups
        pgid: pgid where the dups have to be trimmed
        image: ceph image to be used for trimming

    Returns: Pass -> True, Fail -> False
    """
    host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd)
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    # Collecting the num of dups present on the PG - OSD
    method_should_succeed(
        run_cot_command,
        host=host,
        osd=osd,
        task="log",
        pgid=pgid,
        startosd=0,
        fsid=fsid,
    )

    path = f"/var/log/ceph/{fsid}/osd.{osd}/log-{pgid}.{osd}.log"
    dup_count_pre = get_dups_count(host=host, path=path)
    log.debug(
        f"Dups count on OSD : {osd} for PG : {pgid} before trimming is {dup_count_pre}"
    )

    # injecting dup entries
    method_should_succeed(
        run_cot_command,
        host=host,
        osd=osd,
        task="trim-pg-log-dups",
        pgid=pgid,
        startosd=0,
        image=image,
        fsid=fsid,
    )

    log.debug(f"Trimmed dups from OSD: {osd}, PG: {pgid}")

    # Collecting the num of dups present on the PG - OSD
    method_should_succeed(
        run_cot_command,
        host=host,
        osd=osd,
        task="log",
        pgid=pgid,
        startosd=1,
        fsid=fsid,
    )
    time.sleep(2)
    dup_count_post = get_dups_count(host=host, path=path)
    log.debug(
        f"Dups count on OSD : {osd} for PG : {pgid} post trimming is {dup_count_post}"
    )
    if dup_count_post > 3000:
        log.error(
            f"Dups not trimmed on osd {osd} for PG {pgid} on host {host.hostname}"
        )
        return False
    log.info(
        f"Dups trimmed on osd {osd} for PG {pgid} on host {host.hostname} Successfully!!!"
    )
    return True


def inject_dups(rados_obj, acting_sets, test_image) -> bool:
    """
    Injects duplicate entries into all the OSDs of the acting sets sent
        Args:
        rados_obj: Rados object to perform operations
        acting_sets: Dict of acting sets for the PG
            eg: {'8.0': [0, 5, 10], '9.0': [2, 6, 10]}
        test_image: Test image to be used to inject dups

    Returns: Pass -> True, Fail -> False

    """
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
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
                run_cot_command,
                host=host,
                osd=osd,
                task="log",
                pgid=pgid,
                startosd=0,
                fsid=fsid,
            )
            # checking the logs generated and fetch dups count
            path = f"/var/log/ceph/{fsid}/osd.{osd}/log-{pgid}.{osd}.log"
            dup_count_pre = get_dups_count(host=host, path=path)
            log.debug(f"Dups count on OSD : {osd} for PG : {pgid} is {dup_count_pre}")

            # injecting dup entries
            method_should_succeed(
                run_cot_command,
                host=host,
                osd=osd,
                task="pg-log-inject-dups",
                pgid=pgid,
                startosd=0,
                image=test_image,
                fsid=fsid,
            )

            # Collecting the num of dups present on the PG - OSD
            method_should_succeed(
                run_cot_command,
                host=host,
                osd=osd,
                task="log",
                pgid=pgid,
                startosd=1,
                fsid=fsid,
            )
            # Check the logs generated and fetch dups count, should be 100 more than previous
            dup_count_post = get_dups_count(host=host, path=path)
            log.debug(f"Dups count on OSD : {osd} for PG : {pgid} is {dup_count_post}")
            if not (dup_count_post - dup_count_pre == 100):
                log.error("Could not inject the 100 corrupt dups")
                return False
            log.info(
                f"Finished injecting corrupt dups into OSD : {osd} , part of pg : {pgid}\n"
            )
            rados_obj.change_osd_state(action="stop", target=osd)

        log.debug(f"Starting OSDs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        for osd in acting_sets[pgid]:
            rados_obj.change_osd_state(action="restart", target=osd)
        log.info(
            f"Completed injecting dups into all the OSDs for pg : {pgid}\n OSDs: {acting_sets[pgid]}\n"
        )
    log.info(f"Completed injecting dups into all the acting sets sent: {acting_sets}\n")
    return True


def inflate_dups(rados_obj, acting_sets, pools, test_image):
    """
    Method which waits till the desired levels of dup entries are present on the cluster
    Args:
        rados_obj: Rados object to perform operations
        acting_sets: Dict of acting sets for the PG
            eg: {'8.0': [0, 5, 10], '9.0': [2, 6, 10]}
        pools: test pool names created on cluster
        test_image: Test image to be used to inject/trim dups

    Returns: Pass -> New acting set after removing the offline trimmed OSD, Fail -> False

    """

    osds = []
    for pgid in acting_sets.keys():
        [osds.append(osd) for osd in acting_sets[pgid]]
    osds = set(osds)

    # inflate the dup count to desired levels
    cmd = "ceph config set osd osd_max_pg_log_entries 10"
    rados_obj.node.shell([cmd])
    pglog_items = {}
    trim_tested = False

    random_pgid = random.choice([pgid for pgid in acting_sets.keys()])
    random_osd = random.choice(acting_sets[random_pgid])
    acting_sets[random_pgid].remove(random_osd)

    # Total wait time of 4 hours
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=14400)
    while True:
        # Collecting the approx no of pglog objects from dump_mempools.
        # Let's have an approx of 5M objects across OSDs
        sum_pglog = 0
        for osd in osds:
            pglog_items[osd] = get_pglog_items(obj=rados_obj, osd=osd).get("items")
            log.debug(
                f"Number of pglog items collected from mempools :\n {osd} -> {pglog_items[osd]}"
            )
            sum_pglog += pglog_items[osd]

        if (sum_pglog / len(osds)) >= 5000000:
            log.info(
                f"Inflated the pglog average count to {sum_pglog / len(osds)} across OSDs : {osds}"
            )
            return acting_sets

        log.debug(
            f"pg_log items not filled to expected levels. average count : {sum_pglog / len(osds)}"
        )

        with parallel() as p:
            for pool in pools:
                p.spawn(rwrite_nosave, obj=rados_obj, dur=50, count=1, pool=pool)
                time.sleep(1)

        if (sum_pglog / len(osds)) >= 1000000 and not trim_tested:
            log.info(
                f"Inflated the pglog average count to {sum_pglog / len(osds)} across OSDs"
            )
            log.info("Testing the offline Trimming on one of the affected OSDs")
            trim_tested = True
            log.info(
                f"Picked PGID : {random_pgid} to trim the duplicates. OSD : {random_osd}"
            )
            rados_obj.node.shell(["ceph osd set noout"])
            time.sleep(2)
            if not verify_offline_trimming(
                rados_obj=rados_obj, osd=random_osd, pgid=random_pgid, image=test_image
            ):
                log.error("Failed to test offline trimming of PG dups")
                return False
            rados_obj.node.shell(["ceph osd unset noout"])
            log.info(
                f"Dups trimmed on osd {random_osd} for PG {random_pgid} Successfully!!!"
            )
        if not end_time > datetime.datetime.now():
            log.error("PG log entries not inflated enough even after 4 hours of IOs")
            return False


def get_dups_count(host, path) -> int:
    """
    Gets the count of dups in the PG Log
    Args:
        host: host object where the OSD daemon runs
        path: Path of the file

    Returns: Dups length

    """
    dups_file = host.remote_file(sudo=True, file_name=path, file_mode="r")
    dups = json.loads(dups_file.read())
    return len(dups["pg_log_t"]["dups"])


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
            fsid: fsid of the cluster

    Returns: Pass -> True, Fail -> False

    """
    host = kwargs["host"]
    osd = kwargs["osd"]
    task = kwargs["task"]
    pgid = kwargs["pgid"]
    image = kwargs.get("image")
    startosd = kwargs.get("startosd", 1)
    fsid = kwargs.get("fsid")

    cmd_options = f"-o {osd} -p {pgid} -t {task} -s {startosd} -f {fsid}"
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
    for i in range(count):
        cmd = f"sudo rados --no-log-to-stderr -b 2Kb -p {pool} bench {dur} write"
        try:
            log.info(f"running the bench for {i}-th time")
            obj.node.shell([cmd])
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool}, \n\n {err}")
            return False
    return True


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


def verify_post_upgrade_dups(rados_obj, acting_sets) -> bool:
    """
    Checks the dups count on all the OSDs post upgrade.
        Args:
        rados_obj: Rados object to perform operations
        acting_sets: Dict of acting sets for the PG
            eg: {'8.0': [0, 5, 10], '9.0': [2, 6, 10]}

    Returns: Pass -> True, Fail -> False

    """
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    rados_obj.node.shell(["ceph osd set noout"])
    # Proceeding to check dups count on each OSD
    for pgid in acting_sets.keys():
        log.debug(f"Checking OSDs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        for osd in acting_sets[pgid]:
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd)
            # Collecting the num of dups present on the PG - OSD
            method_should_succeed(
                run_cot_command,
                host=host,
                osd=osd,
                task="log",
                pgid=pgid,
                fsid=fsid,
            )

            path = f"/var/log/ceph/{fsid}/osd.{osd}/log-{pgid}.{osd}.log"
            dup_count = get_dups_count(host=host, path=path)
            log.debug(
                f"Dups count on OSD : {osd} for PG : {pgid} after upgrade is {dup_count}"
            )
            if dup_count > 3000:
                log.error(
                    f"Dups not trimmed on osd {osd} for PG {pgid} on host {host.hostname}"
                )
                return False
            log.info(
                f"Dups trimmed on osd {osd} for PG {pgid} on host {host.hostname} Successfully!!!"
            )
        log.debug(f"Completed checking for all the OSDs of PG : {pgid}")
    rados_obj.node.shell(["ceph osd unset noout"])
    log.debug(f"Completed checking for all the OSDs for all the PGs {acting_sets}")
    return True


def verify_trim_dups_warn_log(
    rados_obj: RadosOrchestrator, acting_sets, start_time, end_time
) -> bool:
    """
    Retrieve slow op requests log using journalctl command and check if the log warning has been generated on OSDs

    2022-07-19T06:17:18.974+0000 7faeabe3b200  0 read_log_and_missing WARN num of dups exceeded 6000.
    You can be hit by THE DUPS BUG https://tracker.ceph.com/issues/53729.
    Consider ceph-objectstore-tool --op trim-pg-log-dups

    Args:
        rados_obj: ceph node details
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        acting_sets: Dict of acting sets for the PG
            eg: {'8.0': [0, 5, 10], '9.0': [2, 6, 10]}
    Returns:  Pass -> True, Fail -> False
    """
    log.debug("Checking if the warning message is generated in OSD logs post upgrade")
    for pgid in acting_sets.keys():
        log.debug(f"Checking OSD logs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        for osd in acting_sets[pgid]:
            log_lines = rados_obj.get_journalctl_log(
                start_time=start_time,
                end_time=end_time,
                daemon_type="osd",
                daemon_id=osd,
            )
            line = "read_log_and_missing WARN num of dups exceeded 6000"
            if line not in log_lines:
                log.error(
                    f" did not find relevant logging on PG : {pgid} - OSD : {osd}"
                )
                return False
            log.debug(f"Found relevant logging on PG : {pgid} - OSD : {osd}")
        log.debug(f"Completed verification on PG : {pgid}")
    log.info("Completed log verification on all the OSDs sent")
    return True


def get_osd_memory_usages(rados_obj: RadosOrchestrator):
    """
    Iterates through all the nodes with OSDs and collects the virtual and RES momories consumed by them
    Args:
        rados_obj: rados cluster object to perform operations

    Returns: dict with memory usage

    """
    regex = r"\s*ceph\s+([\da-z.]*)\s+([\da-z.]*)\s+[/a-z-.]*\s+osd.(\d{1,2})"
    cmd = "top -bc -u ceph -d 10 -n 1 -w512 | awk '{ print $2, $5, $6, $12, $14 }'"
    osd_hosts = rados_obj.ceph_cluster.get_nodes(role="osd")
    memory_use = {}
    for host in osd_hosts:
        log.debug(
            f"Fetching osd memory utilization details from host : {host.hostname}"
        )
        try:
            out, _ = host.exec_command(sudo=True, cmd=cmd)
            usages = re.findall(regex, out)
            for usage in usages:
                memory_use[int(usage[2])] = {
                    "virtual": get_in_gb(usage[0]),
                    "RES": get_in_gb(usage[1]),
                }
        except Exception as err:
            log.error(
                f"Failed to collect the memory usage on host : {host.hostname}\n error: {err}"
            )
    log.debug(f"Completed collecting all the memory usages for OSDs: {memory_use}")
    return memory_use


def get_in_gb(usage) -> float:
    if "g" in usage:
        return float(usage.replace("g", ""))
    else:
        return float(int(usage) / (1024 * 1024))
