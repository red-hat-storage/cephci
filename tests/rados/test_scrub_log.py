"""
Module to Verify if PG scrub & deep-scrub messages are logged into the OSD logs
"""
import datetime
import re
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Test to verify if adequate logging is generated in the OSD logs upon initiating scrub & deep-scrubs
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
    installer = ceph_cluster.get_nodes(role="installer")[0]

    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) >= 5.3:
        log.info(
            "Test running on version less than 5.3, feature not present, skipping test"
        )
        return 0

    try:
        with open(pool_configs_path, "r") as fd:
            pool_conf_file = yaml.safe_load(fd)

        # Disabling recovery, back-fill on the cluster
        rados_obj.change_recovery_flags(action="set")

        pools = []
        acting_sets = {}
        for i in pool_configs:
            pool = pool_conf_file[i["type"]][i["conf"]]
            create_given_pool(rados_obj, pool)
            pools.append(pool["pool_name"])

        log.info(f"Created {len(pools)} pools for testing. pools : {pools}")

        # Identifying 1 PG from each pool to initiate scrubbing
        for pool in pools:
            pool_id = pool_obj.get_pool_id(pool_name=pool)
            pgid = f"{pool_id}.0"
            pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
            acting_sets[pgid] = pg_set
        log.info(f"Identified Acting set of OSDs for the Pools. {acting_sets}")

        log.debug(
            "initiating Scrubbing on PGs and checking if logs are generated in OSD"
        )
        log.debug("Checking the states of PGs before scrub")
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=1200)
        while end_time > datetime.datetime.now():
            for pgid in acting_sets.keys():
                log.debug(
                    f"Checking OSD logs of PG: {pgid}. OSDs : {acting_sets[pgid]}"
                )
                pg_report = rados_obj.check_pg_state(pgid=pgid)
                if "active+clean" in pg_report:
                    break
                log.debug(f"pg {pgid} not in clean state, waiting for 10 seconds")
                time.sleep(10)

        # Testing logs upon scrub operation
        if not check_scrub_workflow(
            installer=installer,
            rados_obj=rados_obj,
            acting_sets=acting_sets,
            task="scrub",
        ):
            log.error("Could not verify logging for scrubs")
            return 1

        # Testing logs upon deep-scrub operation
        if not check_scrub_workflow(
            installer=installer,
            rados_obj=rados_obj,
            acting_sets=acting_sets,
            task="deep-scrub",
        ):
            log.error("Could not verify logging for deep-scrubs")
            return 1

        log.info("Completed verification of logging upon scrubs")
        # Disabling recovery, back-fill on the cluster
        rados_obj.change_recovery_flags(action="unset")
        return 0

    except Exception as err:
        log.error(f"Could not run the workflow Err: {err}")
        return 1


def check_scrub_workflow(installer, rados_obj, acting_sets, task) -> bool:
    """
    receives the task to be done, collects the current time and fetches the logs
    Args:
        installer: Installer node object
        rados_obj: RadosOrchestrator object for general functionality
        acting_sets: acting sets of pools
         eg: {'8.0': [0, 5, 10], '9.0': [2, 6, 10]}
        task: action to be performed, either scrub or deep-scrub

    Returns:  Pass -> True, Fail -> false

    """
    log.debug(f"Started to verify logging upon {task}")
    for pgid in acting_sets.keys():
        log.debug(f"Checking OSD logs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")

        if task == "scrub":
            rados_obj.run_scrub(pgid=pgid)
            log_lines = [f"{pgid} scrub starts", f"{pgid} scrub ok"]
        elif task == "deep-scrub":
            rados_obj.run_deep_scrub(pgid=pgid)
            log_lines = [
                f"{pgid} deep-scrub starts",
                f"{pgid} deep-scrub ok",
            ]
        # sleeping for 10 seconds, waiting for scrubbing to be over
        time.sleep(10)

        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.debug(
            f"Checking {task} logging for OSD : {acting_sets[pgid][0]} in PG : {pgid}"
        )
        if not verify_scrub_log(
            rados_obj=rados_obj,
            osd=acting_sets[pgid][0],
            start_time=init_time,
            end_time=end_time,
            lines=log_lines,
        ):
            log.error(
                f"Could not find the log lines for {task} on PG : {pgid} - OSD : {acting_sets[pgid][0]}"
            )
            return False
        log.info(f"Completed verification of scrub logs for PG : {pgid}")
    log.info(
        f"Completed verification of {task} logs for all the pgs selected : {acting_sets}"
    )
    return True


def verify_scrub_log(
    rados_obj: RadosOrchestrator, osd, start_time, end_time, lines
) -> bool:
    """
    Retrieve osd log using journalctl command and check if the log for scrubs & deep-scrubs have been generated on OSDs

    log_channel(cluster) log [DBG] : 1.0 scrub starts
    log_channel(cluster) log [DBG] : 1.0 scrub ok
    log_channel(cluster) log [DBG] : 1.0 deep-scrub starts
    log_channel(cluster) log [DBG] : 1.0 deep-scrub ok

    Args:
        rados_obj: ceph node details
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        osd: osd ID on which logs need to be checked
        lines: log line that needs to be matched
    Returns:  Pass -> True, Fail -> False
    """
    log.info("Checking if the scrub/deep-scrub log messages are generated in the OSD")
    log_lines = rados_obj.get_journalctl_log(
        start_time=start_time,
        end_time=end_time,
        daemon_type="osd",
        daemon_id=osd,
    )
    log.debug(f"Journalctl logs : {log_lines}")
    for line in lines:
        if line not in log_lines:
            log.error(f" did not find logging on OSD : {osd}")
            log.error(f"Journalctl logs lines: {log_lines}")
            log.error(f"expected logs lines: {lines}")
            return False
    log.info(f"Found relevant scrub/deep-scrub logging on OSD : {osd}")
    return True
