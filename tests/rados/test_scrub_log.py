"""
Module to Verify if PG scrub & deep-scrub messages are logged into the OSD logs
"""

import re
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    1.Test to verify if adequate logging is generated in the OSD logs upon initiating scrub & deep-scrubs
    2. Verification of the logs if the mon_cluster_log_level values are warn and debug.More details are
       updated at - https://bugzilla.redhat.com/show_bug.cgi?id=2053021
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_configs = config["pool_configs"]
    pool_configs_path = config["pool_configs_path"]
    verify_log_check = config["verify_log"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    debug_increased_file_size = ""
    warn_increased_file_size = ""
    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) >= 5.3:
        log.info(
            "Test running on version less than 5.3, feature not present, skipping test"
        )
        return 0
    parm_out_put_check = None
    # Check that paramters are removed from the build
    if verify_log_check:
        try:
            parm_out_put_check = mon_obj.get_config(
                section="mon", param="mon_cluster_log_file_level"
            )
        except Exception as e:
            log.error(
                f"An unexpected error occurred while getting the mon_cluster_log_file_level value: {e}"
            )
        if parm_out_put_check is not None:
            log.error(
                f"The mon_cluster_log_file_level parameter not deleted and the value is {parm_out_put_check}"
            )
        try:
            parm_out_put_check = mon_obj.get_config(
                section="mon", param="mon_cluster_log_to_syslog_level"
            )
        except Exception as e:
            log.error(
                f"An unexpected error occurred getting the mon_cluster_log_to_syslog_level value:  {e}"
            )
        if parm_out_put_check is not None:
            log.error(
                f"The mon_cluster_log_to_syslog_level parameter not deleted and the value is {parm_out_put_check}"
            )
            return 1
        try:
            parm_out_put_check = mon_obj.get_config(
                section="mon", param="mon_cluster_log_level"
            )
        except Exception as e:
            log.error(
                f"An unexpected error occurred while getting the mon_cluster_log_level value: {e}"
            )
        parm_out_put_check = parm_out_put_check.strip()
        if parm_out_put_check != "debug":
            log.error(
                f"The mon_cluster_log_level default expected value should be debug."
                f"The actual value is {parm_out_put_check}"
            )
            return 1
    try:
        for log_level in ["debug", "warn"]:
            log.info(
                f"====START:Testing the scenario with the mon_cluster_log_level value {log_level} ======="
            )
            # enable the file logging
            if not rados_obj.enable_file_logging():
                log.error("Error while setting config to enable logging into file")
                return 1
            # Get the ceph log size before starting the test
            before_test_log_size = get_file_size(rados_obj, installer)
            log.info(
                f"Before testing with {log_level} level the ceph log size is {before_test_log_size} bytes"
            )
            if log_level == "warn":
                if not mon_obj.set_config(
                    section="mon", name="mon_cluster_log_level", value="warn"
                ):
                    log.error("Error setting config ")
                    return 1
                log.info("The mon_cluster_log_level param value is set to warn")

            with open(pool_configs_path, "r") as fd:
                pool_conf_file = yaml.safe_load(fd)

            pools = []
            acting_sets = {}
            for i in pool_configs:
                pool = pool_conf_file[i["type"]][i["conf"]]
                create_given_pool(rados_obj, pool)
                rados_obj.bench_write(
                    pool_name=pool["pool_name"],
                    max_objs=50,
                    check_ec=False,
                    rados_write_duration=20,
                )
                pools.append(pool["pool_name"])

            log.info(f"Created {len(pools)} pools for testing. pools : {pools}")

            # Identifying 1 PG from each pool to initiate scrubbing
            log.debug(
                "Checking the states of PGs before scrub/deep-scrub and selecting a PG which is clean"
            )
            for pool in pools:
                pool_pgids = rados_obj.get_pgid(pool_name=pool)
                for pgid in pool_pgids:
                    log.debug(f"Checking state of PG: {pgid}")
                    pg_report = rados_obj.check_pg_state(pgid=pgid)
                    if "active+clean" in pg_report:
                        pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                        acting_sets[pgid] = pg_set
                        log.debug(f"PG selected for pool : {pool}")
                        break
                    log.debug(
                        f"pg {pgid} not in clean state, Current PG status : {pg_report}"
                        f" Checking another PG to initiate scrubbing tests"
                    )

            # Completed selecting the PGs
            log.info(f"Identified Acting set of OSDs for the Pools. {acting_sets}")

            log.debug(
                "initiating Scrubbing on PGs and checking if logs are generated in OSD"
            )
            # Testing logs upon scrub operation
            if not check_scrub_workflow(
                installer=installer,
                rados_obj=rados_obj,
                acting_sets=acting_sets,
                task="scrub",
            ):
                log.error("Could not verify logging for scrubs")
                return 1

            time.sleep(2)
            log.info("Verified the presence of logging for scrub")
            # Testing logs upon deep-scrub operation
            if not check_scrub_workflow(
                installer=installer,
                rados_obj=rados_obj,
                acting_sets=acting_sets,
                task="deep-scrub",
            ):
                log.error("Could not verify logging for deep-scrubs")
                return 1
            if verify_log_check:
                log.info(
                    f"Verification of the scrub messages with the mon_cluster_log_level value  {log_level}.The "
                    f"expected behaviour is, the scrub messages should generate if the mon_cluster_log_level value is "
                    f"debug and should not get generate during if the value is warn"
                )
                # Customer scenario verification based on the flag check

                mon_scrub_status = check_scrub_workflow(
                    installer=installer,
                    rados_obj=rados_obj,
                    acting_sets=acting_sets,
                    task="scrub",
                    mon_check=True,
                )
                if log_level == "debug" and not mon_scrub_status:
                    log.error(
                        f"The mon_cluster_log_level is {log_level} and could not found the logging for scrubs"
                    )
                    return 1
                elif log_level == "warn" and mon_scrub_status:
                    log.error(
                        f"The mon_cluster_log_level is {log_level} and found the logging for scrubs"
                    )
                    return 1
                log.info(
                    f"Verification of scrub messages with the mon_cluster_log_level value  {log_level} is completed"
                )
                log.info(
                    f"Verification of the deep-scrub messages with the mon_cluster_log_level value  {log_level} "
                )
                deep_scrub_status = check_scrub_workflow(
                    installer=installer,
                    rados_obj=rados_obj,
                    acting_sets=acting_sets,
                    task="deep-scrub",
                    mon_check=True,
                )
                if log_level == "debug" and not deep_scrub_status:
                    log.error(
                        f"The mon_cluster_log_level is {log_level} and could not found the logging for deep-scrubs"
                    )
                    return 1
                elif log_level == "warn" and deep_scrub_status:
                    log.error(
                        f"The mon_cluster_log_level is {log_level} and found the logging for deep-scrubs"
                    )
                    return 1
                log.info(
                    f"Verification of deep-scrub messages with the mon_cluster_log_level "
                    f"value  {log_level} is completed"
                )
                after_test_log_size = get_file_size(rados_obj, installer)
                log.info(
                    f"After testing with {log_level} level the ceph log size is {after_test_log_size} bytes"
                )
                increased_bytes = after_test_log_size - before_test_log_size
                if log_level == "debug":
                    debug_increased_file_size = increased_bytes
                else:
                    warn_increased_file_size = increased_bytes
                # Deleting the pools to execute the next iteration
                for pool in pools:
                    rados_obj.delete_pool(pool=pool)
                log.info(
                    f"====END:Testing the scenario completed with the mon_cluster_log_level value {log_level} ======="
                )
            else:
                log.info(
                    "Test execution is not required with debug and warn parameter values.Skipping the next iteration"
                )
                break

        if verify_log_check:
            if debug_increased_file_size < warn_increased_file_size:
                log.error(
                    f"The ceph log increased more during the mon_cluster_log_level value is warn. The file "
                    f"increased during the debug is {debug_increased_file_size} and during the"
                    f" warn is {warn_increased_file_size}"
                )
                return 1
            log.info(
                f"The ceph log increased more during the mon_cluster_log_level value is debug. The file "
                f"increased during the debug is {debug_increased_file_size} bytes and during the"
                f" warn is {warn_increased_file_size} bytes"
            )
        log.info("Completed verification of logging upon scrubs")
        return 0

    except Exception as err:
        log.error(f"Could not run the workflow Err: {err}")
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(section="mon", name="mon_cluster_log_level")
        for pool in pools:
            rados_obj.delete_pool(pool=pool)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1


def check_scrub_workflow(
    installer, rados_obj, acting_sets, task, mon_check=False
) -> bool:
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
        """
        updated log lines for EC pool in OSD logs.
        Jul 30 04:53:01 ceph-pdhiran-vpbopt-node3 ceph-osd[18769]: log_channel(cluster) log [DBG] : 9.0s0 scrub starts
        Jul 30 04:53:01 ceph-pdhiran-vpbopt-node3 ceph-osd[18769]: log_channel(cluster) log [DBG] : 9.0 scrub ok
        """

        log.debug(f"Checking OSD logs of PG: {pgid}. OSDs : {acting_sets[pgid]}")
        init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.debug(f"Initial time when {task} was started : {init_time}")

        if task == "scrub":
            log.debug(f"Running scrub on pg : {pgid}")
            if not rados_obj.start_check_scrub_complete(pg_id=pgid):
                log.error(f"Could not complete scrub on pg : {pgid}")
                return False
            log_lines = [rf"{pgid}[\S\s]*scrub starts", rf"{pgid}[\S\s]*scrub ok"]

        elif task == "deep-scrub":
            if not rados_obj.start_check_deep_scrub_complete(pg_id=pgid):
                log.error(f"Could not complete deep-scrub on pg : {pgid}")
                return False
            log_lines = [
                rf"{pgid}[\S\s]*deep-scrub starts",
                rf"{pgid}[\S\s]*deep-scrub ok",
            ]

        time.sleep(10)
        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.debug(
            f"Checking {task} logging for OSD : {acting_sets[pgid][0]} in PG : {pgid}"
        )
        if mon_check:
            if not verify_mon_scrub_log(
                rados_obj=rados_obj,
                start_time=init_time,
                end_time=end_time,
                lines=log_lines,
            ):
                log.error(
                    f"Could not find the log lines for {task} on PG : {pgid} on monitor"
                )
                return False
        else:
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
    log.debug(f"\n\nJournalctl logs : {log_lines}\n\n")
    for line in lines:
        if not re.search(line, log_lines):
            log.error(f" did not find logging on OSD : {osd}")
            log.error(f"Journalctl logs lines: {log_lines}")
            log.error(f"expected logs lines: {lines}")
            return False
    log.info(f"Found relevant scrub/deep-scrub logging on OSD : {osd}")
    return True


def verify_mon_scrub_log(
    rados_obj: RadosOrchestrator, start_time, end_time, lines
) -> bool:
    """
    Retrieve mon log using journalctl command and check if the log for scrubs & deep-scrubs have been generated on mon
    Args:
        rados_obj: ceph node details
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        lines: log line that needs to be matched
               Log lines for the scrub are-
                                    <pg_id>  scrub starts
                                     <pg_id> scrub ok
               Log lines for the deep-scrub are-
                                    <pg_id>  deep-scrub starts
                                     <pg_id> deep-scrub ok
    Returns:  Pass -> True, Fail -> False
    """
    log.info("Checking if the scrub/deep-scrub log messages are generated in the Mon")
    mon_daemon = rados_obj.run_ceph_command(cmd="ceph mon stat")["leader"]
    # checking the ceph logs
    log_lines = rados_obj.get_journalctl_log(
        start_time=start_time,
        end_time=end_time,
        daemon_type="mon",
        daemon_id=mon_daemon,
    )
    log.debug(f"\n\nMonitor Journalctl logs : {log_lines}\n\n")
    for line in lines:
        if not re.search(line, log_lines):
            log.error(f" did not find logging on mon : {mon_daemon}")
            log.error(f"Journalctl logs lines: {log_lines}")
            log.error(f"expected logs lines: {lines}")
            return False
    log.info(f"Found relevant scrub/deep-scrub logging on OSD : {mon_daemon}")
    return True


def get_file_size(rados_object, installer_node):
    """
    Method is used to get the ceph.log file size in bytes
    Args:
        rados_object : Rados object
        installer_node : installer node object
    Return:
        size: size of file in bytes
    """
    fsid = rados_object.run_ceph_command(cmd="ceph fsid")["fsid"]
    file_path = f"/var/log/ceph/{fsid}/ceph.log"
    cmd_get_filesize = f"stat -c '%s' {file_path}"
    size, _ = installer_node.exec_command(sudo=True, cmd=cmd_get_filesize)
    size = int(size.strip())
    return size
