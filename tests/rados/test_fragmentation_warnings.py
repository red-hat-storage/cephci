"""
Module to verify fragmentation warnings on the cluster added with 8.1
"""

import datetime
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83607852
    Covers:
        - Feature bug : https://bugzilla.redhat.com/show_bug.cgi?id=2350214
    Test to verify if fragmentation warning is generated on the OSDs
    1. Deploy a ceph cluster
    2. Check the default values of the new params added
    3. Modify the params and write fragmented Objects on OSDs.
    4. Check if the health warning has been generated.
    5. Update the params, check if the warning is removed.
    6. increase the config set again to generated health warning
    7. Check if call home is generated and call home has the warning generated
    8. Check if logging is happening correctly for the OSDs at correct time intervals
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_name = config.get("pool_name", "test_frag_warn_pool")
    pool_name_1 = config.get("pool_name_1", "test_frag_warn_pool_1")

    # feature introduced in 8.1. Returning pass if running on 8.1 or less
    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) >= 8.0:
        log.info(
            "Test running on version less than 8.0, skipping verifying fragmentation scores warning"
        )
        return 0

    log.info(
        "Running test case to verify if highly fragmented OSDs generate health warning on the cluster"
    )

    # logging any existing crashes on the cluster
    crash_list = rados_obj.do_crash_ls()
    if crash_list:
        log.warning(
            "!!!ERROR: Crash exists on the cluster at the start of the test \n\n:"
            f"{crash_list}"
        )

    log.debug("Creating test pool for the tests")
    for pool in [pool_name, pool_name_1]:
        if not rados_obj.create_pool(pool_name=pool, bulk=True):
            log.error("Pool for testing could not be created")
            return 1

    log.debug("Enabling logging on file for the test case.")
    rados_obj.enable_file_logging()
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        # Scenario 1 : Checking the defaults for
        frag_percent_on_cluster = float(
            mon_obj.get_config(
                section="osd", param="bluestore_warn_on_free_fragmentation"
            )
        )
        if frag_percent_on_cluster != 0.8:
            log.error(
                "The default value for bluestore_warn_on_free_fragmentation does not match the default"
                "\nDefault : 0.8 \n"
                "\n On cluster : %s \n",
                frag_percent_on_cluster,
            )
            raise Exception("Default value mismatch error")

        frag_check_peirod_on_cluster = int(
            mon_obj.get_config(
                section="osd", param="bluestore_fragmentation_check_period"
            )
        )
        if frag_check_peirod_on_cluster != 3600:
            log.error(
                "The default value for bluestore_fragmentation_check_period does not match the default"
                "\nDefault : 3600 \n"
                "\n On cluster : %s \n",
                frag_check_peirod_on_cluster,
            )
            raise Exception("Default value mismatch error")

        log.info(
            "Verified the default values for the fragmentation warning params introduced"
        )
        cluster_osds = rados_obj.get_osd_list(status="UP")
        init_frag_scores = {
            osd_id: rados_obj.get_fragmentation_score(osd_id=osd_id)
            for osd_id in cluster_osds
        }
        max_frag_osd = max(init_frag_scores, key=init_frag_scores.get)
        max_frag_score = init_frag_scores[max_frag_osd]
        max_frag_value_chosen = round(max(max_frag_score * 1.1, 0.010), 3)
        log.info(
            "max fragmentation on cluster : %s. Selecting %s as the required"
            "frag percent to generate warnings",
            max_frag_score,
            max_frag_value_chosen,
        )

        for osd_id, score in init_frag_scores.items():
            log.debug("Initial fragmentation score on OSD %s: %s", osd_id, score)

        # Scenario 2: Updating the configs to lower levels and checking if health warning on cluster is generated
        mon_obj.set_config(
            section="osd",
            name="bluestore_warn_on_free_fragmentation",
            value=max_frag_value_chosen,
        )
        mon_obj.set_config(
            section="osd", name="bluestore_fragmentation_check_period", value="60"
        )
        log.debug(
            "Config values set : \n" "bluestore_warn_on_free_fragmentation : %s",
            max_frag_value_chosen,
        )

        if not rados_obj.create_fragmented_osds_on_pool(
            pool=pool_name, required_fragmentation=max_frag_value_chosen
        ):
            log.error(
                "Could not generate fragmented OSDs health warning on the cluster"
            )
            raise Exception("Could not fragment OSDs")

        log.info(
            "Completed creation of Fragmented OSDs on cluster."
            " Proceeding to check if health warning is generated for the same"
        )

        if not rados_obj.check_health_warning(warning="BLUESTORE_FREE_FRAGMENTATION"):
            log.error("Health warning for fragmentation not generated")
            raise Exception("fragmentation health warn not generated")
        log.info("Health warning generated on the cluster for high fragmentation")
        rados_obj.log_cluster_health()

        # Scenario 3: Checking if updating the fragmentation warning threshold, and see if the warnings are gone.

        cluster_osds = rados_obj.get_osd_list(status="UP")
        init_frag_scores = {
            osd_id: rados_obj.get_fragmentation_score(osd_id=osd_id)
            for osd_id in cluster_osds
        }
        max_frag_osd = max(init_frag_scores, key=init_frag_scores.get)
        max_frag_score = init_frag_scores[max_frag_osd]
        max_frag_value_chosen = round(max(max_frag_score * 1.1, 0.020), 3)

        log.info(
            "max fragmentation on cluster : %s. Selecting %s as the required"
            "frag percent to generate warnings",
            max_frag_score,
            max_frag_value_chosen,
        )

        mon_obj.set_config(
            section="osd",
            name="bluestore_warn_on_free_fragmentation",
            value=max_frag_value_chosen,
        )
        log.debug(
            "Config values set : \n" "bluestore_warn_on_free_fragmentation : %s",
            max_frag_value_chosen,
        )

        # Check time for fragmentation level is set at 60 seconds. sleeping for 120 seconds for at-least 1
        # check to have completed on the cluster
        time.sleep(120)

        if rados_obj.check_health_warning(warning="BLUESTORE_FREE_FRAGMENTATION"):
            log.error(
                "Health warning for fragmentation not removed post updating the "
                "bluestore_warn_on_free_fragmentation param on cluster"
            )
            raise Exception("fragmentation health warn not generated")
        log.info("Health warning generated on the cluster for high fragmentation")
        rados_obj.log_cluster_health()

        log.info(
            "Increasing the fragmentation again to check if health warn gets generated again"
        )

        if not rados_obj.create_fragmented_osds_on_pool(
            pool=pool_name_1, required_fragmentation=max_frag_value_chosen
        ):
            log.error(
                "Could not generate fragmented OSDs health warning on the cluster"
            )
            raise Exception("Could not fragment OSDs")

        log.info(
            "Completed creation of Fragmented OSDs on cluster."
            " Proceeding to check if health warning is generated for the same"
        )

        if not rados_obj.check_health_warning(warning="BLUESTORE_FREE_FRAGMENTATION"):
            log.error("Health warning for fragmentation not generated")
            raise Exception("fragmentation health warn not generated")
        log.info("Health warning generated on the cluster for high fragmentation")
        rados_obj.log_cluster_health()

        # Scenario 4: Checking OSD and ceph logs for the warning generated
        """
        OSD logs generated :\n
        Mar 25 14:29:21 ceph-pdhiran-qb3m0z-node2 ceph-osd[24436]:\n
            bluestore.MempoolThread fragmentation_score=0.056921 took=0.002537s
        Mar 25 14:31:21 ceph-pdhiran-qb3m0z-node2 ceph-osd[24436]:\n
            bluestore.MempoolThread fragmentation_score=0.056921 took=0.002666s
        """

        def process_osd_logs(log_data: str):
            pattern = re.compile(
                r"([A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2}) .*?"
                r" bluestore\.MempoolThread fragmentation_score=.*? took=.*?s"
            )

            matches = pattern.findall(log_data)
            if not matches:
                return None

            # Convert timestamps to datetime objects
            timestamps = [
                datetime.datetime.strptime(match, "%b %d %H:%M:%S") for match in matches
            ]

            # Compute time differences
            time_diffs = [
                (timestamps[i] - timestamps[i - 1]).total_seconds()
                for i in range(1, len(timestamps))
            ]

            return {"count": len(matches), "time_differences": time_diffs}

        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        count = 1
        for osd_id in acting_pg_set:
            frag_check_dur = 60 * count
            mon_obj.set_config(
                section="osd",
                name="bluestore_fragmentation_check_period",
                value=frag_check_dur,
            )
            osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
            time_1, _ = osd_host.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")

            log_lines = f"""
            Checking if the checks are occurring and are logged in OSD logs
            and are occurring according to the time set for checks.
            Setting time such that we at least get 4 checks generated.
            This is {count} round of test. the duration is updated to {frag_check_dur}
            Test running on :
            OSD : {osd_id}\n
            Host : {osd_host.hostname}
            """
            count += 1
            log.info(log_lines)

            time.sleep(60 * 5 * count)
            time_2, _ = osd_host.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
            log_lines = rados_obj.get_journalctl_log(
                start_time=time_1,
                end_time=time_2,
                daemon_type="osd",
                daemon_id=osd_id,
            )
            log.debug(f"\n\nJournalctl logs : {log_lines}\n\n")
            report = process_osd_logs(log_data=log_lines)
            if not report:
                log.error(
                    "No log lines related to fragmentation checks on OSD found for osd : %s",
                    osd_id,
                )
                raise Exception("log lines not generated")
            log_lines = (
                f"Found logging for fragmentation checks on the OSD : {osd_id}\n"
                f"Entries found : {report['count']}\n"
                f"Logging interval : {', '.join(map(str, report['time_differences']))}"
            )
            log.info(log_lines)

            # Performing checks on the results
            if report["count"] < 4:
                log.error(
                    "log lines related to fragmentation checks generated less times "
                    "than it should have in the time duration for osd : %s",
                    osd_id,
                )
                raise Exception("log lines not generated correctly")
            for entry in report["time_differences"]:
                if not (frag_check_dur - 3 <= int(entry) <= frag_check_dur + 3):
                    log.error(
                        "log lines related to fragmentation checks generated not generated in accordance of"
                        "the time duration for osd : %s",
                        osd_id,
                    )
                    raise Exception("log lines not generated correctly")
            log.info("Log lines generated correctly for OSD : %s", osd_id)

        log.info("Completed verification of log lines on 3 OSDs" "Scenario Complete.")
        rados_obj.log_cluster_health()

        # Scenario 5: Checking if Call home has the warning. IBM only
        enabled_modules = rados_obj.run_ceph_command("ceph mgr module ls")[
            "enabled_modules"
        ]
        disabled_modules_list = rados_obj.run_ceph_command("ceph mgr module ls")[
            "disabled_modules"
        ]
        disabled_modules = [item["name"] for item in disabled_modules_list]
        if "call_home_agent" in (enabled_modules + disabled_modules):
            log.info(
                "call_home_agent module found on module list"
                "Running tests on IBM builds"
            )
            if "call_home_agent" in enabled_modules:
                log.debug("Call home module enabled, proceeding with checks")
            else:
                cmd = "ceph mgr module enable call_home_agent"
                rados_obj.run_ceph_command(cmd=cmd)
                time.sleep(5)

            # Generating call home report and sending call home report and validating the o/p
            cmd = "ceph callhome show status"
            ch_data = rados_obj.run_ceph_command(cmd=cmd)
            if not ch_data:
                log.error("Call home status could not be collected")
                raise Exception("call home status could not be collected")

            health_wanrs = ch_data["events"][0]["body"]["payload"]["content"]["status"][
                "health_detail"
            ]["checks"]
            key = "BLUESTORE_FREE_FRAGMENTATION"
            if key not in health_wanrs.keys():
                log.error(
                    "Call home status generated is old, from before the test started"
                )
                raise Exception("call home status is old. ")
            log.info("Warning details present in the call home status")

        else:
            log.info(
                "call_home_agent module not found on module list"
                "Running test on RH builds. Not running the call home test workflow"
            )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(
            section="osd", name="bluestore_warn_on_free_fragmentation"
        )
        mon_obj.remove_config(
            section="osd", name="bluestore_fragmentation_check_period"
        )

        # Pool cleanup
        rados_obj.rados_pool_cleanup()

        time.sleep(10)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Fragmentation health warnings verified successfully")
    return 0
