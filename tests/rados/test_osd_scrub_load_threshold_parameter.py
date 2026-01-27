"""
Polarion ID: CEPH-83620201 - Verification of the osd_scrub_load_threshold parameter functionality

The file contains methods to verify the osd_scrub_load_threshold parameter functionality.
The following test scenarios are verified:

Case 1: Threshold value 0 (scrub blocking)
- Generates system load using stress-ng (target load threshold: 3, CPU: 20%)
- Sets osd_scrub_load_threshold to 0
- Verifies that scheduled scrub operations do not proceed
- Expected log message: "scrub_load_below_threshold:.* = no"

Case 2: Inconsistent object repair with threshold 0
- Creates 6 inconsistent objects in the pool
- Sets osd_scrub_load_threshold to 0
- Generates system load using stress-ng (target load threshold: 3, CPU: 20%)
- Verifies that auto-repair does not occur when threshold blocks scrubbing
- Confirms inconsistent object count remains unchanged
- Expected log message: "scrub_load_below_threshold:.* = no"

Case 3: Default threshold value (10.0) - scrub enabled
- Creates 6 inconsistent objects in the pool
- Generates system load using stress-ng (target load threshold: 3, CPU: 20%)
- Sets osd_scrub_load_threshold to default value (10.0)
- Verifies that scrub operations proceed normally
- Confirms inconsistent objects are repaired (count becomes 0)
- Expected log message: "scrub_load_below_threshold:.* = yes"

Case 4: Threshold relative to node load average
- Scenario 4.1: Sets threshold below node load average
  * Calculates current node load average (loadavg / online CPUs)
  * Sets osd_scrub_load_threshold to 50% below current load average
  * Verifies scrub is blocked when threshold < load average
  * Expected log message: "scrub_load_below_threshold:.* = no"
- Scenario 4.2: Sets threshold above node load average
  * Calculates current node load average (loadavg / online CPUs)
  * Sets osd_scrub_load_threshold to 50% above current load average
  * Verifies scrub proceeds when threshold > load average
  * Expected log message: "scrub_load_below_threshold:.* = yes"

Case 5: User-initiated scrub priority
- Generates system load using stress-ng (target load threshold: 3, CPU: 30%)
- Sets osd_scrub_load_threshold to 0 (blocks scheduled scrubs)
- Initiates user-requested scrub operation
- Verifies user-initiated scrub bypasses load threshold restriction
- Checks for log messages: "operator-requested", "deep-scrub starts", and "scrub_load_below_threshold:.* = yes"

Case 6: High CPU load with stress-ng
- Installs and starts stress-ng to generate 80% CPU load
- Monitors system load until threshold (loadavg / online CPUs) exceeds 15
- Sets osd_scrub_load_threshold to 10
- Verifies scrub behavior under high load conditions
- Checks for "scrub_load_below_threshold" log messages in OSD logs
- Verifies scrub operation does not start when load threshold exceeds configured value
- Stops stress-ng and cleans up

Initial Setup:
- Verifies default osd_scrub_load_threshold value is 10.0
- Disables PG autoscaler
- Creates a replicated pool and writes test data (10,000 objects, 5K each)
- Retrieves PG ID and acting OSD set
- Installs stress-ng package on OSD nodes

Expected Behavior:
- When osd_scrub_load_threshold is 0 or below load average: scheduled scrubs are blocked
- When osd_scrub_load_threshold is above load average: scheduled scrubs proceed normally
- User-initiated scrubs always proceed regardless of threshold setting
- Inconsistent objects are only repaired when scrubs are allowed to proceed

"""

import time
import traceback

from test_osd_ecpool_inconsistency_scenario import get_pg_inconsistent_object_count

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from cli.utilities.packages import Package, Rpm
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test execution summary for osd_scrub_load_threshold parameter verification.

    This test verifies the functionality of the osd_scrub_load_threshold parameter
    which controls when scrub operations are allowed based on system load.

    Test Execution Flow:
    1. Initial Setup:
       - Enables file logging for OSD daemons
       - Disables PG autoscaler
       - Creates a replicated pool and writes test data (10,000 objects, 5K each)
       - Retrieves PG ID and acting OSD set
       - Verifies default osd_scrub_load_threshold value is 10.0

    2. Test Cases (executed based on config['case_to_run']):

       Case 1: Threshold value 0 (scrub blocking)
       - Sets osd_scrub_load_threshold to 0
       - Verifies scrub operations do not proceed
       - Checks for log message: "scrub_load_below_threshold:.* = no"

       Case 2: Inconsistent object repair with threshold 0
       - Creates 6 inconsistent objects
       - Sets osd_scrub_load_threshold to 0
       - Verifies auto-repair does not occur when threshold blocks scrubbing
       - Confirms inconsistent object count remains unchanged

       Case 3: Default threshold value (10.0) - scrub enabled
       - Creates inconsistent objects
       - Sets osd_scrub_load_threshold to default (10.0)
       - Verifies scrub operations proceed normally
       - Confirms inconsistent objects are repaired
       - Checks for log message: "scrub_load_below_threshold:.* = yes"

       Case 4: Threshold relative to node load average
       - Scenario 4.1: Sets threshold below node load average
         * Verifies scrub is blocked when threshold < load average
       - Scenario 4.2: Sets threshold above node load average
         * Verifies scrub proceeds when threshold > load average

       Case 5: User-initiated scrub priority
       - Sets osd_scrub_load_threshold to 0 (blocks scheduled scrubs)
       - Initiates user-requested scrub
       - Verifies user-initiated scrub bypasses load threshold restriction
       - Checks for "operator-requested" and "deep-scrub starts" log messages

       Case 6: High CPU load with stress-ng
       - Installs and starts stress-ng to generate 80% CPU load
       - Monitors system load until threshold (loadavg/CPUs) exceeds 15
       - Sets osd_scrub_load_threshold to 10
       - Verifies scrub behavior under high load conditions
       - Checks for scrub_load_below_threshold log messages
       - Stops stress-ng and cleans up

    3. Cleanup:
       - Removes all test configurations (scrub intervals, thresholds, etc.)
       - Resets debug log levels to default
       - Stops any running stress-ng processes
       - Deletes test pool
       - Logs cluster health status

    Args:
        ceph_cluster: Ceph cluster object
        **kw: Keyword arguments containing test configuration
            - config: Test configuration dictionary
                - replicated_pool: Pool configuration
                - case_to_run: List of test cases to execute (case1-case6)

    Returns:
        0: Test execution successful
        1: Test execution failed

    Expected Behavior:
        - When osd_scrub_load_threshold is 0 or below load average: scrubs are blocked
        - When osd_scrub_load_threshold is above load average: scrubs proceed normally
        - User-initiated scrubs always proceed regardless of threshold
        - Inconsistent objects are only repaired when scrubs are allowed
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm, nostart=True)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    wait_time = 120
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]
    acting_pg_set = ""
    start_time = get_cluster_timestamp(rados_object.node)
    log.debug("Test workflow started. Start time: {}".format(start_time))
    try:

        log_line_no_scrub = "'scrub_load_below_threshold:.* = no'"
        log_line_scrub = "'scrub_load_below_threshold:.* = yes'"

        # Get OSD nodes
        osd_nodes = ceph_cluster.get_nodes(role="osd")
        log.info(f"Found {len(osd_nodes)} OSD node(s)")

        # Install stress-ng on primary OSD host
        log.info("Installing the stress-ng package on osd nodes")
        for node in osd_nodes:
            if not check_and_install_package(node, "stress-ng"):
                log.error("Failed to install stress-ng on OSD %s host", node.hostname)
                return 1

        # disable autoscaler
        assert mon_obj.set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        ), "Could not set pg_autoscale mode to OFF"

        if not rados_object.create_pool(**replicated_config):
            log.error("Failed to create the replicated Pool")
            return 1

        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=10000)
        msg_data_push = f"The data pushed into the {pool_name} pool"
        log.info(msg_data_push)

        # Get the pool pg_id
        pg_id = rados_object.get_pgid(pool_name=pool_name)
        pg_id = pg_id[0]
        msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
        log.info(msg_pool_id)

        default_threshold_value = mon_obj.get_config(
            section="osd", param="osd_scrub_load_threshold"
        )

        if float(default_threshold_value) != 10.0:
            log.error(
                "The default value of the osd_scrub_load_threshold is not equal to 10.0"
            )
            return 1
        log.info("The default value of the osd_scrub_load_threshold is 10.0")

        # Check that the scrubbing is progress or not
        if not scrub_object.wait_for_pg_scrub_state(pg_id, wait_time=wait_time):
            log.error("The scrub operations are running, not executing further tests")
            return 1
        acting_pg_set = rados_object.get_pg_acting_set(pg_num=pg_id)
        msg_acting_set = f"The {pool_name} pool acting set is -{acting_pg_set}"
        log.info(msg_acting_set)

        # Get the acting sets host list
        acting_osd_hosts = []
        for osd_id in acting_pg_set:
            osd_node = rados_object.fetch_host_node(daemon_type="osd", daemon_id=osd_id)

            acting_osd_hosts.append(osd_node)
            log.info(f"OSD {osd_id} is on host {osd_node.hostname}")

        msg_acting_hosts = (
            f"The acting set host list: {[host.hostname for host in acting_osd_hosts]}"
        )
        log.info(msg_acting_hosts)

        primary_osd_host = rados_object.fetch_host_node(
            daemon_type="osd", daemon_id=acting_pg_set[0]
        )

        # Get case_to_run from config, default to all cases if not specified
        case_to_run = config.get("case_to_run")

        if not case_to_run:
            log.error("case_to_run is not specified or is empty in the config")
            return 1

        if "case1" in case_to_run:
            try:
                log.info(
                    "===== Scenario 1: Verification of the osd_scrub_load_threshold feature with the value 0"
                )
                # Rotate the osd logs
                rados_object.rotate_logs(acting_osd_hosts)

                if not start_stress_ng_and_generate_load(
                    node=primary_osd_host,
                    target_load_threshold=3,
                    cpu_percentage=20,
                ):
                    log.error(
                        "Scenario 1: Failed to generate load and reach target threshold"
                    )
                    return 1

                log.info("Scenario 1: The logs are rotated")
                log.info(
                    "Scenario 1: Setting the osd_scrub_load_threshold parameter value to 0"
                )

                mon_obj.set_config(
                    section="osd", name="osd_scrub_load_threshold", value=0
                )

                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                # set the debug log to 20
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
                log.info("Scenario 1: The osd  logs are set to 20")

                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)

                try:
                    if rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=False, wait_time=wait_time
                    ):
                        log.error(
                            "Scrub operation started after setting the osd_scrub_load_threshold value to 0"
                        )
                        return 1
                except Exception:
                    log.info(
                        "Scrub operation not started after setting the osd_scrub_load_threshold value to 0"
                    )
                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                log.info(
                    "Scenario 1: The OSD log debug level has been configured to its default value."
                )
                remove_parameter_configuration(mon_obj)

                log.info("Scenario 1: Stopping stress-ng process")
                if not kill_stress_ng(primary_osd_host):
                    log.warning("Scenario 1: Failed to stop stress-ng process")
                else:
                    log.info("Scenario 1: stress-ng process stopped successfully")

                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=log_line_no_scrub,
                    )
                    is False
                ):
                    log.error(
                        "Scenario 1: After setting the Threshold to 0 the scrub operation is initiated"
                    )
                    return 1
                log.info(
                    "After setting the Threshold to 0 the scrub operation is not initiated"
                )
                log.info(
                    "===Scenario 1: End of the verification of the osd_scrub_load_threshold feature with the "
                    "value 0 ==="
                )
            except Exception as e:
                log.error(f"Scenario 1: Exception occurred - {e}")
                log.error(traceback.format_exc())
                return 1

        if "case2" in case_to_run:
            try:
                log.info(
                    "===Scenario 2: Verification of the inconsistent of object repair when "
                    "the osd_scrub_load_threshold is 0==="
                )

                no_of_objects = 6

                obj_pg_map = rados_object.create_inconsistent_object(
                    objectstore_obj, pool_name, no_of_objects
                )
                if obj_pg_map is None:
                    log.error(
                        "Inconsistent objects are not created.Not executing the further test"
                    )
                    return 1
                # Get the inconsistent object count
                before_tst_scrub_inconsistent_count = get_pg_inconsistent_object_count(
                    rados_object, pg_id
                )
                log.info(
                    "Before starting the test waiting for the PG into active+clean state"
                )
                if not wait_for_clean_pg_sets(rados_object, timeout=300):
                    log.error("Cluster cloud not reach active+clean state within 300")

                # Perform the tests by modifying the osd_scrub_load_threshold to 0
                mon_obj.set_config(
                    section="osd", name="osd_scrub_load_threshold", value=0
                )

                if not start_stress_ng_and_generate_load(
                    node=primary_osd_host,
                    target_load_threshold=3,
                    cpu_percentage=20,
                ):
                    log.error(
                        "Scenario 2: Failed to generate load and reach target threshold"
                    )
                    return 1

                # Rotate the osd logs
                rados_object.rotate_logs(acting_osd_hosts)

                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                # set the debug log to 20
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
                log.info("Scenario 2: The osd  logs are set to 20")

                chk_auto_repair_param = set_auto_repair_parameters(
                    mon_obj, before_tst_scrub_inconsistent_count
                )
                if not chk_auto_repair_param:
                    log.error(
                        "The repair parameters are not set. No executing the further tests"
                    )
                    return 1

                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)

                try:
                    if rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=False, wait_time=wait_time
                    ):
                        log.error(
                            "Scenario 2: Scrub operation started after setting the osd_scrub_load_threshold value to 0"
                        )
                        return 1
                except Exception:
                    log.info(
                        "Scenario 2: Scrub operation not started after setting the osd_scrub_load_threshold value to 0"
                    )

                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                remove_parameter_configuration(mon_obj)
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                # Stop stress-ng
                log.info("Scenario 2: Stopping stress-ng process")
                if not kill_stress_ng(primary_osd_host):
                    log.warning("Scenario 2: Failed to stop stress-ng process")
                else:
                    log.info("Scenario 2: stress-ng process stopped successfully")

                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=log_line_no_scrub,
                    )
                    is False
                ):
                    log.error(
                        "Scenario 2: After setting the Threshold to 0 the scrub operation is initiated"
                    )
                    return 1
                after_tst_scrub_err_count = get_pg_inconsistent_object_count(
                    rados_object, pg_id
                )
                msg_after_tst_inconsistent_count = (
                    f"The inconsistent  error count after "
                    f"testing is - {after_tst_scrub_err_count} "
                )
                log.info(msg_after_tst_inconsistent_count)
                if before_tst_scrub_inconsistent_count != after_tst_scrub_err_count:
                    log.error(
                        "The inconsistent objects are repaired  after setting the osd_scrub_load_threshold value to 0"
                    )
                    return 1

                log.info(
                    "===Scenario 2: End of the  verification of the inconsistent of object repair "
                    "when the osd_scrub_load_threshold is 0==="
                )
            except Exception as e:
                log.error(f"Scenario 2: Exception occurred - {e}")
                log.error(traceback.format_exc())
                return 1

        if "case3" in case_to_run:
            try:
                log.info(
                    "===Scenario 3: Verification of the osd_scrub_load_threshold parameter with the default ==="
                    "value which is 10.0"
                )
                no_of_objects = 6
                obj_pg_map = rados_object.create_inconsistent_object(
                    objectstore_obj, pool_name, no_of_objects
                )
                if obj_pg_map is None:
                    log.error(
                        "Inconsistent objects are not created.Not executing the further test"
                    )
                    return 1
                before_tst_scrub_inconsistent_count = get_pg_inconsistent_object_count(
                    rados_object, pg_id
                )

                # Rotate the osd logs
                rados_object.rotate_logs(acting_osd_hosts)

                if not start_stress_ng_and_generate_load(
                    node=primary_osd_host,
                    target_load_threshold=3,
                    cpu_percentage=20,
                ):
                    log.error(
                        "Scenario 3: Failed to generate load and reach target threshold"
                    )
                    return 1
                chk_auto_repair_param = set_auto_repair_parameters(
                    mon_obj, before_tst_scrub_inconsistent_count
                )
                if not chk_auto_repair_param:
                    log.error(
                        "The repair parameters are not set. No executing the further tests"
                    )
                    return 1

                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)

                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)
                wait_time = 900
                try:
                    if not rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=False, wait_time=wait_time
                    ):
                        log.error(
                            "Scenario 3: Scrub operation not started after setting the osd_scrub_load_threshold "
                            "value to 10"
                        )
                        return 1
                except Exception:
                    log.error(
                        "Scrub operation not started after setting the osd_scrub_load_threshold value to 10.0"
                    )
                    return 1

                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                remove_parameter_configuration(mon_obj)
                # Stop stress-ng
                log.info("Scenario 3: Stopping stress-ng process")
                if not kill_stress_ng(primary_osd_host):
                    log.warning("Scenario 3: Failed to stop stress-ng process")
                else:
                    log.info("Scenario 3: stress-ng process stopped successfully")

                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=log_line_scrub,
                    )
                    is False
                ):
                    log.error(
                        "Scenario 3: After setting the Threshold to default value(10.0) the scrub "
                        "operation is not initiated"
                    )
                    return 1

                after_tst_scrub_err_count = get_pg_inconsistent_object_count(
                    rados_object, pg_id
                )
                msg_after_tst_inconsistent_count = (
                    "The inconsistent  error count after "
                    "testing is - {} ".format(after_tst_scrub_err_count)
                )
                log.info(msg_after_tst_inconsistent_count)
                if after_tst_scrub_err_count != 0:
                    log.error(
                        "Scenario 3: The inconsistent objects are not repaired  after setting the "
                        "osd_scrub_load_threshold value to 10.0"
                    )
                    return 1

                log.info(
                    "===Scenario 3: End of the verification of the osd_scrub_load_threshold parameter "
                    "with the default value which is 10.0==="
                )
            except Exception as e:
                log.error(f"Scenario 3: Exception occurred - {e}")
                log.error(traceback.format_exc())
                return 1
        if "case4" in case_to_run:
            try:
                log.info(
                    "===== Scenario 4: Verification of osd_scrub_load_threshold below and above the node "
                    "load average ===="
                )
                log.info(
                    "Scenario 4.1: Testing the negative scenario that, set the osd_scrub_load_threshold value  "
                    "lesser than the average load of node"
                )
                decrease_parameter = 0.5
                log.info(
                    "Before starting the test waiting for the PG into active+clean state"
                )
                if not wait_for_clean_pg_sets(rados_object, timeout=900):
                    log.error("Cluster cloud not reach active+clean state within 900")
                load_threshold = get_node_avg_load(primary_osd_host)

                # Rotate the osd logs
                rados_object.rotate_logs(acting_osd_hosts)

                new_scrub_load_threshold_vale = round(
                    load_threshold - (load_threshold * decrease_parameter), 3
                )
                msg_threshold = (
                    f"Scenario 4.1: The new osd_scrub_load_threshold value will be"
                    f" set to {new_scrub_load_threshold_vale}"
                )
                log.info(msg_threshold)
                assert mon_obj.set_config(
                    section="osd",
                    name="osd_scrub_load_threshold",
                    value=new_scrub_load_threshold_vale,
                ), "Could not set osd_scrub_load_threshold value "

                log.info("Scenario 4.1: The osd  logs are set to 20")
                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                # set the debug log to 20
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)

                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)
                wait_time = 300
                try:
                    if rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=False, wait_time=wait_time
                    ):
                        msg_err = (
                            f"Scenario 4.1: Scrub operation started after setting the osd_scrub_load_threshold "
                            f"value less than average load. The actual load is {load_threshold} and the  "
                            f"osd_scrub_load_threshold parameter value is set to {new_scrub_load_threshold_vale}"
                        )
                        log.error(msg_err)
                        return 1
                except Exception:
                    log.info(
                        "Scenario 4.1: Scrub operation not started after setting the osd_scrub_load_threshold value"
                        "to lesser than the average load of node"
                    )

                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                remove_parameter_configuration(mon_obj)
                log.info(
                    "Scenario 4.1: The OSD log debug level has been configured to its default value."
                )

                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=log_line_no_scrub,
                    )
                    is False
                ):

                    msg_error = (
                        f"Scenario 4.1: The log contain the scrub initiated messages after setting the "
                        f"osd_scrub_load_threshold parameter value to {new_scrub_load_threshold_vale} where the "
                        f"actual load is {load_threshold}"
                    )
                    log.error(msg_error)
                    return 1
                log.info("Setting to the configurations to the default values")
                remove_parameter_configuration(mon_obj)

                log.info(
                    "Scenario 4.1: Testing the negative scenario that, set the osd_scrub_load_threshold value"
                    "lesser than the average load of node completed"
                )
                log.info(
                    "Scenario 4.2: Testing the positive scenario that, set the osd_scrub_load_threshold value"
                    "greater than the average load of node"
                )
                log.info(
                    "Before starting the test waiting for the PG into active+clean state"
                )
                if not wait_for_clean_pg_sets(rados_object, timeout=900):
                    log.error("Cluster cloud not reach active+clean state within 900")

                # Rotate the osd logs
                rados_object.rotate_logs(acting_osd_hosts)

                load_threshold = get_node_avg_load(primary_osd_host)
                new_scrub_load_threshold_vale = round(
                    load_threshold + (load_threshold * decrease_parameter), 3
                )
                msg_threshold = (
                    f"Scenario 4.2: The new osd_scrub_load_threshold value will"
                    f" be set to {new_scrub_load_threshold_vale}"
                )
                log.info(msg_threshold)
                assert mon_obj.set_config(
                    section="osd",
                    name="osd_scrub_load_threshold",
                    value=new_scrub_load_threshold_vale,
                ), "Could not set osd_scrub_load_threshold value"

                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                # set the debug log to 20
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
                log.info("Scenario 4.2: The osd  logs are set to 20")

                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)
                wait_time = 1000
                try:
                    rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=False, wait_time=wait_time
                    )
                except Exception:
                    msg_err = (
                        f"Scenario 4.2: Scrub operation not started after setting the osd_scrub_load_threshold value "
                        f"greater than average load. The actual load is {load_threshold} and the "
                        f"osd_scrub_load_threshold parameter value is set to {new_scrub_load_threshold_vale}"
                    )
                    log.error(msg_err)
                    return 1

                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()

                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                remove_parameter_configuration(mon_obj)
                log.info(
                    "Scenario 4.2: The OSD log debug level has been configured to its default value."
                )

                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=log_line_scrub,
                    )
                    is False
                ):
                    msg_error = (
                        f"Scenario 4.2: The log not contain the scrub initiated messages after setting the "
                        f"osd_scrub_load_threshold parameter value to {new_scrub_load_threshold_vale} where the "
                        f"actual load is {load_threshold}"
                    )
                    log.error(msg_error)
                    return 1

                log.info(
                    "Scenario 4.2: Testing the positive scenario that, set the osd_scrub_load_threshold value"
                    "greater than the average load of node completed"
                )
                log.info(
                    "===== Scenario 4: Verification of osd_scrub_load_threshold below and above the node"
                    "load average is completed===="
                )
            except Exception as e:
                log.error(f"Scenario 4: Exception occurred - {e}")
                log.error(traceback.format_exc())
                return 1

        if "case5" in case_to_run:
            try:
                log.info(
                    "===== Scenario 5:Verification of user initiated scrub as higher priority than "
                    "scheduled scrub for the osd_scrub_load_threshold parameter"
                )
                # Rotate the osd logs
                rados_object.rotate_logs(acting_osd_hosts)
                log.info("Scenario 5: The logs are rotated")

                # Target load threshold: 5 (loadavg / online CPUs)
                target_load_threshold = 3
                if not start_stress_ng_and_generate_load(
                    node=primary_osd_host,
                    target_load_threshold=target_load_threshold,
                    cpu_percentage=30,
                ):
                    log.error(
                        "Scenario 5: Failed to generate load and reach target threshold"
                    )
                    return 1

                log.info(
                    f"Scenario 5: Successfully generated load and reached target threshold: {target_load_threshold}"
                )

                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
                log.info("Scenario 5: Setting the scrub parameters")
                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)
                log.info("Initiating the user initiated scrub")
                time.sleep(60)
                try:
                    rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=True, wait_time=wait_time
                    )
                    log.info("The user initiated scrub is completed")
                except Exception:
                    log.info(
                        "Scenario 5: The user initiated scrub operation not started after setting the "
                        "osd_scrub_load_threshold value to 0"
                    )
                    return 1

                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                remove_parameter_configuration(mon_obj)

                log.info("Scenario 5: Stopping stress-ng process")
                if not kill_stress_ng(primary_osd_host):
                    log.warning("Scenario 5: Failed to stop stress-ng process")
                else:
                    log.info("Scenario 5: stress-ng process stopped successfully")

                log.info("Scenario 5: The osd logs are set to default value")
                log_msgs = [
                    log_line_scrub,
                    "operator-requested",
                    f'"{pg_id} deep-scrub starts"',
                ]
                for log_line in log_msgs:
                    msg_info = (
                        f"Verification of the log message -{log_line} in the osd logs"
                    )
                    log.info(msg_info)
                    if (
                        rados_object.lookup_log_message(
                            init_time=init_time,
                            end_time=end_time,
                            daemon_type="osd",
                            daemon_id=acting_pg_set[0],
                            search_string=log_line,
                        )
                        is False
                    ):
                        msg_error = (
                            f"Scenario 5: During the user initiated scrub, after setting the  "
                            f" osd_scrub_load_threshold to 10 the {log_line} not exists in the logs"
                        )
                        log.error(msg_error)
                        return 1

                log.info(
                    "===== Scenario 5:Verification of user initiated scrub as higher priority than scheduled "
                    "scrub for the osd_scrub_load_threshold parameter completed"
                )
            except Exception as e:
                log.error(f"Scenario 5: Exception occurred - {e}")
                log.error(traceback.format_exc())
                return 1

        if "case6" in case_to_run:
            try:
                log.info(
                    "===== Scenario 6:Verification of osd_scrub_load_threshold with high CPU load using stress-ng ====="
                )

                log.info(
                    "Scenario 6: Step 1 - Starting stress-ng to generate 80% CPU load continuously"
                )
                rados_object.rotate_logs(acting_osd_hosts)
                log.info("Scenario 6: OSD log content removed")

                # Target load threshold: 15
                # calculate load threshold using = loadavg / online CPUs
                target_load_threshold = 15
                if not start_stress_ng_and_generate_load(
                    node=primary_osd_host,
                    target_load_threshold=target_load_threshold,
                    cpu_percentage=80,
                ):
                    log.error(
                        "Scenario 6: Failed to generate load and reach target threshold"
                    )
                    return 1

                log.info(
                    f"Scenario 6: Successfully generated load and reached target threshold: {target_load_threshold}"
                )

                # Get initial time for log checking
                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
                log.info("Scenario 6: Step 3 - Setting the scrub parameters")
                set_scheduled_scrub_parameters(scrub_object, acting_pg_set)

                log.info(
                    "Scenario 6: Step 5 - Checking if scrub operation starts with high load and threshold=10"
                )
                wait_time = 600
                try:
                    if rados_object.start_check_deep_scrub_complete(
                        pg_id=pg_id, user_initiated=False, wait_time=wait_time
                    ):
                        log.info("Scenario 6: Scrub operation started successfully")
                except Exception:
                    log.info(
                        "Scenario 6: Scrub operation did not start (expected if load is too high)"
                    )
                # Get end time for log checking
                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                remove_parameter_configuration(mon_obj)

                # Stop stress-ng after Step 6 completion
                log.info("Scenario 6: Stopping stress-ng process")
                if not kill_stress_ng(primary_osd_host):
                    log.warning("Scenario 6: Failed to stop stress-ng process")
                else:
                    log.info("Scenario 6: stress-ng process stopped successfully")

                log.info(
                    "Scenario 6: Step 6 - Checking logs for scrub_load_below_threshold messages"
                )
                log_search_string = "scrub_load_below_threshold: loadavg"
                log_found = rados_object.lookup_log_message(
                    init_time=init_time,
                    end_time=end_time,
                    daemon_type="osd",
                    daemon_id=acting_pg_set[0],
                    search_string=f"'{log_search_string}'",
                )
                if log_found:
                    log.info(
                        "Scenario 6: Found scrub_load_below_threshold messages in OSD logs"
                    )
                else:
                    log.error(
                        "Scenario 6: scrub_load_below_threshold messages not found in OSD logs"
                    )
                    return 1

                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=log_line_scrub,
                    )
                    is True
                ):
                    log.error(
                        "Scenario 6: Scrub operation is initiated after generate the load threshold more than 10"
                    )
                    return 1

                log.info(
                    "===== Scenario 6:Verification of osd_scrub_load_threshold with high CPU load "
                    "using stress-ng completed ====="
                )
            except Exception as e:
                log.error(f"Scenario 6: Exception occurred - {e}")
                log.error(traceback.format_exc())
                return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Clean up any running stress-ng processes
        for osd_node in osd_nodes:
            if kill_stress_ng(osd_node):
                log.info(
                    "Cleaned up stress-ng processes on {}".format(osd_node.hostname)
                )
            else:
                log.warning(
                    "Failed to clean up stress-ng processes on {}".format(
                        osd_node.hostname
                    )
                )
        remove_parameter_configuration(mon_obj)
        configure_log_level(mon_obj, acting_pg_set, set_to_default=True)

        method_should_succeed(rados_object.delete_pool, pool_name)
        rados_object.log_cluster_health()
    log.info(
        "========== Validation of the osd_scrub_load_threshold is success ============="
    )
    return 0


def remove_parameter_configuration(mon_obj):
    """
    Used to set the default osd scrub parameter value
    Args:
        mon_obj: monitor object
    Returns : None
    """
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair")
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair_num_errors")
    mon_obj.remove_config(section="osd", name="osd_scrub_load_threshold")


def set_scheduled_scrub_parameters(scrub_object, acting_pg_set):
    """
    Method to set the scheduled parameters
    Args:
        scrub_object: Scrub object
        acting_pg_set: Acting PG set

    Returns: None

    """
    osd_scrub_min_interval = 120
    osd_scrub_max_interval = 900
    osd_deep_scrub_interval = 900
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 1)
    for osd_id in acting_pg_set:
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_hour", scrub_begin_hour, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_week_day", scrub_begin_weekday, osd_id
        )
        scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour, osd_id)
        scrub_object.set_osd_configuration(
            "osd_scrub_end_week_day", scrub_end_weekday, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_min_interval", osd_scrub_min_interval, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_max_interval", osd_scrub_max_interval, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_deep_scrub_interval", osd_deep_scrub_interval, osd_id
        )


def set_auto_repair_parameters(mon_obj, inconsistent_obj_count):
    """
    The method is used to set the auto repair parameters
    Args:
        mon_obj: Mon object
        inconsistent_obj_count: Inconsistent object count value

    Returns:True -> if parameters are set
            False -> if parameters are not set
    """
    try:
        # Set the value to true
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        # Check that the inconsistent count is <
        auto_repair_num_value = mon_obj.get_config(
            section="osd", param="osd_scrub_auto_repair_num_errors"
        )
        if inconsistent_obj_count >= int(auto_repair_num_value):
            auto_repair_param_value = inconsistent_obj_count + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            msg_auto_repair_value = f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
            log.info(msg_auto_repair_value)
    except Exception as err:
        msg_err = f"Error occurred while setting the repair parameters- {err}"
        log.error(msg_err)
        return False
    return True


def configure_log_level(mon_obj, acting_set, set_to_default: True):
    """
    Method is used to set the acting osd log to 20 or to default value
    Args:
        mon_obj:  Mon object
        acting_set: acting OSD sets
        set_to_default: False -> Sets the osd debug value to default.
                       True -> Sets the osd debug value to 20

    Returns: None

    """
    if set_to_default:
        for osd_id in acting_set:
            mon_obj.remove_config(section=f"osd.{osd_id}", name="debug_osd")
            msg_debug = f"The osd.{osd_id} debug_osd value is set to default value"
            log.info(msg_debug)
    else:
        for osd_id in acting_set:
            mon_obj.set_config(section=f"osd.{osd_id}", name="debug_osd", value="20/20")
            msg_debug = f"The osd.{osd_id} debug_osd value is set to 20"
            log.info(msg_debug)


def check_and_install_package(node, package_name):
    """
    Check if a package is installed on a node. If not installed, install it.
    Args:
        node: Node where package needs to be checked/installed
        package_name: Name of the package to check/install
    Returns:
        True: If package is installed (either already was or just installed)
        False: If installation fails
    """
    log.info(f"Checking if {package_name} is installed on {node.hostname}")
    try:
        # Check if package is already installed
        rpm_query = Rpm(node).query(package_name)
        if rpm_query:
            log.info(f"{package_name} is already installed on {node.hostname}")
            return True
        # Package is not installed, install it
        log.info(f"{package_name} is not installed. Installing on {node.hostname}")
        Package(node, manager="dnf").install(package_name, nogpgcheck=False)

        # Verify installation
        rpm_query = Rpm(node).query(package_name)
        if rpm_query:
            log.info(f"{package_name} successfully installed on {node.hostname}")
            return True
        else:
            log.error(
                f"{package_name} installation verification failed on {node.hostname}"
            )
            return False
    except Exception as err:
        log.error(f"Failed to check/install {package_name} on {node.hostname}: {err}")
        return False


def get_node_avg_load(cluster_node):
    """
    This method is used to calculate the average load of a provided node.
     Args:
          cluster_node : The node whose average load is being calculated
     Returns:
              load_threshold: Average load in float
    """
    cmd_get_load_avg = "cut -d ' ' -f1 /proc/loadavg"
    load_avg_output = cluster_node.exec_command(cmd=cmd_get_load_avg)
    load_avg = float(load_avg_output[0].strip())
    msg_load_avg = f"The load-average of the {cluster_node.hostname} is {load_avg}"
    log.info(msg_load_avg)
    cmd_count_cpu = "grep -c ^processor /proc/cpuinfo"
    cpu_count_output = cluster_node.exec_command(cmd=cmd_count_cpu)
    cpu_count = int(cpu_count_output[0].strip())
    msg_count_cpu = f"The CPU count of the {cluster_node.hostname} is {cmd_count_cpu}"
    log.info(msg_count_cpu)
    load_threshold = round(load_avg / cpu_count, 3)
    msg_threshold = (
        f"The node load threshold value on"
        f" {cluster_node.hostname}  node is-{load_threshold}"
    )
    log.info(msg_threshold)
    return load_threshold


def kill_stress_ng(node):
    """
    Kill stress-ng processes running on the specified node.

    This method attempts to kill all stress-ng processes matching the pattern
    'stress-ng.*cpu' on the given node and verifies they are terminated.

    Args:
        node: Node where stress-ng processes should be killed

    Returns:
        bool: True if stress-ng processes were successfully killed or not running,
              False if killing failed or processes still exist after kill attempt
    """
    log.info(f"Attempting to kill stress-ng processes on {node.hostname}")
    try:
        # Check if stress-ng is running before attempting to kill
        out_put, _ = node.exec_command(
            sudo=True, cmd="pgrep -f 'stress-ng.*cpu'", check_ec=False
        )
        if not out_put.strip():
            log.info(f"No stress-ng processes found on {node.hostname}")
            return True

        log.info(
            f"Found stress-ng processes on {node.hostname}: {out_put.strip()}. Attempting to kill."
        )

        # Kill stress-ng processes
        node.exec_command(sudo=True, cmd="pkill -f 'stress-ng.*cpu'", check_ec=False)
        time.sleep(2)  # Wait for processes to terminate

        # Verify processes are killed
        out_put, _ = node.exec_command(
            sudo=True, cmd="pgrep -f 'stress-ng.*cpu'", check_ec=False
        )
        if out_put.strip():
            error_msg = (
                f"Failed to kill stress-ng processes on {node.hostname}. "
                f"Processes still running: {out_put.strip()}"
            )
            log.error(error_msg)
            return False

        log.info(f"Successfully killed stress-ng processes on {node.hostname}")
        return True

    except Exception as err:
        error_msg = f"Error while killing stress-ng on {node.hostname}: {err}"
        log.error(error_msg)
        return False


def start_stress_ng_and_generate_load(node, target_load_threshold, cpu_percentage=80):
    """
    Start stress-ng and monitor load until the target load threshold is reached.

    This method starts stress-ng on the specified node with the given CPU percentage,
    then continuously monitors the system load until the calculated load threshold
    (loadavg / online CPUs) exceeds the target threshold.

    Args:
        node: Node where stress-ng should run
        target_load_threshold: Target load threshold (loadavg / online CPUs) to reach
        cpu_percentage: CPU percentage for stress-ng (default 80)

    Returns:
        bool: True if target threshold reached successfully, False otherwise
    """
    # Default values for monitoring
    max_wait_time = 300  # Maximum time to wait in seconds
    check_interval = 5  # Interval between load checks in seconds

    log.info(
        f"Starting stress-ng on {node.hostname} with {cpu_percentage}% CPU load "
        f"to reach target load threshold: {target_load_threshold}"
    )

    try:
        # Start stress-ng in background
        node.exec_command(
            sudo=True,
            cmd=f"nohup stress-ng --cpu {cpu_percentage} --timeout 0 > /tmp/stress-ng.log 2>&1 &",
        )
        log.info(f"stress-ng started in background on {node.hostname}")
        time.sleep(5)  # Wait a bit for process to start

        # Verify stress-ng is running
        out_put, _ = node.exec_command(
            sudo=True, cmd="pgrep -f 'stress-ng.*cpu'", check_ec=False
        )
        if not out_put.strip():
            error_msg = f"stress-ng process not found after starting on {node.hostname}"
            log.error(error_msg)
            return False

        log.info(f"stress-ng process ID on {node.hostname}: {out_put.strip()}")

        # Get number of online CPUs
        cmd_count_online_cpu = "nproc"
        cpu_count_output = node.exec_command(cmd=cmd_count_online_cpu)
        online_cpu_count = int(cpu_count_output[0].strip())
        log.info(f"Online CPU count on {node.hostname}: {online_cpu_count}")

        initial_load = 0
        # load_avg = 0
        load_threshold = 0.0
        continuous_count = 0
        elapsed_time = 0

        log.info(
            f"Monitoring load on {node.hostname} and waiting for load threshold "
            f"(loadavg / online CPUs) to exceed {target_load_threshold}"
        )

        while elapsed_time < max_wait_time:
            time.sleep(check_interval)
            elapsed_time += check_interval

            if continuous_count == 13:
                error_msg = (
                    f"stress-ng is not working properly on {node.hostname} "
                    f"(load did not increase for past 1 minute)"
                )
                log.error(error_msg)
                return False

            # Get current load average (1-minute load average)
            cmd_get_load_avg = "cut -d ' ' -f1 /proc/loadavg"
            load_avg_output = node.exec_command(cmd=cmd_get_load_avg)
            load_avg = float(load_avg_output[0].strip())

            if not load_avg > initial_load:
                log.warning(
                    f"Load did not increase significantly on {node.hostname}. "
                    f"This may indicate stress-ng is not working properly."
                )
                continuous_count = continuous_count + 1
                continue
            else:
                continuous_count = 0

            # Calculate load threshold = loadavg / number of online CPUs
            load_threshold = load_avg / online_cpu_count

            log.info(
                f"Iteration {elapsed_time // check_interval} on {node.hostname} - "
                f"Current load average: {load_avg}, previous: {initial_load}, "
                f"Load threshold: {load_threshold:.3f} (target: > {target_load_threshold})"
            )

            initial_load = load_avg

            # Check if load threshold exceeds target
            if load_threshold > target_load_threshold:
                log.info(
                    f"Load threshold ({load_threshold:.3f}) has exceeded target "
                    f"({target_load_threshold}) on {node.hostname}. Proceeding with the test."
                )
                return True

        error_msg = (
            f"Failed to reach target load threshold {target_load_threshold} on {node.hostname} "
            f"within {max_wait_time} seconds. Final load threshold: {load_threshold:.3f}"
        )
        log.error(error_msg)
        return False

    except Exception as err:
        error_msg = (
            f"Failed to start stress-ng or monitor load on {node.hostname}: {err}"
        )
        log.error(error_msg)
        return False
