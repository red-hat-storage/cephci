"""
Module to Test and verify logging of various operation related to mon, mgr and OSDs
"""

import json

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to Test and verify logging of various operation related to mon, mgr and OSDs
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    config_obj = MonConfigMethods(rados_obj=rados_obj)

    if config.get("test_mgr_ops_tracker"):
        log.info("Testing MGR ops tracker configuration")
        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        try:
            # Get the active mgr daemon ID
            mgr_daemons = rados_obj.run_ceph_command(
                cmd="ceph mgr stat", client_exec=True
            )
            active_mgr = mgr_daemons.get("active_name")
            log.debug("Active MGR daemon: %s", active_mgr)

            # Check if mgr_enable_op_tracker is set to true
            param_name = "mgr_enable_op_tracker"

            # Check runtime value (ceph config show)
            mgr_runtime = config_obj.show_config(
                daemon="mgr", id=active_mgr, param=param_name
            )
            mgr_runtime = str(mgr_runtime).strip()
            log.info(
                "MGR %s - Runtime (config show) %s: %s",
                active_mgr,
                param_name,
                mgr_runtime,
            )

            # Check DB value (ceph config get)
            mgr_db = config_obj.get_config(section="mgr", param=param_name)
            mgr_db = str(mgr_db).strip()
            log.info("MGR - DB (config get) %s: %s", param_name, mgr_db)

            # Verify that mgr_enable_op_tracker is set to true for both runtime and DB
            if mgr_runtime.lower() == "true" and mgr_db.lower() == "true":
                log.info(
                    "SUCCESS: %s is set to true for MGR daemon %s (runtime and DB)",
                    param_name,
                    active_mgr,
                )
            else:
                log.error(
                    "FAILED: %s - Runtime: %s, DB: %s, expected both to be true",
                    param_name,
                    mgr_runtime,
                    mgr_db,
                )
                return 1

            # Enable alerts module and generate MGR operations
            log.info("Enabling alerts module to generate MGR operations")
            rados_obj.run_ceph_command(
                cmd="ceph mgr module enable alerts", client_exec=True
            )
            rados_obj.run_ceph_command(cmd="ceph alerts send", client_exec=True)
            log.info("Alerts sent successfully - MGR operations created")

            active_mgr_node = rados_obj.fetch_host_node(
                daemon_type="mgr", daemon_id=active_mgr
            )
            mgr_ops_base_cmd = f"cephadm shell --name mgr.{active_mgr} -- "

            # cmd 1 : dump_historic_ops
            mgr_ops_cmd_1 = (
                f"{mgr_ops_base_cmd} ceph daemon mgr.{active_mgr} dump_historic_ops"
            )
            out, err = active_mgr_node.exec_command(cmd=mgr_ops_cmd_1, sudo=True)
            if not out:
                log.error("FAILED: Command dump_historic_ops returned no output")
                return 1
            try:
                mgr_dump_historic_ops = json.loads(out)
            except json.JSONDecodeError as e:
                log.error(
                    "FAILED: Unable to parse dump_historic_ops output as JSON: %s", e
                )
                log.error("Output received: %s", out)
                return 1
            log.info("dump_historic_ops output: %s", mgr_dump_historic_ops)

            # Check for logged operations
            ops_list = mgr_dump_historic_ops.get("ops", [])
            if not ops_list or len(ops_list) == 0:
                log.info("No operations found in dump_historic_ops output.")
            else:
                log.info(
                    "Found %s operation(s) in dump_historic_ops output", len(ops_list)
                )

                # Log details of the operations found
                for idx, op in enumerate(ops_list):
                    description = op.get("description", "N/A")
                    duration = op.get("duration", "N/A")
                    log.info(
                        "Operation %s: %s (duration: %ss)",
                        idx + 1,
                        description,
                        duration,
                    )

            # cmd 2 : dump_historic_ops_by_duration
            log.info(
                "Verifying operations sorted by duration using dump_historic_ops_by_duration"
            )
            mgr_ops_cmd_2 = f"{mgr_ops_base_cmd} ceph daemon mgr.{active_mgr} dump_historic_ops_by_duration"
            out, err = active_mgr_node.exec_command(cmd=mgr_ops_cmd_2, sudo=True)
            if not out:
                log.error(
                    "FAILED: Command dump_historic_ops_by_duration returned no output"
                )
                return 1
            try:
                mgr_dump_historic_ops_by_duration = json.loads(out)
            except json.JSONDecodeError as e:
                log.error(
                    "FAILED: Unable to parse dump_historic_ops_by_duration output as JSON: %s",
                    e,
                )
                log.error("Output received: %s", out)
                return 1
            log.info(
                "dump_historic_ops_by_duration output: %s",
                mgr_dump_historic_ops_by_duration,
            )

            # Check for logged operations sorted by duration
            ops_by_duration = mgr_dump_historic_ops_by_duration.get("ops", [])
            if not ops_by_duration or len(ops_by_duration) == 0:
                log.info("No operations found in dump_historic_ops_by_duration output.")
            elif len(ops_by_duration) == 1:
                log.info("Found 1 operation in dump_historic_ops_by_duration output")
                op = ops_by_duration[0]
                description = op.get("description", "N/A")
                duration = op.get("duration", "N/A")
                log.info("Operation 1: %s (duration: %ss)", description, duration)
            else:
                log.info(
                    "Found %s operation(s) in dump_historic_ops_by_duration output",
                    len(ops_by_duration),
                )

                # Verify operations are sorted by duration (descending order)
                durations = [op.get("duration", 0) for op in ops_by_duration]
                is_sorted = all(
                    durations[i] >= durations[i + 1] for i in range(len(durations) - 1)
                )

                if is_sorted:
                    log.info(
                        "SUCCESS: Operations are properly sorted by duration (descending order)"
                    )
                    for idx, op in enumerate(ops_by_duration):
                        description = op.get("description", "N/A")
                        duration = op.get("duration", "N/A")
                        log.info(
                            "Operation %s: %s (duration: %ss)",
                            idx + 1,
                            description,
                            duration,
                        )
                else:
                    log.error("FAILED: Operations are NOT sorted by duration")
                    log.error("Duration sequence: %s", durations)
                    return 1

            # Updating the mgr response times so that ops can qualify for slow ops.
            log.info(
                "Setting mgr_op_complaint_time to 0.05 seconds to make slow operations"
            )
            config_obj.set_config(
                section="mgr",
                name="mgr_op_complaint_time",
                value="0.05",
            )
            log.info("Successfully set mgr_op_complaint_time to 0.05")

            # Send alerts to create MGR ops
            log.info("Sending alerts to create MGR operations")
            rados_obj.run_ceph_command(cmd="ceph alerts send", client_exec=True)
            log.info("Alerts sent successfully - MGR operations created")

            # cmd 3 : dump_historic_slow_ops
            log.info("Checking for slow operations using dump_historic_slow_ops")
            mgr_ops_cmd_3 = f"{mgr_ops_base_cmd} ceph daemon mgr.{active_mgr} dump_historic_slow_ops"
            out, err = active_mgr_node.exec_command(cmd=mgr_ops_cmd_3, sudo=True)
            if not out:
                log.error("FAILED: Command dump_historic_slow_ops returned no output")
                return 1
            try:
                mgr_dump_historic_slow_ops = json.loads(out)
            except json.JSONDecodeError as e:
                log.error(
                    "FAILED: Unable to parse dump_historic_slow_ops output as JSON: %s",
                    e,
                )
                log.error("Output received: %s", out)
                return 1
            log.info("dump_historic_slow_ops output: %s", mgr_dump_historic_slow_ops)

            # Check for slow operations
            slow_ops_list = mgr_dump_historic_slow_ops.get("ops", [])
            if not slow_ops_list or len(slow_ops_list) == 0:
                log.info("No slow operations found in dump_historic_slow_ops output.")
            else:
                log.info(
                    "Found %s slow operation(s) in dump_historic_slow_ops output",
                    len(slow_ops_list),
                )

                # Log details of the slow operations found
                for idx, op in enumerate(slow_ops_list):
                    description = op.get("description", "N/A")
                    duration = op.get("duration", "N/A")
                    log.info(
                        "Slow Operation %s: %s (duration: %ss)",
                        idx + 1,
                        description,
                        duration,
                    )

            # cmd 4 : dump_blocked_ops
            log.info("Checking for blocked operations using dump_blocked_ops")
            mgr_ops_cmd_4 = (
                f"{mgr_ops_base_cmd} ceph daemon mgr.{active_mgr} dump_blocked_ops"
            )
            out, err = active_mgr_node.exec_command(cmd=mgr_ops_cmd_4, sudo=True)
            if not out:
                log.error("FAILED: Command dump_blocked_ops returned no output")
                return 1
            try:
                mgr_dump_blocked_ops = json.loads(out)
            except json.JSONDecodeError as e:
                log.error(
                    "FAILED: Unable to parse dump_blocked_ops output as JSON: %s", e
                )
                log.error("Output received: %s", out)
                return 1
            log.info("dump_blocked_ops output: %s", mgr_dump_blocked_ops)

            # Check for blocked operations
            blocked_ops_list = mgr_dump_blocked_ops.get("ops", [])
            num_blocked_ops = mgr_dump_blocked_ops.get("num_blocked_ops", 0)

            log.info("Number of blocked operations: %s", num_blocked_ops)

            if not blocked_ops_list or len(blocked_ops_list) == 0:
                log.info("No blocked operations found in dump_blocked_ops output.")
            else:
                log.info(
                    "Found %s blocked operation(s) in dump_blocked_ops output",
                    len(blocked_ops_list),
                )

                # Log details of the blocked operations found
                for idx, op in enumerate(blocked_ops_list):
                    description = op.get("description", "N/A")
                    duration = op.get("duration", "N/A")
                    log.info(
                        "Blocked Operation %s: %s (duration: %ss)",
                        idx + 1,
                        description,
                        duration,
                    )

        except Exception as e:
            log.error("Error while testing MGR ops tracker: %s", e)
            return 1
        finally:
            log.info("Reverting configuration changes")
            try:
                # Revert mgr_op_complaint_time configuration
                config_obj.remove_config(section="mgr", name="mgr_op_complaint_time")
                log.info("Successfully reverted mgr_op_complaint_time configuration")
            except Exception as e:
                log.warning(
                    "Failed to revert mgr_op_complaint_time configuration: %s", e
                )

            log.info("Completed MGR ops tracker configuration test")
            rados_obj.log_cluster_health()
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info("All tests related to MGR ops tracker completed successfully")
        return 0

    # If no test was executed, log and return success
    log.info("No ops tracker tests configured to run")
    return 0
