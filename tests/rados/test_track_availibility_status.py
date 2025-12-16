"""
This module is to verify the -
1. The availability-status of the pool during various operations.
2. The enable_availability_tracking parameter checking
3. To clear the status by using the clear_availability_score command
"""

import ast
import datetime
import json
import math
import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Polarion#CEPH-83620501
    1. Verify the Mean Time Between Failures(MTBF),Mean Time To Recover(MTTR) and score of the pool
    2. Verification of the availability-status log messages
    3. Verification of the MTBF,MTTR and score during recovery
    4. Verification of the MTBF,MTTR and score during backfilling
    5. Verification of the MTBF,MTTR and score after performing scrub operations
    6. Verification of the MTBF,MTTR and score after split and merging of PG
    7.Verification of the MTBF,MTTR and score during balancer activity
    8.Verification of the enable_availability_tracking parameter functionality
    9.Verification of the MTBF,MTTR and score after the MON operations
    10.Verification of the clear_availability_score command
    11.Verification of the MTBF,MTTR and score after creating inconsistent objects
    12.Verification of the MTBF,MTTR and score after rebooting the primary osd node
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    pool_obj = PoolFunctions(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]
    mon_workflow_obj = MonitorWorkflows(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm, nostart=True)
    test_start_time = get_cluster_timestamp(rados_object.node)
    log.debug(f"Test workflow started. Start time: {test_start_time}")
    try:
        mon_obj.set_config(
            section="mon", name="enable_availability_tracking", value="true"
        )
        if "case1" in config.get("case_to_run"):
            log.info(
                "==== Case 1: Verification of the Mean Time Between Failures (MTBF),Mean Time To Recover(MTTR) and "
                "score value ===="
            )

            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)

            log.info(
                "==== Case 1: Verification of the Mean Time Between Failures (MTBF) value ===="
            )

            if not test_MTBF_value(client, pool_name):
                log.error("Case 1: Verification of the MTBF failed ")
                return 1

            log.info(
                "==== Case 1: Verification of the Mean Time Between Failures (MTBF) value is completed ===="
            )

            log.info(
                "==== Case 1: Verification of the Mean Time To Recover(MTTR) value ===="
            )

            if not test_MTTR_value(client, pool_name):
                log.error("Case 1: Verification of the MTTR failed ")
                return 1
            log.info(
                "==== Case 1: Verification of the Mean Time To Recover(MTTR) value is completed ===="
            )
            log.info("==== Case 1: Verification of the score value ====")

            if not test_score_value(client, pool_name):
                log.error("Case 1: Verification of the score value failed ")
                return 1
            log.info("==== Case 1: Verification of the score value completed ====")
            log.info(
                "==== Case 1: Verification of the Mean Time Between Failures (MTBF),Mean Time To Recover(MTTR) and "
                "score value  completed===="
            )
        if "case2" in config.get("case_to_run"):
            log.info(
                "==== Case 2:Verification of the score values displayed on multiple pools and verification of the "
                "logs.===="
            )
            log.info(
                "==== Case 2: Verification of the score value displayed on multiple pools===="
            )
            log.info("Setting the configurations to enable the file logging")
            if not rados_object.enable_file_logging():
                log.error(
                    "Case 2: Error while setting config to enable logging into file"
                )
                return 1

            log.info("Case2: Retrieving the MON nodes")
            mon_nodes = ceph_cluster.get_nodes(role="mon")

            init_time, _ = installer.exec_command(
                cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
            )
            init_time = init_time.strip()
            msg_init_time = f"Case2: The initial time to check the logs is -{init_time}"
            log.info(msg_init_time)
            # Truncate the mon logs
            rados_object.remove_log_file_content(mon_nodes, daemon_type="mon")
            mon_obj.set_config(section="mon", name="debug_mon", value="20/20")
            success, previous_pool_data = get_pool_availability_status(client)
            if not success:
                log.error("Case2:Failed to retrieve the pool available data")
                return 1
            previous_total_pool = len(previous_pool_data)
            msg_pre_pool = f"Case2: The number of  pools before starting tests are - {previous_total_pool}"
            log.info(msg_pre_pool)
            new_pool_names = []
            for pool_num in range(20):
                rados_object.create_pool(pool_name=f"test_pool{pool_num}")
                rados_object.bench_write(
                    pool_name=f"test_pool{pool_num}", byte_size="5K", max_objs=1000
                )
                new_pool_names.append(f"test_pool{pool_num}")

            success, after_pool_data = get_pool_availability_status(client)
            if not success:
                log.error("Case2:Failed to retrieve the pool available data")
                cleanup_pools(rados_object, new_pool_names)
                return 1
            after_total_pool = len(after_pool_data)
            msg_after_pool = f"Case2: The total number of pools after creating are - {after_total_pool}"
            log.info(msg_after_pool)
            if (after_total_pool - previous_total_pool) != 20:
                log.error("Case2: The total pool data is not displayed")
                cleanup_pools(rados_object, new_pool_names)
                return 1

            random_pool = random.choice(new_pool_names)
            msg_random_pool = f"Case2:The random pool selected is - {random_pool}"
            log.info(msg_random_pool)

            if not test_all_values(client, random_pool):
                cleanup_pools(rados_object, new_pool_names)
                msg_error = (
                    f"Case2: The track availability tests are failed on {random_pool}"
                )
                log.error(msg_error)
                return 1

            msg_pool_deletion = (
                f"Case2: The {random_pool} is selected to delete the pool "
            )
            log.info(msg_pool_deletion)
            method_should_succeed(rados_object.delete_pool, random_pool)
            new_pool_names.remove(random_pool)
            msg_info = f"Case2: Removed the {random_pool} from the pool list.The new pool list is-{new_pool_names}"
            log.info(msg_info)
            success, after_delete_pool_details = get_pool_availability_status(client)

            if not success:
                cleanup_pools(rados_object, new_pool_names)
                log.error("Case2:Failed to retrieve the pool available data")
                return 1
            if random_pool in after_delete_pool_details:
                msg_error = f"Case2:The {random_pool} pool details exists in the availability-status output"
                log.error(msg_error)
                log.info("Case2:Deleting the created pools from the cluster")
                cleanup_pools(rados_object, new_pool_names)
                return 1
            msg_info = (
                f"Case2:The {random_pool} pool details not exists in the availability-status output."
                f"The pool available status output is -{after_delete_pool_details}"
            )
            log.info(msg_info)
            log.info("Case2:Deleting the created pools from the cluster")
            cleanup_pools(rados_object, new_pool_names)
            mon_obj.remove_config(section="mon", name="debug_mon")
            end_time, _ = installer.exec_command(
                cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
            )
            end_time = end_time.strip()
            msg_end_time = f"Case2: The end time to check the logs is -{end_time}"
            log.info(msg_end_time)

            log.info(
                "==== Case 2: Verification of the score value displayed on multiple pools  completed===="
            )

            log.info(
                "===  Case2: Verification of track availability messages in the logs==="
            )

            leader_mon = mon_workflow_obj.get_mon_quorum_leader()
            log_msgs = [
                "'mgrstat pool_availability'",
                "'mgrstat calc_pool_availability: Adding pool'",
                "'Deleting pool'",
            ]
            for log_line in log_msgs:
                msg_info = (
                    f"Case2:Verification of the log message -{log_line} in the osd logs"
                )
                log.info(msg_info)
                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="mon",
                        daemon_id=leader_mon,
                        search_string=log_line,
                    )
                    is False
                ):
                    msg_error = (
                        f"Case2:During the verification of the track availability tests the {log_line} not exists in "
                        f"the logs"
                    )
                    log.error(msg_error)
                    return 1

            log.info(
                "===  Case 2: Verification of track availability messages in the logs are completed==="
            )
            log.info(
                "==== Case 2: Verification of the score values displayed on multiple pools and verification of the "
                "logs completed ===="
            )
        if "case3" in config.get("case_to_run"):
            log.info(
                "=== Case 3: Verification of track availability tests during recovery state==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            # Get the pool pg_id
            pg_id = rados_object.get_pgid(pool_name=pool_name)
            pg_id = pg_id[0]
            msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
            log.info(msg_pool_id)

            acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
            if not rados_object.change_osd_state(
                action="stop", target=acting_pg_set[0]
            ):
                msg_error = f"Case3:Unable to stop the OSD : {acting_pg_set[0]}"
                log.error(msg_error)
                raise Exception("Exception occurred while stopping the OSD ")

            rados_object.bench_write(
                pool_name=pool_name, byte_size="5K", max_objs=10000
            )
            if not rados_object.change_osd_state(
                action="start", target=acting_pg_set[0]
            ):
                msg_error = f"case3:Unable to stop the OSD : {acting_pg_set[0]}"
                log.error(msg_error)
                raise Exception("Exception occurred while stopping the OSD")
            start_time = datetime.datetime.now()
            recovery_found = False
            timeout = datetime.timedelta(minutes=15)
            while datetime.datetime.now() - start_time < timeout:
                pg_state = rados_object.get_pg_state(pg_id=pg_id)
                if "recovering" in pg_state or "backfilling" in pg_state:
                    log.info("Recovery in started")
                    recovery_found = True
                    break
                else:
                    log.info("No recovery  operation started.")

            if not recovery_found:
                log.error(
                    "The recovery operation did not start; therefore, the subsequent tests will not be executed."
                )
                return 1
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )

            if not test_all_values(client, pool_name):
                msg_error = f"Case3:The track availability tests are failed on {pool_name} during recovery"
                log.error(msg_error)
                return 1
            log.info(
                "=== Case 3: Verification of track availability tests completed during recovery state==="
            )

        if "case4" in config.get("case_to_run"):
            log.info(
                "=== Case 4: Verification of track availability tests during backfill state==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            # Get the pool pg_id
            pg_id = rados_object.get_pgid(pool_name=pool_name)
            pg_id = pg_id[0]
            msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
            log.info(msg_pool_id)
            log.info("Bring the cluster in to the backfilling state")
            pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)

            rados_object.run_ceph_command(cmd=f"ceph osd out {pg_set[0]}")
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=500)
            rados_object.run_ceph_command(cmd=f"ceph osd in {pg_set[0]}")
            timeout = datetime.timedelta(minutes=15)
            start_time = datetime.datetime.now()
            backfill_found = False

            while datetime.datetime.now() - start_time < timeout:
                pg_state = rados_object.get_pg_state(pg_id=pg_id)
                if "backfilling" in pg_state:
                    log.info("backfilling in started")
                    backfill_found = True
                    break
                else:
                    log.info("No backfill  operation started.")
            if not backfill_found:
                log.error(
                    "The backfill operation did not start; therefore, the subsequent tests will not be executed."
                )
                return 1
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )

            if not test_all_values(client, pool_name):
                msg_error = f" The track availability tests are failed on {pool_name} during backfill"
                log.error(msg_error)
                return 1

            log.info(
                "=== Case 4: Verification of track availability tests completed during backfill state==="
            )
        if "case5" in config.get("case_to_run"):
            log.info(
                "=== Case 5: Verification of track availability tests after performing the scrub operations state==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            # Get the pool pg_id
            pg_id = rados_object.get_pgid(pool_name=pool_name)
            pg_id = pg_id[0]
            msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
            log.info(msg_pool_id)
            wait_time = 900
            try:
                rados_object.start_check_scrub_complete(
                    pg_id=pg_id, user_initiated=True, wait_time=wait_time
                )
                log.info("Case 5:The user initiated scrub is completed")
            except Exception:
                log.info("case 5:The user initiated scrub operation not started  ")
                return 1

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 5:The track availability tests are failed on {pool_name} after performing the "
                    f"scrub operation"
                )
                log.error(msg_error)
                return 1
            log.info("Case5:Performing the deep-scrub operations")
            try:
                rados_object.start_check_deep_scrub_complete(
                    pg_id=pg_id, user_initiated=True, wait_time=wait_time
                )
                log.info("Ces5: The user initiated deep-scrub is completed")
            except Exception:
                log.info("case 5:The user initiated deep-scrub operation not started  ")
                return 1

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case5: The track availability tests are failed on {pool_name} after performing the "
                    f"scrub operation"
                )
                log.error(msg_error)
                return 1
            log.info(
                "=== Case 5: Verification of track availability tests after performing the scrub operations state "
                "are completed==="
            )
        if "case6" in config.get("case_to_run"):
            log.info(
                "=== Case 6: Verification of track availability tests by PG split and merging ==="
            )
            log.info(
                "=== Case 6: Verification of track availability tests after pg split ==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=2700
            )
            res, inactive_count = pool_obj.run_autoscaler_bulk_test(
                pool=pool_name,
                overwrite_recovery_threads=True,
                test_pg_split=True,
            )
            if not res:
                log.error(" Case 6:Failed to scale up the pool with bulk flag. Fail")
                return 1

            if not test_all_values(client, pool_name):
                msg_error = f"  Case 6: The track availability tests are failed on {pool_name} during split"
                log.error(msg_error)
                return 1
            log.info(
                "=== Case 6: Verification of track availability tests completed after pg split==="
            )
            log.info(
                "=== Case 6: Verification of track availability tests by pg merge ==="
            )

            res, inactive_count = pool_obj.run_autoscaler_bulk_test(
                pool=pool_name, overwrite_recovery_threads=True, test_pg_merge=True
            )
            if not res:
                log.error("Case 6:Failed to scale up the pool with bulk flag. Fail")
                return 1

            if not test_all_values(client, pool_name):
                msg_error = f"  Case 6: The track availability tests are failed on {pool_name} during pg merge"
                log.error(msg_error)
                return 1

            log.info(
                "=== Case 6: Verification of track availability tests completed by pg merge==="
            )
            log.info(
                "=== Case 6: Verification of track availability tests completed by PG split and merging ==="
            )

        if "case7" in config.get("case_to_run"):
            log.info(
                "=== Case 7: Verification of track availability tests by enabling balancer activity  ==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            # Check ceph balancer status
            if not rados_object.get_balancer_status():
                cmd_enable = "ceph balancer on"
                rados_object.run_ceph_command(cmd_enable)
                if not rados_object.get_balancer_status():
                    log.error("Case7: The ceph balancer is false")
                    return 1
            log.info("Case 7: The ceph balancer is enabled")
            rados_object.reweight_crush_items()

            if not test_all_values(client, pool_name):
                msg_error = f"Case 7: The track availability tests are failed on {pool_name} during balancer activity"
                log.error(msg_error)
                return 1

            log.info(
                "=== Case 7: Verification of track availability tests by enabling balancer activity completed ==="
            )
        if "case8" in config.get("case_to_run"):
            log.info(
                "=== Case 8: Verification enable_availability_tracking parameter  ==="
            )
            mon_obj.remove_config(section="mon", name="enable_availability_tracking")
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            availability_value = mon_obj.get_config(
                section="mon", param="enable_availability_tracking"
            )

            if availability_value == "true":
                log.error(
                    "Case 8:The enable_availability_tracking is true by default it is false"
                )
                return 1
            log.info("Case 8:The enable_availability_tracking default value is false")

            success, out_put = get_pool_availability_status(client)
            if success:
                log.error(
                    "Case 8:Able to retrieve the data after setting the enable_availability_tracking to false"
                )
                return 1

            availability_msg = (
                "Error ENOTSUP: availability tracking is disabled; you can enable it by setting the config "
                "option enable_availability_tracking"
            )
            if availability_msg not in str(out_put):
                msg_error = (
                    f"Case 8:The {availability_msg} is not displayed in the output  when the "
                    f"enable_availability_tracking parameter is false"
                )
                log.error(msg_error)
                return 1
            msg_info = (
                f"Case 8:The {availability_msg} is displayed in the output  when the enable_availability_tracking "
                f"parameter is false"
            )
            log.info(msg_info)
            mon_obj.set_config(
                section="mon", name="enable_availability_tracking", value="true"
            )

            if not test_all_values(client, pool_name):
                msg_error = f"  Case 8: The track availability tests are failed on {pool_name} during balancer activity"
                log.error(msg_error)
                return 1

            log.info(
                "=== Case 8: Verification enable_availability_tracking parameter completed  ==="
            )
        if "case9" in config.get("case_to_run"):
            log.info(
                "=== Case 9: Verification of track availability tests by performing the mon operations ==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            log.info("Case9: Retrieving the MON nodes")
            mon_nodes = ceph_cluster.get_nodes(role="mon")
            leader_mon = mon_workflow_obj.get_mon_quorum_leader()
            if not rados_object.change_daemon_systemctl_state(
                action="stop", daemon_type="mon", daemon_id=leader_mon
            ):
                msg_error = f"Case 9: Failed to stop mon daemon on host {leader_mon}"
                log.error(msg_error)
                raise Exception("Mon stop failure error")
            msg_debug = f"Case 9: Successfully stopped mon.{leader_mon} service"
            log.debug(msg_debug)

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 9: The track availability tests are failed on {pool_name} after performing the scrub "
                    f"operation"
                )
                log.error(msg_error)
                return 1
            for mon in mon_nodes:
                if not rados_object.change_daemon_systemctl_state(
                    action="restart", daemon_type="mon", daemon_id=mon.hostname
                ):
                    log.error(
                        f"Case 9: Failed to restart mon daemon on host {mon.hostname}"
                    )
                    raise Exception("Mon stop failure error")
                msg_debug = f"Case 9: Successfully restarted mon.{mon.hostname} service"
                log.debug(msg_debug)

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 9: The track availability tests are failed on {pool_name} after performing the scrub "
                    f"operation"
                )
                log.error(msg_error)
                return 1

            log.info(
                "=== Case 9: Verification of track availability tests by performing the mon operations are completed "
                "==="
            )
        if "case10" in config.get("case_to_run"):
            log.info(
                "=== Case 10: Verification clear-availability-status command on pool  ==="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            cmd_clear_pool_status = (
                f"ceph osd pool clear-availability-status {pool_name}"
            )
            client.exec_command(cmd=cmd_clear_pool_status)
            success, out_put = get_pool_availability_status(client)
            if not success:
                log.error("Case 10: Failed to retrieve the pool available data")
                return 1
            selected_pool_details = out_put[pool_name]
            if (
                int(selected_pool_details[2]) != 0
                and int(selected_pool_details[4]) != 1
                and int(selected_pool_details[5]) != 1
            ):
                msg_error = f"Case 10: The clear_availability_status command  not clear the {pool_name} pool data"
                log.error(msg_error)
                return 1
            msg_info = f"Case 10: The clear_availability_status command clear the {pool_name} pool data"
            log.info(msg_info)

            log.info(
                "=== Case 10: Verification clear-availability-status command on pool completed  ==="
            )
        if "case11" in config.get("case_to_run"):
            log.info(
                "=== Case 11: Verification of track availability tests by removing object in a OSD ==="
            )
            osd_object_map = {}
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 11: The track availability tests are failed on {pool_name}"
                )
                log.error(msg_error)
                return 1
            before_score = get_score(client, pool_name)
            if before_score is None:
                log.error("Case 11: Not able to retrieve the score")
                return 1

            log.info("Case 11: Getting objects from pool using rados ls command")
            cmd_rados_ls = f"rados -p {pool_name} ls | shuf -n 5"
            try:

                out_put, err = client.exec_command(cmd=cmd_rados_ls)
                object_list = out_put.strip().split("\n")
                log.info("Case 11: The Objects in test_pool are - %s", object_list)
            except Exception as e:
                log.error(f"Case 11: Failed to list objects from test_pool: {e}")
                return 1

            for object_name in object_list:
                primary_osd = rados_object.get_osd_map(pool=pool_name, obj=object_name)[
                    "acting_primary"
                ]
                osd_object_map.setdefault(primary_osd, []).append(object_name)
            for osd_id, objects_list in osd_object_map.items():
                for obj_name in objects_list:
                    obj_str = objectstore_obj.list_objects(
                        osd_id=osd_id, obj_name=obj_name
                    )
                    obj_pg_id = ast.literal_eval(obj_str)[0]
                    json_data = json.dumps(ast.literal_eval(obj_str)[1])

                    objectstore_obj.remove_omap(
                        osd_id=osd_id,
                        pgid=obj_pg_id,
                        obj=json_data,
                        key=f"key-{obj_name}",
                    )
                    objectstore_obj.remove_object(
                        osd_id=osd_id, pgid=obj_pg_id, obj=json_data
                    )
                    msg_obj_remove_omap = f"The {obj_name} object is removed"
                    log.info(msg_obj_remove_omap)
                if not rados_object.change_osd_state(
                    action="start", target=int(osd_id)
                ):
                    msg_osd_start = f"Could not start OSD.{osd_id}"
                    log.error(msg_osd_start)
                    raise Exception("OSD not started")
            rados_object.run_scrub()
            is_inconsistent_exists = False
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
            while end_time > datetime.datetime.now():
                if rados_object.check_health_warning(warning="PG_DAMAGED"):
                    log.info("Case 11: Inconsistent objects are generated")
                    is_inconsistent_exists = True
                    break
                log.info("Inconsistent objects not generated")
            if not is_inconsistent_exists:
                log.error(
                    "The inconsistent object not exists and executing the further tests"
                )
                return 1
            log.info(
                "Case 11: Perform the tests after generating the inconsistent objects"
            )
            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 11: The track availability tests are failed on {pool_name}"
                )
                log.error(msg_error)
                return 1
            after_score = get_score(client, pool_name)
            if after_score is None:
                log.error("Case 11: Not able to retrieve the score")
                return 1
            if not math.isclose(before_score, after_score):
                log.error(
                    "The score before starting test is - %s and after completing the tests is -%s are not same",
                    before_score,
                    after_score,
                )
                return 1
            log.info(
                "The score before starting test is - %s and after completing the tests is -%s are same",
                before_score,
                after_score,
            )
            log.info(
                "=== Case 11:Verification of track availability tests by removing object in a OSD completed ==="
            )
        if "case12" in config.get("case_to_run"):
            log.info(
                "===Case 12: Verification of track availability tests by making PG inactive ===="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 12: The track availability tests are failed on {pool_name}"
                )
                log.error(msg_error)
                return 1
            before_score = get_score(client, pool_name)
            if before_score is None:
                log.error("Case 12: Not able to retrieve the score")
                return 1
            acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
            msg_info = (
                f"Case 12: The acting set of the {pool_name} is - {acting_pg_set}"
            )
            log.info(msg_info)
            for osd_id in acting_pg_set:
                log.info("Case 12:Stopping the %s osd in the acting pg set", osd_id)
                if not rados_object.change_osd_state(action="stop", target=osd_id):
                    log.error(f"Unable to stop the OSD : {osd_id}")
                    raise Exception("Execution error")
            log.info("All OSD are stopped in the acting PG set")
            time.sleep(10)
            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 12: The track availability tests are failed on {pool_name}"
                )
                log.error(msg_error)
                return 1
            after_score = get_score(client, pool_name)
            if after_score is None:
                log.error("Case 12: Not able to retrieve the score")
                return 1
            for osd_id in acting_pg_set:
                log.info("Case 12:Stopping the %s osd in the acting pg set", osd_id)
                if not rados_object.change_osd_state(action="start", target=osd_id):
                    log.error(f"Unable to start the OSD : {osd_id}")
                    raise Exception("Execution error")
            log.info("All OSD are started in the acting PG set")
            if math.isclose(before_score, after_score):
                log.error(
                    "The score before starting test is - %s and after completing the tests is: %s are same",
                    before_score,
                    after_score,
                )
                return 1
            if before_score < after_score:
                log.error(
                    "The score before starting test is - %s  which is greater than after test score: %s",
                    before_score,
                    after_score,
                )
                return 1
            log.info(
                "The score before starting test is - %s and after completing the tests is -%s are not same and "
                "before score is greater than after test score",
                before_score,
                after_score,
            )
            log.info(
                "=== Case 12: Verification of track availability tests by making PG inactive completed ==="
            )

        if "case13" in config.get("case_to_run"):
            log.info(
                "===Case 13: Verification of track availability tests by rebooting the osd node  ===="
            )
            assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )
            acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
            msg_info = (
                f"Case 13: The acting set of the {pool_name} is - {acting_pg_set}"
            )
            log.info(msg_info)
            osd_host = rados_object.fetch_host_node(
                daemon_type="osd", daemon_id=acting_pg_set[0]
            )
            log.info("Case 13: Rebooting the primary osd node")
            osd_host.exec_command(sudo=True, cmd="reboot", long_running=True)
            time.sleep(10)
            # Waiting for recovery to post OSD host reboot
            method_should_succeed(wait_for_clean_pg_sets, rados_object)
            log.info(
                "Case 13: PG's are active + clean post reboot of host %s",
                osd_host.hostname,
            )

            if not test_all_values(client, pool_name):
                msg_error = (
                    f"Case 13: The track availability tests are failed on {pool_name} after performing the scrub "
                    f"operation"
                )
                log.error(msg_error)
                return 1

            log.info(
                "===Case 13: Verification of track availability tests by rebooting the osd node completed  ===="
            )

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_object.delete_pool(pool=pool_name)
        mon_obj.remove_config(section="mon", name="enable_availability_tracking")
        rados_object.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_object.node)
        log.debug(
            f"Test workflow completed. Start time: {test_start_time}, End time: {test_end_time}"
        )
        if rados_object.check_crash_status(
            start_time=test_start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def create_data_pool(mon_obj, rados_object, replicated_config, pool_name):
    """
    Method is used to create the pool and push the data
    Args:
        mon_obj: Mon object
        rados_object: Rados object
        replicated_config: Replicated pool configuration
        pool_name: pool name
    Return:
        True -> Successfully created pool and pushed the data
        False-> Filed to create the pool and pushed the data
    """
    try:
        if not rados_object.create_pool(**replicated_config):
            log.error("Failed to create the replicated Pool")
            return False

        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=50000)
        msg_data_push = f"The data pushed into the {pool_name} pool"
        log.info(msg_data_push)
        return True
    except Exception as err:
        msg_error = f"Hit Exception : {err} during the creating pool"
        log.error(msg_error)
        return False


def get_pool_availability_status(client):
    """
    Method is used to get the availability status of cluster
    Args:
        client : client object
    Returns:
        available_data_in_dict :Available data in dictionary.
        For example:
              POOL             UPTIME  DOWNTIME    NUMFAILURES  MTBF  MTTR  SCORE  AVAILABLE
              .mgr                 4d     0s            0        0s    0s      1          1
          Output : [.mgr] ->['4d','0s','0','0s','0s','1','1']
    """
    cmd = "ceph osd pool availability-status"

    try:
        cmd_out_put = client.exec_command(cmd=cmd, pretty_print=True)
    except Exception as err:
        return (False, err)

    if type(cmd_out_put) is not tuple:
        return (False, "The output is not correct")

    exact_data = cmd_out_put[0]

    # Split into lines and skip the header
    lines = exact_data.strip().split("\n")[1:]
    available_data_in_dict = {}
    for line in lines:
        parts = line.split()
        pool_name = parts[0]
        available_data_in_dict[pool_name] = tuple(parts[1:])
    return (True, available_data_in_dict)


def time_to_seconds(time_str: str) -> int:
    """
    Method is used to convert time string (like '4d', '3h', '10m', '45s') to seconds.
    Args:
        time_str : Time string
    return:
        Converted time into seconds
    """
    units = {
        "d": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
    }
    if not time_str or time_str[-1] not in units:
        raise ValueError(f"Invalid time string: {time_str}")
    value = int(time_str[:-1])  # number part
    unit = units[time_str[-1]]  # suffix part
    delta = datetime.timedelta(**{unit: value})
    return int(delta.total_seconds())


def test_MTBF_value(client, pool_name):
    """
    Method is used to test the Mean Time Between Failures (MTBF) value
    Formula - Mean Time Between Failures (MTBF) = Total uptime of pool/No of Failures
    Args:
        client: Client object
        pool_name : pool name
    Returns:
        True -> if the expected MTBF is same as the actual MTBF
        False -> if the expected MTBF is not same as the actual MTBF

    """

    success, track_available_data = get_pool_availability_status(client)
    if not success:
        log.error("Failed to retrieve the pool available data")
        return False
    pool_data = track_available_data[pool_name]
    log.info(
        " Formula - Mean Time Between Failures (MTBF) = Total uptime of pool/No of Failures"
    )

    total_up_time_pool = time_to_seconds(pool_data[0])
    no_of_failures = int(pool_data[2])
    expected_MTBF = time_to_seconds(pool_data[3])
    # reducing actual output to set the lowest range
    expected_MTBF = expected_MTBF - 30

    # Remove the below 4 lines after the Tracker#71546 fix
    unit = pool_data[0][-1]
    pool_next_up_time = int(pool_data[0][:-1]) + 1
    pool_next_up_time = str(pool_next_up_time) + unit
    pool_next_up_time_seconds = time_to_seconds(pool_next_up_time)
    pool_next_MTBF_output = 0

    if no_of_failures == 0:
        actual_MTBF_output = 0
    else:
        actual_MTBF_output = math.floor(total_up_time_pool / no_of_failures)
        # Remove the below line after the Tracker#71546 fix
        pool_next_MTBF_output = math.floor(pool_next_up_time_seconds / no_of_failures)
    msg_output = f"Case 1: The MTBF actual output is {actual_MTBF_output}"
    log.info(msg_output)

    if expected_MTBF <= actual_MTBF_output <= pool_next_MTBF_output:
        return True
    msg_err = (
        f"The MTBF actual output is{actual_MTBF_output} and expected is {pool_data[3]}, "
        f"which are not same "
    )
    log.error(msg_err)
    return False


def test_MTTR_value(client, pool_name):
    """
    Method is used to test the Mean Time To Recover(MTTR) value.
    Formula - Mean Time To Recover(MTTR) = Total downtime of pool/No of Failures
    Args:
        client: Client object
        pool_name : pool name
    Returns:
        True -> if the expected MTTR is same as the actual MTTR
        False -> if the expected MTTR is not same as the actual MTTR

    """

    success, track_available_data = get_pool_availability_status(client)
    if not success:
        log.error("Failed to retrieve the pool available data")
        return False
    pool_data = track_available_data[pool_name]
    log.info(
        " Formula - Mean Time To Recover(MTTR) = Total downtime of pool/No of Failures"
    )

    total_down_time = time_to_seconds(pool_data[1])
    no_of_failures = int(pool_data[2])
    expected_MTTR = time_to_seconds(pool_data[4])
    # reducing actual output to set the lowest range
    expected_MTTR = expected_MTTR - 30

    # Remove the below 4 lines after the Tracker#71546 fix
    unit = pool_data[1][-1]
    pool_next_down_time = int(pool_data[1][:-1]) + 1
    pool_next_down_time = str(pool_next_down_time) + unit
    pool_next_down_time_seconds = time_to_seconds(pool_next_down_time)
    pool_next_MTTR_output = 0

    if no_of_failures == 0:
        actual_MTTR_output = 0
    else:
        actual_MTTR_output = math.floor(total_down_time / no_of_failures)
        # Remove the below line after the Tracker#71546 fix
        pool_next_MTTR_output = math.floor(pool_next_down_time_seconds / no_of_failures)

    msg_output = f"The MTTR actual output is {actual_MTTR_output}"
    log.info(msg_output)

    if expected_MTTR <= actual_MTTR_output <= pool_next_MTTR_output:
        return True

    msg_err = (
        f"The MTTR actual output is{actual_MTTR_output} and expected is {pool_data[3]}, "
        f"which are not same "
    )
    log.error(msg_err)
    return False


def test_score_value(client, pool_name):
    """
    Method is used to test the score value
    Formula - Score = MTBF/(MTBF+MTTR)
        Args:
            client: Client object
            pool_name : pool name
        Returns:
            True -> if the expected score is same as the actual score
            False -> if the expected score is not same as the actual score

    """

    success, track_available_data = get_pool_availability_status(client)
    if not success:
        log.error("Failed to retrieve the pool available data")
        return False
    pool_data = track_available_data[pool_name]
    MTBF = time_to_seconds(pool_data[3])
    MTTR = time_to_seconds(pool_data[4])
    expected_score = round(float(pool_data[5]), 3)

    if MTBF == 0:
        actual_score = 1
    else:
        actual_score = MTBF / (MTBF + MTTR)
    msg_output = f"The actual score output is {round(actual_score, 3)}"
    log.info(msg_output)
    tolerance = 0.10
    msg_info = f"The tolerance value is -{.10} which is 10%"
    log.info(msg_info)
    actual_score = round(actual_score, 3)
    expected_score = round(expected_score, 3)

    if abs(actual_score - expected_score) > tolerance * expected_score:
        msg_err = (
            f" The score actual output is-{actual_score} and expected is- {expected_score},"
            f"which are not same "
        )
        log.error(msg_err)
        return False
    return True


def test_all_values(client, pool_name):
    """
    Method is used to test the MTBF,MTTR and score values of a provided pool
    Args:
            client: Client object
            pool_name : pool name
    Returns:
            True -> if the expected MTBF,MTTR and score values are same as the actual values
            False -> if the expected MTBF,MTTR and score values are not same as the actual values

    """
    msg_start_test = f"Performing the MTBF test on {pool_name} pool"
    log.info(msg_start_test)
    # result_MTBF = test_MTBF_value(client, pool_name)
    if not test_MTBF_value(client, pool_name):
        msg_error = f"Verification of the MTBF failed on {pool_name} pool"
        log.error(msg_error)
        return False
    msg_success = f"Verification of the MTBF success on {pool_name} pool"
    log.info(msg_success)

    msg_start_test = f"Performing the MTTR test on {pool_name} pool"
    log.info(msg_start_test)

    if not test_MTTR_value(client, pool_name):
        msg_error = f"Verification of the MTTR failed on {pool_name} pool"
        log.error(msg_error)
        return False
    msg_success = f"Verification of the MTTR success on {pool_name} pool"
    log.info(msg_success)
    msg_start_test = f"Performing the score value test on {pool_name} pool"
    log.info(msg_start_test)

    if not test_score_value(client, pool_name):
        msg_error = f"Verification of the score value failed on {pool_name} pool"
        log.error(msg_error)
        return False
    msg_success = f"Verification of the score value success on {pool_name} pool"
    log.info(msg_success)
    return True


def cleanup_pools(rados_object, pool_names):
    """
    Method is used to clean up the multiple pool created
    Args:
         rados_object : Rados object
         pool_names : pools list
    """
    log.info("Deleting the created pools")
    for tmp_pool_name in pool_names:
        method_should_succeed(rados_object.delete_pool, tmp_pool_name)


def get_score(client, pool_name):
    """
    Method is used to get the score of the pool
    Args:
        client: clinet object
        pool_name: pool name

    Returns: None-> if the not able to get the score
             score -> Score value

    """

    success, track_available_data = get_pool_availability_status(client)
    if not success:
        log.error("Failed to retrieve the pool available data")
        return None
    pool_data = track_available_data[pool_name]
    score = round(float(pool_data[5]), 3)
    return score
