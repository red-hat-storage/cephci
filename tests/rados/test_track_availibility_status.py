"""
This module is to verify the -
1. The availability-status of the pool during various operations.
2. The enable_availability_tracking parameter checking
3. To clear the status by using the clear_availability_score command
"""

import datetime
import math
import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
     Polarion#CEPH-83620501
     1. Verify the Mean Time Between Failures(MTBF) of the pool
     2. Verify the Mean Time To Recover(MTTR) of the pool
     3. Verify the score of the pool
     4. Verification of the availability-status log messages
     5. Verification of the MTBF,MTTR and score during recovery
     6. Verification of the MTBF,MTTR and score during backfilling
     7. Verification of the MTBF,MTTR and score during PG split
     8. Verification of the MTBF,MTTR and score during merging pgs
     9. Verification of the MTBF,MTTR and score during balancer activity
    10. Verification of the enable_availability_tracking parameter functionality
    11. Verification of the clear_availability_score command
    12. Verification of the MTBF,MTTR and score after stopping the MON daemon
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    client = ceph_cluster.get_nodes(role="client")[0]
    pool_name = config["pool_name"]

    try:
        assert mon_obj.set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        ), "Could not set pg_autoscale mode to OFF"

        if not rados_object.create_pool(**config):
            log.error("Failed to create the replicated Pool")
            return 1

        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=50000)
        msg_data_push = f"The data pushed into the {pool_name} pool"
        log.info(msg_data_push)
        log.info(
            "==== Case 1: Verification of the Mean Time Between Failures (MTBF) value ===="
        )

        result_MTBF = test_MTBF_value(client, pool_name)
        if not result_MTBF:
            log.error("Case 1: Verification of the MTBF failed ")
            return 1

        log.info(
            "==== Case 1: Verification of the Mean Time Between Failures (MTBF) value is completed ===="
        )

        log.info(
            "==== Case 2: Verification of the Mean Time To Recover(MTTR) value ===="
        )

        result_MTTR = test_MTTR_value(client, pool_name)

        if not result_MTTR:
            log.error("Case 2: Verification of the MTTR failed ")
            return 1
        log.info(
            "==== Case 2: Verification of the Mean Time To Recover(MTTR) value is completed ===="
        )

        log.info("==== Case 3: Verification of the score value ====")
        result_score = test_score_value(client, pool_name)
        if not result_score:
            log.error("Case 3: Verification of the score value failed ")
            return 1
        log.info("==== Case 3: Verification of the score value is completed ====")

        log.info(
            "==== Case 4: Verification of the score value displayed on multiple pools ===="
        )

        success, previous_pool_data = get_pool_availability_status(client)
        if not success:
            log.error("Failed to retrieve the pool available data")
            return 1
        previous_total_pool = len(previous_pool_data)
        msg_pre_pool = (
            f"The number of  pools before starting tests are - {previous_total_pool}"
        )
        log.info(msg_pre_pool)
        new_pool_names = []
        for pool_num in range(20):
            rados_object.create_pool(pool_name=f"test_pool{pool_num}")
            new_pool_names.append(f"test_pool{pool_num}")

        success, after_pool_data = get_pool_availability_status(client)
        if not success:
            log.error("Failed to retrieve the pool available data")
            return 1
        after_total_pool = len(after_pool_data)
        msg_after_pool = (
            f" The total number of pools after creating are - {after_total_pool}"
        )
        log.info(msg_after_pool)
        if (after_total_pool - previous_total_pool) != 20:
            log.error("The total pool data is not displayed")
            return 1

        random_pool = random.choice(new_pool_names)
        msg_random_pool = f"The random pool selected is - {random_pool}"
        log.info(msg_random_pool)
        result_all_tests = test_all_values(client, random_pool)
        if not result_all_tests:
            msg_error = f" The track availability tests are failed on {random_pool}"
            log.error(msg_error)
            return 1

        msg_pool_deletion = f" The {random_pool} is selected to delete the pool "
        log.info(msg_pool_deletion)
        method_should_succeed(rados_object.delete_pool, random_pool)
        new_pool_names.remove(random_pool)
        msg_info = f"Remoed the {random_pool} from the pool list.The new pool list is-{new_pool_names}"
        log.info(msg_info)
        success, after_delete_pool_details = get_pool_availability_status(client)
        if not success:
            log.error("Failed to retrieve the pool available data")
            return 1
        if random_pool in after_delete_pool_details:
            msg_error = f" The {random_pool} pool details exists in the availability-status output"
            log.error(msg_error)
            return 1
        msg_info = (
            f" The {random_pool} pool details not exists in the availability-status output."
            f"The pool available status output is -{after_delete_pool_details}"
        )
        log.info(msg_info)
        log.info("Deleting the created pools from the cluster")
        for tmp_pool_name in new_pool_names:
            method_should_succeed(rados_object.delete_pool, tmp_pool_name)
        # Checking the log messages[TODO]

        log.info(
            "==== Case 4: Verification of the score value displayed on multiple pools  completed===="
        )
        # TODO
        log.info(
            "=== [IN-Progress] Case 5: Verification of track availability messages in the logs==="
        )

        log.info(
            "=== Case 6: Verification of track availability tests during recovery state==="
        )
        acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
        if not rados_object.change_osd_state(action="stop", target=acting_pg_set[0]):
            log.error(f"Unable to stop the OSD : {acting_pg_set[0]}")
            raise Exception("Execution error")

        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=10000)
        if not rados_object.change_osd_state(action="start", target=acting_pg_set[0]):
            log.error(f"Unable to stop the OSD : {acting_pg_set[0]}")
            raise Exception("Execution error")

        timeout = datetime.timedelta(minutes=15)
        start_time = datetime.datetime.now()
        recovering_found = False

        while datetime.datetime.now() - start_time < timeout:
            out_put = rados_object.run_ceph_command(cmd="ceph -s")
            for state in out_put["pgmap"]["pgs_by_state"]:
                if "recovering" in state["state_name"]:
                    recovering_found = True
                    break
                time.sleep(2)
            if recovering_found:
                break
        if not recovering_found:
            log.error(
                "The cluster is not in recovery state. Not executing the further tests"
            )
            return 1
        method_should_succeed(
            wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
        )
        result_all_tests = test_all_values(client, pool_name)
        if not result_all_tests:
            msg_error = f" The track availability tests are failed on {pool_name} during recovery"
            log.error(msg_error)
            return 1

        method_should_succeed(
            wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
        )

        log.info(
            "=== Case 6: Verification of track availability tests completed during recovery state==="
        )
        log.info(
            "=== Case 7: Verification of track availability tests during backfill state==="
        )

        log.info("Bring the cluster in to the backfilling state")
        pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)

        rados_object.run_ceph_command(cmd=f"ceph osd out {pg_set[0]}")
        method_should_succeed(
            wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
        )
        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=500)
        rados_object.run_ceph_command(cmd=f"ceph osd in {pg_set[0]}")

        backfill_found = False
        while datetime.datetime.now() - start_time < timeout:
            out_put = rados_object.run_ceph_command(cmd="ceph -s")
            for state in out_put["pgmap"]["pgs_by_state"]:
                if "backfilling" in state["state_name"]:
                    backfill_found = True
                    break
                time.sleep(2)
            if backfill_found:
                break
        if not backfill_found:
            log.error(
                "The cluster is not in backfill state. Not executing the further tests"
            )
            return 1
        method_should_succeed(
            wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
        )
        result_all_tests = test_all_values(client, pool_name)
        if not result_all_tests:
            msg_error = f" The track availability tests are failed on {pool_name} during backfill"
            log.error(msg_error)
            return 1

        log.info(
            "=== Case 7: Verification of track availability tests completed during backfill state==="
        )
        log.info(
            "Case 8: Verification of track availability tests after pg split and Case 9: Verification of track "
            "availability tests after pg merge is commented due to the Tracker#72745"
        )
        # log.info(
        #     "=== Case 8: Verification of track availability tests after pg split ==="
        # )
        # res, inactive_count = pool_obj.run_autoscaler_bulk_test(
        #     pool=pool_name,
        #     overwrite_recovery_threads=True,
        #     test_pg_split=True,
        # )
        # if not res:
        #     log.error(" Case 8:Failed to scale up the pool with bulk flag. Fail")
        #     return 1
        # if inactive_count > 5:
        #     log.error(
        #         " Case 8:Observed multiple PGs in inactive state during PG scale up. Fail"
        #     )
        #     return 1
        #
        # result_all_tests = test_all_values(client, pool_name)
        # if not result_all_tests:
        #     msg_error = f"  Case 8: The track availability tests are failed on {pool_name} during split"
        #     log.error(msg_error)
        #     return 1
        # log.info(
        #     "=== Case 8: Verification of track availability tests completed after pg split==="
        # )
        # log.info(
        #     "=== Case 9: Verification of track availability tests after pg merge ==="
        # )
        #
        # res, inactive_count = pool_obj.run_autoscaler_bulk_test(
        #     pool=pool_name, overwrite_recovery_threads=True, test_pg_merge=True
        # )
        # if not res:
        #     log.error("Case 9:Failed to scale up the pool with bulk flag. Fail")
        #     return 1
        # if inactive_count > 5:
        #     log.error(
        #         "Case 9: Observed multiple PGs in inactive state during PG scale down. Fail"
        #     )
        #     return 1
        #
        # result_all_tests = test_all_values(client, pool_name)
        # if not result_all_tests:
        #     msg_error = f"  Case 9: The track availability tests are failed on {pool_name} during pg merge"
        #     log.error(msg_error)
        #     return 1
        #
        # log.info(
        #     "=== Case 9: Verification of track availability tests completed after pg merge==="
        # )

        log.info(
            "=== Case 10: Verification of track availability tests by enabling balancer activity  ==="
        )
        # Check ceph balancer status
        if not rados_object.get_balancer_status():
            cmd_enable = "ceph balancer on"
            rados_object.run_ceph_command(cmd_enable)
            if not rados_object.get_balancer_status():
                log.error("The ceph balancer is false")
                return 1
        log.info("The ceph balancer is enabled")
        rados_object.reweight_crush_items()
        result_all_tests = test_all_values(client, pool_name)
        if not result_all_tests:
            msg_error = f"  Case 10: The track availability tests are failed on {pool_name} during balancer activity"
            log.error(msg_error)
            return 1
        log.info(
            "=== Case 10: Verification of track availability tests by enabling balancer activity completed ==="
        )
        log.info(
            "=== Case 11: Verification enable_availability_tracking parameter  ==="
        )

        availability_value = mon_obj.get_config(
            section="mon", param="enable_availability_tracking"
        )
        if availability_value == "false":
            log.info("The enable_availability_tracking is false by default it is true")
            return 1
        log.info("The enable_availability_tracking default value is true")

        log.info("setting the enable_availability_tracking to false")
        mon_obj.set_config(
            section="mon", name="enable_availability_tracking", value="false"
        )

        success, out_put = get_pool_availability_status(client)
        if success:
            log.error(
                " Able to retrieve the data after setting the enable_availability_tracking to false"
            )
            return 1

        availability_msg = (
            "Error ENOTSUP: availability tracking is disabled; you can enable it by setting the config "
            "option enable_availability_tracking"
        )
        if availability_msg not in str(out_put):
            msg_error = (
                f"The {availability_msg} is not displayed in the output  when the enable_availability_tracking "
                f"parameter is set to false"
            )
            log.error(msg_error)
            return 1
        msg_info = (
            f"The {availability_msg} is displayed in the output  when the enable_availability_tracking "
            f"parameter is set to false"
        )
        log.info(msg_info)
        mon_obj.set_config(
            section="mon", name="enable_availability_tracking", value="true"
        )
        result_all_tests = test_all_values(client, pool_name)
        if not result_all_tests:
            msg_error = f"  Case 8: The track availability tests are failed on {pool_name} during balancer activity"
            log.error(msg_error)
            return 1
        log.info(
            "=== Case 11: Verification enable_availability_tracking parameter completed  ==="
        )
        log.info(
            "=== Case 12: Verification clear-availability-status command on pool  ==="
        )

        cmd_clear_pool_status = f"ceph osd pool clear-availability-status {pool_name}"
        client.exec_command(cmd=cmd_clear_pool_status)
        success, out_put = get_pool_availability_status(client)
        if not success:
            log.error("Failed to retrieve the pool available data")
            return 1
        selected_pool_details = out_put[pool_name]
        if (
            int(selected_pool_details[2]) != 0
            and int(selected_pool_details[4]) != 1
            and int(selected_pool_details[5]) != 1
        ):
            msg_error = f"The clear_availability_status command  not clear the {pool_name} pool data"
            log.error(msg_error)
            return 1
        msg_info = (
            f"The clear_availability_status command clear the {pool_name} pool data"
        )
        log.info(msg_info)
        log.info(
            "=== Case 12: Verification clear-availability-status command on pool completed  ==="
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
        mon_obj.set_config(
            section="mon", name="enable_availability_tracking", value="true"
        )
    return 0


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
        cmd_out_put = client.exec_command(cmd=cmd)
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
        return 1
    pool_data = track_available_data[pool_name]
    log.info(
        " Formula - Mean Time Between Failures (MTBF) = Total uptime of pool/No of Failures"
    )

    total_up_time_pool = time_to_seconds(pool_data[0])
    no_of_failures = int(pool_data[2])
    expected_MTBF = time_to_seconds(pool_data[3])

    if no_of_failures == 0:
        actual_MTBF_output = 0
    else:
        actual_MTBF_output = math.floor(total_up_time_pool / no_of_failures)
    msg_output = f"Case 1: The MTBF actual output is {actual_MTBF_output}"
    log.info(msg_output)

    if actual_MTBF_output != expected_MTBF:
        msg_err = (
            f"The MTBF actual output is{actual_MTBF_output} and expected is {pool_data[3]}, "
            f"which are not same "
        )
        log.error(msg_err)
        return False
    return True


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
        return 1
    pool_data = track_available_data[pool_name]
    log.info(
        " Formula - Mean Time To Recover(MTTR) = Total downtime of pool/No of Failures"
    )

    total_down_time = time_to_seconds(pool_data[1])
    no_of_failures = int(pool_data[2])
    expected_MTTR = time_to_seconds(pool_data[4])

    if no_of_failures == 0:
        actual_MTTR_output = 0
    else:
        actual_MTTR_output = math.floor(total_down_time / no_of_failures)
    msg_output = f"The MTTR actual output is {actual_MTTR_output}"
    log.info(msg_output)

    if actual_MTTR_output != expected_MTTR:
        msg_err = (
            f"The MTTR actual output is{actual_MTTR_output} and expected is {pool_data[3]}, "
            f"which are not same "
        )
        log.error(msg_err)
        return False
    return True


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
        return 1
    pool_data = track_available_data[pool_name]

    total_up_time_pool = time_to_seconds(pool_data[0])
    total_down_time = time_to_seconds(pool_data[1])
    no_of_failures = int(pool_data[2])
    expected_score = float(pool_data[5])

    if no_of_failures == 0:
        actual_MTBF_output = 0
    else:
        actual_MTBF_output = math.floor(total_up_time_pool / no_of_failures)

    if no_of_failures == 0:
        actual_MTTR_output = 0
    else:
        actual_MTTR_output = math.floor(total_down_time / no_of_failures)

    if actual_MTBF_output == 0 & actual_MTTR_output == 0:
        actual_score = 1
    else:
        ratio = actual_MTBF_output / (actual_MTBF_output + actual_MTTR_output)
        actual_score = round(ratio, 4)
    msg_output = f"The actual score output is {actual_score}"
    log.info(msg_output)

    if not math.isclose(actual_score, expected_score, rel_tol=1e-4, abs_tol=1e-4):
        msg_err = (
            f" The score actual output is{actual_score} and expected is {expected_score}, "
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
    result_MTBF = test_MTBF_value(client, pool_name)
    if not result_MTBF:
        msg_error = f"Verification of the MTBF failed on {pool_name} pool"
        log.error(msg_error)
        return False
    msg_success = f"Verification of the MTBF success on {pool_name} pool"
    log.info(msg_success)

    msg_start_test = f"Performing the MTTR test on {pool_name} pool"
    log.info(msg_start_test)

    result_MTTR = test_MTTR_value(client, pool_name)
    if not result_MTTR:
        msg_error = f"Verification of the MTTR failed on {pool_name} pool"
        log.error(msg_error)
        return False
    msg_success = f"Verification of the MTTR success on {pool_name} pool"
    log.info(msg_success)
    msg_start_test = f"Performing the score value test on {pool_name} pool"
    log.info(msg_start_test)
    result_score = test_score_value(client, pool_name)
    if not result_score:
        msg_error = f"Verification of the score value failed on {pool_name} pool"
        log.error(msg_error)
        return False
    msg_success = f"Verification of the score value success on {pool_name} pool"
    log.info(msg_success)
    return True
