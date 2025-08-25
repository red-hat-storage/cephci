import datetime
import math
import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)

    client = ceph_cluster.get_nodes(role="client")[0]

    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]

    try:
        assert mon_obj.set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        ), "Could not set pg_autoscale mode to OFF"

        if not rados_object.create_pool(**replicated_config):
            log.error("Failed to create the replicated Pool")
            return 1

        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=10000)
        msg_data_push = f"The data pushed into the {pool_name} pool"
        log.info(msg_data_push)
        log.info(
            "==== Case 1: Verification of the Mean Time Between Failures (MTBF) value ===="
        )

        result_MTBF = test_MTBF_value(client, pool_name)
        if not result_MTBF:
            log.error("Case 1: Verification of the MTBF failed ")

        log.info(
            "==== Case 1: Verification of the Mean Time Between Failures (MTBF) value is completed ===="
        )

        log.info(
            "==== Case 2: Verification of the Mean Time To Recover(MTTR) value ===="
        )

        result_MTTR = test_MTTR_value(client, pool_name)

        if not result_MTTR:
            log.error("Case 2: Verification of the MTTR failed ")

        log.info(
            "==== Case 2: Verification of the Mean Time To Recover(MTTR) value is completed ===="
        )

        log.info("==== Case 3: Verification of the score value ====")
        result_score = test_score_value(client, pool_name)
        if not result_score:
            log.error("Case 3: Verification of the score value failed ")
        log.info("==== Case 3: Verification of the score value is completed ====")

        log.info(
            "==== Case 4: Verification of the score value displayed on multiple pools ===="
        )
        previous_pool_data = get_pool_availability_status(client)
        previous_total_pool = len(previous_pool_data)
        msg_pre_pool = (
            f"The number of  pools before starting tests are - {previous_total_pool}"
        )
        log.info(msg_pre_pool)
        new_pool_names = []
        for pool_num in range(20):
            rados_object.create_pool(pool_name=f"test_pool{pool_num}")
            new_pool_names.append(f"test_pool{pool_num}")

        after_pool_data = get_pool_availability_status(client)
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
        after_delete_pool_details = get_pool_availability_status(client)
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
        # Checking the log messages

        log.info(
            "==== Case 4: Verification of the score value displayed on multiple pools  completed===="
        )

        log.info(
            "=== Case 6: Verification of track availability tests during recovery state==="
        )
        cmd_get_replicat_size = f"ceph osd pool get {pool_name} size"
        output = rados_object.run_ceph_command(cmd=cmd_get_replicat_size)
        current_replicate_size = output["size"]

        cmd_increase_replica_size = (
            f"ceph osd pool set {pool_name} size {current_replicate_size + 1} "
        )
        rados_object.run_ceph_command(cmd_increase_replica_size)
        timeout = datetime.timedelta(minutes=15)
        start_time = datetime.datetime.now()
        recovering_found = False
        while datetime.datetime.now() - start_time < timeout:
            out_put = rados_object.run_ceph_command(cmd="ceph -s")
            for state in out_put["pgmap"]["pgs_by_state"]:
                if "recovering" in state["state_name"]:
                    recovering_found = True
                    break
                if recovering_found:
                    break
                time.sleep(10)
        if not recovering_found:
            log.error(
                "The cluster is not in recovery state. Not executing the further tests"
            )
            return 1
        result_all_tests = test_all_values(client, random_pool)
        if not result_all_tests:
            msg_error = f" The track availability tests are failed on {pool_name} during recovery"
            log.error(msg_error)
            return 1
        log.info(
            "=== Case 6: Verification of track availability tests completed during recovery state==="
        )
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # if config.get("delete_pool"):
        #     method_should_succeed(rados_object.delete_pool, pool_name)
    return 0


# ceph osd pool availability-status


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
    cmd_out_put = client.exec_command(cmd=cmd)
    #
    exact_data = cmd_out_put[0]

    # Split into lines and skip the header
    lines = exact_data.strip().split("\n")[1:]
    available_data_in_dict = {}
    for line in lines:
        parts = line.split()
        pool_name = parts[0]
        available_data_in_dict[pool_name] = tuple(parts[1:])
    return available_data_in_dict


def time_to_seconds(time_str: str) -> int:
    """
    Method is used to convert time string (like '4d', '3h', '10m', '45s') to seconds.
    Args:
        time_str : Time string
    return:
        Converted time into seconds
    """
    if time_str.endswith("d"):
        return int(time_str[:-1]) * 24 * 60 * 60
    elif time_str.endswith("h"):
        return int(time_str[:-1]) * 60 * 60
    elif time_str.endswith("m"):
        return int(time_str[:-1]) * 60
    elif time_str.endswith("s"):
        return int(time_str[:-1])
    else:
        raise ValueError("Unsupported time format. Use d, h, m, or s at the end.")


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
    track_available_data = get_pool_availability_status(client)
    pool_data = track_available_data[pool_name]
    log.info(
        " Formula - Mean Time Between Failures (MTBF) = Total uptime of pool/No of Failures"
    )
    try:
        total_up_time_pool = time_to_seconds(pool_data[0])
        no_of_failures = int(pool_data[2])
        expected_MTBF = time_to_seconds(pool_data[3])
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    if no_of_failures == 0:
        actual_MTBF_output = 0
    else:
        actual_MTBF_output = math.floor(total_up_time_pool / no_of_failures)
    msg_output = f"Case 1: The MTBF actual output is {actual_MTBF_output}"
    log.info(msg_output)

    if actual_MTBF_output != expected_MTBF:
        msg_err = (
            f"Case 1: The MTBF actual output is{actual_MTBF_output} and expected is {pool_data[3]}, "
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
    track_available_data = get_pool_availability_status(client)
    pool_data = track_available_data[pool_name]
    log.info(
        " Formula - Mean Time To Recover(MTTR) = Total downtime of pool/No of Failures"
    )
    try:
        total_down_time = time_to_seconds(pool_data[1])
        no_of_failures = int(pool_data[2])
        expected_MTTR = time_to_seconds(pool_data[4])
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    if no_of_failures == 0:
        actual_MTTR_output = 0
    else:
        actual_MTTR_output = math.floor(total_down_time / no_of_failures)
    msg_output = f"Case 2: The MTTR actual output is {actual_MTTR_output}"
    log.info(msg_output)

    if actual_MTTR_output != expected_MTTR:
        msg_err = (
            f"Case 2: The MTTR actual output is{actual_MTTR_output} and expected is {pool_data[3]}, "
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
    track_available_data = get_pool_availability_status(client)
    pool_data = track_available_data[pool_name]
    try:
        total_up_time_pool = time_to_seconds(pool_data[0])
        total_down_time = time_to_seconds(pool_data[1])
        no_of_failures = int(pool_data[2])
    except ValueError as e:
        print(f"Error: {e}")
        return 1
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
        actual_score = math.floor(
            actual_MTBF_output / (actual_MTBF_output + actual_MTTR_output)
        )
    msg_output = f"The actual score output is {actual_score}"
    log.info(msg_output)

    if actual_score != actual_score:
        msg_err = (
            f" The score actual output is{actual_score} and expected is {actual_score}, "
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
