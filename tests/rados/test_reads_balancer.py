"""
Module to test Reads balancer functionality on RHCS 7.0 and above clusters

"""

import datetime
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import OsdToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to test Reads balancer functionality on RHCS 7.0 and above clusters
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    osdtool_obj = OsdToolWorkflows(node=cephadm)
    rhbuild = config.get("rhbuild")
    client_node = ceph_cluster.get_nodes(role="client")[0]

    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) >= 7.0:
        log.info(
            "Test running on version less than 7.0, skipping verifying Reads Balancer functionality"
        )
        return 0

    try:
        log.info(
            "Starting the test to verify reads balancer functionality on RHCS cluster"
        )
        log.debug("Creating multiple pools for testing")
        rados_obj.configure_pg_autoscaler(default_mode="warn")
        time.sleep(10)
        pools = config["create_pools"]
        for each_pool in pools:
            cr_pool = each_pool["create_pool"]
            if cr_pool.get("pool_type", "replicated") == "erasure":
                method_should_succeed(
                    rados_obj.create_erasure_pool, name=cr_pool["pool_name"], **cr_pool
                )
            else:
                method_should_succeed(rados_obj.create_pool, **cr_pool)
            method_should_succeed(rados_obj.bench_write, **cr_pool)
        log.info("Completed creating pools and writing test data into the pools")

        existing_pools = rados_obj.list_pools()

        log.debug(
            "Checking the scores on each pool on the cluster, if the pool is replicated pool"
        )
        for pool_name in existing_pools:
            pool_details = rados_obj.get_pool_details(pool=pool_name)
            log.debug(f"Selected pool name : {pool_name}")
            if not pool_details["erasure_code_profile"]:
                log.debug(
                    f"Selected pool is a replicated pool. Checking read balancer scores.\n"
                    f" Fetched from pool {pool_details['read_balance']}"
                )
                read_scores = pool_details["read_balance"]
                if read_scores["score_acting"] > pool_details["size"]:
                    log.error(
                        f"The balance score on the pool is more than the size of the pool."
                        f"Balance score : {read_scores['score_acting']}, Size of poool : {pool_details['size']}"
                    )
                    log.error(
                        "This failure will be ignored because of the bug "
                        ": https://bugzilla.redhat.com/show_bug.cgi?id=2237352 and the failure will be added post fix"
                    )

                    # todo: uncomment the below exception once the bug is fixed
                    # raise Exception("Scores out of bound failure")

                log.debug("Checking if the scores are calculated properly")
                # Balance_score = raw_score / Optimal_score
                expected_score = (
                    read_scores["raw_score_acting"] / read_scores["optimal_score"]
                )
                if round(read_scores["score_acting"], 1) != round(expected_score, 1):
                    log.error(
                        f"The scores for the pool is not calculated properly. "
                        f"acting score {round(read_scores['score_acting'], 2)}"
                        f"Expected : {round(read_scores['raw_score_acting'] / read_scores['optimal_score'], 2)}"
                    )
                    raise Exception("Scores out of bound failure")
                log.info(
                    "Completed checking the balance scores for the pool. No errors"
                )
            else:
                log.info(
                    f"Selected pool {pool_name} is a EC pool, skipping check of balance scores."
                )
            log.info("Completed verification of balance scores for all the pools")

        # Trying to change the primary without setting the set-require-min-compat-client to reef. -ve test
        log.info(
            "Trying to change the primary without setting the set-require-min-compat-client to reef. -ve test"
        )
        test_pool = existing_pools[0]
        # Getting a sample PG from the pool
        pool_id = pool_obj.get_pool_id(pool_name=test_pool)
        pgid = f"{pool_id}.0"
        acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)

        log.debug(f"Acting set for PG-id {pgid} in pool {test_pool} is {acting_set}")

        new_osd = acting_set[1]
        change_cmd = f"ceph osd pg-upmap-primary {pgid} {new_osd}"
        """
        Error String seen on the cluster:
        Error EPERM: min_compat_client luminous < reef, which is required for pg-upmap-primary.
        Try 'ceph osd set-require-min-compat-client reef' before using the new interface
        """
        error_str = r"Error EPERM: min_compat_client \w+ < reef, which is required for pg-upmap-primary."

        try:
            out, err = client_node.exec_command(sudo=True, cmd=change_cmd)
        except Exception as e:
            error_message = str(e).strip()
            if not re.search(error_str, error_message):
                log.error(
                    "Correct error string not seen in stderr stream"
                    f"Expected : {error_str} \n Got : {e}"
                )
                raise Exception("Wrong error string obtained")
            log.debug(
                "Could not change the primary before setting the set-require-min-compat-client to reef. Pass"
            )
            log.error(f"An error occurred but was expected: {e}")

        log.debug(
            "Setting config to allow clients to perform read balancing on the clusters."
        )
        config_cmd = "ceph osd set-require-min-compat-client reef"
        rados_obj.run_ceph_command(cmd=config_cmd)
        time.sleep(5)

        if config.get("online_command_verification"):
            log.info(
                "Verifying the usage of online commands to change the acting set of the pool"
            )
            for pool_name in existing_pools:
                pool_details = rados_obj.get_pool_details(pool=pool_name)
                log.debug(f"Selected pool name : {pool_name}")

                # Testing on replicated pools
                if not pool_details["erasure_code_profile"]:
                    pool = pool_details["pool_name"]

                    # Fetching the acting set for the pool
                    # Getting a sample PG from the pool
                    pool_id = pool_obj.get_pool_id(pool_name=pool)
                    pgid = f"{pool_id}.0"
                    acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)

                    log.debug(
                        f"Acting set for PG-id {pgid} in pool {pool} is {acting_set}"
                    )

                    # Trying to change the primary with existing primary. -ve test
                    log.info(
                        "rying to change the primary with existing primary. -ve test"
                    )
                    new_osd = acting_set[0]
                    change_cmd = f"ceph osd pg-upmap-primary {pgid} {new_osd}"
                    error_str = (
                        f"Error EINVAL: osd.{new_osd} is already primary for pg {pgid}"
                    )

                    try:
                        out, err = client_node.exec_command(sudo=True, cmd=change_cmd)
                    except Exception as e:
                        if error_str not in str(e).strip():
                            log.error(
                                "Correct error string not seen in stderr stream"
                                f"Expected : {error_str} \n Got : {e}"
                            )
                            raise Exception("Wrong error string obtained")
                        log.debug(
                            "Could not change the primary with the existing primary. Pass"
                        )
                        log.error(f"An error occurred but was expected: {e}")

                    # Trying to set a non-existant OSD as for the PG primary. -ve test
                    log.info(
                        "Trying to set a non-existant OSD as for the PG primary. -ve test"
                    )
                    new_osd = 250
                    change_cmd = f"ceph osd pg-upmap-primary {pgid} {new_osd}"
                    error_str = f"Error ENOENT: osd.{new_osd} does not exist"
                    try:
                        out, err = client_node.exec_command(sudo=True, cmd=change_cmd)
                    except Exception as e:
                        if error_str not in str(e).strip():
                            log.error(
                                "Correct error string not seen in stderr stream"
                                f"Expected : {error_str} \n Got : {e}"
                            )
                            raise Exception("Wrong error string obtained")
                        log.error(f"An error occurred but was expected: {e}")
                        log.debug(
                            "Could not change the primary with non existent OSD. Pass"
                        )

                    # Trying to set a OSD which is not present in the acting set. -ve test
                    log.info(
                        " Trying to set a OSD which is not present in the acting set. -ve test"
                    )
                    osd_list = []
                    for node in ceph_cluster:
                        if node.role == "osd":
                            node_osds = rados_obj.collect_osd_daemon_ids(node)
                            osd_list = osd_list + node_osds

                    pgid = f"{pool_id}.0"
                    acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                    for item in osd_list:
                        if item not in acting_set:
                            new_osd = item
                            break

                    change_cmd = f"ceph osd pg-upmap-primary {pgid} {new_osd}"
                    error_str = f"Error EINVAL: osd.{new_osd} is not in acting set for pg {pgid}"
                    try:
                        out, err = client_node.exec_command(sudo=True, cmd=change_cmd)
                    except Exception as e:
                        if error_str not in str(e).strip():
                            log.error(
                                "Correct error string not seen in stderr stream"
                                f"Expected : {error_str} \n Got : {e}"
                            )
                            raise Exception("Wrong error string obtained")
                        log.error(f"An error occurred but was expected: {e}")
                        log.debug(
                            "Could not change the PG set with OSD which is not present in the acting set.. Pass"
                        )

                    # Trying to change the primary with secondary. Should be possible
                    log.info(
                        " Trying to change the primary with secondary. Should be possible"
                    )
                    pgid = f"{pool_id}.0"
                    acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                    new_osd = acting_set[1]
                    change_cmd = f"ceph osd pg-upmap-primary {pgid} {new_osd}"
                    error_str = f"change primary for pg {pgid} to osd.{new_osd}"
                    out, err = client_node.exec_command(sudo=True, cmd=change_cmd)
                    if error_str not in err.strip():
                        log.error(
                            "Correct pass string not seen in stderr stream"
                            f"Expected : {error_str} \n Got : {err}"
                        )
                        raise Exception("wrong pass string obtained")

                    log.debug("Successfully changed the PG set with OSD.. Pass")

                    # Checking the change in the new acting set
                    new_acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                    if new_acting_set[0] != acting_set[1]:
                        log.error(
                            f"The acting set has not changed."
                            f"Old AC : {acting_set}, New AC : {new_acting_set}"
                        )
                        raise Exception("AC not changed error")
                    log.info(
                        f"Acting set for PGID {pgid} changed successfully from {acting_set} to {new_acting_set}"
                    )

                    time.sleep(5)
                    # Checking if it is possible to revert the changes made previously via "pg-upmap-primary"
                    log.info(
                        "Checking if it is possible to revert the changes made previously via pg-upmap-primary"
                    )
                    rm_cmd = f"ceph osd rm-pg-upmap-primary {pgid}"
                    out, err = client_node.exec_command(sudo=True, cmd=rm_cmd)
                    # Checking the change in the new acting set
                    time.sleep(20)
                    final_acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                    if final_acting_set[0] != acting_set[0]:
                        log.error(
                            f"The acting set has not reverted to original."
                            f"Old AC : {acting_set}, final AC : {final_acting_set}"
                        )
                        raise Exception("AC not changed error")

                    log.info("Completed tests on replicated pools for online commands")
                    break
                else:
                    log.info(f"Selected pool {pool_name} is a EC pool, skipping")

            for pool_name in existing_pools:
                pool_details = rados_obj.get_pool_details(pool=pool_name)
                log.debug(f"Selected pool name : {pool_name}")
                # Testing on EC pools
                if pool_details["erasure_code_profile"]:
                    pool = pool_details["pool_name"]

                    # Fetching the acting set for the pool
                    # Getting a sample PG from the pool
                    pool_id = pool_obj.get_pool_id(pool_name=pool)
                    pgid = f"{pool_id}.0"
                    acting_set = rados_obj.get_pg_acting_set(pg_num=pgid)

                    log.debug(
                        f"Acting set for PG-id {pgid} in pool {pool} is {acting_set}"
                    )

                    # Trying to change Primary PG for a EC pool. should not be possible. -ve test
                    log.info(
                        "Trying to change Primary PG for a EC pool. should not be possible. -ve test"
                    )
                    new_osd = acting_set[1]
                    change_cmd = f"ceph osd pg-upmap-primary {pgid} {new_osd}"
                    error_str = "Error EINVAL: pg-upmap-primary is only supported for replicated pools"
                    try:
                        out, err = client_node.exec_command(sudo=True, cmd=change_cmd)
                    except Exception as e:
                        if error_str not in str(e).strip():
                            log.error(
                                "Correct error string not seen in stderr stream"
                                f"Expected : {error_str} \n Got : {e}"
                            )
                            raise Exception("Wrong error string obtained")
                        log.error(f"An error occurred but was expected: {e}")
                        log.debug("Could not change the primary for EC pools. Pass")
                    break

            log.info("Verified the Online commands to change acting set")
            return 0

        if config.get("offline_command_verification"):
            log.info(
                "Verifying the usage of offline commands to change the acting set of the pool"
            )
            for pool_name in existing_pools:
                pool_details = rados_obj.get_pool_details(pool=pool_name)
                log.debug(f"Selected pool name : {pool_name}")

                # Testing on replicated pools
                if not pool_details["erasure_code_profile"]:
                    init_read_scores = pool_details["read_balance"]
                    init_score = init_read_scores["score_acting"]
                    log.info(
                        f"Selected pool {pool_name} is a replicated pool. Read score : {init_score}\n"
                        f"initial scores Fetched from pool is {init_read_scores}"
                    )

                    # Fetching the osdmap from the cluster
                    status, osd_map_loc = osdtool_obj.generate_osdmap()
                    if not status:
                        log.error(
                            f"failed to generate osdmap file at loc : {osd_map_loc}"
                        )
                        raise Exception("osdmap file not generated")
                    log.debug(f"osdmap file generated at loc : {osd_map_loc}")

                    # Generating upmap balancer recommendations for the osdmap generated.
                    status, upmap_file_loc = osdtool_obj.apply_osdmap_upmap(
                        source_loc=osd_map_loc
                    )
                    if not status:
                        log.error(
                            f"failed to generate upmap file at loc : {upmap_file_loc}"
                        )
                        raise Exception("upmap file not generated")
                    log.debug(f"upmap file generated at loc : {upmap_file_loc}")

                    # Executing the upmap results on the cluster.
                    try:
                        client_node.exec_command(
                            sudo=True, cmd=f"source {upmap_file_loc}"
                        )
                    except Exception as e:
                        log.error(
                            f"Exception hit during setting the upmap recommendations. {e}"
                        )
                        raise Exception("upmap recommendations not set")

                    # Sleeping for 20 seconds for the upmap recommendations to take effect
                    time.sleep(20)

                    # Executing the read operation on the osdmap generated
                    status, read_file_loc = osdtool_obj.apply_osdmap_read(
                        source_loc=osd_map_loc, pool_name=pool_name
                    )
                    if not status:
                        log.error(
                            f"failed to generate read balancer file at loc : {read_file_loc}"
                        )
                        raise Exception("read balancer file not generated")

                    log.debug(f"read balancer file generated at loc : {read_file_loc}")

                    # Checking the state of pool before applying the reads balancing
                    disallowed_states = [
                        "remapped",
                        "degraded",
                        "incomplete",
                        "undersized",
                    ]
                    wait_time = 2400
                    start_time = datetime.datetime.now()
                    clean_pgs = False
                    while datetime.datetime.now() <= start_time + datetime.timedelta(
                        seconds=wait_time
                    ):
                        if rados_obj.check_pool_pg_states(
                            pool=pool_name, disallowed_states=disallowed_states
                        ):
                            log.info(f"PGs on pool : {pool_name} in clean state. ")
                            clean_pgs = True
                            break
                        log.debug(
                            f"Pool {pool_name} PGs not in active + clean state yet. sleeping for 20 seconds"
                        )
                        time.sleep(20)

                    if not clean_pgs:
                        log.error(f"{pool_name} could not reach clean state. Failure")
                        raise Exception("PGs not clean error")

                    log.info(
                        "Checking if any read recommendations are invalid, "
                        "i.e suggestions for already existing primary"
                    )
                    try:
                        valid_change = True
                        out, error = client_node.exec_command(
                            sudo=True, cmd=f"cat {read_file_loc}"
                        )
                        log.debug(f"Read file generated : {out}")
                        regx = r"ceph\sosd\spg-upmap-primary\s(\w*\.\w*)\s(\d*)"
                        items = re.findall(regx, out)
                        if items:
                            for item in items:
                                pg_set = rados_obj.get_pg_acting_set(pg_num=item[0])
                                log.debug(
                                    f"Read suggestion on PG:{item[0]} - New Suggested primary {item[1]} "
                                    f"Existing primary : {pg_set[0]}"
                                )

                                if pg_set[0] == item[1]:
                                    log.error(
                                        f"Suggested change is not valid"
                                        f"PG:{item[0]} - New Suggested primary {item[1]} "
                                        f"Existing primary : {pg_set[0]}"
                                    )
                                    valid_change = False
                        if not valid_change:
                            log.error("Changes suggested which are invalid.")
                            if float(rhbuild.split("-")[0]) >= 7.1:
                                log.error(
                                    "Still hitting the bug :  https://bugzilla.redhat.com/show_bug.cgi?id=2237574"
                                    "which was fixed in 7.1."
                                )
                                raise Exception("Invalid recommendations given")

                    except Exception as err:
                        log.error(
                            "Could not read the read suggestions from the cluster"
                            f"Proceeding further. Error hit : {err}"
                        )

                    # Executing the read results on the cluster.
                    try:
                        client_node.exec_command(
                            sudo=True, cmd=f"source {read_file_loc}"
                        )
                    except Exception as e:
                        log.error(
                            f"Exception hit during setting the read recommendations. {e}"
                        )
                        log.debug(
                            "The exception hit is expected due to bug :"
                            " https://bugzilla.redhat.com/show_bug.cgi?id=2237574."
                            " The below exception to be uncommented post bug fix."
                        )
                        if float(rhbuild.split("-")[0]) >= 7.1:
                            log.error(
                                "Still hitting the bug :  https://bugzilla.redhat.com/show_bug.cgi?id=2237574"
                                "which was fixed in 7.1."
                            )
                            raise Exception("read recommendations not set")

                    log.info(
                        f"Successfully set the read recommendations on the pool : {pool_name}"
                    )
                    time.sleep(5)
                    # There should be no data movement b/w the PGs when read balancing was performed.
                    # Checking the same if we had clean pgs to start with
                    if clean_pgs:
                        if not rados_obj.check_pool_pg_states(
                            pool=pool_name, disallowed_states=disallowed_states
                        ):
                            log.error(
                                "All the PGs are not active + clean post addition of reads balancing"
                                f"There should be no data movement. Observed with pool {pool_name}"
                            )
                            raise Exception("Data movement post reads balancing error")
                        log.debug(f"Clean PGs on pool {pool_name} post reads balancing")

                    log.debug("Sleeping for 20 seconds for scores to be updated")
                    time.sleep(20)
                    pool_details = rados_obj.get_pool_details(pool=pool_name)
                    final_read_scores = pool_details["read_balance"]
                    final_score = final_read_scores["score_acting"]
                    log.info(
                        f"After applying read balancing on pool {pool_name} the Read score is : {final_score}\n"
                        f"Final scores Fetched from pool is {final_read_scores}"
                    )

                    # The final scores should be less than or equal to initial scores
                    if init_score < final_score:
                        log.error(
                            f"The scores are worse post application of reads balancing for pool {pool_name}"
                            f"init score {init_score} ---- Final score : {final_score}"
                        )
                        raise Exception("Final scores are not correct")
                    log.info(
                        f"Completed offline tool scenario on pool {pool_name} for reads balancing"
                    )

                    # tbd: Checking the read throughput of the pools post reads balancer application

                else:
                    log.info(f"Selected pool {pool_name} is a EC pool.")

                    # Fetching the osdmap from the cluster
                    status, osd_map_loc = osdtool_obj.generate_osdmap()
                    if not status:
                        log.error(
                            f"failed to generate osdmap file at loc : {osd_map_loc}"
                        )
                        raise Exception("osdmap file not generated")
                    log.debug(f"osdmap file generated at loc : {osd_map_loc}")

                    error_str = f"{pool_name} is an erasure coded pool;"
                    # Executing the read operation on the osdmap generated
                    try:
                        osdtool_obj.apply_osdmap_read(
                            source_loc=osd_map_loc, pool_name=pool_name
                        )
                    except Exception as e:
                        if error_str not in str(e).strip():
                            log.error(
                                "Correct error string not seen in stderr stream"
                                f"Expected : {error_str} \n Got : {e}"
                            )
                            raise Exception("Wrong error string obtained")
                        log.debug(
                            "Could not apply reads balancing to EC pool via offline tool. Pass"
                        )
                        log.debug(f"An error occurred but was expected: {e}")

                    # ecpool_2 is an erasure coded pool; please try again with a replicated pool.

            log.info("Verified the Online commands to change acting set")
            return 0

    except Exception as err:
        log.error(
            f"hit Exception : {err} during the testing of reads balancer functionality"
        )
        return 1

    finally:
        log.info("\n\n\nIn the finally block of reads balancer tests\n\n\n")
        rados_obj.configure_pg_autoscaler(default_mode="on")
        if config.get("delete_pools"):
            for pool_name in config.get("delete_pools"):
                rados_obj.delete_pool(pool=pool_name)
        time.sleep(60)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
