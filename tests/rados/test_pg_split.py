import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.rados_test_util import (
    create_pools,
    get_device_path,
    wait_for_device_rados,
    write_to_pools,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Automates PG split and merge test scenarios.
    1. Create replicated pool
    2. Verify the pg count and bulk flag
    3. Set bulk flag to true
    4. Wait for pg to be active+clean
    5. Verify new pg_num has increased and greater that initial pg_num
    6. Set bulk flag to false
    7. Wait for pg to be active+clean
    8. Verify the latest pg_num has decreased.
    9. Verify restart osd when split is in progress.
    10. Verify delete object when split is in progress.
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    cluster_nodes = ceph_cluster.get_nodes()
    timeout = config.get("timeout", 10800)
    add_network_delay = config.get("add_network_delay", False)

    try:
        if add_network_delay:
            for host in cluster_nodes:
                rados_obj.add_network_delay_on_host(
                    hostname=host.hostname, delay="5ms", set_delay=True
                )
            log.info("Added network delays on the cluster")

        log.info("Running PG split merge scenarios")
        pool = create_pools(config, rados_obj, client_node)
        pool_name = pool["pool_name"]
        should_not_be_empty(pool, "Failed to retrieve pool details")
        write_to_pools(config, rados_obj, client_node)
        log.debug("Completed writing objects into pool")
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )

        init_pg_count = rados_obj.get_pool_property(
            pool=pool["pool_name"], props="pg_num"
        )["pg_num"]
        if not pool["pg_num"] == init_pg_count:
            log.error(
                f"Actual pg_num {init_pg_count} does not match with expected pg_num {pool['pg_num']}"
            )
            raise Exception(
                f"Actual pg_num {init_pg_count} does not match with expected pg_num {pool['pg_num']}"
            )
        bulk = pool_obj.get_bulk_details(pool["pool_name"])
        if bulk:
            log.error("Expected bulk flag should be False.")
            raise Exception("Expected bulk flag should be False.")

        log.debug(
            f"Bulk flag not enabled on pool {pool['pool_name']}, Proceeding to enable bulk"
        )
        new_bulk = pool_obj.set_bulk_flag(pool["pool_name"])
        if not new_bulk:
            log.error("Expected bulk flag should be True.")
            raise Exception("Expected bulk flag should be True.")
        # Sleeping for 60 seconds for bulk flag application and PG count to be increased.
        time.sleep(60)

        pg_count_bulk_true = rados_obj.get_pool_details(pool=pool["pool_name"])[
            "pg_num_target"
        ]
        log.debug(
            f"PG count on pool {pool['pool_name']} post addition of bulk flag : {pg_count_bulk_true}"
        )
        if pg_count_bulk_true < init_pg_count:
            raise Exception(
                f"Actual pg_num {pg_count_bulk_true} is expected to be greater than {init_pg_count}"
            )
        if pool.get("restart_osd", False):
            log.debug(
                f"Proceeding to restart OSDs when PG splits are in progress for pool {pool['pool_name']}"
            )
            acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
            log.info(f"Acting set {acting_pg_set} for pool {pool['pool_name']}")
            if not acting_pg_set:
                log.error("Failed to retrieve acting pg set")
                raise Exception("Failed to retrieve acting pg set")
            for osd_id in acting_pg_set:
                if not rados_obj.change_osd_state(action="restart", target=osd_id):
                    log.error(f"Unable to restart the OSD : {osd_id}")
                    raise Exception(f"Unable to restart the OSD : {osd_id}")
            log.info("Completed reboots for OSDs during PG split scenarios")

        if pool.get("remove_add_osd", False):
            log.debug("Proceeding to add remove a OSD ")
            log.info(
                "---- Starting workflow ----\n---- Removal and addition of OSD daemons"
            )
            pg_set = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
            log.debug(f"Acting set for removal and addition of OSDs {pg_set}")
            target_osd = pg_set[0]
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)

            dev_path = get_device_path(host, target_osd)
            log.debug(
                f"osd device path  : {dev_path}, osd_id : {target_osd}, host.hostname : {host.hostname}"
            )

            utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=True)
            method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
            time.sleep(20)
            utils.osd_remove(ceph_cluster, target_osd)
            time.sleep(20)
            method_should_succeed(
                utils.zap_device, ceph_cluster, host.hostname, dev_path
            )
            method_should_succeed(
                wait_for_device_rados, host, target_osd, action="remove"
            )
            time.sleep(60)

            # Adding the removed OSD back and checking the cluster status
            utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)
            method_should_succeed(wait_for_device_rados, host, target_osd, action="add")
            time.sleep(20)

            utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=False)

        if pool.get("del_obj", False):
            log.info(
                f"Deleting objects from the pool when PG splits are in progress"
                f"\n on Pool : {pool['pool_name']}"
            )
            del_objects = [
                {"name": f"obj{i}"} for i in range(pool.get("objs_to_del", 5))
            ]
            if not pool_obj.do_rados_delete(
                pool_name=pool["pool_name"], objects=del_objects
            ):
                log.error("Failed to delete objects from pool.")
                raise Exception("Failed to delete objects from pool.")
            log.info("Completed deletion of objects from the pool")

        time.sleep(40)
        log.debug(
            f" waiting for PGs to settle down on pool {pool['pool_name']} after applying bulk flag"
        )

        endtime = datetime.datetime.now() + datetime.timedelta(seconds=10000)
        while datetime.datetime.now() < endtime:
            pool_pg_num = rados_obj.get_pool_property(
                pool=pool["pool_name"], props="pg_num"
            )["pg_num"]
            if pool_pg_num == pg_count_bulk_true:
                log.debug(f"PG count on pool {pool['pool_name']} is achieved")
                break
            log.debug(
                f"PG count on pool {pool['pool_name']} has not reached desired levels."
                f"Expected : {pg_count_bulk_true}, Current : {pool_pg_num}"
            )
            log.info("Sleeping for 20 secs and checking again")
            time.sleep(20)
        else:
            raise Exception(
                f"pg_num on pool {pool['pool_name']} did not reach the desired levels of PG count "
                f"with bulk flag enabled"
                f"Expected : {pg_count_bulk_true}"
            )
        log.info(
            "PGs increased to desired levels after application of bulk flag on the pool"
        )
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )

        log.info(
            f"Proceeding to remove the bulk flag from the pool : {pool['pool_name']}"
        )
        if not pool_obj.rm_bulk_flag(pool["pool_name"]):
            log.error(
                f"Could not remove the bulk flag from the pool {pool['pool_name']}"
            )
            raise Exception("Expected bulk flag should be False.")

        time.sleep(60)
        log.debug(
            f" waiting for PGs to settle down on pool {pool['pool_name']} after removing bulk flag"
        )
        if pool.get("check_premerge_pgs", False):
            log.debug(
                "Checking for premerge PGs in the cluster during PG autoscaler activity"
            )
            no_premerge_pgs = False
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=900)
            while end_time > datetime.datetime.now():
                flag = True
                status_report = rados_obj.run_ceph_command(
                    cmd="ceph report", client_exec=True
                )

                # Proceeding to check if all PG's are in active + clean
                for entry in status_report["num_pg_by_state"]:
                    if "premerge" in [key for key in entry["state"].split("+")]:
                        flag = False
                        log.debug(f"Observed premerge PGs : {entry['state']}")

                    if flag:
                        log.info(
                            "The recovery and back-filling of the OSD is completed"
                        )
                        no_premerge_pgs = True
                        break
                    log.info(
                        f"Waiting for active + clean and checking for premerge PGs."
                        f" Active alerts: {status_report['health']['checks'].keys()},"
                        f"PG States : {status_report['num_pg_by_state']}"
                        f" checking status again in {10} seconds"
                    )
                    time.sleep(10)
            if not no_premerge_pgs:
                log.error(
                    "Observed that there are PGs in 'premerge' state for more than 900 seconds. fail"
                    "bug fix verification : https://bugzilla.redhat.com/show_bug.cgi?id=1810949 "
                )
                raise Exception("Premerge Pgs on the cluster error")
            log.info(
                "Completed checking PG states on merge. No premerge PGs seen for extended duration"
            )

        pg_count_bulk_false = rados_obj.get_pool_details(pool=pool["pool_name"])[
            "pg_num_target"
        ]
        log.debug(
            f"PG count on pool {pool['pool_name']} post removal of bulk flag : {pg_count_bulk_false}"
        )
        if pg_count_bulk_true < pg_count_bulk_false:
            raise Exception(
                f"pg_num {pg_count_bulk_true} is expected to be greater than {pg_count_bulk_false} after bulk removal"
            )

        endtime = datetime.datetime.now() + datetime.timedelta(seconds=10000)
        while datetime.datetime.now() < endtime:
            pool_pg_num = rados_obj.get_pool_property(
                pool=pool["pool_name"], props="pg_num"
            )["pg_num"]
            if pool_pg_num == pg_count_bulk_false:
                log.debug(f"PG count on pool {pool['pool_name']} is achieved")
                break
            log.debug(
                f"PG count on pool {pool['pool_name']} has not reached desired levels."
                f"Expected : {pg_count_bulk_false}, Current : {pool_pg_num}"
            )
            log.info("Sleeping for 20 secs and checking again")
            time.sleep(20)
        else:
            raise Exception(
                f"pg_num on pool {pool['pool_name']} did not reach the desired levels of PG count"
                f"with bulk flag disabled \n Expected : {pg_count_bulk_false}"
            )
        log.info(
            "PGs decreased to desired levels after removal of bulk flag on the pool"
        )
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )

        # Checking cluster health after OSD removal
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info("Sanity check post test execution, Test complete, Pass")

        if add_network_delay:
            for host in cluster_nodes:
                rados_obj.add_network_delay_on_host(
                    hostname=host.hostname, set_delay=False
                )
            log.info("Removed the network delays on the cluster")

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info("*********** Execution of finally block starts ***********")

        if "target_osd" in locals() or "target_osd" in globals():
            active_osd_list = rados_obj.run_ceph_command(cmd="ceph osd ls")
            log.debug(f"List of active OSDs: \n{active_osd_list}")
            if target_osd not in active_osd_list:
                utils.set_osd_devices_unmanaged(
                    ceph_cluster, target_osd, unmanaged=True
                )
                utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)
                method_should_succeed(
                    wait_for_device_rados, host, target_osd, action="add"
                )
            utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=False)

        if config.get("delete_pools"):
            for name in config["delete_pools"]:
                method_should_succeed(rados_obj.delete_pool, name)
            log.info("deleted all the given pools successfully")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
