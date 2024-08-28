"""
The program verifies the CPU and memory consumption of OSD during the scheduled scrub
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    scrub_object = RadosScrubber(node=cephadm)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    ceph_nodes = kw.get("ceph_nodes")
    # Storing the pg dump log before setting the scrub parameters
    before_scrub_log = scrub_object.get_pg_dump("pgid", "last_scrub_stamp")
    scrub_check_flag = True

    osd_scrub_min_interval = 120
    osd_scrub_max_interval = 3600

    try:
        # creating pool
        pools = config["create_pools"]
        cr_pool = pools[0]["create_pool"]
        pool_name = config["create_pools"][0]["create_pool"]["pool_name"]
        log.info(f"Creating the {pool_name}")
        method_should_succeed(rados_object.create_pool, **cr_pool)
        log.info(f"The {pool_name} pool created")
        log.info(f"writing test data into the {pool_name}pool")
        method_should_succeed(rados_object.bench_write, **cr_pool)
        log.info(f"writing test data into the {pool_name}pool is completed")
        (
            scrub_begin_hour,
            scrub_begin_weekday,
            scrub_end_hour,
            scrub_end_weekday,
        ) = scrub_object.add_begin_end_hours(0, 1)

        scrub_object.set_osd_configuration(
            "osd_scrub_begin_week_day", scrub_begin_weekday
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_week_day", scrub_begin_weekday
        )
        scrub_object.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
        scrub_object.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
        scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)
        scrub_object.set_osd_configuration(
            "osd_scrub_min_interval", osd_scrub_min_interval
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_max_interval", osd_scrub_max_interval
        )

        # Scheduled scrub verification
        endTime = datetime.datetime.now() + datetime.timedelta(minutes=60)
        while datetime.datetime.now() <= endTime:
            after_scrub_log = scrub_object.get_pg_dump("pgid", "last_scrub_stamp")
            scrub_status = scrub_object.verify_scrub_deepscrub(
                before_scrub_log, after_scrub_log, "scrub"
            )
            if scrub_status == 0:
                scrub_check_flag = False
                log.info(f'{"Scrubbing is in progress.."}')
                # After scheduled scrub starts, and for the next thirty minutes CPU and memory verification
                # will get continue.
                time_execution = datetime.datetime.now() + datetime.timedelta(
                    minutes=30
                )

                while datetime.datetime.now() < time_execution:
                    # Get the memory usage of the OSDs
                    for node in ceph_nodes:
                        if node.role == "osd":
                            node_osds = rados_object.collect_osd_daemon_ids(node)
                            for osd_id in node_osds:
                                # Get the OSD memory
                                osd_memory_usage = rados_object.get_osd_memory_usage(
                                    node, osd_id
                                )
                                time.sleep(3)
                                if osd_memory_usage > 80:
                                    log.error(
                                        f"The memory usage on the {node.hostname} -{osd_id} is {osd_memory_usage} "
                                    )
                                    return 1
                                log.info(
                                    f"The memory usage on the {node.hostname} -{osd_id} is {osd_memory_usage} "
                                )
                                # Get the OSD CPU usage
                                osd_cpu_usage = rados_object.get_osd_cpu_usage(
                                    node, osd_id
                                )
                                if osd_cpu_usage > 80:
                                    log.error(
                                        f"The cpu usage on the {node.hostname} -{osd_id} is {osd_cpu_usage} "
                                    )
                                    return 1
                                log.info(
                                    f"The cpu usage on the {node.hostname} -{osd_id} is {osd_cpu_usage} "
                                )
                            log.info(
                                f"Memory and CPU check completed on the node-{node.hostname}."
                            )
                    log.info("Memory and CPU usage verification are in progress")
                    # intentional sleep for 10 seconds  after calculating memory and CPU usage.
                    time.sleep(10)
                # Break after the verification of 30 minutes
                break
            log.info("Scrub not started.Wait for 30 seconds")
            time.sleep(30)
        if scrub_check_flag:
            log.error("The scrub not initiated on the cluster")
            return 1
    except Exception as er:
        log.error(f"Exception hit while command execution. {er}")
        return 1
    finally:
        log.info("\n--Executing finally block--\n")
        log.info(f"Deleting the {pool_name} pool")
        method_should_succeed(rados_object.delete_pool, pool_name)
        log.info(f"{pool_name} pool is deleted")
        log.info("Setting the parameters in to default  values")
        set_default_params(mon_obj)

        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    return 0


def set_default_params(mon_object):
    """
    Used to set the default osd scrub parameter value
    Args:
        mon_object: Monitor Object

    Returns : None
    """
    mon_object.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_object.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_object.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_object.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_object.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_object.remove_config(section="osd", name="osd_scrub_end_hour")
