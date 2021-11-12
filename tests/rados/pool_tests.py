import datetime
import logging
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Performs various pool related validation tests
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    if config.get("ec_pool_recovery_improvement"):
        ec_config = config.get("ec_pool_recovery_improvement")
        if not rados_obj.create_erasure_pool(name="recovery", **ec_config):
            log.error("Failed to create the EC Pool")
            return 1

        if not rados_obj.bench_write(**ec_config):
            log.error("Failed to write objects into the EC Pool")
            return 1
        rados_obj.bench_read(**ec_config)
        log.info("Created the EC Pool, Finished writing data into the pool")

        # getting the acting set for the created pool
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=ec_config["pool_name"])
        if len(acting_pg_set) != ec_config["k"] + ec_config["m"]:
            log.error(
                f"acting set consists of only these : {acting_pg_set} OSD's, less than k+m"
            )
            return 1
        log.info(f" Acting set of the pool consists of OSD's : {acting_pg_set}")
        log.info(
            f"Killing m, i.e {ec_config['m']} OSD's from acting set to verify recovery"
        )
        stop_osds = [acting_pg_set.pop() for _ in range(ec_config["m"])]
        for osd_id in stop_osds:
            if not rados_obj.change_osd_state(action="stop", target=osd_id):
                log.error(f"Unable to stop the OSD : {osd_id}")
                return 1

        log.info("Stopped 'm' number of OSD's from, starting to wait for recovery")
        rados_obj.change_recover_threads(config=ec_config, action="set")

        # Sleeping for 25 seconds ( "osd_heartbeat_grace": "20" ) for osd's to be marked down
        time.sleep(25)

        # Waiting for up to 2.5 hours for the recovery to complete and PG's to enter active + Clean state
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=9000)
        while end_time > datetime.datetime.now():
            flag = True
            status_report = rados_obj.run_ceph_command(cmd="ceph report")

            # Proceeding to check if all PG's are in active + clean
            for entry in status_report["num_pg_by_state"]:
                rec = (
                    "backfilling",
                    "degraded",
                    "incomplete",
                    "recovering",
                    "recovery_wait",
                    "backfilling_wait",
                    "peered",
                    "undersized",
                )
                if any(key in rec for key in entry["state"].split("+")):
                    flag = False

            if flag:
                log.info("The recovery and back-filling of the OSD is completed")
                break
            log.info(
                f"Waiting for active + clean. Active aletrs: {status_report['health']['checks'].keys()},"
                f"PG States : {status_report['num_pg_by_state']}"
                f" checking status again in 1 minute"
            )
            time.sleep(60)

        # getting the acting set for the created pool after recovery
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=ec_config["pool_name"])
        if len(acting_pg_set) != ec_config["k"] + ec_config["m"]:
            log.error(
                f"acting set consists of only these : {acting_pg_set} OSD's, less than k+m"
            )
            return 1
        log.info(f" Acting set of the pool consists of OSD's : {acting_pg_set}")
        # Changing recovery threads back to default
        rados_obj.change_recover_threads(config=ec_config, action="rm")

        log.debug("Starting the stopped OSD's")
        for osd_id in stop_osds:
            if not rados_obj.change_osd_state(action="restart", target=osd_id):
                log.error(f"Unable to restart the OSD : {osd_id}")
                return 1

        # Sleep for 5 seconds for OSD's to join the cluster
        time.sleep(5)

        if not flag:
            log.error("The pool did not reach active + Clean state after recovery")
            return 1

        # Deleting the pool created
        if not rados_obj.detete_pool(pool=ec_config["pool_name"]):
            log.error(f"the pool {ec_config['pool_name']} could not be deleted")
            return 1

        log.info("Successfully tested EC pool recovery with K osd's surviving")
        return 0
