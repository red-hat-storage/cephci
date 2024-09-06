"""
Method to create OSDMAPs and verify automatic trimming and logging in mon logs
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-10046
    Automatic trimming of OSDMAP
    1. Create a replicated pool with default config
    2. Fetch the primary osd of first PG
    3. Retrieve initial osdmap stats
    4. Trigger background iops and create pool snapshots
    5. Monitor trimming of osdmaps for 10 mins
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    mon_trim_log_list = []
    _pool_name = "test-osdmap"
    log.info("Running test case to verify trimming of OSDMAPs")

    def manipulate_osdmaps(p_name):
        # create and delete pool snaps to increase osdmaps
        pool_obj.create_pool_snap(pool_name=p_name, count=50)
        pool_obj.delete_pool_snap(pool_name=p_name)
        time.sleep(10)

    try:
        # enable required log levels for OSD and MON
        assert mon_obj.set_config(
            section="osd", name="osd_beacon_report_interval", value="30"
        )
        assert mon_obj.set_config(section="mon", name="debug_paxos", value="10/10")
        assert mon_obj.set_config(
            section="mon", name="paxos_service_trim_min", value="100"
        )
        assert mon_obj.set_config(
            section="mon", name="mon_min_osdmap_epochs", value="100"
        )

        # create pool with given config
        log.info("Creating a replicated pool with default config")
        assert rados_obj.create_pool(pool_name=_pool_name)

        # fetch acting set and primary osd for the created pool
        acting_set = rados_obj.get_pg_acting_set(pool_name=_pool_name)
        primary_osd = acting_set[0]
        log.info(f"Acting set for {_pool_name}: {acting_set}")

        # fetch osdmap for the primary osd before starting the test
        init_osdmap = rados_obj.get_osd_status(osd_id=primary_osd)
        init_oldest_map = init_osdmap["oldest_map"]
        init_newest_map = init_osdmap["newest_map"]
        log.info(f"ceph osd status for {primary_osd}: \n {init_osdmap}")
        log.info(f"Oldest map before test for {primary_osd}: \n {init_oldest_map}")
        log.info(f"Newest map before test for {primary_osd}: \n {init_newest_map}")

        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        osdmap_first_committed = status_report["osdmap_first_committed"]
        osdmap_last_committed = status_report["osdmap_last_committed"]

        log.info(f"osdmap_first_committed from ceph report: {osdmap_first_committed}")
        log.info(f"osdmap_last_committed from ceph report: {osdmap_last_committed}")

        # initiate background iops on the pool
        bench_cfg = {
            "pool_name": _pool_name,
            "byte_size": "10KB",
            "rados_write_duration": 600,
            "background": True,
        }
        rados_obj.bench_write(**bench_cfg)

        # fetch leader mon and record logging start time
        leader_mon_daemon = rados_obj.run_ceph_command(cmd="ceph mon stat")["leader"]
        mon_host = rados_obj.fetch_host_node(
            daemon_type="mon", daemon_id=leader_mon_daemon
        )
        init_time, _ = mon_host.exec_command(cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True)

        # call method to create and remove pool snaps
        manipulate_osdmaps(p_name=_pool_name)
        time.sleep(10)

        # while loop to ascertain trimming of osdmaps
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=600)
        while datetime.datetime.now() < endtime:
            try:
                curr_osdmap = rados_obj.get_osd_status(osd_id=primary_osd)
                curr_oldest_map = curr_osdmap["oldest_map"]
                curr_status_report = rados_obj.run_ceph_command(
                    cmd="ceph report", client_exec=True
                )
                curr_osdmap_first_committed = curr_status_report[
                    "osdmap_first_committed"
                ]
            except Exception:
                log.info("Fetching OSD status failed, retrying after 15 secs")
                time.sleep(15)
                continue
            if (
                curr_oldest_map > init_oldest_map
                and curr_oldest_map == curr_osdmap_first_committed
            ):
                log.info(
                    "Current 'oldest osd map' is greater than initial 'oldest osd map"
                )
                log.info(f"{curr_oldest_map} > {init_oldest_map}")
                log.info(
                    "Current 'oldest osd map' is equal to 'osdmap first committed' "
                    "which signifies trimming has occured and completed."
                )
                log.info(f"{curr_oldest_map} == {curr_osdmap_first_committed}")
                end_time, _ = mon_host.exec_command(
                    cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True
                )
                break
            else:
                if datetime.datetime.now() >= endtime:
                    log.error("osdmaps were not trimmed even once within timeout")
                    raise Exception("osdmaps were not trimmed even once within timeout")
                log.info("Trimming is yet to occur, creating more osdmaps")
                manipulate_osdmaps(p_name=_pool_name)
                time.sleep(10)

        leader_mon_daemon = rados_obj.run_ceph_command(cmd="ceph mon stat")["leader"]
        end_time, _ = mon_host.exec_command(cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True)
        mon_logs = rados_obj.get_journalctl_log(
            start_time=init_time,
            end_time=end_time,
            daemon_type="mon",
            daemon_id=leader_mon_daemon,
        )

        for line in mon_logs.splitlines():
            if "paxosservice(osdmap" in line and (
                "maybe_trim trim" in line or "trim from" in line
            ):
                mon_trim_log_list.append(line)

        mon_trim_log = "\n".join(mon_trim_log_list)

        log.info(
            f"\n ==========================================================================="
            f"\n osdmap trimming entries in mon.{leader_mon_daemon} log: \n {mon_trim_log}"
            f"\n ==========================================================================="
        )
        log.info("Automatic trimming of OSDMAPs has been verified")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info("*********** Execution of finally block starts ***********")
        # remove all the modified configs
        mon_obj.remove_config(section="osd", name="osd_beacon_report_interval")
        mon_obj.remove_config(section="mon", name="debug_paxos")
        mon_obj.remove_config(section="mon", name="paxos_service_trim_min")
        mon_obj.remove_config(section="mon", name="mon_min_osdmap_epochs")
        # Delete the created osd pool
        rados_obj.delete_pool(pool=_pool_name)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
