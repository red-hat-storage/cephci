"""
Module to test hard limit for pg log trimming
Bugzilla:
 - https://bugzilla.redhat.com/show_bug.cgi?id=1608060
 - https://bugzilla.redhat.com/show_bug.cgi?id=1644409
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83573252
    - https://bugzilla.redhat.com/show_bug.cgi?id=1608060
    - https://bugzilla.redhat.com/show_bug.cgi?id=1644409
    1. Create a replicated pool with default config
    2. Check the default value of osd config parameter 'osd_max_pg_log_entries'
        should default to 10K
    3. Set the OSD level flag 'pglog_hardlimit'
    4. Set OSD config parameter 'osd_pg_max_log_entries' to 2000
    5. Trigger sequential I/Os followed by OSD restart until PG log size reaches 2000
    6. Trigger background IOPS on the created pool and monitor PG log size growth and trimming
    7. Ensure pg log does not increase beyond value set in 'osd_max_pg_log_entries', i.e. 2000
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pg_trim_log_list = []
    _pool_name = "test-pg-log-limit"

    try:
        # create pool with given config
        log.info("Creating a replicated pool with default config")
        assert rados_obj.create_pool(pool_name=_pool_name, pg_num=1, pg_num_max=1)

        # set OSD log level to 10 to capture pg log trimming
        assert mon_obj.set_config(section="osd", name="debug_osd", value="10/10")

        def_value = mon_obj.get_config(section="osd", param="osd_max_pg_log_entries")
        log.info(f"Default value of parameter osd_max_pg_log_entries: {def_value}")
        assert int(def_value) == 10000

        # set pglog_hardlimit flag and 2000 value for 'osd_max_pg_log_entries'
        out, _ = cephadm.shell(args=["ceph osd set pglog_hardlimit"])
        assert mon_obj.set_config(
            section="osd", name="osd_max_pg_log_entries", value="2000"
        )

        pg_id = rados_obj.get_pgid(pool_name=_pool_name)[0]
        primary_osd = rados_obj.get_pg_acting_set(pg_num=pg_id)[0]
        log.info(f"Primary OSD for pg {pg_id}: {primary_osd}")
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=primary_osd)

        # PG log stats before test commences
        pg_query = rados_obj.run_ceph_command(cmd=f"ceph pg {pg_id} query")
        init_log_size = pg_query["info"]["stats"]["log_size"]
        log.info(f"Log size of pg {pg_id} before I/Os: {init_log_size}")

        # rados bench config for foreground I/Os
        fore_bench_cfg = {
            "pool_name": _pool_name,
            "byte_size": "10KB",
            "rados_write_duration": 90,
        }

        # while loop to decide start time of capturing OSD logs
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=1800)
        while datetime.datetime.now() < endtime:
            curr_pg_query = rados_obj.run_ceph_command(cmd=f"ceph pg {pg_id} query")
            curr_log_size = curr_pg_query["info"]["stats"]["log_size"]
            if int(curr_log_size) >= 2000:
                log.info(
                    f"PG log size is now greater than or equal to 2000: {curr_log_size}"
                )
                log.info("Fetching machine time to determine OSD log start time")
                init_time, _ = osd_host.exec_command(
                    cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True
                )
                break
            log.info(f"PG log is increasing but currently below 2000: {curr_log_size}")
            log.info("Triggering bench IOPS for 90 secs followed by recovery")
            rados_obj.bench_write(**fore_bench_cfg)
            rados_obj.change_osd_state(action="restart", target=primary_osd)
        else:
            log.error("PG logs could not increase beyond 2000 within timeout 1800 secs")
            raise Exception("PG logs could not increase beyond 2000 within timeout.")

        """ Now that PG log count has reached 2K, we trigger background IOPS for a
        duration of 120 secs and observe the trimming of these log beyond 2000."""
        # initiate background iops on the pool
        back_bench_cfg = {
            "pool_name": _pool_name,
            "byte_size": "11KB",
            "rados_write_duration": 120,
            "background": True,
        }
        rados_obj.bench_write(**back_bench_cfg)

        # while loop to monitor PG log growth and to ascertain trimming of pg logs
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=140)
        while datetime.datetime.now() < endtime:
            curr_pg_query = rados_obj.run_ceph_command(cmd=f"ceph pg {pg_id} query")
            curr_log_size = curr_pg_query["info"]["stats"]["log_size"]
            # Trimming of pg log is not absolute and immediate, therefore
            # to avoid false failures, a margin of 150 has been provided on top
            # of set limit of 2000. PG log growth beyond 2100 would suggest
            # irregular trimming.
            if int(curr_log_size) >= 2100:
                error_msg = (
                    f"Excepted trimming of PG log did not take place."
                    f"\n Current value of PG log: {curr_log_size}, ideally should"
                    f"have been below 2100."
                )
                log.error(error_msg)
                raise Exception(error_msg)
            log.info(f"Current PG log size: {curr_log_size}")
            time.sleep(8)

        end_time, _ = osd_host.exec_command(cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True)
        osd_logs = rados_obj.get_journalctl_log(
            start_time=init_time,
            end_time=end_time,
            daemon_type="osd",
            daemon_id=primary_osd,
        )

        for line in osd_logs.splitlines():
            if f"pg[{pg_id}(" in line and "calc_trim_to_aggressive" in line:
                pg_trim_log_list.append(line)

        pg_trim_log = "\n".join(pg_trim_log_list)
        log.info(
            f"\n ==========================================================================="
            f"\n PG log trimming entries in OSD.{primary_osd} log: \n {pg_trim_log}"
            f"\n ==========================================================================="
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(section="osd", name="debug_osd")
        mon_obj.remove_config(section="osd", name="osd_max_pg_log_entries")
        rados_obj.delete_pool(pool=_pool_name)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
