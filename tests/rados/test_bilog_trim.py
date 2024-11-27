"""
Tier-2 test to ensure PG deep-scrub does not report inconsistent PGs
when radosgw bilog trimming occurs on PGs where secondary OSD is down
Customer Bug: 2056818 - [GSS][RADOS][RGW] Scrub errors (omap_digest_mismatch) on
PGs of RGW metadata pools after upgrade to RHCS 5

Test needs to run as part of rgw/tier-2_rgw_rados_multisite_ecpool.yaml
as it needs a rgw multisite setup along with data present in RGW pools
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils as rados_utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83575437
    # BZ-2056818
    This test is to verify no issues are observed during deep-scrub when rgw bilog
    trimming occurs with OSD down
    Steps:
    Ref:
    - https://bugzilla.redhat.com/show_bug.cgi?id=2056818#c42
    - https://bugzilla.redhat.com/show_bug.cgi?id=2056818#c50
    1. Deploy minimal RGW multisite setup
    2. Add data to both the sites in S3 buckets and let the minimal client IO running
    3. Keep running the deep-scrub for index pool PGs
    4. Set noout flag
    5. stop the secondary OSD on the primary site(site1)
    6. run the following command
       radosgw-admin bilog autotrim
    7. Once the above command finishes, start the primary OSD that was stopped in step 4
    8. Retrigger the deep-scrub for eight PGs in index pool
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    rgw_node = ceph_cluster.get_nodes(role="rgw")[0]

    try:
        # to-do: Write a module to trigger background RGW IOs
        """
        # start background IOs using RGW
        log.info("Starting background IOs with RGW client")
        cfg_path = (
            "/home/cephuser/rgw-ms-tests/ceph-qe-scripts/rgw/v2/tests/"
            + "s3_swift/multisite_configs/test_Mbuckets_with_Nobjects.yaml"
        )

        script_path = (
            "/home/cephuser/rgw-ms-tests/ceph-qe-scripts/rgw/v2/tests/"
            + "s3_swift/test_Mbuckets_with_Nobjects.py"
        )

        rgw_node_ip = rgw_node.ip_address

        # modify object count to 1000
        _cmd = f"sed -i 's/100/1000/g' {cfg_path}"
        rados_obj.client.exec_command(cmd=_cmd, sudo=True)

        run_cmd = f"sudo venv/bin/python {script_path} -c {cfg_path} --rgw-node {rgw_node_ip} &> /dev/null &"
        rados_obj.client.exec_command(cmd=run_cmd, sudo=True, check_ec=False)
        """

        # set noout flag
        if not rados_utils.configure_osd_flag(ceph_cluster, action="set", flag="noout"):
            log.error("Could not set noout flag on the cluster")
            raise Exception("Could not set noout flag on the cluster")

        # get acting set for the rgw index pool
        _pool_name = "primary.rgw.buckets.index"
        acting_set = rados_obj.get_pg_acting_set(pool_name=_pool_name)
        log.info(f"Acting set for pool {_pool_name}: {acting_set}")

        # secondary osd of rgw index pool
        second_osd = acting_set[1]

        # trigger deep-scrub on the rgw index pool
        rados_obj.run_deep_scrub(pool=_pool_name)

        if not rados_obj.change_osd_state(action="stop", target=int(second_osd)):
            log.error(f"Could not stop OSD.{second_osd}")
            raise Exception(f"Could not stop OSD.{second_osd}")

        # for a duration of 7 mins, trigger bi-log trimming along with deep-scrub
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=420)
        while datetime.datetime.now() < timeout_time:
            rados_obj.client.exec_command(cmd="radosgw-admin bilog autotrim", sudo=True)
            rados_obj.run_deep_scrub(pool=_pool_name)
            time.sleep(30)

        # start the stopped OSDs
        if not rados_obj.change_osd_state(action="start", target=int(second_osd)):
            log.error(f"Could not start OSD.{second_osd}")
            raise Exception(f"Could not start OSD.{second_osd}")

        # triggering deep scrub on the rgw index pool now that OSDs have been started
        rados_obj.run_deep_scrub(pool=_pool_name)
        pg_id = rados_obj.get_pgid(pool_name=_pool_name)[0]
        if not rados_obj.start_check_deep_scrub_complete(pg_id=pg_id):
            log.error(f"PG {pg_id} could not be deep-scrubbed in time")
            raise

        # not warning should show up in cluster health
        health_detail, _ = cephadm.shell(args=["ceph health detail"])
        log.info(f"Cluster health: \n {health_detail}")
        assert (
            "inconsistent" not in health_detail
        ), "'inconsistent' health warning unexpected"
        assert (
            "scrub errors" not in health_detail
        ), "scrub errors reported in cluster health"
        assert "data damage" not in health_detail, "cluster health reported data damage"

        log.info(
            "Verification completed, no issues observed during scrubbing and bi-log trimming "
            "in a cluster with down OSDs"
        )
    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # unset noout flag
        if not rados_utils.configure_osd_flag(
            ceph_cluster, action="unset", flag="noout"
        ):
            log.error("Could not unset noout flag on the cluster")
            return 1

        # start the stopped OSDs
        if "second_osd" in locals() or "second_osd" in globals():
            if not rados_obj.change_osd_state(action="start", target=int(second_osd)):
                log.error(f"Could not start OSD.{second_osd}")
                return 1

        # wait for active+clean PGs
        if not wait_for_clean_pg_sets(rados_obj, timeout=600):
            log.error("Cluster cloud not reach active+clean state within 600 secs")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
