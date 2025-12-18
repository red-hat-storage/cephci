"""
This file contains various tests/ validations related to OSD crashes
Tests included:
1. Bug 2335317 - multiple OSDs crashing while replaying bluefs -- ceph_assert(delta.offset == fnode.allocated)
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Performs tests related to OSD crashes
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    if config.get("verify_allocator_corruption"):
        desc = (
            "\n#CEPH-83609785 \n"
            "Bugzilla trackers -\n"
            "7.1: BZ-2335317 \n"
            "8.0: BZ-2338096 \n"
            "8.1: BZ-2338097 \n"
            "This test is to verify whether OSDs deployed with NCB allocator file"
            "face corruption during subsequent restarts and upgrades \n"
            "1. Create a pool with single PG \n"
            "2. Obtain the acting set of the created pool \n"
            "3. Re-deploy the OSDs part of acting set after faking their underlying disk as non-rotational \n"
            "4. Check the current value of bluestore_allocation_from_file \n"
            "5. Check the current value of osd_fast_shutdown \n"
            "6. Check existence of Allocator NCB file in Bluefs files List \n"
            "7. Use fragmentation python script to write first set of fragmented objects \n"
            "8. Stop primary OSD using systemctl and verify that the last "
            "file saved during shutdown is allocator file(ino 27) \n"
            "9. Use fragmentation python script to write second set of fragmented objects \n"
            "10. Stop primary OSD using systemctl and verify the "
            "increase in number of extents for allocator file(ino 27) \n"
            "11. Use fragmentation python script to write third set of fragmented objects \n"
            "12. Stop primary OSD using systemctl and verify the "
            "increase in number of extents for allocator file(ino 27) \n"
            "13. Remove around 10K fragmented objects  at different offsets \n"
            "14. Stop primary OSD using systemctl and verify the "
            "increase in number of extents for allocator file(ino 27) \n"
            "15. Upgrade the cluster and ensure cluster Health is OK \n"
        )

        log.info(desc)
        _config = config.get("verify_allocator_corruption")
        pool_cfg = config.get("pool_config", {})
        pool_name = pool_cfg.get("pool_name", "osd-alloc-pool")
        ino_lines = []
        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        try:
            if _config.get("issue_reproduction"):
                log.info(
                    "\n \n Running test to reproduce OSD crash in case of Allocator File corruption"
                )
                log.debug("Creating pool with given config")
                # create pool with given config
                assert rados_obj.create_pool(**pool_cfg), "Pool creation failed"

                # get acting set of created pool
                acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                prim_osd = acting_set[0]
                log.debug("Acting set for pool %s : %s" % (pool_name, acting_set))

                # for each OSD in acting set, change the underlying disk type
                for _osd in acting_set:
                    rados_obj.switch_osd_device_type(osd_id=_osd, rota_val=0)
                time.sleep(5)

                # ensure acting set is same as before
                endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
                while datetime.datetime.now() < endtime:
                    new_acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                    log.debug(
                        "Acting set for pool %s after switching OSD device class: %s"
                        % (pool_name, new_acting_set)
                    )
                    if acting_set == new_acting_set:
                        break
                    log.error("Acting set has changed, retrying after 20 secs")
                    time.sleep(20)
                else:
                    log.error(
                        "Acting set for pool %s has changed.\n"
                        "Old acting set: %s | New acting set: %s"
                        % (pool_name, acting_set, new_acting_set)
                    )
                    raise Exception("Acting set for pool %s has changed" % pool_name)

                log.debug(
                    "Checking current value of bluestore_allocation_from_file & osd_fast_shutdown"
                )
                assert mon_obj.show_config(
                    daemon="osd", id=prim_osd, param="bluestore_allocation_from_file"
                ), "bluestore_allocation_from_file is not true"
                assert mon_obj.show_config(
                    daemon="osd", id=prim_osd, param="osd_fast_shutdown"
                ), "osd_fast_shutdown is not true"

                # check for existence of NCB allocator file
                # this file exists only for non-rotational OSDs
                # and is created during deployment
                log.debug(
                    "Check existence of NCB allocator file for SSD OSDs: %s"
                    % acting_set
                )
                for _osd in acting_set:
                    _cmd = f"ceph tell osd.{_osd} bluefs files list"
                    out = rados_obj.run_ceph_command(cmd=_cmd, client_exec=True)
                    assert (
                        "ALLOCATOR" in out[0]["name"]
                    ), f"ALLOCATOR file not found for OSD {_osd}"

                log.debug("Fetching script to generate fragmented objects")
                # pull script to write fragmented objects
                script_loc = (
                    "https://raw.githubusercontent.com/red-hat-storage/cephci/"
                    "main/utility/generate_frag_objs.py"
                )
                client_node.exec_command(
                    sudo=True,
                    cmd=f"curl -k {script_loc} -O",
                )
                # Setup Script pre-requisites : docopt
                client_node.exec_command(
                    sudo=True, cmd="pip3 install docopt", long_running=True
                )

                obj_sizes = [4, 8, 12]
                log.debug("Writing fragmented objects to the pool %s" % pool_name)
                for size in obj_sizes:
                    # index is calculated to verify extent count
                    # and to check the number of objects written to the pool
                    # each iteration is supposed to add 5K objects to the pool
                    index = obj_sizes.index(size) + 1
                    _exp_objs = index * 5000
                    cmd_options = f"--pool {pool_name} --create 10000 --remove 0 --size $((1024*{size}))"
                    _cmd = f"python3 generate_frag_objs.py {cmd_options}"

                    out, err = client_node.exec_command(sudo=True, cmd=_cmd)
                    log.debug(out + err)

                    # smart wait to let objects show up in cephdf
                    if not rados_obj.verify_pool_stats(
                        pool_name=pool_name, exp_objs=_exp_objs
                    ):
                        log.error("Objects in the pool are not as expected.")
                        raise Exception("Objects in the pool are not as expected.")

                    log.debug(
                        "Verify increase in extents in bluefs-log-dump "
                        "file ino 27 after addition of fragmented objects"
                    )
                    out = bluestore_obj.get_bluefs_log_dump(osd_id=prim_osd)
                    log.info("Output of ceph-bluestore-tool bluefs-log-dump: \n")
                    log.info(out)

                    for line in out.splitlines():
                        if "op_file_update  file(ino 27" in line:
                            assert (
                                f"{10000 * index} extents" in line
                            ), "expected extents not found"
                            ino_lines.append(line)

                rm_obj_sizes = [12, 8]
                log.debug("Removing fragmented objects from the pool %s" % pool_name)
                for size in rm_obj_sizes:
                    _exp_objs = _exp_objs - 5000
                    cmd_options = f"--pool {pool_name} --create 0 --remove 10000 --size $((1024*{size}))"
                    _cmd = f"python3 generate_frag_objs.py {cmd_options}"
                    out, err = client_node.exec_command(sudo=True, cmd=_cmd)
                    log.debug(out + err)

                    if not rados_obj.verify_pool_stats(
                        pool_name=pool_name, exp_objs=_exp_objs
                    ):
                        log.error("Objects in the pool are not as expected.")
                        raise Exception("Objects in the pool are not as expected.")

                log.debug(
                    "Verify increase in extents in bluefs-log-dump "
                    "file ino 27 after removal of fragmented objects"
                )
                out = bluestore_obj.get_bluefs_log_dump(osd_id=prim_osd)
                log.info("Output of ceph-bluestore-tool bluefs-log-dump: \n")
                log.info(out)
                for line in out.splitlines():
                    if "op_file_update  file(ino 27" in line:
                        assert (
                            f"{10000 * (index+1)} extents" in line
                        ), "expected extents not found"
                        ino_lines.append(line)
                log.info("Relevant log lines from bluefs_log_dump \n \n")
                log.info(ino_lines)

                # validation to check ceph assert during OSD restart after upgrading to RHCS 7.1z2
            if _config.get("check_assert"):
                # the cluster was setup for failure, now it will be upgraded to RHCS 7.1z2 which
                # does not contain the fix, expectation if that OSD part of the acting set will
                # error out with expected 'ceph_assert'
                log.info(desc)
                log.info(
                    "\n \n Running test to verify OSD recovery in case of Allocator File corruption"
                )
                log.info(
                    "now that cluster has been upgraded to the 7.1z2 build, one or more OSDs part of"
                    "the acting set would fail with ceph_assert error upon restart\n"
                )

                # get acting set of created pool
                acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                prim_osd = acting_set[0]
                log.debug("Acting set for pool %s : %s" % (pool_name, acting_set))

                try:
                    out = bluestore_obj.get_bluefs_log_dump(osd_id=prim_osd)
                    log.info("Output of ceph-bluestore-tool bluefs-log-dump: \n")
                    log.info(out)
                    for line in out.splitlines():
                        if "op_file_update  file(ino 27" in line:
                            assert "20000 extents" in line, "expected extents not found"
                            ino_lines.append(line)
                    log.info("Relevant log lines from bluefs_log_dump \n \n")
                    log.info(ino_lines)
                except Exception as Err:
                    log.error(
                        "Expected failure, OSD.%s has failed with error : \n%s"
                        % (prim_osd, str(Err))
                    )
                    log.exception(Err)
                down_osds = rados_obj.get_osd_list(status="down")
                log.info("Down OSDs: %s " % down_osds)

            if _config.get("verify_fix"):
                # now that cluster has been upgraded to the fixed build, ensure all OSDs are UP
                # and cluster is in HEALTH OK State.
                # Ensure OSD in question has recovered successfully
                log.info(desc)
                log.info(
                    "\n \n Running test to verify OSD recovery in case of Allocator File corruption"
                )
                log.info(
                    "now that cluster has been upgraded to the fixed build, ensure all OSDs are UP"
                    "and cluster is in HEALTH OK State.\n"
                    "Ensure OSD in question has recovered successfully"
                )

                # rationale: fetch the list of all OSDs that belong to device class 'ssd'
                log.debug("Getting the list of SSD device class OSDs on the cluster")
                ssd_osd_list = rados_obj.run_ceph_command(
                    cmd="ceph osd crush class ls-osd ssd", client_exec=True
                )
                # all the ssd OSDs should be up
                log.debug("Ensuring all SSD OSDs are UP")
                for _osd in ssd_osd_list:
                    osd_status, status_desc = rados_obj.get_daemon_status(
                        daemon_type="osd", daemon_id=_osd
                    )
                    log.info(
                        "OSD.%s - osd_status: %s, status_desc: %s"
                        % (_osd, osd_status, status_desc)
                    )
                    assert osd_status == 1, f"OSD.{_osd} is in down state"

                cluster_health = rados_obj.run_ceph_command(
                    cmd="ceph health detail", client_exec=True
                )
                log.info("Cluster health: \n\n %s" % cluster_health)
                assert (
                    "HEALTH_OK" in cluster_health["status"]
                ), "Cluster not in Healthy state post upgrade"

                log.info(
                    "Verification completed around recovery of failed OSD due"
                    " to corruption in Allocator file during upgrade"
                )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            # log cluster health
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )

            rados_obj.change_recovery_flags(action="unset")
            if _config.get("verify_fix"):
                rados_obj.delete_pool(pool=pool_name)
                # restore modified OSDs
                ssd_osd_list = rados_obj.run_ceph_command(
                    cmd="ceph osd crush class ls-osd ssd", client_exec=True
                )
                if ssd_osd_list:
                    for osd_id in ssd_osd_list:
                        rados_obj.switch_osd_device_type(osd_id=osd_id, rota_val=1)
                        time.sleep(5)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0
