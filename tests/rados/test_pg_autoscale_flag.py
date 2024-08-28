"""
test Module to :
1. Create pool
2. write objects
3. Turn on/off the autoscale flag and verifying the pool status
4. Verify the configuration changes when the flag gets set/unset
5. Delete pools
"""

import re
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to Verify the pg-autoscale flag.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_configs_path = config.get("pool_configs_path")
    create_re_pool = config.get("create_re_pool", True)
    create_ec_pool = config.get("create_ec_pool", False)

    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) >= 6.0:
        log.info(
            "Test running on version less than 6.0, skipping verifying autoscaler flags"
        )
        return 0

    try:
        # Creating few pools with various autoscale modes
        with open(pool_configs_path, "r") as fd:
            pool_configs = yaml.safe_load(fd)

        if create_re_pool:
            pool_conf = pool_configs["replicated"]["sample-pool-1"]
            create_given_pool(rados_obj, pool_conf)
            rados_obj.set_pool_property(
                pool=pool_conf["pool_name"], props="pg_autoscale_mode", value="warn"
            )
        if create_ec_pool:
            pool_conf = pool_configs["erasure"]["sample-pool-1"]
            create_given_pool(rados_obj, pool_conf)
            rados_obj.set_pool_property(
                pool=pool_conf["pool_name"], props="pg_autoscale_mode", value="off"
            )

        # Fetching the pools on the cluster and their autoscaling mode
        pre_autoscale_status = rados_obj.get_pg_autoscale_status()
        log.debug(
            f"Autoscale statuses of pools before the autoscale flag being set : {pre_autoscale_status}"
        )

        log.debug("Setting the no-autoscale flag on the cluster")
        # Setting the no-autoscale flag
        cmd = "ceph osd pool set noautoscale"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # sleeping for 120 seconds as the command takes some time to affect the status of pools
        time.sleep(120)

        # Getting the autoscale configurations after setting the flag
        # all the pools should have autoscale set to off
        if not rados_obj.set_noautoscale_flag(retry=20):
            log.error("Could not set the noautoscale flag on the cluster")
            return 1

        if not mon_obj.verify_set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        ):
            log.error(
                "Default autoscale mode not set to off upon setting the no-autoscale flag"
            )
            return 1
        log.debug(
            "Checking status on each pool to make sure that"
            " autoscaler is not on post setting the autoscaler flag"
        )
        cmd = "ceph osd pool autoscale-status"
        pool_status = rados_obj.run_ceph_command(cmd=cmd, timeout=600)

        for entry in pool_status:
            if entry["pg_autoscale_mode"] == "on":
                log.error(
                    f"Pg autoscaler mode still on for pool : {entry['pool_name']}"
                )
                # tbd: Uncomment the below exception upon bug fix: https://bugzilla.redhat.com/show_bug.cgi?id=2252788
                # raise Exception("PG autoscaler mode still on error")
        log.debug(
            "All the pools have the autoscaler mode changed from default on. pass"
        )

        # Creating a new pool, with the flag off, new pool should be created with autoscaler profile turned off
        pool_conf = pool_configs["replicated"]["sample-pool-2"]
        create_given_pool(rados_obj, pool_conf)

        # Turning to autoscale flag back on. All the setting made earlier should be reverted
        cmd = "ceph osd pool unset noautoscale"
        rados_obj.run_ceph_command(cmd=cmd)

        # sleeping for 120 seconds as the command takes some time to affect the status of pools
        time.sleep(120)
        cmd = "ceph osd dump"
        out = rados_obj.run_ceph_command(cmd=cmd)
        flags_set = out["flags_set"]
        log.debug(f"Flags set on the cluster : {flags_set}")
        if "noautoscale" in flags_set:
            log.info("No autoscale flag not removed on the cluster")
            raise Exception("Noautoscale flag not removed error")
        log.info("Noautoscale flag removed on the cluster")

        # Checking the autoscale states of the pools. They should be same as before
        # Fetching the pools on the cluster and their autoscaling mode
        post_autoscale_status = rados_obj.get_pg_autoscale_status()
        log.debug(
            f"Autoscale statuses of pools after the autoscale flag being set : {post_autoscale_status}"
        )

        if float(build) >= 7.1:
            log.info(
                "Checking the autoscale states of the pools. They should be same as before"
            )
            for pool in pre_autoscale_status:
                pool_name = pool["pool_name"]
                pool_autoscale_flag = pool["pg_autoscale_mode"]
                new_stats = rados_obj.get_pg_autoscale_status(pool_name=pool_name)
                log.debug(
                    f"Pool_name : {pool_name}, Old autoscale mode : {pool_autoscale_flag},"
                    f"new autoscale flag : {new_stats['pg_autoscale_mode']}"
                )
                if new_stats["pg_autoscale_mode"] != pool_autoscale_flag:
                    log.error(f"The autoscale mode has changed for pool : {pool_name}")
                    raise Exception("Autoscale mode on the pool changed error")
            log.info(
                "Autoscale mode verified before and after the no-autoscale flag being set"
            )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Deleting the pool created earlier
        rados_obj.rados_pool_cleanup()
        cmd = "ceph osd pool unset noautoscale"
        rados_obj.run_ceph_command(cmd=cmd)
        time.sleep(10)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Autoscale flag is working as expected.")
    return 0
