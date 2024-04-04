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
    pool_configs_path = "conf/pacific/rados/test-confs/pool-configurations.yaml"

    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) > 5.0:
        log.info(
            "Test running on version less than 5.1, skipping verifying autoscaler flags"
        )
        return 0

    try:
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

        if not mon_obj.verify_set_config(
            section="mgr", name="mgr/pg_autoscaler/noautoscale", value="true"
        ):
            log.error(
                "autoscale Flag not set to true upon setting the no-autoscale flag"
            )
            return 1

        # Creating a new pool, with the flag off, new pool should be created with autoscaler profile turned off
        with open(pool_configs_path, "r") as fd:
            pool_configs = yaml.safe_load(fd)

        pool_conf = pool_configs["replicated"]["sample-pool-2"]
        create_given_pool(rados_obj, pool_conf)

        # Turning the autoscale flag back on. All the setting made earlier should be reverted
        cmd = "ceph osd pool unset noautoscale"
        rados_obj.run_ceph_command(cmd=cmd)

        # sleeping for 120 seconds as the command takes some time to affect the status of pools
        time.sleep(120)

        if not mon_obj.verify_set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="on"
        ):
            log.error(
                "Default autoscale mode not set to true upon removing the no-autoscale flag"
            )
            return 1

        if not mon_obj.verify_set_config(
            section="mgr", name="mgr/pg_autoscaler/noautoscale", value="false"
        ):
            log.error(
                "autoscale Flag not set to false upon removing the no-autoscale flag"
            )
            return 1

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(
            section="global", name="osd_pool_default_pg_autoscale_mode"
        )
        mon_obj.remove_config(section="mgr", name="mgr/pg_autoscaler/noautoscale")
        # Deleting the pool created earlier
        if not rados_obj.delete_pool(pool=pool_conf["pool_name"]):
            log.error(f"the pool {pool_conf['pool_name']} could not be deleted")
            return 1

        # log cluster health
        rados_obj.log_cluster_health()

    log.info("Autoscale flag is working as expected.")
    return 0
