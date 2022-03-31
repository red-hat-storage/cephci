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

    # Setting the no-autoscale flag
    cmd = "ceph osd pool set noautoscale"
    rados_obj.run_ceph_command(cmd=cmd)

    # sleeping for 5 seconds as the command takes some time to affect the status of pools
    time.sleep(5)

    # Getting the autoscale configurations after setting the flag
    # all the pools should have autoscale set to off
    cmd = "ceph osd pool autoscale-status"
    pool_status = rados_obj.run_ceph_command(cmd=cmd)

    for entry in pool_status:
        if entry["pg_autoscale_mode"] == "on":
            log.error(f"Pg autoscaler not turned off for pool : {entry['pool_name']}")
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
        log.error("autoscale Flag not set to true upon setting the no-autoscale flag")
        return 1

    # Creating a new pool, with the flag off, new pool should be created with autoscaler profile turned off
    with open(pool_configs_path, "r") as fd:
        pool_configs = yaml.safe_load(fd)

    pool_conf = pool_configs["replicated"]["sample-pool-2"]
    create_given_pool(rados_obj, pool_conf)

    cmd = "ceph osd pool autoscale-status"
    pool_status = rados_obj.run_ceph_command(cmd=cmd)

    for entry in pool_status:
        if entry["pool_name"] == pool_conf["pool_name"]:
            if entry["pg_autoscale_mode"] == "on":
                log.error(
                    f"Pg autoscaler not turned off for the new pool : {entry['pool_name']} "
                    f"created with flag turned off"
                )
                return 1

    # Turning the autoscale flag back on. All the setting made earlier should be reverted
    cmd = "ceph osd pool unset noautoscale"
    pool_status = rados_obj.run_ceph_command(cmd=cmd)

    # sleeping for 5 seconds as the command takes some time to affect the status of pools
    time.sleep(5)

    for entry in pool_status:
        if entry["pg_autoscale_mode"] == "off":
            log.error(f"Pg autoscaler not turned on for pool : {entry['pool_name']}")
            return 1

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
        log.error("autoscale Flag not set to false upon removing the no-autoscale flag")
        return 1

    # Deleting the pool created earlier
    if not rados_obj.detete_pool(pool=pool_conf["pool_name"]):
        log.error(f"the pool {pool_conf['pool_name']} could not be deleted")
        return 1

    log.info("Autoscale flag is working as expected.")
    return 0
