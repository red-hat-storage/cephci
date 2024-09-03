"""
This module tests :
1. Changes to monitor config database by setting new config.
2. Verifies if the config can be reverted to any version and the config changes made are reverted.
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verifies the config change reverts in monitor configuration database changes taken from logs
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    try:
        init_config = mon_obj.get_ceph_log(count=1)[0]
        if not mon_obj.set_config(
            section="mon", name="mon_max_log_epochs", value="1000"
        ):
            log.error("Error setting config ")
            return 1
        log.info(
            f"Proceeding with reverting the last config change, selecting version: {init_config['version']}"
        )
        if not mon_obj.ceph_config_reset(version=init_config["version"]):
            log.error(
                f"Could not revert to the selected version : {init_config['version']}"
            )
            return 1

        log.info(
            "Reverted to selected version. Checking if the config value is removed"
        )
        if mon_obj.verify_set_config(
            section="mon", name="mon_max_log_epochs", value="1000"
        ):
            log.error("Config is still set after the reset")
            return 1

        test_config = mon_obj.get_ceph_log(count=1)[0]
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(section="mon", name="mon_max_log_epochs")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info(f"reverted successfully to previous versions. config log : {test_config}")

    log.info("The ceph config log is successfully updated after changes ")
    return 0
