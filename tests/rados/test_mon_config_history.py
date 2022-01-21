"""
This module tests :
1. Changes to monitor config database by setting new config
2. Verifies if the config change is successfully logged into the config history and a new version is created
"""
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verifies the config change history in monitor configuration database changes
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    # getting the last config change, to which we will roll back later
    init_config = mon_obj.get_ceph_log(count=1)[0]
    log.info(
        "Config at the beginning of test. \n"
        f"Version: {init_config['version']}"
        f"Changes made: {init_config['changes']}"
    )

    log.info(
        "Setting new changes and verifying if the changes are reflected in the log"
    )
    if not mon_obj.set_config(section="osd", name="osd_max_scrubs", value="8"):
        log.error("Error setting config ")
        return 1

    # Checking the versions and changes made.
    test_config = mon_obj.get_ceph_log(count=1)[0]
    log.info(
        "Config changes made for test. \n"
        f"Version: {test_config['version']}"
        f"Changes made: {test_config['changes']}"
    )

    if not test_config["version"] > init_config["version"]:
        log.error(
            f"The log is not updated with new config changes."
            f"Version: {test_config['version']}"
        )
        return 1
    try:
        name = test_config["changes"][0].get("name")
        value = str(test_config["changes"][0].get("new_value"))
        if not name == "osd/osd_max_scrubs" and value == "8":
            log.error(
                f"The log is not updated with new config changes."
                f"Changes made: {test_config['changes']}"
            )
            return 1
    except Exception:
        log.error("The log collected does not contain the value and changes made")
        return 1

    log.info("The ceph config log is successfully updated after changes ")
    return 0
