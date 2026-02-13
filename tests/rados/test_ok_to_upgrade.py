from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.osd_ok_to_upgrade_utils import (
    OsdOkToUpgradeCommand,
    execute_negative_scenario,
)
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test ceph osd ok-to-upgrade command.
    negative scenarios:
    - max value < 0
    - crush bucket type must be rack/chassis/host/osd, not root
    - non-existent crush bucket
    - invalid Ceph version format
    - missing required ceph_version parameter
    - crush bucket with no OSDs (has no children)
    Polarion: CEPH-83632279
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    start_time = get_cluster_timestamp(rados_obj.node)
    scenarios = config.get("scenarios", [])
    log.info("Test workflow started for osd ok-to-upgrade negative scenarios")
    log.debug(f"Start time: {start_time}, scenarios: {scenarios}")

    try:
        if len(scenarios) == 0:
            log.error("config.scenarios cannot be empty")
            return 1

        if "negative" in scenarios:
            log.info("Running negative scenarios for ceph osd ok-to-upgrade")

            # max value < 0
            log.info("Negative scenario: max value must be non-negative")
            in_osds = rados_obj.get_osd_list(status="in")
            osd_id = in_osds[0]
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    f"osd.{osd_id}", "20.3.0-3803-g63ca1ffb5a21", rados_obj=rados_obj
                ).add_max(-100),
                expected_err_substring="'max' must be non-negative",
            )
            log.debug("Passed: max=-100 rejected as expected")

            # crush bucket of type root
            log.info(
                "Negative scenario: crush bucket type must be rack/chassis/host/osd, not root"
            )
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    "default", "20.3.0-3803-g63ca1ffb5a21", rados_obj=rados_obj
                ),
                expected_err_substring="valid types are: 'rack', 'chassis', 'host' and 'osd'",
            )
            log.debug("Passed: root bucket 'default' rejected as expected")

            # bucket does not exist
            log.info("Negative scenario: non-existent crush bucket")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    "test", "20.3.0-3803-g63ca1ffb5a21", rados_obj=rados_obj
                ),
                expected_err_substring="does not exist",
            )
            log.debug("Passed: non-existent bucket rejected as expected")

            # garbage value for ceph version
            log.info("Negative scenario: invalid Ceph version format")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand("test", "garbage", rados_obj=rados_obj),
                expected_err_substring="Invalid Ceph version (short) format",
            )
            log.debug("Passed: garbage version rejected as expected")

            # empty version string
            log.info("Negative scenario: missing required ceph_version parameter")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand("test", "", rados_obj=rados_obj),
                expected_err_substring="missing required parameter ceph_version",
            )
            log.debug("Passed: empty version rejected as expected")

            # no osd in crush bucket
            log.info("Negative scenario: crush bucket with no OSDs (has no children)")
            rados_obj.run_ceph_command(cmd="ceph osd crush add-bucket rack11 rack")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    "rack11", "20.3.0-3803-g63ca1ffb5a21", rados_obj=rados_obj
                ),
                expected_err_substring="has no children",
            )
            rados_obj.run_ceph_command(cmd="ceph osd crush rm rack11")
            log.debug("Passed: empty bucket rejected and rack11 cleaned up")

            log.info("All negative scenarios completed successfully")
        else:
            log.error('config.scenarios must be valid. Allowed are ["negative"]')
            return 1

    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
        log.info("Test workflow finished; no crashes detected")
    return 0
