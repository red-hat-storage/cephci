"""
Test Module to test functionalities of bluestore data compression.
scenario-1: Validate default compression values
scenario-2: Enable bluestore_write_v2 and validate
scenario-3: Disable bluestore_write_v2 and validate
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_bluestore_comp_enhancements_class import BluestoreDataCompression
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Module to test functionalities of bluestore data compression.
        scenario-1: Validate default compression values
        scenario-2: Enable bluestore_write_v2 and validate
        scenario-3: Disable bluestore_write_v2 and validate
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    scenarios_to_run = config.get(
        "scenarios_to_run",
        [
            "scenario-1",
            "scenario-2",
            "scenario-3",
            "scenario-4",
            "scenario-5",
            "scenario-6",
            "scenario-7",
            "scenario-8",
            "scenario-9",
        ],
    )
    compression_config = {
        "rados_obj": rados_obj,
        "mon_obj": mon_obj,
        "cephadm": cephadm,
        "client_node": client_node,
    }
    bluestore_compression = BluestoreDataCompression(**compression_config)
    try:

        log.info(
            "\n\n ************ Execution begins for bluestore data compression scenarios ************ \n\n"
        )

        if "scenario-1" in scenarios_to_run:
            log.info("STARTED: Scenario 1: Validate default compression values")
            bluestore_compression.validate_default_compression_values()
            log.info("COMPLETED: Scenario 1: Validate default compression values")

        if "scenario-2" in scenarios_to_run:
            log.info("STARTED: Scenario 2: Enable bluestore_write_v2 and validate")
            bluestore_compression.toggle_bluestore_write_v2(toggle_value="true")
            log.info("COMPELTED: Scenario 2: Enable bluestore_write_v2 and validate")

        if "scenario-3" in scenarios_to_run:
            log.info("STARTED: Scenario 3: Disable bluestore_write_v2 and validate")
            bluestore_compression.toggle_bluestore_write_v2(toggle_value="false")
            log.info("COMPELTED: Scenario 3: Disable bluestore_write_v2 and validate")

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

        # delete all rados pools
        rados_obj.rados_pool_cleanup()
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test executio
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed validation of bluestore v2 data compression.")
    return 0
