"""
Test suite that verifies NVMe alerts and NVMe Health checks.
"""

from json import loads
from random import choice

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.parallel import parallel
from tests.nvmeof.test_ceph_nvmeof_high_availability import (
    HighAvailability,
    configure_subsystems,
    deploy_nvme_service,
    teardown,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry

LOG = Log(__name__)


def test_ceph_83610948(ceph_cluster, config):
    """[CEPH-83610948] Raise a healthcheck when a gateway is in UNAVAILABLE state.

    This test case validates the warning which would be get hit on any gateway unavailability
    and Raise a healthcheck warning under ceph status and ceph health.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    rbd_pool = config["rbd_pool"]
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

    # Validate the Health warning states
    @retry(ValueError, tries=6, delay=5)
    def check_warning_status(gw, alert=True):
        health, _ = ha.orch.shell(args=["ceph", "health", "detail", "--format", "json"])
        health = loads(health)

        if not alert:
            if "NVMEOF_GATEWAY_DOWN" not in health["checks"]:
                LOG.info(
                    f"NVMEOF_GATEWAY_DOWN health check warning alerted disappeared - {health}."
                )
                if ha.check_gateway_availability(gw.ana_group_id, state="AVAILABLE"):
                    return True
            raise ValueError(f"NVMEOF_GATEWAY_DOWN warning still appears!!! - {health}")

        if alert and "NVMEOF_GATEWAY_DOWN" in health["checks"]:
            nvme_gw_down = health["checks"]["NVMEOF_GATEWAY_DOWN"]
            if HEALTH_CHECK_WARN == nvme_gw_down:
                LOG.info(
                    f"NVMEOF_GATEWAY_DOWN health check warning alerted SUCCESSFULLY - {nvme_gw_down}."
                )
                if ha.check_gateway_availability(gw.ana_group_id, state="UNAVAILABLE"):
                    return True
            raise ValueError(
                f"NVMEOF_GATEWAY_DOWN is alerted, but different messages - {nvme_gw_down}"
            )
        raise ValueError(f"NVMEOF_GATEWAY_DOWN health warning not alerted - {health}.")

    # Fail a Gateway
    fail_gw = choice(ha.gateways)
    HEALTH_CHECK_WARN = {
        "severity": "HEALTH_WARN",
        "summary": {
            "message": "1 gateway(s) are in unavailable state; gateway might be down, try to redeploy.",
            "count": 1,
        },
        "detail": [
            {
                "message": f"NVMeoF Gateway 'client.{fail_gw.daemon_name}' is unavailable."
            },
        ],
        "muted": False,
    }
    if not ha.system_control(fail_gw, action="stop", wait_for_active_state=False):
        raise Exception(f"[ {fail_gw.daemon_name} ] GW Stop failed..")
    check_warning_status(fail_gw)

    # Start GW back and alert should not be visible
    if not ha.system_control(fail_gw, action="start"):
        raise Exception(f"[ {fail_gw.daemon_name} ] GW Start failed..")
    check_warning_status(fail_gw, alert=False)

    LOG.info(
        "CEPH-83610948 - GW Unavailability healthcheck warning validated successfully."
    )


testcases = {"CEPH-83610948": test_ceph_83610948}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof HA test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform failover and failback.
    - Validate the IO continuation prior and after to failover and failback

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF deployment
                desc: Configure NVMEoF gateways and initiators
                config:
                    gw_nodes:
                     - node6
                     - node7
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    cleanup-only: true                          # only for cleanup

    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    try:
        # NVMe alert test case to run
        if config.get("test_case"):
            test_case_run = testcases[config["test_case"]]
            test_case_run(ceph_cluster, config)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
