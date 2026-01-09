"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

from ceph.ceph import Ceph
from ceph.ceph_admin.helper import check_service_exists
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    get_nvme_service_name,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id

LOG = Log(__name__)


def test_ceph_83595464(ceph_cluster, config, rbd_obj):
    """Switch mTLS to non-mTLS and Vice Versa in NVMe service.

    This test case is partially automated due to Bug,
    which doesn't hold the spec file data structure with certs.
    In this case, Invalid data structure create problematic with redeployment.

    Todo: Need to revisit once the fix is available.

    Bugzilla: https://bugzilla.redhat.com/show_bug.cgi?id=2299705

    - Get the NVMe service config.
    - Disable the mTLS setting.
    - Run NVMe CLI w/o certs, keys to validate the scenario

    Args:
        obj: HA instance object
    """
    rbd_pool = config["rbd_pool"]

    nvme_service = NVMeService(config, ceph_cluster)
    LOG.info("Deploy NVMe service")
    nvme_service.deploy()
    LOG.info("Initialize gateways")
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)
    ha.run()

    # Update the config
    config["mtls"] = False
    service_name = get_nvme_service_name(rbd_pool, config.get("gw_group", None))

    subsystem = config["subsystems"][0]
    subsystem["nqn"] += generate_unique_id(length=2)
    subsystem["ceph_cluster"] = ceph_cluster

    # Deploy/Reconfigure the service without mTLS
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()

    @retry(IOError, tries=3, delay=3)
    def redeploy_svc():
        ha.orch.op("redeploy", {"pos_args": [service_name]})
        if not check_service_exists(
            ha.orch.installer,
            service_type="nvmeof",
            service_name=service_name,
            interval=20,
            timeout=600,
        ):
            raise IOError("service check failed, Try again....")

    redeploy_svc()

    # Validate the deployment without mTLS
    ha.mtls = False
    for gw in ha.gateways:
        gw.mtls = False
        gw.setter("mtls", False)
    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)


testcases = {"CEPH-83595464": test_ceph_83595464}


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
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    cleanup-only: true                          # only for cleanup
                    rep_pool_config:
                      pool: rbd
                    install: true                               # Run SPDK with all pre-requisites
                    subsystems:                                 # Configure subsystems with all sub-entities
                      - nqn: nqn.2016-06.io.spdk:cnode3
                        serial: 3
                        bdevs:
                          count: 1
                          size: 100G
                        listener_port: 5002
                        allow_host: "*"
                    initiators:                             # Configure Initiators with all pre-req
                      - nqn: connect-all
                        listener_port: 4420
                        node: node10
                    fault-injection-methods:                # fail-tool: systemctl, nodes-to-be-failed: node names
                      - tool: systemctl
                        nodes: node6
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    try:
        # Any test case to run
        if config.get("test_case"):
            test_case_run = testcases[config["test_case"]]
            test_case_run(ceph_cluster, config, rbd_obj)
            return 0

        if config.get("install"):
            LOG.info("Deploy NVMe service")
            nvme_service = NVMeService(config, ceph_cluster)
            nvme_service.deploy()
            LOG.info("Initialize gateways")
            nvme_service.init_gateways()

        LOG.info("Initialize HA")
        ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
        ha.initialize_gateways()

        # Configure Subsystem
        LOG.info("Configure subsystems")
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        # HA failover and failback
        LOG.info("Run HA failover and failback")
        ha.run()
        LOG.info("HA failover and failback completed")
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
