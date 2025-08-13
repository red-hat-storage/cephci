"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import check_service_exists
from ceph.nvmeof.initiators.linux import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
    deploy_nvme_service,
    get_nvme_service_name,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_listeners(ha_obj, nodes, config, action="add"):
    """Configure Listeners on subsystem.

    Args:
        ha_obj: HA object
        nodes: List of GW nodes
        config: listener config
        action: listener add, del
    """
    lb_group_ids = {}
    for node in nodes:
        nvmegwcli = ha_obj.check_gateway(node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        listener_config = {
            "args": {
                "subsystem": config["nqn"],
                "traddr": nvmegwcli.node.ip_address,
                "trsvcid": config["listener_port"],
                "host-name": hostname,
            }
        }
        method = fetch_method(nvmegwcli.listener, action)
        method(**listener_config)
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def configure_subsystems(pool, ha, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": config["nqn"]}
    ceph_cluster = config["ceph_cluster"]

    nvmegwcli = ha.gateways[0]
    # Add Subsystem
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    "enable-ha": config.get("enable_ha", False),
                    **(
                        {"no-group-append": config.get("no-group-append", True)}
                        if ceph_cluster.rhcs_version >= "8.0"
                        else {}
                    ),
                },
            }
        }
    )

    # Add Listeners
    listeners = [nvmegwcli.node.hostname]
    if config.get("listeners"):
        listeners = config["listeners"]
    lb_groups = configure_listeners(ha, listeners, config)

    # Add Host access
    if config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(config["allow_host"])}}}
        )

    if config.get("hosts"):
        for host in config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = Initiator(initiator_node)
            host_nqn = initiator.nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

    # Add Namespaces
    if config.get("bdevs"):
        bdev_configs = config["bdevs"]
        if isinstance(config["bdevs"], dict):
            bdev_configs = [config["bdevs"]]
        for bdev_cfg in bdev_configs:
            name = generate_unique_id(length=4)
            namespace_args = {
                **sub_args,
                **{
                    "rbd-pool": pool,
                    "rbd-create-image": True,
                    "size": bdev_cfg["size"],
                },
            }
            with parallel() as p:
                # Create namespace in gateway
                for num in range(bdev_cfg["count"]):
                    ns_args = deepcopy(namespace_args)
                    ns_args["rbd-image"] = f"{name}-image{num}"
                    if bdev_cfg.get("lb_group"):
                        lbgid = lb_groups[
                            get_node_by_id(ceph_cluster, bdev_cfg["lb_group"]).hostname
                        ]
                        ns_args["load-balancing-group"] = lbgid
                    ns_args = {"args": ns_args}
                    p.spawn(nvmegwcli.namespace.add, **ns_args)


def disconnect_initiator(ceph_cluster, node):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(ceph_cluster, initiator_cfg["node"])

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def test_ceph_83595464(ceph_cluster, config):
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

    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)
    ha.run()

    # Update the config
    config["mtls"] = False
    service_name = get_nvme_service_name(rbd_pool, config.get("gw_group", None))

    subsystem = config["subsystems"][0]
    subsystem["nqn"] += generate_unique_id(length=2)
    subsystem["ceph_cluster"] = ceph_cluster

    # Deploy/Reconfigure the service without mTLS
    deploy_nvme_service(ceph_cluster, config)

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
    configure_subsystems(rbd_pool, ha, subsystem)


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
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    try:
        # Any test case to run
        if config.get("test_case"):
            test_case_run = testcases[config["test_case"]]
            test_case_run(ceph_cluster, config)
            return 0

        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
        ha.initialize_gateways()

        # Configure Subsystem
        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    subsys_args["ceph_cluster"] = ceph_cluster
                    p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

        # HA failover and failback
        ha.run()
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
