"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_gateway import NVMeGateway
from tests.nvmeof.workflows.nvme_utils import delete_nvme_service, deploy_nvme_service
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_listeners(ha_obj, nodes, config):
    """Configure Listeners on subsystem."""
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
        nvmegwcli.listener.add(**listener_config)
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def configure_namespaces(nvmegwcli, config, lb_groups, sub_args, pool, ceph_cluster):
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
    nvmegwcli.host.add(
        **{"args": {**sub_args, **{"host": repr(config.get("allow_host", "*"))}}}
    )

    if config.get("hosts"):
        for host in config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = Initiator(initiator_node)
            host_nqn = initiator.nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

    # Add Namespaces
    if config.get("bdevs"):
        configure_namespaces(nvmegwcli, config, lb_groups, sub_args, pool, ceph_cluster)


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


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof Load balancing test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform scaleup and scaledown.
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
                    load_balancing:
                        - scale_down: ["node6", "node7"]             # scale down
                        - scale_up: ["node6", "node7"]               # scale up
                        - scale_up: ["node10", "node11"]               # new nodes scale up
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    initiators = config["initiators"]
    io_tasks = []
    executor = ThreadPoolExecutor()

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    try:
        # Deploy NVMe services
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

        # Configure Subsystem
        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    subsys_args["ceph_cluster"] = ceph_cluster
                    p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

        # Initiate scale-down and scale-up
        if config.get("load_balancing"):
            for lb_config in config.get("load_balancing"):
                # Scale down
                if lb_config.get("scale_down"):
                    gateway_nodes = lb_config["scale_down"]
                    # Prepare FIO Execution
                    namespaces = ha.fetch_namespaces(ha.gateways[0])
                    ha.prepare_io_execution(initiators)

                    # Check for targets at clients
                    ha.compare_client_namespace([i["uuid"] for i in namespaces])

                    # Start IO Execution
                    LOG.info("Initiating IO before scaling operations")
                    for initiator in ha.clients:
                        io_tasks.append(executor.submit(initiator.start_fio))
                    time.sleep(20)  # time sleep for IO to Kick-in

                    ha.scale_down(gateway_nodes)

                # Scale up
                if lb_config.get("scale_up"):
                    scaleup_nodes = lb_config["scale_up"]
                    gateway_nodes = config["gw_nodes"]

                    # Prepare FIO Execution
                    gw_node = get_node_by_id(ceph_cluster, config["gw_nodes"][-1])
                    gateways = NVMeGateway(gw_node, ha.mtls)
                    namespaces = ha.fetch_namespaces(gateways)
                    ha.prepare_io_execution(initiators)

                    # Check for targets at clients
                    ha.compare_client_namespace([i["uuid"] for i in namespaces])

                    # Start IO Execution
                    LOG.info("Initiating IO before scale up")
                    for initiator in ha.clients:
                        io_tasks.append(executor.submit(initiator.start_fio))
                    time.sleep(20)  # time sleep for IO to Kick-in

                    # Perform scale-up of new nodes
                    if not all(
                        [node in set(config["gw_nodes"]) for node in scaleup_nodes]
                    ):
                        # Perform scale up
                        namespaces = ha.scale_up(scaleup_nodes)
                        for scaleup_node in scaleup_nodes:
                            gw_node = get_node_by_id(ceph_cluster, scaleup_node)
                            ha.gateways.append(NVMeGateway(gw_node))
                            for subsys_args in config["subsystems"]:
                                configure_listeners(ha, [scaleup_node], subsys_args)
                        ha.validate_scaleup(scaleup_nodes, namespaces)
                    else:
                        namespaces = ha.scale_up(scaleup_nodes)
                        ha.validate_scaleup(scaleup_nodes, namespaces)
        return 0

    except Exception as err:
        LOG.error(err)
        return 1

    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)
            if io_tasks:
                LOG.info("Waiting for completion of IOs.")
                executor.shutdown(wait=True, cancel_futures=True)
