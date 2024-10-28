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


def parse_namespaces(config, namespaces):
    all_namespaces = []
    for subsystem in config["subsystems"]:
        sub_name = subsystem["nqn"]
        for ns in namespaces:
            # <subsystem>|<nsid>|<pool_name>|<image>
            ns_info = f"nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{ns['rbd_image_name']}"
            all_namespaces.append(f"{sub_name}|{ns_info}")
    return all_namespaces


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof Load balancing test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform scaleup and scaledown.
    - Validate the IO continuation prior and after to scaleup and scaledown

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
        gw_nodes = ha.gateways

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
                    gateway_nodes_to_be_deployed = lb_config["scale_down"]
                    LOG.info(f"Started scaling down {gateway_nodes_to_be_deployed}")

                    # Prepare FIO Execution
                    namespaces = ha.fetch_namespaces(ha.gateways[0])
                    ha.prepare_io_execution(initiators)

                    # Check for targets at clients
                    ha.compare_client_namespace([i["uuid"] for i in namespaces])

                    # Start IO Execution
                    LOG.info("Initiating IO before scale down")
                    for initiator in ha.clients:
                        io_tasks.append(executor.submit(initiator.start_fio))
                    time.sleep(20)  # time sleep for IO to Kick-in

                    ha.scale_down(gateway_nodes_to_be_deployed)

                # Scale up
                if lb_config.get("scale_up"):
                    scaleup_nodes = lb_config["scale_up"]
                    gateway_nodes = config["gw_nodes"]
                    LOG.info(f"Started scaling up {scaleup_nodes}")

                    # Prepare FIO execution for existing namespaces
                    old_namespaces = ha.fetch_namespaces(ha.gateways[0])
                    ha.prepare_io_execution(initiators)

                    # Start IO Execution into already existing namespaces/nodes
                    LOG.info("Initiating IO before scale up ")
                    for initiator in ha.clients:
                        io_tasks.append(executor.submit(initiator.start_fio))
                    time.sleep(20)  # time sleep for IO to Kick-in

                    # Perform scale-up of new nodes
                    if not all([node in set(gateway_nodes) for node in scaleup_nodes]):

                        # Perform scale up
                        old_namespaces = parse_namespaces(config, old_namespaces)
                        ha.scale_up(scaleup_nodes, gw_nodes, old_namespaces)

                        # Add listeners and namespaces to newly added GWs
                        LOG.info(f"Adding listeners for {scaleup_nodes}")
                        for subsys_args in config["subsystems"]:
                            sub_args = {"subsystem": subsys_args["nqn"]}
                            lb_groups = configure_listeners(
                                ha, scaleup_nodes, subsys_args
                            )

                        # Create new namespaces to newly added GWs that will take ANA_GRP of new GWs
                        LOG.info(f"Adding namespaces for {scaleup_nodes}")
                        for subsys_args in config["subsystems"]:
                            sub_args = {"subsystem": subsys_args["nqn"]}
                            configure_namespaces(
                                ha.gateways[-1],
                                subsys_args,
                                lb_groups,
                                sub_args,
                                rbd_pool,
                                ceph_cluster,
                            )

                        # Prepare FIO Execution for new namespaces
                        ha.prepare_io_execution(initiators)
                        new_namespaces = ha.fetch_namespaces(ha.gateways[-1])

                        # Check for targets at clients for new namespaces
                        ha.compare_client_namespace([i["uuid"] for i in new_namespaces])

                        # Start IO Execution for new namespaces
                        for initiator in ha.clients:
                            io_tasks.append(executor.submit(initiator.start_fio))
                        time.sleep(20)

                        # Validate IO for old namespaces
                        LOG.info("Validating IO for old namespaces post scaleup")
                        ha.validate_scaleup(scaleup_nodes, old_namespaces)

                        # Validate IO for new namespaces
                        LOG.info("Validating IO for new namespaces post scaleup")
                        namespaces = parse_namespaces(config, new_namespaces)
                        ha.validate_scaleup(scaleup_nodes, namespaces)

                    # Perform scale-up of old GW nodes(replacement)
                    else:
                        old_namespaces = parse_namespaces(config, old_namespaces)
                        ha.scale_up(scaleup_nodes, gw_nodes, old_namespaces)
                        ha.validate_scaleup(scaleup_nodes, old_namespaces)
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
