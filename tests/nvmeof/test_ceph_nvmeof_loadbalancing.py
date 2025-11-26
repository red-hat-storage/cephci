"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmeof.initiators.linux import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
    deploy_nvme_service,
)
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


def delete_namespaces(nvmegwcli, ns_count, subsystem):
    with parallel() as p:
        for num in range(1, ns_count + 1):
            ns_args = {
                "args": {
                    "nsid": num,
                    "subsystem": subsystem,
                }
            }
            p.spawn(nvmegwcli.namespace.delete, **ns_args)


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
            host_nqn = initiator.initiator_nqn()
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


def test_ceph_83608838(ceph_cluster, config):
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Configure subsystems
    LOG.info("Configure subsystems")
    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

    # Configure namespaces
    LOG.info("Configure namespaces")
    for subsystem in config["subsystems"]:
        for image_num in range(1, 11):
            sub_name = subsystem["nqn"]
            nvmegwcl = ha.gateways[0]
            image = f"image-{generate_unique_id(6)}-{image_num}"
            rbd_obj.create_image(rbd_pool, image, "1G")
            img_args = {
                "subsystem": f"{sub_name}",
                "rbd-pool": rbd_pool,
                "rbd-image": image,
                "load-balancing-group": 4,
            }
            nvmegwcl.namespace.add(**{"args": {**img_args}})

    # wait for 180 seconds and check for autoload balancing
    LOG.info("wait for 180 seconds and check for autoload balancing")
    time.sleep(180)
    ha.validate_auto_loadbalance()

    # Delete namespaces related to one load balancing group in each subsysyem
    LOG.info("Delete namespaces related to one load balancing group in each subsysyem")
    for subsystem in config["subsystems"]:
        sub_name = subsystem["nqn"]
        img_args = {"subsystem": f"{sub_name}"}
        namespace_list = nvmegwcl.namespace.list(
            **{"args": {**img_args}, "base_cmd_args": {"format": "json"}}
        )
        # Get the nsids related each load-balancing-group
        parsed_data = json.loads(namespace_list[0])
        grouped_nsids = dict()
        for ns in parsed_data["namespaces"]:
            group = ns["load_balancing_group"]
            nsid = ns["nsid"]
            if group not in grouped_nsids:
                grouped_nsids[group] = list()
            grouped_nsids[group].append(nsid)
        nsids_to_delete = grouped_nsids[4]
        for nsid in nsids_to_delete:
            img_args = {"subsystem": f"{sub_name}", "nsid": f"{nsid}"}
            nvmegwcl.namespace.delete(**{"args": {**img_args}})

    # wait for 180 seconds and check for autoload balancing
    LOG.info("wait for 180 seconds and check for autoload balancing")
    time.sleep(180)
    ha.validate_auto_loadbalance()

    LOG.info(
        "CEPH-83608838 - Test load balancing for namespace addition and deletion \
             with same LB group test validated successfully."
    )


def test_ceph_83609769(ceph_cluster, config):
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    gateway_group = config.get("gw_group", "")
    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    config["spec_deployment"] = True
    config["rebalance_period"] = True
    config["rebalance_period_sec"] = 0
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

    # Configure subsystems
    LOG.info("Configure subsystems")
    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

    # Configure namespaces
    LOG.info("Configure namespaces")
    load_balancing_group = 1
    for subsystem in config["subsystems"]:
        for image_num in range(1, 11):
            sub_name = subsystem["nqn"]
            nvmegwcl = ha.gateways[0]
            image = f"image-{generate_unique_id(6)}-{image_num}"
            rbd_obj.create_image(rbd_pool, image, "1G")
            img_args = {
                "subsystem": f"{sub_name}",
                "rbd-pool": rbd_pool,
                "rbd-image": image,
                "load-balancing-group": load_balancing_group,
            }
            nvmegwcl.namespace.add(**{"args": {**img_args}})
        load_balancing_group += 1

    # Check for num-namespaces is 10 in all gateways
    LOG.info("Check for num-namespaces is 10 in all gateways")
    out, _ = ha.orch.shell(
        args=["ceph", "nvme-gw", "show", config["rbd_pool"], repr(gateway_group)]
    )
    out = json.loads(out)
    LOG.info(f"ceph nvme-gw show output is \n {out}")
    gateways = out.get("Created Gateways:", [])
    for gw_detail in gateways:
        if int(gw_detail["num-namespaces"]) == 10:
            gw_id = gw_detail["gw-id"]
            LOG.info(f"Namespaces created for gw-id {gw_id} is 10")
        else:
            gw_id = gw_detail["gw-id"]
            num_namespaces = gw_detail["num-namespaces"]
            raise Exception(f"Namespaces created for gw-id {gw_id} is {num_namespaces}")

    # Delete namespaces related to one load balancing group in each subsysyem
    LOG.info("Delete namespaces related to one load balancing group in each subsysyem")
    nvmegwcl = ha.gateways[0]
    for subsystem in config["subsystems"]:
        sub_name = subsystem["nqn"]
        img_args = {"subsystem": f"{sub_name}"}
        namespace_list = nvmegwcl.namespace.list(
            **{"args": {**img_args}, "base_cmd_args": {"format": "json"}}
        )
        # Get the nsids related each load-balancing-group
        parsed_data = json.loads(namespace_list[1])
        grouped_nsids = dict()
        for ns in parsed_data["namespaces"]:
            group = ns["load_balancing_group"]
            nsid = ns["nsid"]
            if group not in grouped_nsids:
                grouped_nsids[group] = list()
            grouped_nsids[group].append(nsid)
        nsids_to_delete = grouped_nsids.get(4, [])
        for nsid in nsids_to_delete:
            img_args = {"subsystem": f"{sub_name}", "nsid": f"{nsid}"}
            nvmegwcl.namespace.delete(**{"args": {**img_args}})

    # Sleep for 60 seconds becfore checking namespace count because auto namespace loading will happen within 60 seconds
    time.sleep(60)
    # Check for num-namespaces is 0 for one gateway node
    LOG.info("Check for num-namespaces is 0 for one gateway node")
    out, _ = ha.orch.shell(
        args=["ceph", "nvme-gw", "show", config["rbd_pool"], repr(gateway_group)]
    )
    out = json.loads(out)
    LOG.info(f"ceph nvme-gw show output is \n {out}")
    zero_namespaces = any(
        int(gw["num-namespaces"]) == 0 for gw in out.get("Created Gateways:", [])
    )
    if zero_namespaces:
        LOG.info(
            "Namespace for one gateway node is zero and auto load balancing is not happened"
        )
    else:
        raise Exception("Namespace for one gateway node is not zero")

    # Scale down one gateway node
    LOG.info("Scale down one gateway node")
    gateway_nodes = deepcopy(config.get("gw_nodes"))
    config.update({"gw_nodes": config.get("gw_nodes")[0:3]})
    deploy_nvme_service(ceph_cluster, config)

    # Check for num-namespaces
    LOG.info("After Scale down Check for num-namespaces is 10 for every gateway node")
    out, _ = ha.orch.shell(
        args=["ceph", "nvme-gw", "show", config["rbd_pool"], repr(gateway_group)]
    )
    out = json.loads(out)
    LOG.info(f"ceph nvme-gw show output is \n {out}")
    gateways = out.get("Created Gateways:", [])
    for gw_detail in gateways:
        gw_id = gw_detail["gw-id"]
        if int(gw_detail["num-namespaces"]) == 10:
            LOG.info(f"Namespaces created for gw-id {gw_id} is 10")
        else:
            num_namespaces = gw_detail["num-namespaces"]
            raise Exception(f"Namespaces created for gw-id {gw_id} is {num_namespaces}")

    # Scale up one gateway node
    LOG.info("Scale up one gateway node")
    config.update({"gw_nodes": gateway_nodes})
    deploy_nvme_service(ceph_cluster, config)

    # wait for 120 seconds and check autoload balancing not happened
    LOG.info("wait for 120 seconds and check autoload balancing not happened")
    time.sleep(120)
    LOG.info("Check for num-namespaces is 0 for one gateway node")
    out, _ = ha.orch.shell(
        args=["ceph", "nvme-gw", "show", config["rbd_pool"], repr(gateway_group)]
    )
    out = json.loads(out)
    LOG.info(f"ceph nvme-gw show output is \n {out}")
    zero_namespaces = any(
        int(gw["num-namespaces"]) == 0 for gw in out.get("Created Gateways:", [])
    )
    if zero_namespaces:
        LOG.info(
            "Namespace for one gateway node is zero and auto load balancing is not happened"
        )
    else:
        raise Exception("Namespace for one gateway node is not zero")
    LOG.info(
        "CEPH-83609769 - Test disabling auto namespace load balancing in a GW group test validated successfully."
    )


def thread_pool_executor(num_devices, initiators):
    if int(num_devices) >= 1:
        max_workers = (
            len(initiators) * num_devices if initiators else num_devices
        )  # 20 devices + 10 buffer per initiator
        executor = ThreadPoolExecutor(
            max_workers=max_workers,
        )
        return executor
    else:
        return None


testcases = {
    "CEPH-83608838": test_ceph_83608838,
    "CEPH-83609769": test_ceph_83609769,
}


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
    initiators = config.get("initiators")
    # Set max_workers to accommodate all FIO processes per initiator
    num_devices = 0
    for subsystem in config.get("subsystems", []):
        bdevs = subsystem.get("bdevs", [])
        if bdevs:
            num_devices += bdevs[0].get("count", 0)

    executor = thread_pool_executor(num_devices, initiators)

    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)

    try:
        if config.get("test_case"):
            kwargs["config"].update(
                {
                    "do_not_create_image": True,
                    "rep-pool-only": True,
                    "rep_pool_config": {"pool": rbd_pool},
                }
            )
            rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
            test_case_run = testcases[config["test_case"]]
            config.update({"rbd_obj": rbd_obj})
            test_case_run(ceph_cluster, config)
        else:
            # Deploy NVMe services
            if config.get("install"):
                deploy_nvme_service(ceph_cluster, config)

            ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
            ha.initialize_gateways()
            gw_nodes = ha.gateways
            gw = gw_nodes[0]

            # Configure Subsystem
            if config.get("subsystems"):
                with parallel() as p:
                    for subsys_args in config["subsystems"]:
                        subsys_args["ceph_cluster"] = ceph_cluster
                        p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)
                if ceph_cluster.rhcs_version > "8.0":
                    time.sleep(120)
                    ha.validate_auto_loadbalance()

            # Initiate scale-down and scale-up
            if config.get("load_balancing"):
                for lb_config in config.get("load_balancing"):
                    # namespace addition
                    if lb_config.get("ns_add"):
                        config = lb_config["ns_add"]
                        subsystems = config["subsystems"]
                        for subsystem in subsystems:
                            sub_args = {"subsystem": subsystem["nqn"]}
                            lb_groups = None
                            LOG.info(sub_args)
                            configure_namespaces(
                                gw,
                                subsystem,
                                lb_groups,
                                sub_args,
                                rbd_pool,
                                ceph_cluster,
                            )
                        if ceph_cluster.rhcs_version > "8.0":
                            ha.validate_auto_loadbalance()

                    # namespace deletion
                    if lb_config.get("ns_del"):
                        ns_del_config = lb_config["ns_del"]
                        LOG.info(ns_del_config)
                        ns_del_count = ns_del_config["count"]
                        subsystems = ns_del_config["subsystems"]
                        for subsystem in subsystems:
                            delete_namespaces(ha.gateways[0], ns_del_count, subsystem)
                        if ceph_cluster.rhcs_version > "8.0":
                            time.sleep(120)
                            ha.validate_auto_loadbalance()

                    # Scale down
                    if lb_config.get("scale_down"):
                        io_tasks_scale_down = []
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
                            io_tasks_scale_down.append(
                                executor.submit(initiator.start_fio)
                            )
                        time.sleep(20)  # time sleep for IO to Kick-in

                        ha.scale_down(gateway_nodes_to_be_deployed)

                        # Wait for IO to complete and collect FIO outputs
                        fio_outputs = []
                        if io_tasks_scale_down:
                            LOG.info("Waiting for completion of IOs.")
                            executor.shutdown(wait=True, cancel_futures=True)

                            for task in io_tasks_scale_down:
                                try:
                                    fio_outputs.append(task.result())
                                except Exception as e:
                                    LOG.error(f"FIO execution failed: {e}")

                    # Scale up
                    if lb_config.get("scale_up"):
                        io_tasks_scale_up = []
                        scaleup_nodes = lb_config["scale_up"]
                        gateway_nodes = config["gw_nodes"]
                        LOG.info(f"Started scaling up {scaleup_nodes}")

                        # Prepare FIO execution for existing namespaces
                        old_namespaces = ha.fetch_namespaces(gw)
                        ha.prepare_io_execution(initiators)

                        # Start IO Execution into already existing namespaces/nodes
                        LOG.info("Initiating IO before scale up ")
                        executor = thread_pool_executor(num_devices, initiators)
                        for initiator in ha.clients:
                            io_tasks_scale_up.append(
                                executor.submit(initiator.start_fio)
                            )
                        time.sleep(20)  # time sleep for IO to Kick-in

                        # Perform scale-up of new nodes
                        if not all(
                            [node in set(gateway_nodes) for node in scaleup_nodes]
                        ):
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
                            ha.compare_client_namespace(
                                [i["uuid"] for i in new_namespaces]
                            )

                            # Validate IO for old namespaces
                            LOG.info("Validating IO for old namespaces post scaleup")
                            ha.validate_scaleup(scaleup_nodes, old_namespaces)
                            # Wait for IO to complete and collect FIO outputs
                            fio_outputs = []
                            if io_tasks_scale_up:
                                LOG.info("Waiting for completion of IOs.")
                                executor.shutdown(wait=True, cancel_futures=True)

                                for task in io_tasks_scale_up:
                                    try:
                                        fio_outputs.append(task.result())
                                    except Exception as e:
                                        LOG.error(f"FIO execution failed: {e}")

                            # Start IO Execution for new namespaces
                            io_tasks_new_namespaces = []
                            executor = thread_pool_executor(num_devices, initiators)
                            for initiator in ha.clients:
                                io_tasks_new_namespaces.append(
                                    executor.submit(initiator.start_fio)
                                )
                            time.sleep(20)

                            # Validate IO for new namespaces
                            LOG.info("Validating IO for new namespaces post scaleup")
                            namespaces = parse_namespaces(config, new_namespaces)
                            ha.validate_scaleup(scaleup_nodes, namespaces)

                            # Wait for IO to complete and collect FIO outputs
                            fio_outputs = []
                            if io_tasks_new_namespaces:
                                LOG.info("Waiting for completion of IOs.")
                                executor.shutdown(wait=True, cancel_futures=True)

                                for task in io_tasks_new_namespaces:
                                    try:
                                        fio_outputs.append(task.result())
                                    except Exception as e:
                                        LOG.error(f"FIO execution failed: {e}")

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
