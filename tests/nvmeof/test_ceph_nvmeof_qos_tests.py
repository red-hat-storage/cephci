"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.initiators.linux import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
    deploy_nvme_service,
    nvme_gw_cli_version_adapter,
    validate_qos,
    verify_qos,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsys_config, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": subsys_config["nqn"]}
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    **(
                        {"no-group-append": config.get("no-group-append", True)}
                        if ceph_cluster.rhcs_version >= "8.0"
                        else {}
                    ),
                },
            }
        }
    )
    listener_cfg = {
        "host-name": nvmegwcli.fetch_gateway_hostname(),
        "traddr": nvmegwcli.node.ip_address,
        "trsvcid": subsys_config["listener_port"],
    }
    nvmegwcli.listener.add(**{"args": {**listener_cfg, **sub_args}})
    if subsys_config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(subsys_config["allow_host"])}}}
        )

    if subsys_config.get("hosts"):
        for host in subsys_config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = Initiator(initiator_node)
            host_nqn = initiator.initiator_nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

    if subsys_config.get("bdevs"):
        name = generate_unique_id(length=4)
        with parallel() as p:
            count = subsys_config["bdevs"].get("count", 1)
            size = subsys_config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                p.spawn(rbd.create_image, pool, f"{name}-image{num}", size)
        namespace_args = {**sub_args, **{"rbd-pool": pool}}
        with parallel() as p:
            # Create namespace in gateway
            for num in range(count):
                ns_args = deepcopy(namespace_args)
                ns_args.update({"rbd-image": f"{name}-image{num}"})
                ns_args = {"args": ns_args}
                p.spawn(nvmegwcli.namespace.add, **ns_args)

    # Set and verify QoS for namespaces
    if subsys_config.get("bdevs").get("qos"):
        for qos_args in subsys_config.get("bdevs").get("qos"):
            prev_key = None
            for key in [
                "r-megabytes-per-second",
                "w-megabytes-per-second",
                "rw-megabytes-per-second",
            ]:
                if key in qos_args:
                    qos_arg = {
                        "nsid": qos_args["nsid"],
                        "subsystem": qos_args["subsystem"],
                        key: qos_args[key],
                    }

                    if prev_key:
                        qos_arg[prev_key] = 0

                    nvmegwcli.namespace.set_qos(
                        **{
                            "args": qos_arg,
                        }
                    )
                    verify_qos(qos_arg, nvmegwcli)
                    prev_key = key

                    run_io_and_validate_qos(
                        ceph_cluster, config, key, qos_args, nvmegwcli
                    )

            # Update QoS values
            qos_arg = {
                "nsid": qos_args["nsid"],
                "subsystem": qos_args["subsystem"],
                "r-megabytes-per-second": 10,
                "w-megabytes-per-second": 20,
                "rw-megabytes-per-second": 30,
                "rw-ios-per-second": 2000,
            }
            nvmegwcli.namespace.set_qos(
                **{
                    "args": qos_arg,
                }
            )
            verify_qos(qos_arg, nvmegwcli)


def disconnect_initiator(ceph_cluster, node, subnqn):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect(**{"nqn": subnqn})


def disconnect_all_initiator(ceph_cluster, nodes):
    """Disconnect all connections on Initiator."""
    for node in nodes:
        node = get_node_by_id(ceph_cluster, node)
        initiator = Initiator(node)
        initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, nvmegwcli, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(
                ceph_cluster, initiator_cfg["node"], initiator_cfg["nqn"]
            )

    if "disconnect_all" in config["cleanup"]:
        nodes = config.get("disconnect_all")
        if not nodes:
            nodes = [i["node"] for i in config["initiators"]]

        disconnect_all_initiator(ceph_cluster, nodes)

    # Delete the multiple subsystems across multiple gateways
    if "subsystems" in config["cleanup"]:
        config_sub_node = config["subsystems"]
        if not isinstance(config_sub_node, list):
            config_sub_node = [config_sub_node]
        for sub_cfg in config_sub_node:
            node = config["gw_node"] if "node" not in sub_cfg else sub_cfg["node"]
            LOG.info(f"Deleting subsystem {sub_cfg['nqn']} on gateway {node}")
            nvmegwcli.subsystem.delete(
                **{"args": {"subsystem": sub_cfg["nqn"], "force": True}}
            )

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def trigger_fio(ceph_cluster, config, io_mode, key):
    results = []
    client = get_node_by_id(ceph_cluster, config["initiators"][0]["node"])
    # TODO: change to nvme initiator
    initiator = Initiator(client)

    try:
        # List NVMe targets
        paths = initiator.list_spdk_drives()
        if not paths:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        LOG.debug(paths)

        with parallel() as p:
            for path in paths:
                try:
                    _io_args = {
                        "device_name": path,
                        "client_node": client,
                        "long_running": True,
                        "io_type": io_mode,
                        "run_time": "50",
                        "size": "100%",
                        "iodepth": 4,
                        "time_based": True,
                    }
                    if key == "rw-ios-per-second":
                        _io_args.update({"rwmixread": "50"})
                    p.spawn(run_fio, **_io_args)
                except Exception as fio_err:
                    LOG.error(f"Error running FIO on target {path}: {fio_err}")
                    results.append(f"Failed: {fio_err}")

            for op in p:
                results.append(op)

    except Exception as err:
        LOG.error(f"Error in trigger_fio: {err}")
        raise

    return results


def run_io_and_validate_qos(ceph_cluster, config, key, qos_args, nvmegwcli):
    executor = ThreadPoolExecutor()
    lsblk_devs = {}

    initiators = config["initiators"]

    ha = HighAvailability(ceph_cluster, [config["gw_node"]], **config)
    ha.prepare_io_execution(nvmegwcli, initiators)
    devices_dict = ha.clients[0].fetch_lsblk_nvme_devices_dict()
    lsblk_devs = [device["name"] for device in devices_dict]
    io_mode = {
        "r-megabytes-per-second": "read",
        "w-megabytes-per-second": "write",
        "rw-megabytes-per-second": "randrw",
        "rw-ios-per-second": "randrw",
    }[key]

    # Start IO Execution
    io_tasks = []
    try:
        io_tasks.append(
            executor.submit(trigger_fio, ceph_cluster, config, io_mode, key)
        )
        time.sleep(20)

        for initiator in initiators:
            client = get_node_by_id(ceph_cluster, initiator["node"])

            # Validate each QoS parameter one at a time
            validate_qos(client, lsblk_devs[0], **{key: qos_args[key]})

            # Update QoS and verify while IO is in progress
            qos_arg = {
                "nsid": qos_args["nsid"],
                "subsystem": qos_args["subsystem"],
                key: str(int(qos_args[key]) + 100),
            }

            nvmegwcli.namespace.set_qos(
                **{
                    "args": qos_arg,
                }
            )
            LOG.info(f"Updating QoS for namespace {qos_args['nsid']} is successful.")

            verify_qos(qos_arg, nvmegwcli)

    except BaseException as err:  # noqa
        raise Exception(err)
    finally:
        if io_tasks:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=True, cancel_futures=True)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configures Initiators, run FIO on NVMe targets, verify QoS.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})

    nvmegwcli = None
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    try:
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        nvmegwcli = create_gateway(
            nvme_gw_cli_version_adapter(ceph_cluster),
            gw_node,
            mtls=config.get("mtls"),
            shell=getattr(ceph, "shell"),
            port=config.get("gw_port", 5500),
            gw_group=config.get("gw_group"),
        )

        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    p.spawn(
                        configure_subsystems,
                        ceph_cluster,
                        rbd_obj,
                        rbd_pool,
                        nvmegwcli,
                        subsys_args,
                        config,
                    )

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)

    return 1
