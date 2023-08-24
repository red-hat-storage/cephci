"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
import json

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.nvmeof.gateway import (
    Gateway,
    configure_spdk,
    delete_gateway,
    fetch_gateway_log,
)
from ceph.nvmeof.initiator import Initiator
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def configure_subsystems(rbd, pool, gw, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_nqn = config["nqn"]
    max_ns = config.get("max_ns", 32)
    gw.create_subsystem(sub_nqn, config["serial"], max_ns)
    gw.create_listener(sub_nqn, config["listener_port"])
    gw.add_host(sub_nqn, config["allow_host"])
    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        count = config["bdevs"].get("count", 1)
        size = config["bdevs"].get("size", "1G")
        # Create image on ceph cluster
        for num in range(count):
            rbd.create_image(pool, f"{name}-image{num}", size)

        # Create block device and add namespace in gateway
        for num in range(count):
            gw.create_block_device(f"{name}-bdev{num}", f"{name}-image{num}", pool)
            gw.add_namespace(sub_nqn, f"{name}-bdev{num}")

    gw.get_subsystems()


def initiators(ceph_cluster, gateway, config):
    """Run IOs from NVMe Initiators.

    - Discover NVMe targets
    - Connect to subsystem
    - List targets and Run FIO on target devices.

    Args:
        ceph_cluster: Ceph cluster
        gateway: Ceph-NVMeoF Gateway.
        config: Initiator config

    Example::
        config:
            subnqn: nqn.2016-06.io.spdk:cnode2
            listener_port: 5002
            node: node7
    """
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = Initiator(client)
    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
        "trsvcid": config["listener_port"],
    }
    json_format = {"output-format": "json"}

    # Discover the subsystems
    _disc_cmd = {**cmd_args, **json_format}
    sub_nqns, _ = initiator.discover(**_disc_cmd)
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == str(config["listener_port"]):
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {cmd_args}")

    # Connect to the subsystem
    LOG.debug(initiator.connect(**cmd_args))


def run_io(ceph_cluster, io):
    """

    Args:
        io: io args
        ceph_cluster: Ceph cluster
    """
    json_format = {"output-format": "json"}
    LOG.info(io)
    client = get_node_by_id(ceph_cluster, io["node"])
    initiator = Initiator(client)
    # List NVMe targets.
    targets, _ = initiator.list(**json_format)
    LOG.debug(targets)

    for target in json.loads(targets)["Devices"]:
        io_args = {
            "device_name": target["DevicePath"],
            "client_node": client,
            "run_time": "10",
            "long_running": True,
            "io_type": io["io_type"],
            "cmd_timeout": "notimeout",
        }
        result = run_fio(**io_args)
        if result == 1:
            raise Exception("FIO failure")


def cleanup(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            node = get_node_by_id(ceph_cluster, initiator_cfg["node"])
            initiator = Initiator(node)
            initiator.disconnect(**{"nqn": initiator_cfg["subnqn"]})

    if "subsystems" in config["cleanup"]:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gateway = Gateway(gw_node)
        for sub_cfg in config["subsystems"]:
            gateway.delete_subsystem(**{"subnqn": sub_cfg["nqn"]})

        if "gateway" in config["cleanup"]:
            delete_gateway(gw_node)

    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Ceph NVMeoF deployment
            desc: Configure NVMEoF gateways and initiators
            config:
                gw_node: node6
                rbd_pool: rbd
                do_not_create_image: true
                rep-pool-only: true
                rep_pool_config:
                  pool: rbd
                  install: true                             # Run SPDK with all pre-requisites
                subsystems:                                 # Configure subsystems with all sub-entities
                  - nqn: nqn.2016-06.io.spdk:cnode2
                    serial: 2
                    bdevs:
                      count: 1
                      size: 100G
                    listener_port: 5002
                    allow_host: "*"
                initiators:                                 # Configure Initiators with all pre-req
                  - subnqn: nqn.2016-06.io.spdk:cnode2
                    listener_port: 5002
                    node: node6
                run_io:
                  - node: node6
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gateway = Gateway(gw_node)

        if config.get("install"):
            configure_spdk(gw_node, rbd_pool)

        if config.get("subsystems"):
            for subsystem in config["subsystems"]:
                configure_subsystems(rbd_obj, rbd_pool, gateway, subsystem)

        if config.get("initiators"):
            for initiator in config["initiators"]:
                initiators(ceph_cluster, gateway, initiator)

        if config.get("run_io"):
            for io in config["run_io"]:
                run_io(ceph_cluster, io)

        instance = CephAdmin(cluster=ceph_cluster, **config)
        ceph_usage, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph df", sudo=True
        )
        LOG.info(ceph_usage)

        rbd_usage, _ = instance.installer.exec_command(
            cmd="cephadm shell rbd du", sudo=True
        )
        LOG.info(rbd_usage)

        health, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph -s", sudo=True
        )
        LOG.info(health)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        fetch_gateway_log(gw_node)
        if config.get("cleanup"):
            rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
            cleanup(ceph_cluster, rbd_obj, config)

    return 1
