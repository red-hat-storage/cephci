"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
import json

from ceph.ceph import Ceph
from ceph.nvmeof.gateway import Gateway, configure_spdk
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def configure_subsystems(rbd, pool, gw, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_nqn = config["nqn"]
    gw.create_subsystem(sub_nqn, config["serial"])
    gw.create_listener(sub_nqn, config["listener_port"])
    gw.add_host(sub_nqn, config["allow_host"])
    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        with parallel() as p:
            count = config["bdevs"].get("count", 1)
            size = config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                p.spawn(rbd.create_image, pool, f"{name}-image{num}", size)

        with parallel() as p:
            # Create block device in gateway
            for num in range(count):
                p.spawn(
                    gw.create_block_device,
                    f"{name}-bdev{num}",
                    f"{name}-image{num}",
                    pool,
                )

        with parallel() as p:
            # Add namespace
            for num in range(count):
                p.spawn(gw.add_namespace, sub_nqn, f"{name}-bdev{num}")


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
    sub_nqns, _ = initiator.discover(**{**cmd_args | json_format})
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == str(config["listener_port"]):
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {cmd_args}")

    # Connect to the subsystem
    LOG.debug(initiator.connect(**cmd_args))

    # List NVMe targets.
    targets, _ = initiator.list(**json_format)
    LOG.debug(targets)

    with parallel() as p:
        for target in json.loads(targets)["Devices"]:
            io_args = {
                "device_name": target["DevicePath"],
                "size": "100%",
                "client_node": client,
                "long_running": True,
            }
            p.spawn(run_fio, **io_args)


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
                  - nqn: nqn.2016-06.io.spdk:cnode3
                    serial: 3
                    bdevs:
                      count: 1
                      size: 100G
                    listener_port: 5002
                    allow_host: "*"
                initiators:                                 # Configure Initiators with all pre-req
                  - subnqn: nqn.2016-06.io.spdk:cnode2
                    listener_port: 5002
                    node: node7
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])

        gateway = Gateway(gw_node)
        if config.get("install"):
            configure_spdk(gw_node)

        if config.get("subsystems"):
            with parallel() as p:
                for subsystem in config["subsystems"]:
                    p.spawn(configure_subsystems, rbd_obj, rbd_pool, gateway, subsystem)

        if config.get("initiators"):
            with parallel() as p:
                for initiator in config["initiators"]:
                    p.spawn(initiators, ceph_cluster, gateway, initiator)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            rbd_obj.clean_up(pools=[rbd_pool])

    return 1
