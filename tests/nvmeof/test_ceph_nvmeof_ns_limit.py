"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway with incremental namespace scaling
to find namespace limitations

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
from utility.retry import retry
from utility.utils import run_fio

LOG = Log(__name__)


def initiators(ceph_cluster, gateway, config):
    """Configure NVMe Initiators.

    - Discover NVMe targets
    - Connect to subsystem

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


@retry(Exception, tries=3, delay=10)
def run_io(ceph_cluster, num, io):
    """Run IO on newly added namespace.

    Args:
        io: io args
        num: Current Namespace number
        ceph_cluster: Ceph cluster
    """
    json_format = {"output-format": "json"}
    LOG.info(io)
    client = get_node_by_id(ceph_cluster, io["node"])
    initiator = Initiator(client)
    # List NVMe targets.
    targets, _ = initiator.list(**json_format)
    data = json.loads(targets)
    device_path = next(
        (
            device["DevicePath"]
            for device in data["Devices"]
            if device["NameSpace"] == num
        ),
        None,
    )
    LOG.debug(device_path)

    if device_path is None:
        raise Exception("nvme volume not found")
    else:
        io_args = {
            "device_name": device_path,
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
    - Incrementally scale nvmeof namespaces to find limitations
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
                install: true
                subsystems:
                  nqn: nqn.2016-06.io.spdk:cnode1
                  serial: 1
                  max_ns: 65000
                  listener_port: 5001
                  allow_host: "*"
                namespaces:
                  start_count: 1
                  end_count: 10000
                  sub_nqn: nqn.2016-06.io.spdk:cnode1
                  size: 50M
                initiators:
                  subnqn: nqn.2016-06.io.spdk:cnode1
                  listener_port: 5001
                  node: node6
                run_io:
                  node: node6
                  io_type: write
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gateway = Gateway(gw_node)
        sub_config = config.get("subsystems")
        init_config = config.get("initiators")
        start_count = config["namespaces"].get("start_count", 1)
        end_count = config["namespaces"].get("end_count", 65000)
        sub_nqn = config["namespaces"].get("sub_nqn")
        size = config["namespaces"].get("size", "1G")

        if config.get("install"):
            configure_spdk(gw_node, rbd_pool)

        if config.get("subsystems"):
            gateway.create_subsystem(
                sub_config["nqn"], sub_config["serial"], sub_config["max_ns"]
            )
            gateway.create_listener(sub_config["nqn"], sub_config["listener_port"])
            gateway.add_host(sub_config["nqn"], sub_config["allow_host"])
        initiators(ceph_cluster, gateway, init_config)

        # Create image on ceph luster and block device and namespace in gateway
        for num in range(start_count, end_count):
            rbd_obj.create_image(rbd_pool, f"image{num}", size)
            gateway.create_block_device(f"bdev{num}", f"image{num}", rbd_pool)
            gateway.add_namespace(sub_nqn, f"bdev{num}")
            run_io(ceph_cluster, num, config.get("run_io"))

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
