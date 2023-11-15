"""
Test module to test NVMeOF single GW deployment and scale namespaces in single subsystem.
"""
import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.nvmeof.initiator import Initiator
from ceph.nvmeof.nvmeof_gwcli import NVMeCLI, find_client_daemon_id
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id, run_fio

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
    initiator.disconnect_all()
    discover_cmd_args = {
        "transport": "tcp",
        "traddr": gateway.ip_address,
        "trsvcid": "4420",
    }
    connect_cmd_args = {
        "transport": "tcp",
        "traddr": gateway.ip_address,
        "trsvcid": config["listener_port"],
    }
    json_format = {"output-format": "json"}

    # Discover the subsystems
    sub_nqns, _ = initiator.discover(**{**discover_cmd_args | json_format})
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["subnqn"] == str(config["subnqn"]):
            connect_cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {connect_cmd_args}")

    # Connect to the subsystem
    LOG.debug(initiator.connect(**connect_cmd_args))


@retry(Exception, tries=5, delay=10)
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
        if result != 0:
            raise Exception("FIO failure")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Return the status of the test execution run with the provided keyword arguments.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Scale to 256 namespaces in single subsystem on NVMeOF GW
            desc: test with 1 subsystem and 256 namespaces in Single GW
            config:
                steps:
                    - config:
                        command: get_subsystems
                    - config:
                        command: create_block_device
                    - config:
                         command: create_subsystem
                         args:
                           subnqn: nqn.2016-06.io.spdk:cnode1
                           serial_num: 1
                    - config:
                          command: create_listener
                          args:
                            subnqn: nqn.2016-06.io.spdk:cnode1
                            port: 5001
                    - config:
                          command: add_host
                          args:
                            subnqn: nqn.2016-06.io.spdk:cnode1
                            hostnqn: *
                    - config:
                          command: add_namespace
                          args:
                            count: 256
                            image_size: 500M
                            pool: rbd
                            subnqn: nqn.2016-06.io.spdk:cnode1
    """
    LOG.info("Configure Ceph NVMeoF entities at scale over CLI.")
    config = deepcopy(kwargs["config"])
    node = get_node_by_id(ceph_cluster, config["node"])

    rbd_pool = config.get("rbd_pool", "rbd_pool")
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": rbd_pool},
        }
    )
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeCLI.CEPH_NVMECLI_IMAGE = value
            break
    nvme_cli = NVMeCLI(node, config.get("port", 5500))
    try:
        steps = config.get("steps", [])
        init_config = config.get("initiators")
        for step in steps:
            cfg = step["config"]
            command = cfg.pop("command")

            if command in ["create_listener", "delete_listener"]:
                client_id = find_client_daemon_id(
                    ceph_cluster, pool_name=cfg["args"].pop("pool")
                )
                cfg["args"].update(
                    {"gateway-name": client_id, "traddr": node.ip_address}
                )

            if command in ["add_namespace"]:
                nqn = cfg["args"].pop("subnqn")
                image_size = cfg["args"].pop("image_size")
                pool = cfg["args"].pop("pool")
                initiators(ceph_cluster, node, init_config)
                name = generate_unique_id(length=4)
                for num in range(
                    cfg["args"].pop("start_count"), cfg["args"].pop("end_count")
                ):
                    rbd_obj.create_image(
                        rbd_pool, f"{name}-image{num}", image_size
                    )  # create image

                    cfg["args"].clear()
                    cfg["args"].update(
                        {
                            "name": f"{name}-bdev{num}",
                            "image": f"{name}-image{num}",
                            "pool": pool,
                        }
                    )
                    bdev_func = fetch_method(
                        nvme_cli, "create_block_device"
                    )  # create_bdev
                    bdev_func(**cfg["args"])
                    cfg["args"].clear()

                    cfg["args"].update({"bdev": f"{name}-bdev{num}", "subnqn": nqn})
                    namespace_func = fetch_method(nvme_cli, command)  # add namespaces
                    namespace_func(**cfg["args"])
                    for io in config["run_io"]:
                        run_io(ceph_cluster, num, io)
                    mem_usage, _ = node.exec_command(
                        cmd="ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20",
                        sudo=True,
                    )
                    LOG.info(mem_usage)
                    top_usage, _ = node.exec_command(
                        cmd="top -b | head -n 20", sudo=True
                    )
                    LOG.info(top_usage)
            else:
                func = fetch_method(nvme_cli, command)

                # Avoiding exception if no args
                if "args" not in cfg:
                    cfg["args"] = {}
                func(**cfg["args"])

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
        health_detail, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph health detail", sudo=True
        )
        LOG.info(health_detail)
        mem_usage, _ = node.exec_command(cmd="cat /proc/meminfo", sudo=True)
        LOG.info(mem_usage)
        cpu, _ = node.exec_command(cmd="cat /proc/cpuinfo", sudo=True)
        LOG.info(cpu)
        top_usage, _ = node.exec_command(cmd="top -b | head -n 20", sudo=True)
        LOG.info(top_usage)

    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
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
        health_detail, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph health detail", sudo=True
        )
        LOG.info(health_detail)
        memory, _ = node.exec_command(cmd="cat /proc/meminfo", sudo=True)
        LOG.info(memory)
        cpu, _ = node.exec_command(cmd="cat /proc/cpuinfo", sudo=True)
        LOG.info(cpu)
        mem_usage, _ = node.exec_command(
            cmd="ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20", sudo=True
        )
        LOG.info(mem_usage)
        top_usage, _ = node.exec_command(cmd="top -b | head -n 20", sudo=True)
        LOG.info(top_usage)
        return 1
    return 0
