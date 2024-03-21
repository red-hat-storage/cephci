"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway with incremental subsystem scaling
to find subsystem limitations
"""
import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.nvmegw_cli.common import NVMeCLI, find_client_daemon_id
from ceph.nvmeof.initiator import Initiator
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


def run_io(ceph_cluster, node, config, io):
    """Run IO on newly added namespace.
    - discover, connect to a subsystem
    - Run IO
    Args:
        config: initiator config
        gateway: gateway node
        io: io args
        ceph_cluster: Ceph cluster
    """
    LOG.info(io)
    client = get_node_by_id(ceph_cluster, io["node"])
    initiator = Initiator(client)
    initiator.disconnect_all()
    cmd_args = {
        "transport": "tcp",
        "traddr": node.ip_address,
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
    targets = initiator.list_spdk_drives()
    if not targets:
        raise Exception(f"NVMe Targets not found on {client.hostname}")
    LOG.debug(targets)
    for target in targets:
        io_args = {
            "device_name": target["DevicePath"],
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
            cmd = cfg.pop("command")
            commands = ["create_listener", "add_host", "add_namespace"]
            listener_port = 5001
            image_size = "50M"
            pool = "rbd"
            if cmd in ["create_subsystem"]:
                for num in range(
                    cfg["args"].pop("start_count"), cfg["args"].pop("end_count")
                ):
                    sub = {
                        "subnqn": f"nqn.2016-06.io.spdk:cnode{num}",
                        "serial_num": num,
                    }
                    subsystem_func = fetch_method(nvme_cli, cmd)
                    subsystem_func(**sub)
                    for command in commands:
                        if command in ["create_listener"]:
                            listener = {
                                "subnqn": f"nqn.2016-06.io.spdk:cnode{num}",
                                "port": listener_port,
                            }
                            client_id = find_client_daemon_id(node)
                            listener.update(
                                {"gateway-name": client_id, "traddr": node.ip_address}
                            )
                            listener_func = fetch_method(nvme_cli, command)
                            listener_func(**listener)

                        if command in ["add_host"]:
                            host = {
                                "subnqn": f"nqn.2016-06.io.spdk:cnode{num}",
                                "hostnqn": "*",
                            }
                            host_func = fetch_method(nvme_cli, command)
                            host_func(**host)

                        if command in ["add_namespace"]:
                            rbd_obj.create_image(
                                pool, f"image{num}", image_size
                            )  # create image

                            bdev = {
                                "name": f"bdev{num}",
                                "image": f"image{num}",
                                "pool": pool,
                            }
                            bdev_func = fetch_method(
                                nvme_cli, "create_block_device"
                            )  # create_bdev
                            bdev_func(**bdev)

                            namespace = {
                                "bdev": f"bdev{num}",
                                "subnqn": f"nqn.2016-06.io.spdk:cnode{num}",
                            }
                            namespace_func = fetch_method(
                                nvme_cli, command
                            )  # add namespaces
                            namespace_func(**namespace)

                            init_config["listener_port"] = listener_port
                            for io in config["run_io"]:
                                run_io(ceph_cluster, node, init_config, io)
                            listener_port += 1

            else:
                func = fetch_method(nvme_cli, cmd)

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
        return 1
    return 0
