from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.nvmegw_cli.connection import Connection
from ceph.nvmegw_cli.gateway import Gateway
from ceph.nvmegw_cli.host import Host
from ceph.nvmegw_cli.listener import Listener
from ceph.nvmegw_cli.log_level import LogLevel
from ceph.nvmegw_cli.namespace import Namespace
from ceph.nvmegw_cli.subsystem import Subsystem
from ceph.nvmegw_cli.version import Version
from ceph.nvmeof.initiator import Initiator
from ceph.nvmeof.nvmeof_gwcli import find_client_daemon_id
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)

services = {
    "log_level": LogLevel,
    "version": Version,
    "gateway": Gateway,
    "connection": Connection,
    "subsystem": Subsystem,
    "listener": Listener,
    "host": Host,
    "namespace": Namespace,
}


def initiators(ceph_cluster, gateway, config):
    """Configure NVMe Initiators."""
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = Initiator(client)
    initiator.disconnect_all()

    connect_cmd_args = {
        "transport": "tcp",
        "traddr": gateway.ip_address,
        "trsvcid": config["listener_port"],
        "nqn": config["subnqn"],
    }
    LOG.debug(initiator.connect(**connect_cmd_args))


@retry(Exception, tries=5, delay=10)
def run_io(ceph_cluster, num, io):
    """Run IO on newly added namespace."""
    LOG.info(io)
    client = get_node_by_id(ceph_cluster, io["node"])
    initiator = Initiator(client)

    targets = initiator.list_spdk_drives()
    if not targets:
        raise Exception(f"NVMe Targets not found on {client.hostname}")

    device_path = next(
        (device["DevicePath"] for device in targets if device["NameSpace"] == num), None
    )
    LOG.debug(device_path)

    if device_path is None:
        raise Exception("NVMe volume not found")
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


def configure_subsystem(config, _cls, command):
    max_ns = config["args"].pop("max-namespaces")
    for num in range(1, config["args"].pop("subsystems") + 1):
        config["args"].update(
            {
                "subsystem": f"nqn.2016-06.io.spdk:cnode{num}",
                "s": f"{num}",
                "m": max_ns,
            }
        )
        subsystem_func = fetch_method(_cls, command)
        subsystem_func(**config)
        config["args"].clear()


def configure_listener(config, _cls, command, ceph_cluster, node):
    port = config["args"].pop("port")
    pool_name = config["args"].pop("pool")

    for num in range(1, config["args"].pop("subsystems") + 1):
        client_id = find_client_daemon_id(
            ceph_cluster, pool_name, node_name=node.hostname
        )
        config["args"].update(
            {
                "subsystem": f"nqn.2016-06.io.spdk:cnode{num}",
                "gateway-name": client_id,
                "traddr": node.ip_address,
                "trsvcid": port,
            }
        )

        listener_func = fetch_method(_cls, command)
        listener_func(**config)
        config["args"].clear()


def configure_host(config, _cls, command):
    hostnqn = config["args"].pop("hostnqn", None) or "'*'"
    for num in range(1, config["args"].pop("subsystems") + 1):
        config["args"].update(
            {
                "subsystem": f"nqn.2016-06.io.spdk:cnode{num}",
                "host": hostnqn,
            }
        )
        host_access_func = fetch_method(_cls, command)
        host_access_func(**config)
        config["args"].clear()


def configure_namespace(
    config, _cls, command, ceph_cluster, node, init_config, rbd_obj, io_param
):
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    image_size = config["args"].pop("image_size")
    pool = config["args"].pop("pool")
    half_sub = int(subsystems / 2)
    namespaces_sub = int(namespaces / subsystems)

    LOG.info(half_sub)
    LOG.info(namespaces_sub)

    for sub_num in range(1, subsystems + 1):
        init_config.update({"subnqn": f"nqn.2016-06.io.spdk:cnode{sub_num}"})
        initiators(ceph_cluster, node, init_config)
        name = generate_unique_id(length=4)
        LOG.info(sub_num)
        LOG.info(subsystems)

        for num in range(1, namespaces_sub + 1):
            rbd_obj.create_image(pool, f"{name}-image{num}", image_size)
            LOG.info(num)
            LOG.info(namespaces)
            config["args"].clear()
            config["args"].update(
                {
                    "rbd-image": f"{name}-image{num}",
                    "rbd-pool": pool,
                    "subsystem": f"nqn.2016-06.io.spdk:cnode{sub_num}",
                }
            )
            config["args"]["load-balancing-group"] = 1 if sub_num < half_sub + 1 else 2
            namespace_func = fetch_method(_cls, command)
            namespace_func(**config)

            for io in io_param:
                run_io(ceph_cluster, num, io)

            mem_usage, _ = node.exec_command(
                cmd="ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20",
                sudo=True,
            )
            LOG.info(mem_usage)
            top_usage, _ = node.exec_command(cmd="top -b | head -n 20", sudo=True)
            LOG.info(top_usage)


def execute_and_log_results(node):
    commands = [
        "cephadm shell ceph df",
        "cephadm shell rbd du",
        "cephadm shell ceph -s",
        "cephadm shell ceph health detail",
        "cat /proc/meminfo",
        "cat /proc/cpuinfo",
        "ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20",
        "top -b | head -n 20",
    ]
    for cmd in commands:
        output, _ = node.installer.exec_command(cmd, sudo=True)
        LOG.info(output)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    LOG.info("Configure Ceph NVMeoF entities at scale over CLI.")
    config = deepcopy(kwargs["config"])
    node = get_node_by_id(ceph_cluster, config["node"])
    port = config.get("port", 5500)
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
    cli_image = None
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            cli_image = value
            break

    try:
        steps = config.get("steps", [])
        init_config = config.get("initiators")
        io = config.get("run_io")

        for step in steps:
            cfg = step["config"]
            service = cfg.pop("service")
            command = cfg.pop("command")

            _cls = services[service](node, port)
            if cli_image:
                _cls.NVMEOF_CLI_IMAGE = cli_image

            if service == "subsystem":
                configure_subsystem(cfg, _cls, command)
            elif service == "listener":
                configure_listener(cfg, _cls, command, ceph_cluster, node)
            elif service == "host":
                configure_host(cfg, _cls, command)
            elif service == "namespace":
                configure_namespace(
                    cfg, _cls, command, ceph_cluster, node, init_config, rbd_obj, io
                )
            else:
                func = fetch_method(_cls, command)

                if "args" not in cfg:
                    cfg["args"] = {}

                func(**cfg["args"])

        instance = CephAdmin(cluster=ceph_cluster, **config)
        execute_and_log_results(instance)

    except BaseException as be:
        LOG.error(be, exc_info=True)
        instance = CephAdmin(cluster=ceph_cluster, **config)
        execute_and_log_results(instance)
        mem_usage, _ = node.exec_command(
            cmd="ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20",
            sudo=True,
        )
        LOG.info(mem_usage)
        top_usage, _ = node.exec_command(cmd="top -b | head -n 20", sudo=True)
        LOG.info(top_usage)

        return 1

    return 0
