import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.initiators.linux import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    nvme_gw_cli_version_adapter,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def fetch_drive(node, uuid):
    """Fetch Drive from node using UUID."""
    # lsblk -np -r -o "name,wwn"
    out, _ = node.exec_command(cmd=f"lsblk -np -r -o name,wwn | grep {uuid}", sudo=True)
    if out:
        return out.split(" ")[0]


def initiators(ceph_cluster, gateway, config):
    """Configure NVMe Initiators."""
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = Initiator(client)
    initiator.disconnect_all()

    connect_cmd_args = {
        "transport": "tcp",
        "traddr": gateway.ip_address,
        "trsvcid": 8009,
    }
    LOG.debug(initiator.connect_all(**connect_cmd_args))


@retry(Exception, tries=5, delay=10)
def run_io(ceph_cluster, ns_uuid, io):
    """Run IO on newly added namespace."""
    LOG.info(io)
    client = get_node_by_id(ceph_cluster, io["node"])
    device_path = fetch_drive(client, ns_uuid)

    if device_path is None:
        raise Exception(f"NVMe volume not found for {ns_uuid}")

    LOG.debug(device_path)
    io_args = {
        "device_name": device_path,
        "client_node": client,
        "run_time": "10",
        "io_type": io["io_type"],
        "long_running": True,
        "cmd_timeout": 600,
    }
    result = run_fio(**io_args)
    if result != 0:
        raise Exception("FIO failure")


def configure_subsystems(config, _cls, command):
    max_ns = config["args"].pop("max-namespaces")

    def configure_subsystem(num):
        args_copy = config["args"].copy()
        args_copy.update(
            {
                "subsystem": f"nqn.2016-06.io.spdk:cnode{num}",
                "s": f"{num}",
                "m": max_ns,
            }
        )
        subsystem_func = fetch_method(_cls, command)
        subsystem_func(**{"args": args_copy})

    # Create subsystems in parallel
    with parallel() as p:
        for num in range(1, config["args"].pop("subsystems") + 1):
            p.spawn(configure_subsystem, num)


def configure_listeners(config, _cls, command, ceph_cluster):
    port = config["args"].pop("port")
    subsystems = config["args"].pop("subsystems")
    nodes = config["args"].pop("nodes")
    group = config["args"].pop("group", None)
    LOG.info(nodes)

    def configure_listener(num, listener_node):
        subnqn = (
            f"nqn.2016-06.io.spdk:cnode{num}{f'.{group}' if group is not None else ''}"
        )

        args_copy = config["args"].copy()
        args_copy.update(
            {
                "subsystem": subnqn,
                "host-name": listener_node.hostname,
                "traddr": listener_node.ip_address,
                "trsvcid": port,
            }
        )

        base_cmd_args_copy = {"server-address": listener_node.ip_address}

        listener_func = fetch_method(_cls, command)
        listener_func(**{"args": args_copy, "base_cmd_args": base_cmd_args_copy})

    # Configure listeners in parallel
    with parallel() as p:
        for node in nodes:
            LOG.info(node)
            listener_node = get_node_by_id(ceph_cluster, node)
            for num in range(1, subsystems + 1):
                p.spawn(configure_listener, num, listener_node)


def configure_hosts(config, _cls, command):
    hostnqn = config["args"].pop("hostnqn", None) or "'*'"
    group = config["args"].pop("group", None)
    subsystems = config["args"].pop("subsystems")

    def configure_host(num):
        subnqn = (
            f"nqn.2016-06.io.spdk:cnode{num}{f'.{group}' if group is not None else ''}"
        )

        args_copy = config["args"].copy()
        args_copy.update(
            {
                "subsystem": subnqn,
                "host": hostnqn,
            }
        )

        host_access_func = fetch_method(_cls, command)
        host_access_func(**{"args": args_copy})

    # Configure hosts for subsystems in parallel
    with parallel() as p:
        for num in range(1, subsystems + 1):
            p.spawn(configure_host, num)


def set_qos(namespaces_sub, sub_num, group, _cls):
    def apply_qos(config):
        qos_func = fetch_method(_cls, "set_qos")
        qos_func(**config)

    def verify_qos(subnqn, expected_config, nsid):
        namespace_list = fetch_method(_cls, "list")
        _config = {
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": subnqn, "nsid": nsid},
        }
        _, namespace = namespace_list(**_config)
        namespace_data = json.loads(namespace)["namespaces"][0]

        def transform_rw_ios(value):
            quotient = value // 1000
            if value % 1000 == 0:
                return value
            transformed_quotient = quotient + 1
            return transformed_quotient * 1000

        for key, expected_value in expected_config.items():
            actual_value = namespace_data.get(
                key.replace("-", "_").replace("megabytes", "mbytes"), ""
            )
            if key == "rw-ios-per-second":
                expected_value = transform_rw_ios(expected_value)
            if int(actual_value) != int(expected_value):
                raise Exception(
                    f"QoS verification failed for {key}: Expected {expected_value}, got {actual_value}"
                )

        LOG.info("Verification of QoS values is successful")

    for num in range(1, namespaces_sub + 1):
        qos_settings = [
            {
                "rw-ios-per-second": 1500,
                "rw-megabytes-per-second": 6,
                "r-megabytes-per-second": 3,
                "w-megabytes-per-second": 3,
            },
            {
                "rw-ios-per-second": 150,
                "rw-megabytes-per-second": 59,
                "r-megabytes-per-second": 59,
                "w-megabytes-per-second": 59,
            },
        ]
        for _, qos_values in enumerate(qos_settings):
            subnqn = f"nqn.2016-06.io.spdk:cnode{sub_num}{f'.{group}' if group else ''}"
            config = {"args": {"nsid": num, "subsystem": subnqn, **qos_values}}

            apply_qos(config)
            LOG.info(f"Applied QoS for NSID {num} on subsystem {subnqn}")

            verify_qos(subnqn, qos_values, num)
            LOG.info(f"Updated QoS for NSID {num} on subsystem {subnqn}")


def configure_namespaces(
    config, _cls, command, ceph_cluster, node, init_config, rbd_obj, io_param
):
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    image_size = config["args"].pop("image_size", None)
    group = config["args"].pop("group", None)
    pool = config["args"].pop("pool", None)
    namespaces_sub = int(namespaces / subsystems)
    if command == "add":
        for sub_num in range(1, subsystems + 1):
            init_config.update(
                {
                    "subnqn": f"nqn.2016-06.io.spdk:cnode{sub_num}{f'.{group}' if group is not None else ''}"
                }
            )
            initiators(ceph_cluster, node, init_config)
            name = generate_unique_id(length=4)
            LOG.info(sub_num)
            LOG.info(subsystems)

            def configure_namespace(num, config):
                rbd_obj.create_image(pool, f"{name}-image{num}", image_size)
                LOG.info(num)
                config["args"].clear()
                subnqn = f"nqn.2016-06.io.spdk:cnode{sub_num}{f'.{group}' if group is not None else ''}"
                config = {
                    "base_cmd_args": {"format": "json"},
                    "args": {
                        "rbd-image": f"{name}-image{num}",
                        "rbd-pool": pool,
                        "subsystem": subnqn,
                        "force": True,
                    },
                }
                namespace_func = fetch_method(_cls, command)
                namespaces, _ = namespace_func(**config)
                nsid = json.loads(namespaces)["nsid"]

                _config = {
                    "base_cmd_args": {"format": "json"},
                    "args": {"subsystem": subnqn, "nsid": nsid},
                }
                namespace_list = fetch_method(_cls, "list")
                namespace, _ = namespace_list(**_config)
                ns_uuid = json.loads(namespace)["namespaces"][0]["uuid"]

                for io in io_param:
                    run_io(ceph_cluster, ns_uuid, io)

                mem_usage, _ = node.exec_command(
                    cmd="ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20",
                    sudo=True,
                )
                LOG.info(mem_usage)
                top_usage, _ = node.exec_command(cmd="top -b | head -n 20", sudo=True)
                LOG.info(top_usage)

            # Configure namespaces in parallel
            for num in range(1, namespaces_sub + 1):
                with parallel() as p:
                    p.spawn(configure_namespace, num, config)

    # Apply QoS to all created namespaces
    if command == "set_qos":
        with parallel() as p:
            for sub_num in range(1, subsystems + 1):
                p.spawn(set_qos, namespaces_sub, sub_num, group, _cls)


def execute_and_log_results(node, rbd_pool):
    commands = [
        "cephadm shell -- ceph df",
        f"cephadm shell -- rbd du -p {rbd_pool}",
        "cephadm shell -- ceph -s",
        "cephadm shell -- ceph health detail",
        "cephadm shell -- ceph orch ls | grep nvmeof",
        "cat /proc/meminfo | grep -iE 'hugepage|^mem|swap'",
        "cat /proc/cpuinfo | grep -iE 'cpu'",
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
    rbd_pool = config.get("rbd_pool", "rbd_pool")
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": rbd_pool},
        }
    )
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = None
    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)

    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    try:
        steps = config.get("steps", [])
        init_config = config.get("initiators")
        io = config.get("run_io")

        for step in steps:
            cfg = step["config"]
            service = cfg.pop("service")
            command = cfg.pop("command")

            _cls = fetch_method(nvmegwcli, service)

            if service == "subsystem":
                configure_subsystems(cfg, _cls, command)
            elif service == "listener":
                configure_listeners(cfg, _cls, command, ceph_cluster)
            elif service == "host":
                configure_hosts(cfg, _cls, command)
            elif service == "namespace":
                configure_namespaces(
                    cfg, _cls, command, ceph_cluster, node, init_config, rbd_obj, io
                )
            else:
                func = fetch_method(_cls, command)

                if "args" not in cfg:
                    cfg["args"] = {}

                func(**cfg["args"])

        instance = CephAdmin(cluster=ceph_cluster, **config)
        execute_and_log_results(instance, rbd_pool)

    except BaseException as be:
        LOG.error(be, exc_info=True)
        instance = CephAdmin(cluster=ceph_cluster, **config)
        execute_and_log_results(instance, rbd_pool)
        mem_usage, _ = node.exec_command(
            cmd="ps -eo pid,ppid,cmd,comm,%mem,%cpu --sort=-%mem | head -20",
            sudo=True,
        )
        LOG.info(mem_usage)
        top_usage, _ = node.exec_command(cmd="top -b | head -n 20", sudo=True)
        LOG.info(top_usage)

        return 1

    return 0
