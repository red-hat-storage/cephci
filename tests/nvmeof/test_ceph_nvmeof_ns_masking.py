import json
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmeof.initiators.linux import Initiator
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)
io_tasks = []
executor = None


def execute_io_ns_masking(ha, init_nodes, images, negative=False):
    """
    Execute IO on the namespaces after validating the namespace masking.
    """
    for node in init_nodes:
        ha.prepare_io_execution(
            [{"nqn": "connect-all", "listener_port": 5500, "node": node}]
        )
        if isinstance(images, dict):
            images_node = images.get(node, [])
        else:
            images_node = images
        num_devices = len(images_node)
        max_workers = num_devices if num_devices > 32 else 32
        executor = ThreadPoolExecutor(
            max_workers=max_workers,
        )
        initiator = [client for client in ha.clients if client.node.id == node][0]
        io_tasks.append(executor.submit(initiator.start_fio))
        time.sleep(20)  # time sleep for IO to Kick-in

        ha.validate_io(images_node, negative=negative)


def add_namespaces(config, command, init_nodes, hostnqn_dict, rbd_obj, ha):
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    image_size = config["args"].pop("image_size")
    group = config["args"].pop("group", None)
    pool = config["args"].pop("pool")
    no_auto_visible = config["args"].pop("no-auto-visible")
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    namespaces_sub = int(namespaces / subsystems)
    base_cmd_args = {"format": "json"}
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    expected_visibility = False if no_auto_visible else True
    nvmegwcli = ha.gateways[0]
    rbd_images_subsys = []

    for sub_num in range(1, subsystems + 1):
        subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"
        name = generate_unique_id(length=4)
        LOG.info(f"Subsystem {sub_num}/{subsystems}")
        for num in range(1, namespaces_sub + 1):
            LOG.info(f"Namespace {num}/{namespaces_sub}")
            # Create the image
            rbd_obj.create_image(pool, f"{name}-image{num}", image_size)
            LOG.info(f"Creating image {name}-image{num}/{namespaces}")

            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "rbd-image": f"{name}-image{num}",
                    "rbd-pool": pool,
                    "subsystem": subnqn,
                },
            }
            if no_auto_visible:
                config["args"]["no-auto-visible"] = ""
                expected_visibility = False
            namespaces_response, _ = nvmegwcli.namespace.add(**config)
            nsid = json.loads(namespaces_response)["nsid"]

            _config = {
                "base_cmd_args": base_cmd_args,
                "args": {
                    "nsid": nsid,
                    "subsystem": subnqn,
                },
            }

            namespace_response, _ = nvmegwcli.namespace.list(**_config)
            ns_visibility = json.loads(namespace_response)["namespaces"][0][
                "auto_visible"
            ]
            LOG.info(ns_visibility)
            ha.validate_namespace_masking(
                num,
                subnqn,
                namespaces_sub,
                hostnqn_dict,
                ns_visibility,
                command,
                expected_visibility,
            )
            rbd_images_subsys.append(f"{subnqn}|{pool}|{name}-image{num}")
    if validate_initiators:
        ha.validate_init_namespace_masking(command, init_nodes, expected_visibility)

    negative = not expected_visibility
    execute_io_ns_masking(ha, init_nodes, rbd_images_subsys, negative=negative)
    return executor, io_tasks


def add_host(config, command, init_nodes, hostnqn_dict, ha):
    """Add host NQN to namespaces"""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    group = config["args"].pop("group", None)
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    LOG.info(hostnqn_dict)
    expected_visibility = False
    nvmegwcli = ha.gateways[0]
    namespaces_sub = int(namespaces / subsystems)
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    num_namespaces_per_initiator = namespaces_sub // len(hostnqn_dict)
    rbd_images_subsys = {}

    for sub_num in range(1, subsystems + 1):
        LOG.info(f"Subsystem {sub_num}/{subsystems}")
        for num in range(1, namespaces_sub + 1):
            LOG.info(f"Namespace {num}/{namespaces_sub}")
            config["args"].clear()
            subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"

            # Determine the correct host for the current namespace
            for i, node in enumerate(
                hostnqn_dict.keys(), start=1
            ):  # Calculate the range of namespaces this initiator should handle
                if num in range(
                    (i - 1) * num_namespaces_per_initiator + 1,
                    i * num_namespaces_per_initiator + 1,
                ):
                    host = node
                    LOG.info(host)
                    host_nqn = hostnqn_dict[node]
                    break

            if host_nqn is None:
                raise Exception(
                    f"Host NQN not found for namespace {num} in subsystem {subnqn}"
                )

            LOG.info(f"Adding host {host_nqn} to namespace {num} in subsystem {subnqn}")
            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "host": host_nqn,
                    "subsystem": subnqn,
                },
            }

            nvmegwcli.namespace.add_host(**config)
            _config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "subsystem": subnqn,
                },
            }
            # list namespaces and check visibility
            namespace_response, _ = nvmegwcli.namespace.list(**_config)
            ns_obj = json.loads(namespace_response)["namespaces"][0]
            if rbd_images_subsys.get(host):
                rbd_images_subsys[host].extend(
                    [f"{subnqn}|{ns_obj['rbd_pool_name']}|{ns_obj['rbd_image_name']}"]
                )
            else:
                rbd_images_subsys[host] = [
                    f"{subnqn}|{ns_obj['rbd_pool_name']}|{ns_obj['rbd_image_name']}"
                ]
            ns_visibility = ns_obj["hosts"]
            ha.validate_namespace_masking(
                num,
                subnqn,
                namespaces_sub,
                hostnqn_dict,
                ns_visibility,
                command,
                expected_visibility,
            )
            if validate_initiators:
                validate_config = {
                    "args": {
                        "sub_num": sub_num,
                        "nsid": num,
                        "init_node": host,
                    },
                }
                ha.validate_init_namespace_masking(
                    command, init_nodes, expected_visibility, validate_config
                )

    execute_io_ns_masking(ha, init_nodes, rbd_images_subsys, negative=False)


def del_host(config, command, init_nodes, hostnqn_dict, ha):
    """Delete host NQN to namespaces"""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    group = config["args"].pop("group", None)
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    LOG.info(hostnqn_dict)
    namespaces_sub = int(namespaces / subsystems)
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    num_namespaces_per_initiator = namespaces_sub // len(hostnqn_dict)
    nvmegwcli = ha.gateways[0]
    rbd_images_subsys = {}

    for sub_num in range(1, subsystems + 1):
        LOG.info(f"Subsystem {sub_num}/{subsystems}")
        for num in range(1, namespaces_sub + 1):
            LOG.info(f"Namespace {num}/{namespaces_sub}")
            config["args"].clear()
            subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"

            # Determine the correct host for the current namespace
            for i, node in enumerate(hostnqn_dict.keys(), start=1):
                # Iterate over hostnqn_dict # Calculate the range of namespaces this initiator should handle
                if num in range(
                    (i - 1) * num_namespaces_per_initiator + 1,
                    i * num_namespaces_per_initiator + 1,
                ):
                    host = node
                    LOG.info(host)
                    host_nqn = hostnqn_dict[node]
                    break

            if host_nqn is None:
                raise Exception(
                    f"Host NQN not found for namespace {num} in subsystem {subnqn}"
                )
            LOG.info(
                f"Deleting host {host_nqn} from namespace {num} in subsystem {subnqn}"
            )
            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "host": host_nqn,
                    "subsystem": subnqn,
                },
            }
            nvmegwcli.namespace.del_host(**config)

            _config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "subsystem": subnqn,
                },
            }
            # list namespaces and check visibility
            namespace_response, _ = nvmegwcli.namespace.list(**_config)
            ns_obj = json.loads(namespace_response)["namespaces"][0]
            if rbd_images_subsys.get(host):
                rbd_images_subsys[host].extend(
                    [f"{subnqn}|{ns_obj['rbd_pool_name']}|{ns_obj['rbd_image_name']}"]
                )
            else:
                rbd_images_subsys[host] = [
                    f"{subnqn}|{ns_obj['rbd_pool_name']}|{ns_obj['rbd_image_name']}"
                ]
            ns_visibility = ns_obj["hosts"]
            expected_visibility = []
            ha.validate_namespace_masking(
                num,
                subnqn,
                namespaces_sub,
                hostnqn_dict,
                ns_visibility,
                command,
                expected_visibility,
            )

            if validate_initiators:
                validate_config = {
                    "args": {
                        "sub_num": sub_num,
                        "nsid": num,
                        "init_node": host,
                    },
                }
                ha.validate_init_namespace_masking(
                    command, init_nodes, expected_visibility, validate_config
                )

    execute_io_ns_masking(ha, init_nodes, rbd_images_subsys, negative=True)


def change_visibility(config, command, init_nodes, hostnqn_dict, ha):
    """Change visibility of namespaces."""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    group = config["args"].pop("group", None)
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    auto_visible = config["args"].pop("auto-visible")
    namespaces_sub = int(namespaces / subsystems)
    nvmegwcli = ha.gateways[0]
    base_cmd_args = {"format": "json"}
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    expected_visibility = True if auto_visible == "yes" else False
    rbd_images_subsys = []

    for sub_num in range(1, subsystems + 1):
        subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"
        LOG.info(f"Subsystem {sub_num}/{subsystems}")
        for num in range(1, namespaces_sub + 1):
            LOG.info(f"Namespace {num}/{namespaces_sub}")
            config["args"].clear()
            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "subsystem": subnqn,
                    "auto-visible": auto_visible,
                    "force": "",
                },
            }
            nvmegwcli.namespace.change_visibility(**config)
            # Listing namespace and checking visibility
            _config = {
                "base_cmd_args": base_cmd_args,
                "args": {"nsid": num, "subsystem": subnqn},
            }
            namespace_response, _ = nvmegwcli.namespace.list(**_config)
            ns_obj = json.loads(namespace_response)["namespaces"][0]
            rbd_images_subsys.append(
                f"{subnqn}|{ns_obj['rbd_pool_name']}|{ns_obj['rbd_image_name']}"
            )
            ns_visibility = ns_obj["auto_visible"]

            # Validate the visibility of the namespace
            ha.validate_namespace_masking(
                num,
                subnqn,
                namespaces_sub,
                hostnqn_dict,
                ns_visibility,
                command,
                expected_visibility,
            )
    if validate_initiators:
        ha.validate_init_namespace_masking(command, init_nodes, expected_visibility)

    negative = not expected_visibility
    execute_io_ns_masking(ha, init_nodes, rbd_images_subsys, negative=negative)
    return executor, io_tasks


def run(ceph_cluster: Ceph, **kwargs) -> int:
    LOG.info("Add ns and Configure NS masking")
    config = deepcopy(kwargs["config"])
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
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)

    try:
        steps = config.get("steps", [])
        init_nodes = config.get("initiators")
        hostnqn_dict = {}
        LOG.info(init_nodes)
        ha = HighAvailability(ceph_cluster, config["nodes"], **config)
        ha.initialize_gateways()

        for node in init_nodes:
            initiator_node = get_node_by_id(ceph_cluster, node)
            initiator = Initiator(initiator_node)
            initiator.disconnect_all()
            hostnqn, _ = initiator_node.exec_command(
                cmd="cat /etc/nvme/hostnqn",
                sudo=True,
            )
            hostnqn = hostnqn.strip()
            hostnqn_dict[node] = hostnqn
        LOG.info("Hostnqn's are: %s", hostnqn_dict)

        for step in steps:
            cfg = step["config"]
            command = cfg.pop("command")

            if command == "add":
                add_namespaces(cfg, command, init_nodes, hostnqn_dict, rbd_obj, ha)
            if command == "add_host":
                add_host(cfg, command, init_nodes, hostnqn_dict, ha)
            if command == "del_host":
                del_host(cfg, command, init_nodes, hostnqn_dict, ha)
            if command == "change_visibility":
                change_visibility(cfg, command, init_nodes, hostnqn_dict, ha)
        return 0
    except BaseException as be:
        LOG.error(be, exc_info=True)
        return 1
    finally:
        if io_tasks and executor:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=True, cancel_futures=True)
