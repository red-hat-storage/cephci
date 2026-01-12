import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.initiators.linux import Initiator
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.ns_masking import NamespaceMasking
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def add_namespaces(
    config, command, init_nodes, hostnqn_dict, rbd_obj, nvmegwcli, ns_masking
):
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    image_size = config["args"].pop("image_size")
    pool = config["args"].pop("pool")
    no_auto_visible = config["args"].pop("no-auto-visible")
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    namespaces_sub = int(namespaces / subsystems)
    base_cmd_args = {"format": "json"}
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    expected_visibility = False if no_auto_visible else True
    rbd_images_subsys = []

    for sub_num in range(1, subsystems + 1):
        subnqn = f"{subnqn_template.format(sub_num)}"
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
            ns_masking.validate_namespace_masking(
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
        ns_masking.validate_init_namespace_masking(
            command, init_nodes, expected_visibility
        )


def add_host(config, command, init_nodes, hostnqn_dict, nvmegwcli, ns_masking):
    """Add host NQN to namespaces"""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    LOG.info(hostnqn_dict)
    expected_visibility = False
    namespaces_sub = int(namespaces / subsystems)
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    num_namespaces_per_initiator = namespaces_sub // len(hostnqn_dict)
    rbd_images_subsys = {}

    for sub_num in range(1, subsystems + 1):
        LOG.info(f"Subsystem {sub_num}/{subsystems}")
        for num in range(1, namespaces_sub + 1):
            LOG.info(f"Namespace {num}/{namespaces_sub}")
            config["args"].clear()
            subnqn = f"{subnqn_template.format(sub_num)}"

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
                    "force": "",
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
            ns_masking.validate_namespace_masking(
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
                ns_masking.validate_init_namespace_masking(
                    command, init_nodes, expected_visibility, validate_config
                )

    ns_masking.execute_io_ns_masking(init_nodes, rbd_images_subsys, negative=False)


def del_host(config, command, init_nodes, hostnqn_dict, nvmegwcli, ns_masking):
    """Delete host NQN to namespaces"""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    LOG.info(hostnqn_dict)
    namespaces_sub = int(namespaces / subsystems)
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    num_namespaces_per_initiator = namespaces_sub // len(hostnqn_dict)
    rbd_images_subsys = {}

    for sub_num in range(1, subsystems + 1):
        LOG.info(f"Subsystem {sub_num}/{subsystems}")
        for num in range(1, namespaces_sub + 1):
            LOG.info(f"Namespace {num}/{namespaces_sub}")
            config["args"].clear()
            subnqn = f"{subnqn_template.format(sub_num)}"

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
            ns_masking.validate_namespace_masking(
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
                ns_masking.validate_init_namespace_masking(
                    command, init_nodes, expected_visibility, validate_config
                )

    ns_masking.execute_io_ns_masking(init_nodes, rbd_images_subsys, negative=True)


def change_visibility(config, command, init_nodes, hostnqn_dict, nvmegwcli, ns_masking):
    """Change visibility of namespaces."""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    validate_initiators = config["args"].pop("validate_ns_masking_initiators", None)
    auto_visible = config["args"].pop("auto-visible")
    namespaces_sub = int(namespaces / subsystems)
    base_cmd_args = {"format": "json"}
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    expected_visibility = True if auto_visible == "yes" else False
    rbd_images_subsys = []

    for sub_num in range(1, subsystems + 1):
        subnqn = f"{subnqn_template.format(sub_num)}"
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
            ns_masking.validate_namespace_masking(
                num,
                subnqn,
                namespaces_sub,
                hostnqn_dict,
                ns_visibility,
                command,
                expected_visibility,
            )
    if validate_initiators:
        ns_masking.validate_init_namespace_masking(
            command, init_nodes, expected_visibility
        )
    negative = not expected_visibility
    ns_masking.execute_io_ns_masking(init_nodes, rbd_images_subsys, negative=negative)


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
        # Deploy nvmeof service
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            LOG.info("deploy nvme service")
            nvme_service.deploy()

        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        if config.get("subsystems"):
            configure_subsystems(nvme_service)
            configure_listeners(nvme_service.gateways, nvme_service.config)
            configure_hosts(nvme_service.gateways[0], nvme_service.config)

        ns_masking = NamespaceMasking(
            ceph_cluster,
            nvme_service.gateways,
            [],
            Orch(ceph_cluster),
        )

        for node in init_nodes:
            initiator_node = get_node_by_id(ceph_cluster, node)
            initiator = Initiator(initiator_node)
            initiator.disconnect_all()
            configure_cmds = [
                "nvme gen-hostnqn > /etc/nvme/hostnqn",
                "uuidgen > /etc/nvme/hostid",
            ]
            for cmd in configure_cmds:
                initiator_node.exec_command(cmd=cmd, sudo=True)
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
                add_namespaces(
                    cfg,
                    command,
                    init_nodes,
                    hostnqn_dict,
                    rbd_obj,
                    nvmegwcli,
                    ns_masking,
                )
            if command == "add_host":
                add_host(cfg, command, init_nodes, hostnqn_dict, nvmegwcli, ns_masking)
            if command == "del_host":
                del_host(cfg, command, init_nodes, hostnqn_dict, nvmegwcli, ns_masking)
            if command == "change_visibility":
                change_visibility(
                    cfg, command, init_nodes, hostnqn_dict, nvmegwcli, ns_masking
                )
        return 0
    except BaseException as be:
        LOG.error(be, exc_info=True)
        return 1
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)
