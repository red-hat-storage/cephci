import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmeof.initiator import Initiator
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def add_namespaces(config, command, hostnqn_dict, rbd_obj, ha):
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    image_size = config["args"].pop("image_size")
    group = config["args"].pop("group", None)
    pool = config["args"].pop("pool")
    no_auto_visible = config["args"].pop("no-auto-visible")
    namespaces_sub = int(namespaces / subsystems)

    base_cmd_args = {"format": "json"}
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    expected_visibility = "False" if no_auto_visible == "true" else "True"
    nvmegwcli = ha.gateways[0]

    # Iterate over subsystems
    for sub_num in range(1, subsystems + 1):
        subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"
        name = generate_unique_id(length=4)
        LOG.info(f"Subsystem {sub_num}/{subsystems}")

        # Iterate over namespaces for each subsystem
        for num in range(1, namespaces_sub + 1):
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
                expected_visibility = "false"

            # create the namespace
            _, namespaces_response = nvmegwcli.namespace.add(**config)
            nsid = json.loads(namespaces_response)["nsid"]

            # listing namespace
            _config = {
                "base_cmd_args": base_cmd_args,
                "args": {
                    "nsid": nsid,
                    "subsystem": subnqn,
                },
            }

            # list namespaces and check visibility
            _, namespace_response = nvmegwcli.namespace.list(**_config)
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


def add_host(config, command, hostnqn_dict, ha):
    """Add host NQN to namespaces"""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    group = config["args"].pop("group", None)
    LOG.info(hostnqn_dict)
    expected_visibility = False
    nvmegwcli = ha.gateways[0]

    namespaces_sub = int(namespaces / subsystems)
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    num_namespaces_per_initiator = namespaces_sub // len(hostnqn_dict)

    # Iterate over each subsystem
    for sub_num in range(1, subsystems + 1):
        LOG.info(sub_num)

        # Iterate over each namespace in the current subsystem
        for num in range(1, namespaces_sub + 1):
            LOG.info(num)
            config["args"].clear()
            subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"

            # Determine the correct host for the current namespace `num`
            for i, node in enumerate(
                hostnqn_dict.keys(), start=1
            ):  # Iterate over hostnqn_dict # Calculate the range of namespaces this node should handle
                if num in range(
                    (i - 1) * num_namespaces_per_initiator + 1,
                    i * num_namespaces_per_initiator + 1,
                ):
                    host = hostnqn_dict[node]
                    break

            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "host": host,
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
            _, namespace_response = nvmegwcli.namespace.list(**_config)
            ns_visibility = json.loads(namespace_response)["namespaces"][0]["hosts"]
            ha.validate_namespace_masking(
                num,
                subnqn,
                namespaces_sub,
                hostnqn_dict,
                ns_visibility,
                command,
                expected_visibility,
            )


def del_host(config, command, hostnqn_dict, ha):
    """Delete host NQN to namespaces"""
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    group = config["args"].pop("group", None)
    LOG.info(hostnqn_dict)
    namespaces_sub = int(namespaces / subsystems)
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    num_namespaces_per_initiator = namespaces_sub // len(hostnqn_dict)
    nvmegwcli = ha.gateways[0]

    for sub_num in range(1, subsystems + 1):
        LOG.info(sub_num)
        for num in range(1, namespaces_sub + 1):
            LOG.info(num)
            config["args"].clear()
            subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"

            # Determine the correct host for the current namespace `nsid`
            for i, node in enumerate(
                hostnqn_dict.keys(), start=1
            ):  # Iterate over hostnqn_dict
                # Calculate the range of namespaces this node should handle
                if num in range(
                    (i - 1) * num_namespaces_per_initiator + 1,
                    i * num_namespaces_per_initiator + 1,
                ):
                    host = hostnqn_dict[node]
                    break

            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "host": host,
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
            _, namespace_response = nvmegwcli.namespace.list(**_config)
            ns_visibility = json.loads(namespace_response)["namespaces"][0]["hosts"]
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


def change_visibility(config, command, hostnqn_dict, ha):
    """Change visibility of namespaces."""
    # Extract parameters from config
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces")
    group = config["args"].pop("group", None)
    auto_visible = config["args"].pop("auto-visible")
    namespaces_sub = int(namespaces / subsystems)
    nvmegwcli = ha.gateways[0]

    base_cmd_args = {"format": "json"}
    subnqn_template = "nqn.2016-06.io.spdk:cnode{}"
    expected_visibility = "True" if auto_visible == "yes" else "False"

    # Iterate over subsystems
    for sub_num in range(1, subsystems + 1):
        # Update the initiator configuration
        subnqn = f"{subnqn_template.format(sub_num)}{f'.{group}' if group else ''}"
        LOG.info(f"Subsystem {sub_num}/{subsystems}")

        # Iterate over namespaces for each subsystem
        for num in range(1, namespaces_sub + 1):
            config["args"].clear()
            config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "nsid": num,
                    "subsystem": subnqn,
                    "auto-visible": auto_visible,
                },
            }
            nvmegwcli.namespace.change_visibility(**config)
            # Listing namespace and checking visibility
            _config = {
                "base_cmd_args": base_cmd_args,
                "args": {"nsid": num, "subsystem": subnqn},
            }
            _, namespace_response = nvmegwcli.namespace.list(**_config)
            ns_visibility = json.loads(namespace_response)["namespaces"][0][
                "auto_visible"
            ]

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
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    try:
        steps = config.get("steps", [])
        init_nodes = config.get("initiators")
        hostnqn_dict = {}
        LOG.info(init_nodes)
        ha = HighAvailability(ceph_cluster, config["nodes"], **config)

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
                add_namespaces(cfg, command, hostnqn_dict, rbd_obj, ha)
            if command == "add_host":
                add_host(cfg, command, hostnqn_dict, ha)
            if command == "del_host":
                del_host(cfg, command, hostnqn_dict, ha)
            if command == "change_visibility":
                change_visibility(cfg, command, hostnqn_dict, ha)

    except BaseException as be:
        LOG.error(be, exc_info=True)
        return 1

    return 0
