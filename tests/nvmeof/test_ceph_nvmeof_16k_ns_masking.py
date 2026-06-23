"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway at scale
with supported entities like subsystems, namespaces, etc.
"""

import json
from collections import defaultdict
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_subsystems(config, _cls, command):
    max_ns = config["args"].pop("max-namespaces")

    def configure_subsystem(num):
        args_copy = config["args"].copy()
        args_copy.update(
            {
                "nqn": f"nqn.2016-06.io.spdk:cnode{num}",
                "serial_number": f"{num}",
                "max_namespaces": max_ns,
            }
        )
        subsystem_func = fetch_method(_cls, command)
        subsystem_func(**{"args": args_copy})

    # Create subsystems in parallel
    with parallel() as p:
        for num in range(1, config["args"].pop("subsystems") + 1):
            p.spawn(configure_subsystem, num)


def configure_listeners(config, nvme_service, command, ceph_cluster):
    port = config["args"].pop("port")
    subsystems = config["args"].pop("subsystems")
    nodes = config["args"].pop("nodes")
    group = config["args"].pop("group", None)
    LOG.info(nodes)

    def configure_listener(num, listener_node, _cls):
        subnqn = (
            f"nqn.2016-06.io.spdk:cnode{num}{f'.{group}' if group is not None else ''}"
        )

        args_copy = config["args"].copy()
        args_copy.update(
            {
                "nqn": subnqn,
                "host_name": listener_node.hostname,
                "traddr": listener_node.ip_address,
                "trsvcid": port,
            }
        )

        listener_func = fetch_method(_cls, command)
        listener_func(**{"args": args_copy})

    # Configure listeners in parallel
    with parallel() as p:
        for node in nvme_service.gateways:
            _cls = fetch_method(node, "listener")
            listener_node = get_node_by_id(ceph_cluster, node.node.id)
            for num in range(1, subsystems + 1):
                p.spawn(configure_listener, num, listener_node, _cls)


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
                "nqn": subnqn,
                "host_nqn": hostnqn,
            }
        )

        host_access_func = fetch_method(_cls, command)
        host_access_func(**{"args": args_copy})

    # Configure hosts for subsystems in parallel
    with parallel() as p:
        for num in range(1, subsystems + 1):
            p.spawn(configure_host, num)


def configure_namespaces(config, _cls, command, rbd_obj):
    subsystems = config["args"].pop("subsystems")
    namespaces = config["args"].pop("namespaces", None)
    namespaces_per_subsystem = config["args"].pop("namespaces_per_subsystem", None)
    image_size = config["args"].pop("image_size", None)
    group = config["args"].pop("group", None)
    pool = config["args"].pop("pool", None)
    if namespaces_per_subsystem is not None:
        namespaces_sub = int(namespaces_per_subsystem)
    else:
        namespaces_sub = int(namespaces / subsystems) if namespaces else 0

    if command != "add":
        return

    namespace_func = fetch_method(_cls, command)
    for sub_num in range(1, subsystems + 1):
        LOG.info("Subsystem %s", sub_num)
        name = generate_unique_id(length=4)
        subnqn = f"nqn.2016-06.io.spdk:cnode{sub_num}{f'.{group}' if group else ''}"

        def _add_namespace(num):
            rbd_obj.create_image(pool, f"{name}-image{num}", image_size)
            ns_config = {
                "base_cmd_args": {"format": "json"},
                "args": {
                    "rbd_image_name": f"{name}-image{num}",
                    "rbd_pool": pool,
                    "nqn": subnqn,
                    "force": True,
                },
            }
            namespace_func(**ns_config)

        with parallel() as p:
            for num in range(1, namespaces_sub + 1):
                p.spawn(_add_namespace, num)


def change_visibility(_cls):
    """Change visibility of namespaces."""
    list_config = {"base_cmd_args": {"format": "json"}}
    out, _ = fetch_method(_cls, "list")(**list_config)
    namespaces = json.loads(out)
    change_visibility_func = fetch_method(_cls, "change_visibility")
    for namespace in namespaces["namespaces"]:
        subsystem_nqn = namespace["ns_subsystem_nqn"]
        namespace_id = namespace["nsid"]
        change_visibility_func(
            **{
                "args": {
                    "nqn": subsystem_nqn,
                    "nsid": namespace_id,
                    "auto_visible": "false",
                    "force": "",
                }
            }
        )
        LOG.info(
            "Changed visibility of namespace %s in subsystem %s",
            namespace_id,
            subsystem_nqn,
        )


def _add_hosts_for_subsystem_namespaces(
    subsystem_nqn, ns_list, add_host_func, host_nqn_list
):
    """Add all host NQNs to every namespace under one subsystem (call serially per gateway)."""
    for ns in ns_list:
        namespace_id = ns["nsid"]
        for host_nqn in host_nqn_list:
            add_host_func(
                **{
                    "args": {
                        "nqn": subsystem_nqn,
                        "nsid": namespace_id,
                        "host_nqn": host_nqn,
                    }
                }
            )
            LOG.info(
                "Added host nqn %s to namespace %s in subsystem %s",
                host_nqn,
                namespace_id,
                subsystem_nqn,
            )


def add_host(config, _cls, nvmegwcli, ceph_cluster, init_config):
    """Add host to namespaces and run IO."""
    config["args"].pop("subsystems", None)
    host_nqn_list = []
    initiator_list = []
    for init_entry in init_config:
        node_id = (
            init_entry.get("node", init_entry)
            if isinstance(init_entry, dict)
            else init_entry
        )
        initiator_node = get_node_by_id(ceph_cluster, node_id)
        initiator = NVMeInitiator(initiator_node)
        initiator.disconnect_all()
        host_nqn_list.append(initiator.initiator_nqn())
        initiator_list.append(initiator)

    # Add dummy host NQNs for testing
    for i in range(10, 24):
        host_nqn_list.append(
            f"nqn.2014-08.org.nvmexpress:uuid:6df361f5-7895-438f-a33e-5c056f1b65{i}"
        )

    list_config = {"base_cmd_args": {"format": "json"}}
    out, _ = fetch_method(_cls, "list")(**list_config)
    namespaces = json.loads(out)
    add_host_func = fetch_method(_cls, "add_host")

    by_subsystem = defaultdict(list)
    for namespace in namespaces["namespaces"]:
        by_subsystem[namespace["ns_subsystem_nqn"]].append(namespace)
    all_subsystem_nqns = set(by_subsystem.keys())

    # Configure hosts for subsystems in parallel
    with parallel() as p:
        for subsystem_nqn, ns_list in by_subsystem.items():
            p.spawn(
                _add_hosts_for_subsystem_namespaces,
                subsystem_nqn,
                ns_list,
                add_host_func,
                host_nqn_list,
            )

    initiator = initiator_list[0]
    first_init = init_config[0]
    init_cfg = (
        dict(first_init) if isinstance(first_init, dict) else {"node": first_init}
    )
    init_cfg.update({"nqn": next(iter(all_subsystem_nqns)), "listener_port": "4420"})
    initiator.connect_targets(nvmegwcli, init_cfg)
    paths = initiator.list_devices()
    initiator.start_fio(paths=paths[0:10], test_name="nvmeof_16k_ns_masking")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution at scale.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.
    - Scale subsystems and namespaces

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW scale test
            - test:
                name: Ceph NVMeoF deployment at scale
                desc: Configure NVMEoF gateways and initiators at scale
                config:
                    node: node7
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    steps:
                      - config:
                          service: subsystem
                          command: add
                          args:
                            subsystems: 2
                            max-namespaces: 1024
                      - config:
                          service: listener
                          command: add
                          args:
                            group: group1
                            subsystems: 2
                            port: 4420
                            nodes:
                              - node7
                              - node8
                    initiators:
                      listener_port: 4420
                      node: node11
                    run_io:
                      - node: node11
                        io_type: write
    """
    LOG.info("Configure Ceph NVMeoF entities at scale over CLI.")
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

    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    nvme_service = None

    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            LOG.info("deploy nvme service")
            nvme_service.deploy()
        LOG.info("initialize gateways")
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        steps = config.get("steps", [])
        init_config = config.get("initiators")

        for step in steps:
            cfg = step["config"]
            service = cfg.pop("service")
            command = cfg.pop("command")

            _cls = fetch_method(nvmegwcli, service)

            if service == "subsystem":
                configure_subsystems(cfg, _cls, command)
            elif service == "listener":
                configure_listeners(cfg, nvme_service, command, ceph_cluster)
            elif service == "host":
                configure_hosts(cfg, _cls, command)
            elif service == "namespace" and command == "add":
                configure_namespaces(cfg, _cls, command, rbd_obj)
            elif service == "namespace" and command == "change_visibility":
                change_visibility(_cls)
            elif service == "namespace" and command == "add_host":
                add_host(cfg, _cls, nvmegwcli, ceph_cluster, init_config)
            else:
                func = fetch_method(_cls, command)

                if "args" not in cfg:
                    cfg["args"] = {}

                func(**cfg["args"])

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
