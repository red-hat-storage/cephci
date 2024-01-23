"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmegw_cli.subsystem import Subsystem
from ceph.nvmeof.initiator import Initiator
from ceph.nvmeof.nvmeof_gwcli import find_client_daemon_id
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.cephadm import test_nvmeof, test_orch
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def configure_subsystems(rbd, pool, subsystem, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": config["nqn"]}
    subsystem.add(
        **{"args": {**sub_args, **{"max-namespaces": config.get("max_ns", 32)}}}
    )

    listener_cfg = {
        "gateway-name": config.pop("gateway-name"),
        "traddr": subsystem.node.ip_address,
        "trsvcid": config["listener_port"],
    }
    subsystem.listener.add(**{"args": {**listener_cfg, **sub_args}})
    subsystem.host.add(**{"args": {**sub_args, **{"host": config["allow_host"]}}})
    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        with parallel() as p:
            count = config["bdevs"].get("count", 1)
            size = config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                p.spawn(rbd.create_image, pool, f"{name}-image{num}", size)
        namespace_args = {**sub_args, **{"rbd-pool": pool}}
        with parallel() as p:
            # Create namespace in gateway
            for num in range(count):
                ns_args = deepcopy(namespace_args)
                ns_args.update({"rbd-image": f"{name}-image{num}"})
                ns_args = {"args": ns_args}
                p.spawn(subsystem.namespace.add, **ns_args)


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
    # import pdb
    # pdb.set_trace()
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = Initiator(client)
    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
    }
    json_format = {"output-format": "json"}

    # Discover the subsystems
    discovery_port = {"trsvcid": 8009}
    _disc_cmd = {**cmd_args, **discovery_port, **json_format}
    sub_nqns, _ = initiator.discover(**_disc_cmd)
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == str(config["listener_port"]):
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {cmd_args}")

    # Connect to the subsystem
    conn_port = {"trsvcid": config["listener_port"]}
    _conn_cmd = {**cmd_args, **conn_port}
    LOG.debug(initiator.connect(**_conn_cmd))

    # List NVMe targets
    targets = initiator.list_spdk_drives()
    if not targets:
        raise Exception(f"NVMe Targets not found on {client.hostname}")
    LOG.debug(targets)
    results = []
    io_args = {"size": "100%"}
    if config.get("io_args"):
        io_args = config["io_args"]
    with parallel() as p:
        for target in targets:
            _io_args = {}
            if io_args.get("test_name"):
                test_name = (
                    f"{io_args['test_name']}-"
                    f"{target['DevicePath'].replace('/', '_')}"
                )
                _io_args.update({"test_name": test_name})
            _io_args.update(
                {
                    "device_name": target["DevicePath"],
                    "client_node": client,
                    "long_running": True,
                    "cmd_timeout": "notimeout",
                }
            )
            _io_args = {**io_args, **_io_args}
            p.spawn(run_fio, **_io_args)
        for op in p:
            results.append(op)
    return results


def disconnect_initiator(ceph_cluster, node, subnqn):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect(**{"nqn": subnqn})


def teardown(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(
                ceph_cluster, initiator_cfg["node"], initiator_cfg["subnqn"]
            )

    # Delete the multiple subsystems across multiple gateways
    if "subsystems" in config["cleanup"]:
        config_sub_node = config["subsystems"]
        if not isinstance(config_sub_node, list):
            config_sub_node = [config_sub_node]
        for sub_cfg in config_sub_node:
            node = config["gw_node"] if "node" not in sub_cfg else sub_cfg["node"]
            sub_node = get_node_by_id(ceph_cluster, node)
            sub_gw = Subsystem(sub_node, 5500)
            LOG.info(f"Deleting subsystem {sub_cfg['nqn']} on gateway {node}")
            sub_gw.delete(**{"args": {"subsystem": sub_cfg["nqn"]}})

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        cfg = {
            "config": {
                "command": "remove",
                "service": "nvmeof",
                "args": {"service_name": f"nvmeof.{config['rbd_pool']}"},
            }
        }
        test_orch.run(ceph_cluster, **cfg)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF deployment
                desc: Configure NVMEoF gateways and initiators
                config:
                    gw_node: node6
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    cleanup-only: true                          # only for cleanup
                    rep_pool_config:
                      pool: rbd
                    install: true                               # Run SPDK with all pre-requisites
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

        # Cleanup-only
            - test:
                  abort-on-fail: true
                  config:
                    gw_node: node6
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    rep_pool_config:
                    subsystems:
                      - nqn: nqn.2016-06.io.spdk:cnode1
                    initiators:
                        - subnqn: nqn.2016-06.io.spdk:cnode1
                          node: node7
                    cleanup-only: true                          # Important param for clean up
                    cleanup:
                        - pool
                        - subsystems
                        - initiators
                        - gateway
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            Subsystem.NVMEOF_CLI_IMAGE = value
            break

    if config.get("cleanup-only"):
        teardown(ceph_cluster, rbd_obj, config)
        return 0

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gw_port = config.get("gw_port", 5500)
        subsystem = Subsystem(gw_node, gw_port)
        if config.get("install"):
            cfg = {
                "config": {
                    "command": "apply",
                    "service": "nvmeof",
                    "args": {"placement": {"nodes": [gw_node.hostname]}},
                    "pos_args": [rbd_pool],
                }
            }
            test_nvmeof.run(ceph_cluster, **cfg)

        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    subsys_args["gateway-name"] = find_client_daemon_id(
                        ceph_cluster, rbd_pool, node_name=gw_node.hostname
                    )
                    p.spawn(
                        configure_subsystems, rbd_obj, rbd_pool, subsystem, subsys_args
                    )

        if config.get("initiators"):
            with parallel() as p:
                for initiator in config["initiators"]:
                    p.spawn(initiators, ceph_cluster, subsystem, initiator)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
