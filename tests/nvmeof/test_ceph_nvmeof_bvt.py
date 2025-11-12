"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
    deploy_nvme_service,
    nvme_gw_cli_version_adapter,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": config["nqn"]}
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    **(
                        {"no-group-append": config.get("no-group-append", True)}
                        if ceph_cluster.rhcs_version >= "8.0"
                        else {}
                    ),
                },
            }
        }
    )
    listener_cfg = {
        "host-name": nvmegwcli.fetch_gateway_hostname(),
        "traddr": nvmegwcli.node.ip_address,
        "trsvcid": config["listener_port"],
    }
    nvmegwcli.listener.add(**{"args": {**listener_cfg, **sub_args}})
    if config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(config["allow_host"])}}}
        )

    if config.get("hosts"):
        for host in config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = NVMeInitiator(initiator_node)
            host_nqn = initiator.initiator_nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

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
                p.spawn(nvmegwcli.namespace.add, **ns_args)


def initiators(ceph_cluster, gateway, config):
    """
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
            node: node4
    """
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = NVMeInitiator(client)
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
    paths = initiator.list_devices()
    LOG.info("Disocvered nvme namespaces in client and paths found are %s", paths)


def disconnect_initiator(ceph_cluster, node, subnqn):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = NVMeInitiator(node)
    initiator.disconnect(**{"nqn": subnqn})


def disconnect_all_initiator(ceph_cluster, nodes):
    """Disconnect all connections on Initiator."""
    for node in nodes:
        node = get_node_by_id(ceph_cluster, node)
        initiator = NVMeInitiator(node)
        initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, nvmegwcli, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    try:
        # Delete the multiple Initiators across multiple gateways
        if "initiators" in config["cleanup"]:
            for initiator_cfg in config["initiators"]:
                disconnect_initiator(
                    ceph_cluster, initiator_cfg["node"], initiator_cfg["subnqn"]
                )

        if "disconnect_all" in config["cleanup"]:
            nodes = config.get("disconnect_all")
            if not nodes:
                nodes = [i["node"] for i in config["initiators"]]

            disconnect_all_initiator(ceph_cluster, nodes)

        # Delete the multiple subsystems across multiple gateways
        if "subsystems" in config["cleanup"]:
            if not nvmegwcli:
                raise Exception(
                    "NVMe Gateway CLI instance not found to delete subsystems"
                )
            config_sub_node = config["subsystems"]
            if not isinstance(config_sub_node, list):
                config_sub_node = [config_sub_node]
            for sub_cfg in config_sub_node:
                node = config["gw_node"] if "node" not in sub_cfg else sub_cfg["node"]
                LOG.info("Deleting subsystem %s on gateway %s", sub_cfg["nqn"], node)
                nvmegwcli.subsystem.delete(
                    **{"args": {"subsystem": sub_cfg["nqn"], "force": True}}
                )

        # Delete the gateway
        if "gateway" in config["cleanup"]:
            delete_nvme_service(ceph_cluster, config)

        # Delete the pool
        if "pool" in config["cleanup"]:
            rbd_obj.clean_up(pools=[config["rbd_pool"]])
    except Exception as e:
        LOG.error(f"Teardown failed with error: {e}")
        return 1
    return 0


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure nvme service, subsystems, initiators and namespaces

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    """
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    custom_config = kwargs.get("test_data", {}).get("custom-config")

    ceph = Orch(ceph_cluster, **{})

    nvmegwcli = None
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    try:
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        nvmegwcli = create_gateway(
            nvme_gw_cli_version_adapter(ceph_cluster),
            gw_node,
            mtls=config.get("mtls"),
            shell=getattr(ceph, "shell"),
            port=config.get("gw_port", 5500),
            gw_group=config.get("gw_group"),
        )

        if config.get("cleanup-only"):
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)
            return 0

        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    p.spawn(
                        configure_subsystems,
                        ceph_cluster,
                        rbd_obj,
                        rbd_pool,
                        nvmegwcli,
                        subsys_args,
                    )

        if config.get("initiators"):
            with parallel() as p:
                for initiator in config["initiators"]:
                    p.spawn(initiators, ceph_cluster, nvmegwcli, initiator)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            rc = teardown(ceph_cluster, rbd_obj, nvmegwcli, config)
            if rc != 0:
                return rc

    return 1
