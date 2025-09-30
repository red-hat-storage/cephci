"""
Module for the test
CEPH-83627298: Verify NVMe namespace reservation types across multiple initiators
"""

import json
import random
from copy import deepcopy

from ceph.ceph import Ceph, CommandFailed
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
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)
RESERVATION_NAMES = {
    1: "Write Exclusive Reservation",
    2: "Exclusive Access Reservation",
    3: "Write Exclusive - Registrants Only Reservation",
    4: "Exclusive Access - Registrants Only Reservation",
    5: "Write Exclusive - All Registrants Reservation",
    6: "Exclusive Access - All Registrants Reservation",
}
RESERVATION_EXPECTATIONS = {
    1: {
        "c1_reg_write": True,
        "c2_reg_write": False,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": True,
    },  # Write Exclusive
    2: {
        "c1_reg_write": True,
        "c2_reg_write": False,
        "c2_reg_read": False,
        "c2_write": False,
        "c2_read": False,
    },  # Exclusive Access
    3: {
        "c1_reg_write": True,
        "c2_reg_write": False,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": False,
    },  # WE - Registrants Only
    4: {
        "c1_reg_write": True,
        "c2_reg_write": False,
        "c2_reg_read": False,
        "c2_write": False,
        "c2_read": False,
    },  # EA - Registrants Only
    5: {
        "c1_reg_write": True,
        "c2_reg_write": True,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": False,
    },  # WE - All Registrants
    6: {
        "c1_reg_write": True,
        "c2_reg_write": True,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": False,
    },  # EA - All Registrants
}


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
    """Run IOs from NVMe Initiators.

    - Discover NVMe targets
    - Connect to subsystem
    - List targets and Run FIO on target devices.

    Args:
        ceph_cluster: Ceph cluster
        gateway: Ceph-NVMeoF Gateway.
        config: Initiator config
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

    results = []
    io_args = {"size": "100%"}
    if config.get("io_args"):
        io_args = config["io_args"]
    with parallel() as p:
        for path in paths:
            _io_args = {}
            if io_args.get("test_name"):
                test_name = f"{io_args['test_name']}-" f"{path.replace('/', '_')}"
                _io_args.update({"test_name": test_name})
            _io_args.update(
                {
                    "device_name": path,
                    "client_node": client,
                    "long_running": True,
                    "cmd_timeout": "notimeout",
                }
            )
            _io_args = {**io_args, **_io_args}
            p.spawn(run_fio, **_io_args)
        for op in p:
            if op != 0:
                raise RuntimeError(f"FIO failed with exit code : {op}")
            results.append(op)
    return results


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
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(
                ceph_cluster, initiator_cfg["node"], initiator_cfg["subnqn"]
            )

    if "disconnect_all" in config["cleanup"]:
        nodes = config.get("disconnect_all")
        if not nodes:
            nodes = config["initiators"]

        disconnect_all_initiator(ceph_cluster, nodes)

    # Delete the multiple subsystems across multiple gateways
    if "subsystems" in config["cleanup"]:
        config_sub_node = config["subsystems"]
        if not isinstance(config_sub_node, list):
            config_sub_node = [config_sub_node]
        for sub_cfg in config_sub_node:
            node = config["gw_node"] if "node" not in sub_cfg else sub_cfg["node"]
            LOG.info(f"Deleting subsystem {sub_cfg['nqn']} on gateway {node}")
            nvmegwcli.subsystem.delete(
                **{"args": {"subsystem": sub_cfg["nqn"], "force": True}}
            )

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def test_reservation(
    rtype,
    initiator_1,
    initiator_2,
    client_node_1,
    client_node_2,
    dev_to_be_reserved,
    dev_nsid_to_be_reserved,
    rkey,
):
    expectations = RESERVATION_EXPECTATIONS[rtype]
    rtype_name = RESERVATION_NAMES[rtype]

    LOG.info("\n\n=== Testing %s (rtype=%s) ===\n\n", rtype_name, rtype)

    # Step 1: Register + Acquire reservation on Client1

    LOG.info("Registering Client 1 for reservation")
    initiator_1.register_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "nrkey": rkey,
        }
    )
    LOG.info("Client 1 acquiring lock..")
    initiator_1.acquire_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "rtype": rtype,
            "crkey": rkey,
        }
    )
    report = initiator_1.report_reservation(
        **{"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved}
    )
    LOG.info("Report:\n %s", report)

    # Step 2: If registrants-only or all-registrants, Client2 also registers

    LOG.info("Registering Client 2 for reservation")
    initiator_2.register_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "nrkey": rkey,
        }
    )
    report = initiator_2.report_reservation(
        **{"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved}
    )
    LOG.info("Report:\n %s", report)

    # Step 3: Client1 Write
    LOG.info(" rtype-%s : Reservation holder Client1 Write test", rtype)
    try:
        run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_1,
            io_type="write",
            size="100M",
        )
        assert expectations[
            "c1_reg_write"
        ], "Reservation holder Client1 write succeeded but should have failed"
        LOG.info("rtype-%s : Reservation holder Client1 write succeeded", rtype)
    except CommandFailed:
        assert not expectations[
            "c1_reg_write"
        ], "Reservation holder Client1 write failed unexpectedly"
        LOG.info(
            "rtype-%s : Reservation holder Client1 write failed as expected", rtype
        )

    # Step 4: Registered Client2 Write

    LOG.info("rtype-%s : Registered [Client2]  Write test", rtype)
    try:
        run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="write",
            size="100M",
        )
        assert expectations[
            "c2_reg_write"
        ], "Registered Client2 write succeeded but should have failed"
        LOG.info("rtype-%s : Registered Client2 write succeeded", rtype)
    except CommandFailed:
        assert not expectations[
            "c2_reg_write"
        ], "Registered Client2 write failed unexpectedly"
        LOG.info("rtype-%s : Registered Client2 write failed as expected", rtype)

    # Step 5: Registered Client2 Read

    LOG.info("rtype-%s : Registered [Client2] Read test", rtype)
    try:
        run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="read",
            size="100M",
        )
        assert expectations[
            "c2_reg_read"
        ], "Registered Client2 read succeeded but should have failed"
        LOG.info("rtype-%s : Registered Client2 read succeeded", rtype)
    except CommandFailed:
        assert not expectations[
            "c2_reg_read"
        ], "Registered Client2 read failed unexpectedly"
        LOG.info("rtype-%s : Registered Client2 read failed as expected", rtype)

    LOG.info("Unregistering Client 2 reservation")
    initiator_2.register_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "crkey": rkey,
            "rrega": 1,
        }
    )

    report = initiator_2.report_reservation(
        **{"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved}
    )
    LOG.info("Report:\n %s", report)

    # Step 6: Unregistered Client2 Write

    LOG.info("rtype - %s : Unregistered Client2 Write test", rtype)
    try:
        run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="write",
            size="100M",
        )
        assert expectations[
            "c2_reg_read"
        ], "Unregistered Client2 write succeeded but should have failed"
        LOG.info("rtype - %s : Unregistered Client2 write succeeded", rtype)
    except CommandFailed:
        assert not expectations[
            "c2_write"
        ], "Unregistered Client2 write failed unexpectedly"
        LOG.info("rtype - %s : Unregistered Client2 write failed as expected", rtype)

    # Step 7: Unregistered Client2 Read

    LOG.info("rtype-%s : Unregistered Client2 Read test", rtype)
    try:
        run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="read",
            size="100M",
        )
        assert expectations[
            "c2_read"
        ], "Unregistered Client2 read succeeded but should have failed"
        LOG.info("rtype-%s : Unregistered Client2 read succeeded", rtype)
    except CommandFailed:
        assert not expectations[
            "c2_read"
        ], "Unregistered Client2 read failed unexpectedly"
        LOG.info("rtype-%s : Unregistered Client2 read failed as expected", rtype)
    LOG.info("Releasing lock by Client1")
    initiator_1.release_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "rtype": rtype,
            "crkey": rkey,
            "rrela": 1,
        }
    )
    report = initiator_1.report_reservation(
        **{"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved}
    )
    LOG.info("Report:\n %s", report)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"][0])
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
        inits = []
        devs = {}
        client_nodes = []
        for node in config["initiators"]:
            client_node = get_node_by_id(ceph_cluster, node)
            init = NVMeInitiator(client_node)
            init.connect_targets(nvmegwcli, {"nqn": "connect-all"})
            devs[node] = init.list_spdk_drives(nsid_device_pair=1)
            inits.append(init)
            client_nodes.append(client_node)

        initiator_1 = inits[0]
        client_node_1 = client_nodes[0]
        initiator_2 = inits[1]
        client_node_2 = client_nodes[1]
        LOG.debug("Namespace info:  %s", devs)
        dev_to_be_reserved = devs[config["initiators"][0]][0]["Namespace"]
        dev_nsid_to_be_reserved = devs[config["initiators"][0]][0]["NSID"]
        # Generate a random reservation key
        rkey = random.randint(1000, 9999)
        LOG.debug("Using randomly generated 4 digit key %s", rkey)
        for rtype in RESERVATION_NAMES.keys():
            test_reservation(
                rtype,
                initiator_1,
                initiator_2,
                client_node_1,
                client_node_2,
                dev_to_be_reserved,
                dev_nsid_to_be_reserved,
                rkey,
            )
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)

    return 1
