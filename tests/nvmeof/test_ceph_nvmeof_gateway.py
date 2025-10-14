"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import json

from ceph.ceph import Ceph
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


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
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    custom_config = kwargs.get("test_data", {}).get("custom-config")

    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    try:
        nvme_service = NVMeService(config, ceph_cluster)

        if config.get("install"):
            nvme_service.deploy()

        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        if config.get("cleanup-only"):
            teardown(nvme_service, rbd_obj)
            return 0

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

        if config.get("initiators"):
            with parallel() as p:
                for initiator in config["initiators"]:
                    p.spawn(initiators, ceph_cluster, nvmegwcli, initiator)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
