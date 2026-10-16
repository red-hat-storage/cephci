"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import json
from time import sleep

from ceph.ceph import Ceph
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    supports_subsystem_model_name,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


def models_from_nvme_list(nvme_list_output):
    """Return Model column values from the ``nvme list`` table.

    Column bounds come from the header row (Model through Namespace), so both
    the current layout (with Generic) and the older layout are handled.
    """
    lines = [
        line.rstrip() for line in str(nvme_list_output).splitlines() if line.strip()
    ]
    header_idx = None
    for index, line in enumerate(lines):
        if "Model" in line and "Namespace" in line and line.strip().startswith("Node"):
            header_idx = index
            break
    if header_idx is None:
        raise RuntimeError(
            f"nvme list header with Model column not found:\n{nvme_list_output}"
        )

    header = lines[header_idx]
    start = header.index("Model")
    end = header.index("Namespace")
    models = []
    for line in lines[header_idx + 1 :]:
        if set(line.strip()) <= set("- "):
            continue
        if not line.strip().startswith("/dev/"):
            continue
        models.append(line[start:end].strip())
    return models


def verify_nvme_list_model(initiator, expected_model, tries=4, delay=3):
    """Fail unless ``expected_model`` appears under Model in ``nvme list``."""
    last_out = ""
    for attempt in range(1, tries + 1):
        out, _ = initiator.list()
        last_out = out or ""
        LOG.info("nvme list on %s:\n%s", initiator.node.hostname, last_out)
        models = models_from_nvme_list(last_out)
        LOG.info("nvme list Model column values: %s", models)
        if expected_model in models:
            LOG.info(
                "Model %s is reflected in nvme list on %s",
                expected_model,
                initiator.node.hostname,
            )
            return
        if attempt < tries:
            sleep(delay)
    raise RuntimeError(
        f"Model {expected_model!r} not found under Model in nvme list "
        f"on {initiator.node.hostname}.\n{last_out}"
    )


def model_name_for_initiator(initiator_cfg, model_by_nqn):
    """Return the model configured for this initiator's subsystem, if any."""
    subnqn = initiator_cfg.get("subnqn") or initiator_cfg.get("nqn")
    if not subnqn or subnqn == "connect-all":
        return None
    if subnqn in model_by_nqn:
        return model_by_nqn[subnqn]
    for nqn, model_name in model_by_nqn.items():
        # Gateway may append ".<group>" when no-group-append is false.
        if subnqn.startswith(f"{nqn}.") or nqn.startswith(f"{subnqn}."):
            return model_name
    return None


def initiators(ceph_cluster, gateway, config, expected_model=None):
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

    # On Ceph 9.2+, a subsystem created with --model_name must show that
    # name under Model in the initiator's nvme list table.
    if expected_model:
        verify_nvme_list_model(initiator, expected_model)
        paths = initiator.list_devices(model=expected_model)
    else:
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
            if config.get("output"):
                _io_args.update(
                    {
                        "output": config["output"],
                    }
                )
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
            if "/tmp/" in str(op):
                results.append(op)
            elif op != 0:
                raise RuntimeError(f"FIO failed with exit code : {op}")
            else:
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
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        model_by_nqn = {}
        if supports_subsystem_model_name(ceph_cluster, nvme_service.ceph_version):
            for sub in config.get("subsystems") or []:
                if not isinstance(sub, dict) or not sub.get("model_name"):
                    continue
                nqn = sub.get("nqn") or sub.get("subnqn")
                if nqn:
                    model_by_nqn[nqn] = sub["model_name"]

        if config.get("initiators"):
            with parallel() as p:
                for initiator in config["initiators"]:
                    expected_model = model_name_for_initiator(initiator, model_by_nqn)
                    p.spawn(
                        initiators,
                        ceph_cluster,
                        nvmegwcli,
                        initiator,
                        expected_model,
                    )
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
