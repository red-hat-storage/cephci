"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
import json
from copy import deepcopy
from time import sleep

from ceph.ceph import Ceph
from ceph.nvmeof.initiator import Initiator
from ceph.nvmeof.nvmeof_gwcli import NVMeCLI, find_client_daemon_id
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.cephadm import test_nvmeof, test_orch
from tests.nvmeof.test_ceph_nvmeof_gateway import (
    configure_subsystems,
    initiators,
    teardown,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import find_free_port, generate_unique_id

LOG = Log(__name__)


def remove(ceph_cluster, rbd, pool, config):
    """Remove the image during NVMe images are in use."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)
    listener_port = find_free_port(gw_node)
    subsystem = dict()
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:cnode_negative",
            "serial": 111,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    subsystem["gateway-name"] = find_client_daemon_id(ceph_cluster, pool)
    configure_subsystems(rbd, pool, gateway, subsystem)
    name = generate_unique_id(length=4)

    # Create image
    img = f"{name}-image"
    rbd.create_image(pool, img, "1G")
    gateway.create_block_device(img, img, pool)
    gateway.add_namespace(subsystem["nqn"], img)

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:cnode_negative",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    config.update(initiator_cfg)
    with parallel() as p:
        p.spawn(initiators, ceph_cluster, gateway, initiator_cfg)
        sleep(20)
        out, err = rbd.remove_image(pool, img, **{"all": True, "check_ec": False})
        if "rbd: error: image still has watchers" not in out + err:
            raise Exception("RBD image removed when its in use.")
        LOG.info("RBD image removal failed as expected when its in use....")

    cleanup_cfg = {
        "gw_node": config["gw_node"],
        "initiators": [initiator_cfg],
        "cleanup": ["initiators", "gateway", "pool"],
        "rbd_pool": pool,
    }
    teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83576084(ceph_cluster, rbd, pool, config):
    """Validate test case CEPH-83576084."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83576084",
            "serial": 112,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    subsystem["gateway-name"] = find_client_daemon_id(ceph_cluster, pool)
    configure_subsystems(rbd, pool, gateway, subsystem)
    name = generate_unique_id(length=4)

    # Create image
    img = f"{name}-image"
    rbd.create_image(pool, img, "1G")
    gateway.create_block_device(img, img, pool)
    gateway.add_namespace(subsystem["nqn"], img)

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83576084",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    config.update(initiator_cfg)
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = Initiator(client)
    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
        "trsvcid": listener_port,
    }

    json_format = {"output-format": "json"}
    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    def check_client(verify=False):
        _disc_cmd = {**cmd_args, **json_format}
        initiator.disconnect_all()
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == str(config["listener_port"]):
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        LOG.debug(initiator.connect(**_cmd_args))
        targets, _ = initiator.list(**json_format)
        _target = json.loads(targets)["Devices"][0]["DevicePath"]
        if not verify:
            return _target
        client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

    target = check_client()
    client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
    client.exec_command(sudo=True, cmd=f"mkfs.ext4 {target}")
    client.exec_command(sudo=True, cmd=f"mount {target} {_dir}")
    client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")

    for _ in "check":
        client.exec_command(sudo=True, cmd=f"umount {_dir}")
        gateway.remove_namespace(subsystem["nqn"], img)
        gateway.add_namespace(subsystem["nqn"], img)
        check_client(verify=True)

    LOG.info("Validation of CEPH-83576084 is successful.")

    cleanup_cfg = {
        "gw_node": config["gw_node"],
        "initiators": [initiator_cfg],
        "cleanup": ["initiators", "gateway", "pool"],
        "rbd_pool": pool,
    }
    teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83575467(ceph_cluster, rbd, pool, config):
    """Validate test case CEPH-83575467."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83575467",
            "serial": 111,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    subsystem["gateway-name"] = find_client_daemon_id(ceph_cluster, pool)
    configure_subsystems(rbd, pool, gateway, subsystem)
    name = generate_unique_id(length=4)

    # Create images
    for i in range(5):
        img = f"{name}-image{i}"
        rbd.create_image(pool, img, "500M")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83575467",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    config.update(initiator_cfg)
    initiators(ceph_cluster, gateway, initiator_cfg)
    _, gw_info_bkp = gateway.get_subsystems()
    gw_info_bkp = json.loads(gw_info_bkp.split("\n", 1)[1])

    # restart nvmeof service
    restart_cfg = {
        "config": {
            "service": f"nvmeof.{pool}",
            "command": "restart",
            "args": {"verify": True},
            "pos_args": [f"nvmeof.{pool}"],
        }
    }
    test_orch.run(ceph_cluster, **restart_cfg)
    _, gw_info = gateway.get_subsystems()
    gw_info = json.loads(gw_info.split("\n", 1)[1])

    if gw_info != gw_info_bkp:
        raise Exception(
            f"GW Entities aren't same. Actual: {gw_info_bkp} Current: {gw_info}"
        )
    LOG.info("Validation of Ceph-83575467 is successful.")

    cleanup_cfg = {
        "gw_node": config["gw_node"],
        "initiators": [initiator_cfg],
        "cleanup": ["initiators", "gateway", "pool"],
        "rbd_pool": pool,
    }
    teardown(ceph_cluster, rbd, cleanup_cfg)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.
    - Runs Image operations and validate the results

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF imaage operation test
                desc: validate RBD image operations on NVMe devices
                config:
                    gw_node: node6
                    operation: remove

    """

    LOG.info("Running Ceph Ceph NVMEoF Negative tests.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeCLI.CEPH_NVMECLI_IMAGE = value
            break

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        cfg = {
            "config": {
                "command": "apply",
                "service": "nvmeof",
                "args": {"placement": {"nodes": [gw_node.hostname]}},
                "pos_args": [rbd_pool],
            }
        }
        test_nvmeof.run(ceph_cluster, **cfg)
        if config["operation"] == "remove":
            remove(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83576084":
            test_ceph_83576084(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83575467":
            test_ceph_83575467(ceph_cluster, rbd_obj, rbd_pool, config)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
