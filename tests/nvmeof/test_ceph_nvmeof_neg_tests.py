"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
from time import sleep

from ceph.ceph import Ceph
from ceph.nvmeof.nvmeof_gwcli import NVMeCLI, find_client_daemon_id
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.cephadm import test_nvmeof
from tests.nvmeof.test_ceph_nvmeof_gateway import (
    configure_subsystems,
    initiators,
    teardown,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def remove(ceph_cluster, rbd, pool, config):
    """Remove the image during NVMe images are in use."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:cnode_negative",
            "serial": 111,
            "listener_port": 5006,
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
        "listener_port": 5006,
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
    rbd_pool = "rbd"
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

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
