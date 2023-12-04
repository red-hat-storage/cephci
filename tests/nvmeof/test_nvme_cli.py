"""
Test suite that verifies the deployment of Red Hat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.nvmeof.nvmeof_gwcli import NVMeCLI, find_client_daemon_id
from ceph.utils import get_node_by_id
from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Return the status of the test execution run with the provided keyword arguments.

    Unlike other test suites, "steps" has been introduced to support workflow style
    execution along with customization.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Manage NVMeoF Gateway entities
            desc: Deploy a minimal cluster
            config:
                steps:
                    - config:
                        command: get_subsystems
                    - config:
                        command: create_block_device
                    - config:
                         command: create_subsystem
                         args:
                           subnqn: nqn.2016-06.io.spdk:cnode1
                           serial_num: 1
                    - config:
                          command: create_listener
                          args:
                            subnqn: nqn.2016-06.io.spdk:cnode1
                            port: 5001
                    - config:
                          command: add_host
                          args:
                            subnqn: nqn.2016-06.io.spdk:cnode1
                            hostnqn: *
                    - config:
                          command: create_block_device
                          args:
                            name: bdev1
                            image: image1
                            pool: rbd
                    - config:
                          command: add_namespace
                          args:
                            subnqn: nqn.2016-06.io.spdk:cnode1
                            bdev: bdev1
    """
    LOG.info("Manage Ceph NVMeoF entities over CLI.")
    config = deepcopy(kwargs["config"])
    node = get_node_by_id(ceph_cluster, config["node"])

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeCLI.CEPH_NVMECLI_IMAGE = value
            break
    nvme_cli = NVMeCLI(node, config.get("port", 5500))

    try:
        steps = config.get("steps", [])
        for step in steps:
            cfg = step["config"]
            command = cfg.pop("command")

            if command in ["create_listener", "delete_listener"]:
                client_id = find_client_daemon_id(
                    ceph_cluster,
                    pool_name=cfg["args"].pop("pool"),
                    node_name=node.hostname,
                )
                cfg["args"].update(
                    {"gateway-name": client_id, "traddr": node.ip_address}
                )
            func = fetch_method(nvme_cli, command)

            # Avoiding exception if no args
            if "args" not in cfg:
                cfg["args"] = {}
            func(**cfg["args"])
    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    return 0
