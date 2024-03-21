"""
Test suite that verifies the deployment of Red Hat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.nvmegw_cli.common import find_client_daemon_id
from ceph.nvmegw_cli.connection import Connection
from ceph.nvmegw_cli.gateway import Gateway
from ceph.nvmegw_cli.host import Host
from ceph.nvmegw_cli.listener import Listener
from ceph.nvmegw_cli.log_level import LogLevel
from ceph.nvmegw_cli.namespace import Namespace
from ceph.nvmegw_cli.subsystem import Subsystem
from ceph.nvmegw_cli.version import Version
from ceph.utils import get_node_by_id
from utility.log import Log

LOG = Log(__name__)


services = {
    "log_level": LogLevel,
    "version": Version,
    "gateway": Gateway,
    "connection": Connection,
    "subsystem": Subsystem,
    "listener": Listener,
    "host": Host,
    "namespace": Namespace,
}


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
                    service: version          # CLI Version
                    command: version
                    base_cmd_args:
                        format: json
                        output: log
                - config:
                    service: gateway
                    command: version         # gateway Version
                    base_cmd_args:
                        format: json
                        output: log
    """
    LOG.info("Manage Ceph NVMeoF entities over CLI.")
    config = deepcopy(kwargs["config"])
    node = get_node_by_id(ceph_cluster, config["node"])
    port = config.get("port", 5500)

    overrides = kwargs.get("test_data", {}).get("custom-config")
    cli_image = None
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            cli_image = value
            break

    try:
        steps = config.get("steps", [])
        for step in steps:
            cfg = step["config"]
            service = cfg.pop("service")
            command = cfg.pop("command")

            _cls = services[service](node, port)
            if cli_image:
                _cls.NVMEOF_CLI_IMAGE = cli_image
            if service in "listener" and command in ["add", "delete"]:
                gw_node = get_node_by_id(ceph_cluster, cfg["args"]["gateway-name"])
                client_id = find_client_daemon_id(gw_node)
                cfg["args"].update(
                    {"gateway-name": client_id, "traddr": gw_node.ip_address}
                )
            func = fetch_method(_cls, command)
            func(**cfg)
    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    return 0
