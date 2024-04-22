"""
Test suite that verifies the deployment of Red Hat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmegw_cli.common import find_gateway_hostname
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
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    nvmegwcli = NVMeGWCLI(node, port)
    try:
        steps = config.get("steps", [])
        for step in steps:
            cfg = step["config"]
            service = cfg.pop("service")
            command = cfg.pop("command")

            _cls = fetch_method(nvmegwcli, service)
            if service in "listener" and command in ["add", "delete"]:
                gw_node = get_node_by_id(ceph_cluster, cfg["args"]["host-name"])
                hostname = find_gateway_hostname(gw_node)
                cfg["args"].update(
                    {"host-name": hostname, "traddr": gw_node.ip_address}
                )
                cfg["base_cmd_args"] = {"server-address": gw_node.ip_address}
            func = fetch_method(_cls, command)
            func(**cfg)
    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    return 0
