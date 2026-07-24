"""
Test suite that verifies the deployment of Red Hat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""

from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    nvme_gw_cli_version_adapter,
    validate_nvme_metadata,
)
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
    pool = config.get("pool")
    group = config.get("gw_group", "")
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    ceph = Orch(ceph_cluster, **{})

    nvmegwcli = None
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    try:
        nvmegwcli = create_gateway(
            nvme_gw_cli_version_adapter(ceph_cluster),
            node,
            mtls=config.get("mtls"),
            shell=getattr(ceph, "shell"),
            port=config.get("gw_port", 5500),
            gw_group=config.get("gw_group"),
        )

        steps = config.get("steps", [])
        for step in steps:
            cfg = deepcopy(step["config"])
            service = cfg.pop("service")
            command = cfg.pop("command")

            _cls = fetch_method(nvmegwcli, service)
            if service in "listener" and command in ["add", "delete"]:
                gw_node = get_node_by_id(ceph_cluster, cfg["args"]["host-name"])
                cfg["args"].update(
                    {"host-name": gw_node.hostname, "traddr": gw_node.ip_address}
                )
                if nvmegwcli.cli_version != "v2":
                    cfg["base_cmd_args"] = {"server-address": gw_node.ip_address}
            func = fetch_method(_cls, command)
            func(**cfg)

            # Validate NVMe metadata in OMAP
            if "validate" in cfg:
                if cfg["validate"].get("omap"):
                    validate_nvme_metadata(
                        cluster=ceph_cluster,
                        config=step["config"],
                        pool=pool,
                        group=group,
                    )
    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    return 0
