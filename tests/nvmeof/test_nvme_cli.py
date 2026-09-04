"""
Test suite that verifies the deployment of Red Hat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""

import ipaddress
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


def _resolve_network_mask(ceph_cluster, network_mask, fallback_node=None):
    """Resolve node-id based network-mask values to CIDR (e.g. node6 -> x.x.x.0/24).

    Raises ValueError if a non-CIDR value cannot be resolved to a node IP.
    """
    if not network_mask or "/" in str(network_mask):
        return network_mask

    node = get_node_by_id(ceph_cluster, network_mask)
    if node is None and fallback_node is not None:
        # Suites often use the GW CLI node id (e.g. node6) as network-mask shorthand.
        fb_id = getattr(fallback_node, "id", None)
        fb_host = getattr(fallback_node, "hostname", "") or ""
        if network_mask == fb_id or network_mask in fb_host.split("-"):
            node = fallback_node

    if node is None:
        raise ValueError(
            f"Unable to resolve network-mask '{network_mask}' to a CIDR; "
            "provide a CIDR (e.g. 10.0.0.0/24) or a valid cluster node id"
        )

    ip = getattr(node, "ip_address", None)
    if not ip:
        raise ValueError(
            f"Node '{network_mask}' has no ip_address; cannot build network-mask CIDR"
        )

    subnet = getattr(node, "subnet", None)
    if subnet and "/" in str(subnet):
        return str(ipaddress.ip_network(str(subnet), strict=False))

    return str(ipaddress.ip_network(f"{ip}/24", strict=False))


def _execute_nvme_step(func, cfg, expect_failure):
    """Run a NVMe CLI step; invert success/failure when expect_failure is set."""
    if expect_failure:
        try:
            func(**cfg)
        except Exception as exc:
            LOG.info("Expected NVMe CLI failure: %s", exc)
            return
        raise AssertionError(f"Expected NVMe CLI step to fail but it succeeded: {cfg}")
    func(**cfg)


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
            if service == "listener" and command in ["add", "delete"]:
                gw_node = get_node_by_id(ceph_cluster, cfg["args"]["host-name"])
                cfg["args"].update(
                    {"host-name": gw_node.hostname, "traddr": gw_node.ip_address}
                )
                if nvmegwcli.cli_version != "v2":
                    cfg["base_cmd_args"] = {"server-address": gw_node.ip_address}
            # Resolve node-id network masks (e.g. node6 -> x.x.x.0/24) for any
            # subsystem command that accepts network_mask (add, add_network, del_network).
            # Required for suite step subsystem add cnode3 (network-mask: node6).
            if service == "subsystem" and cfg.get("args"):
                for key in ("network-mask", "network_mask"):
                    if key in cfg["args"]:
                        resolved = _resolve_network_mask(
                            ceph_cluster,
                            cfg["args"][key],
                            fallback_node=node,
                        )
                        LOG.info(
                            "Resolved subsystem %s %s=%r -> %r",
                            command,
                            key,
                            cfg["args"][key],
                            resolved,
                        )
                        cfg["args"][key] = resolved
            func = fetch_method(_cls, command)
            expect_failure = cfg.pop("expect_failure", False)
            _execute_nvme_step(func, cfg, expect_failure)

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
