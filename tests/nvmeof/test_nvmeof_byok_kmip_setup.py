"""Suite-level dummy KMIP server setup and teardown for NVMe-oF BYOK tests.

Started once after client configure; reused by TC-01–TC-04 via cephci
``test_data``. Per-test keys and subsystem endpoints are still registered
inside each TC.

Config:
  command: setup | teardown
"""

from ceph.ceph import Ceph
from tests.nvmeof.workflows.kmip_utils import (
    SHARED_KMIP_KEY,
    setup_kmip_for_nvmeof,
    teardown_kmip,
)
from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """cephci entry point."""
    config = kwargs.get("config") or {}
    custom_data = kwargs.get("test_data")
    if custom_data is None:
        custom_data = {}
        kwargs["test_data"] = custom_data

    command = config.get("command", "setup")
    if command == "teardown":
        return _teardown(custom_data)
    return _setup(ceph_cluster, config, custom_data)


def _setup(ceph_cluster, config, custom_data):
    """Start dummy KMIP, copy certs to GW nodes, stash handle on test_data."""
    config.setdefault("use_dummy_kmip", True)
    config.setdefault("luks_combos", [])
    LOG.info("Starting suite-level dummy KMIP server for BYOK tests")
    kmip_info = setup_kmip_for_nvmeof(ceph_cluster, config, custom_data)
    custom_data[SHARED_KMIP_KEY] = kmip_info
    kmip_cfg = kmip_info.get("kmip_cfg") or {}
    LOG.info(
        "Suite KMIP server ready at %s:%s (server_name=%s)",
        kmip_cfg.get("host"),
        kmip_cfg.get("port"),
        kmip_cfg.get("server_name"),
    )
    return 0


def _teardown(custom_data):
    """Stop the suite-level dummy KMIP server if present."""
    kmip_info = custom_data.pop(SHARED_KMIP_KEY, None)
    if not kmip_info:
        LOG.info("No suite-level KMIP server to tear down")
        return 0
    LOG.info("Tearing down suite-level dummy KMIP server")
    teardown_kmip(kmip_info)
    return 0
