"""
TLS-PSK + DHCHAP In-Band Auth Stacking Tests — TS-08.

Both TLS-PSK transport encryption and DHCHAP in-band auth must be correct
for a session to succeed. Tests 3 negative combinations.

Procedure (IBM doc + DHCHAP):
  Phase 1: openssl key + orch spec (enable_encryption: True)
  Phase 2-3: initiator TLS setup (ktls-utils, tlshd, nvme gen-tls-key, check-tls-key)
  Phase 4: subsystem --secure_listeners, listener --secure
  Phase 5: host add --psk KEY_DATA --dhchap-key HOST_KEY
  Phase 6: nvme connect --tls --dhchap-secret HOST_KEY

Reference: https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1)
  node6  — nvmeof-gw (GW2)
  node10 — client / initiator
"""

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.test_ceph_nvmeof_tlspsk_basic import _setup_tlspsk_subsystem
from tests.nvmeof.workflows.gateway_entities import teardown
from tests.nvmeof.workflows.inband_auth import create_dhchap_key
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import (
    assert_plain_connect_refused,
    connect_with_tls,
    disconnect_nqn,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _setup_tlspsk_dhchap_subsystem(
    ceph_cluster, config, nvme_service, initiator_node, rbd_obj
):
    """Full doc Phase 2-5 setup with DHCHAP on top. Returns (psk_info, dhchap_key)."""
    nqn = config["subsystems"][0]["nqn"]
    initiator_node_id = config["initiator_node"]

    # Subsystem DHCHAP key, then a separate host key. A subsystem that has a
    # key rejects host add unless the host has its own DH-HMAC-CHAP key.
    # The initiator proves the host key with --dhchap-secret.
    # The shared setup then creates the subsystem, listeners, and host PSK.
    subsys_cfg = config["subsystems"][0]
    dhchap_cfg = {"subnqn": nqn, "hosts": [{"node": initiator_node_id}]}
    if not subsys_cfg.get("dhchap-key"):
        create_dhchap_key(dhchap_cfg, ceph_cluster)
        subsys_cfg["dhchap-key"] = dhchap_cfg.get("dhchap-key") or dhchap_cfg.get(
            "subsys_key"
        )
    else:
        dhchap_cfg["dhchap-key"] = subsys_cfg["dhchap-key"]
        dhchap_cfg["subsys_key"] = subsys_cfg["dhchap-key"]
    create_dhchap_key(dhchap_cfg, ceph_cluster, update_host_key=True)
    host_key = dhchap_cfg["hosts"][0]["dhchap-key"]

    psk_info = _setup_tlspsk_subsystem(
        ceph_cluster,
        config,
        nvme_service,
        initiator_node,
        rbd_obj,
        dhchap_key=host_key,
    )
    return psk_info, host_key


def test_ceph_tlspsk_with_dhchap(ceph_cluster, config, nvme_service, rbd_obj):
    """TS-08: TLS-PSK + DHCHAP stacked authentication.

    Steps:
        1. Full doc Phase 2-5 setup + DHCHAP key generation.
        2. Positive: nvme connect --tls --dhchap-secret KEY → success; FIO.
        3. Negative-A: --tls only (no DHCHAP) → refused.
        4. Negative-B: DHCHAP only (no --tls) → refused.
        5. Negative-C: neither → refused.
        6. Re-verify correct path; FIO read; disconnect.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)
    gw_ip = nvme_service.gateways[0].node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    psk_info, dhchap_key = _setup_tlspsk_dhchap_subsystem(
        ceph_cluster, config, nvme_service, initiator_node, rbd_obj
    )

    # Positive: --tls + DHCHAP (doc-correct connect flags)
    LOG.info("TS-08: Positive — nvme connect --tls --dhchap-secret KEY")
    connect_with_tls(
        initiator_node,
        gw_ip,
        listener_port,
        nqn,
        dhchap_secret=dhchap_key,
    )

    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts08-tls-dhchap-fio"
    )
    disconnect_nqn(initiator_node, nqn)

    # Negative-A: --tls only, no DHCHAP
    LOG.info("TS-08: Negative-A — --tls only, no DHCHAP → must fail")
    connect_with_tls(
        initiator_node, gw_ip, listener_port, nqn, expect_failure=True
    )

    # Negative-B: DHCHAP only, no --tls
    LOG.info("TS-08: Negative-B — DHCHAP only, no --tls → must fail")
    connect_with_tls(
        initiator_node,
        gw_ip,
        listener_port,
        nqn,
        tls=False,
        dhchap_secret=dhchap_key,
        expect_failure=True,
    )

    # Negative-C: neither
    LOG.info("TS-08: Negative-C — no --tls, no DHCHAP → must fail")
    assert_plain_connect_refused(
        initiator_node, gw_ip, listener_port, nqn, context="TS-08 Negative-C"
    )

    # Re-verify correct path
    LOG.info("TS-08: Re-verify --tls + DHCHAP; FIO read; disconnect")
    connect_with_tls(
        initiator_node,
        gw_ip,
        listener_port,
        nqn,
        dhchap_secret=dhchap_key,
    )
    initiator.start_fio(
        io_size="256m",
        serial=True,
        io_type="read",
        execute_blkdiscard=False,
        test_name="ts08-verify",
    )
    disconnect_nqn(initiator_node, nqn)

    LOG.info("TS-08 PASSED — TLS-PSK + DHCHAP stacked: all paths verified")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    custom_data = kwargs.get("test_data", {})
    custom_cfg = custom_data.get("custom-config")
    config.update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )
    rbd_obj = None
    nvme_service = None
    try:
        rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_cfg)
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        nvme_service.wait_for_gateways_ready()
        test_ceph_tlspsk_with_dhchap(ceph_cluster, config, nvme_service, rbd_obj)
        return 0
    except Exception as err:
        LOG.exception("TLS-PSK + DHCHAP test FAILED: %s", err)
        return 1
    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
