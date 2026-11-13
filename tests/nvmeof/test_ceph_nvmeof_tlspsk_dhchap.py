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
from tests.nvmeof.workflows.gateway_entities import (
    configure_namespaces,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.inband_auth import create_dhchap_key
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import (
    add_host_with_psk,
    add_secure_listener,
    disconnect_nqn,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _setup_tlspsk_dhchap_subsystem(
    ceph_cluster, config, nvme_service, initiator_node, rbd_obj
):
    """Full doc Phase 2-5 setup with DHCHAP on top. Returns (psk_info, dhchap_key)."""
    gw = nvme_service.gateways[0]
    gw_ip = gw.node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    initiator_node_id = config["initiator_node"]

    # Phases 2+3: TLS-PSK initiator setup
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)

    # Subsystem DHCHAP key, then a separate host key. A subsystem that has a
    # key rejects host add unless the host has its own DH-HMAC-CHAP key.
    # The initiator proves the host key with --dhchap-secret.
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

    # Phase 4: subsystem (with DHCHAP), namespace, secure listener
    configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
    configure_namespaces(gw, config, rbd_obj=rbd_obj)
    add_secure_listener(gw, nqn, gw_ip, listener_port, gw.node.hostname)
    for extra_gw in nvme_service.gateways[1:]:
        add_secure_listener(
            extra_gw,
            nqn,
            extra_gw.node.ip_address,
            listener_port,
            extra_gw.node.hostname,
        )

    # Phase 5: host add --psk and the host DHCHAP key
    add_host_with_psk(
        gw, nqn, psk_info["hostnqn"], psk_info["key_data"], dhchap_key=host_key
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
    tls_dhchap_cmd = (
        f"nvme connect --transport tcp --traddr {gw_ip} --trsvcid {listener_port} "
        f"--nqn {nqn} --tls --dhchap-secret {dhchap_key}"
    )
    try:
        initiator_node.exec_command(cmd=tls_dhchap_cmd, sudo=True)
        LOG.info("TS-08: --tls + DHCHAP connect succeeded (correct)")
    except Exception as exc:
        raise AssertionError(f"TS-08: --tls + DHCHAP connect failed: {exc}") from exc

    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts08-tls-dhchap-fio"
    )
    disconnect_nqn(initiator_node, nqn)

    # Negative-A: --tls only, no DHCHAP
    LOG.info("TS-08: Negative-A — --tls only, no DHCHAP → must fail")
    try:
        initiator_node.exec_command(
            cmd=f"nvme connect --transport tcp --traddr {gw_ip} "
            f"--trsvcid {listener_port} --nqn {nqn} --tls",
            sudo=True,
        )
        raise AssertionError("TS-08 Negative-A FAILED: --tls without DHCHAP succeeded")
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info("TS-08: Negative-A correct: %s", exc)

    # Negative-B: DHCHAP only, no --tls
    LOG.info("TS-08: Negative-B — DHCHAP only, no --tls → must fail")
    try:
        initiator_node.exec_command(
            cmd=f"nvme connect --transport tcp --traddr {gw_ip} "
            f"--trsvcid {listener_port} --nqn {nqn} --dhchap-secret {dhchap_key}",
            sudo=True,
        )
        raise AssertionError("TS-08 Negative-B FAILED: DHCHAP without --tls succeeded")
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info("TS-08: Negative-B correct: %s", exc)

    # Negative-C: neither
    LOG.info("TS-08: Negative-C — no --tls, no DHCHAP → must fail")
    try:
        initiator_node.exec_command(
            cmd=f"nvme connect --transport tcp --traddr {gw_ip} "
            f"--trsvcid {listener_port} --nqn {nqn}",
            sudo=True,
        )
        raise AssertionError("TS-08 Negative-C FAILED: plain connect succeeded")
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info("TS-08: Negative-C correct: %s", exc)

    # Re-verify correct path
    LOG.info("TS-08: Re-verify --tls + DHCHAP; FIO read; disconnect")
    initiator_node.exec_command(cmd=tls_dhchap_cmd, sudo=True)
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
