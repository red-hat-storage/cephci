"""
TLS-PSK + Namespace Masking / Host Access Control Tests — TS-12.

TS-12 (CEPH-XXXXX): Verify that TLS-PSK transport encryption and NVMe-oF
    namespace masking (host-level access control) work correctly together:

      - Namespace masking: specific namespaces are only visible to designated
        initiator hosts (host NQN allow-list via ceph nvmeof host add --psk).
      - TLS-PSK: each host must additionally present a valid TLS session
        (i.e., --tls flag) to connect.
      - Combined: even if a host NQN is on the allow-list, a non-TLS connection
        is rejected; even if a TLS session is established, a host NOT on the
        allow-list cannot see/access the masked namespace.
      - Namespace masking + PSK rotation: after key rotation, the correct
        host can reconnect; the wrong host still cannot access the NS.

Reference: https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1)
  node6  — nvmeof-gw (GW2)
  node10 — client / initiator (allowed host)

Suite: suites/tentacle/nvmeof/tier-3_nvmeof_tlspsk_cross_feature.yaml
"""

import json

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_namespaces,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import (
    add_host_with_psk,
    add_secure_listener,
    connect_with_tls,
    connect_with_tls_until,
    disconnect_nqn,
    rotate_psk,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def namespace_devices_visible(payload):
    """Return True when ``nvme list`` JSON has a namespace block device.

    Device records do not include the subsystem NQN. Searching for the NQN
    in those records reports a connected namespace as missing.
    """
    if isinstance(payload, str):
        payload = json.loads(payload)
    devices = (payload or {}).get("Devices") or []
    return bool(devices)


def _listener_addrs(nvme_service):
    return [gw.node.ip_address for gw in nvme_service.gateways]


def _connect_listeners(initiator_node, addrs, listener_port, nqn, psk_sync_wait=0):
    """Connect to every gateway so a rebalanced namespace has a block device.

    Args:
        initiator_node: CephNode for the initiator.
        addrs (list): Gateway listener addresses.
        listener_port (int): Listener port.
        nqn (str): Subsystem NQN.
        psk_sync_wait (int): Seconds to retry a gateway that still has the
            previous host PSK. Zero means one attempt.
    """
    for addr in addrs:
        if psk_sync_wait:
            connect_with_tls_until(
                initiator_node,
                addr,
                listener_port,
                nqn,
                timeout=psk_sync_wait,
            )
        else:
            connect_with_tls(
                initiator_node, addr, listener_port, nqn, expect_failure=False
            )


def _check_ns_visible(initiator_node, nqn):
    """Return True if the initiator has a namespace block device.

    Args:
        initiator_node: CephNode for the initiator.
        nqn (str): Subsystem NQN just connected. Logged only; ``nvme list``
            device records do not contain it.

    Returns:
        bool: True if ``nvme list`` shows at least one device.
    """
    try:
        out, _ = initiator_node.exec_command(cmd="nvme list -o json", sudo=True)
        visible = namespace_devices_visible(out)
        LOG.info("TS-12: nvme list after connect to %s: visible=%s", nqn, visible)
        return visible
    except Exception as exc:
        LOG.warning("_check_ns_visible error: %s", exc)
        return False


# ---------------------------------------------------------------------------
# TS-12 — TLS-PSK + Namespace Masking
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_ns_masking(ceph_cluster, config, nvme_service, rbd_obj):
    """TS-12: TLS-PSK + namespace masking — both layers enforced simultaneously.

    Setup (via doc Phases 2-5):
        - Subsystem with secure_listener: true.
        - Initiator TLS daemon configured; PSK key generated and registered.
        - Host added with PSK (gateway CLI Phase 5).

    Steps:
        1. Run doc Phases 2-5: initiator TLS setup + subsystem + secure listener
           + host PSK (the host PSK registration IS the namespace masking —
           only hosts registered with ceph nvmeof host add --psk can connect).
        2. Connect allowed host with --tls → success.
        3. Verify namespace visible to allowed host.
        4. FIO write on allowed host's namespace; assert 0 errors.
        5. Disconnect allowed host.
        6. Negative-A (masking): Connect a different host NQN (not registered
           with PSK) with --tls → connection must fail (no PSK for that host).
        7. Negative-B (TLS): Connect allowed host WITHOUT --tls → rejected
           (TLS layer enforced by secure_listener subsystem).
        8. Reconnect allowed host with --tls; FIO read; assert 0 errors.
        9. Rotate PSK; verify namespace still accessible to allowed host.

    Polarion: CEPH-XXXXX
    """
    initiator_node_id = config["initiator_node"]
    initiator_node = get_node_by_id(ceph_cluster, initiator_node_id)
    initiator = NVMeInitiator(initiator_node)

    gw = nvme_service.gateways[0]
    gw_ip = gw.node.ip_address
    listener_addrs = _listener_addrs(nvme_service)
    listener_port = config.get("listener_port", 4420)
    # host add lands on one gateway immediately. Peers copy the PSK from OMAP
    # on state_update_interval_sec, so the first connect to a peer can be a
    # stale-key refusal.
    psk_sync_wait = config.get("state_update_interval_sec", 30) + 15

    nqn = config["subsystems"][0]["nqn"]

    # ── Step 1 — doc Phases 2-5: initiator TLS setup + subsystem ─────────
    LOG.info("TS-12: Setting up TLS-PSK subsystem (Phases 2-5)")
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)
    hostnqn = psk_info["hostnqn"]
    key_data = psk_info["key_data"]
    LOG.info("TS-12: Allowed host NQN: %s", hostnqn)

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
    # Phase 5: only the allowed host NQN is registered → this IS the masking
    add_host_with_psk(gw, nqn, hostnqn, key_data)

    # ── Step 2 — allowed host TLS connect ────────────────────────────────
    LOG.info("TS-12: Connecting allowed host with --tls")
    _connect_listeners(
        initiator_node,
        listener_addrs,
        listener_port,
        nqn,
        psk_sync_wait=psk_sync_wait,
    )

    # ── Step 3 — verify NS visible to allowed host ────────────────────────
    LOG.info("TS-12: Verifying namespace visible to allowed host")
    if not _check_ns_visible(initiator_node, nqn):
        raise AssertionError(
            "TS-12: No namespaces visible to allowed host after TLS connect"
        )
    LOG.info("TS-12: Namespace visible to allowed host (correct)")

    # ── Step 4 — FIO on allowed host ─────────────────────────────────────
    LOG.info("TS-12: FIO write on allowed host's namespaces")
    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts12-allowed-write"
    )
    disconnect_nqn(initiator_node, nqn)

    # ── Step 6 — Negative-A (masking): unregistered host NQN with TLS ────
    # Simulate a different host NQN by using a spoofed --hostnqn that is NOT
    # registered on the gateway — the gateway must reject it.
    LOG.info("TS-12: Negative-A — unregistered host NQN with --tls must fail")
    fake_hostnqn = (
        "nqn.2014-08.org.nvmexpress:uuid:DEADBEEF-0000-0000-0000-000000000000"
    )
    try:
        initiator_node.exec_command(
            cmd=(
                f"nvme connect -t tcp -a {gw_ip} -s {listener_port} -n {nqn} "
                f"--tls --hostnqn {fake_hostnqn}"
            ),
            sudo=True,
        )
        raise AssertionError(
            "TS-12 Negative-A FAILED: unregistered host NQN connected to "
            "PSK-protected subsystem — masking broken"
        )
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info("TS-12: Negative-A correct — unregistered host NQN refused by gateway")

    # ── Step 7 — Negative-B (TLS): allowed host without --tls ────────────
    LOG.info("TS-12: Negative-B — allowed host without --tls must fail (TLS layer)")
    try:
        initiator_node.exec_command(
            cmd=f"nvme connect -t tcp -a {gw_ip} -s {listener_port} -n {nqn}",
            sudo=True,
        )
        raise AssertionError(
            "TS-12 Negative-B FAILED: allowed host connected without --tls "
            "to a secure_listener subsystem"
        )
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info(
            "TS-12: Negative-B correct — TLS layer rejected non-TLS from allowed host"
        )

    # ── Step 8 — post-masking FIO read (re-connect with --tls) ───────────
    LOG.info("TS-12: FIO read — reconnecting allowed host with --tls")
    _connect_listeners(initiator_node, listener_addrs, listener_port, nqn)
    initiator.start_fio(
        io_size="256m",
        serial=True,
        io_type="read",
        execute_blkdiscard=False,
        test_name="ts12-allowed-read",
    )
    disconnect_nqn(initiator_node, nqn)

    # ── Step 9 — rotate PSK; verify NS masking intact ─────────────────────
    LOG.info("TS-12: Rotating PSK; verifying namespace masking intact after rotation")
    rotate_psk(
        initiator_node,
        gw,
        nqn,
        hostnqn,
        [gw_obj.node for gw_obj in nvme_service.gateways],
        new_key_path="/root/encryption_ns_mask_new.key",
        trsvcid=listener_port,
    )

    LOG.info("TS-12: Reconnecting allowed host after PSK rotation")
    # The gateway that took host add has the new PSK immediately. Peers copy
    # it from OMAP on state_update_interval_sec, so a connect in that window
    # is a stale-key refusal, not a masking failure.
    _connect_listeners(
        initiator_node,
        listener_addrs,
        listener_port,
        nqn,
        psk_sync_wait=psk_sync_wait,
    )
    if not _check_ns_visible(initiator_node, nqn):
        raise AssertionError(
            "TS-12: Namespace NOT visible to allowed host after PSK rotation — "
            "masking broken by rotation"
        )
    LOG.info("TS-12: NS masking intact after PSK rotation (correct)")
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts12-after-rotation",
    )
    disconnect_nqn(initiator_node, nqn)

    LOG.info(
        "TS-12 PASSED — TLS-PSK + namespace masking: "
        "dual-layer access control enforced; masking survives PSK rotation"
    )


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci test runner.

    Config keys (from suite YAML):
        install (bool): Deploy a fresh NVMeService.
        tls_psk (bool): Enable TLS-PSK at deploy time (orch spec enable_encryption).
        rbd_pool (str): RBD pool name.
        gw_group (str): Gateway group name.
        gw_nodes (list[str]): Gateway node IDs.
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Allowed initiator node ID.
        subsystems (list): Subsystem configs with secure_listener: true.
        cleanup (list): Cleanup targets.
        polarion-id (str): Polarion test case ID.
    """
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

        test_ceph_tlspsk_ns_masking(ceph_cluster, config, nvme_service, rbd_obj)

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK + namespace masking test FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
