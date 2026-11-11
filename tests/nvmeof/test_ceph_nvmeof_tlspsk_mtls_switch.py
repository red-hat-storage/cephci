"""
TLS-PSK Mode Switch Tests — TS-06.

TS-06 (CEPH-XXXXX): Gateway transitions between TLS-PSK (enable_encryption)
    and plaintext, and runs TLS-PSK together with mTLS.
    Verifies that:
      - TS-06a: TLS-PSK → plaintext → TLS-PSK round-trip. Plain connect
        succeeds in plaintext mode; is refused after TLS-PSK re-enabled.
      - TS-06b: mTLS (enable_auth) and TLS-PSK (enable_encryption) together.
        mTLS authenticates nvme CLI commands to the gateway. TLS-PSK encrypts
        initiator IO on the way to the gateway. Neither replaces the other.

    Procedure followed (from IBM docs):
        Phase 1  — openssl key + orch spec (enable_encryption: True) deployed
                   by NVMeService.deploy()
        Phase 2  — tlshd installed and configured on initiator
        Phase 3  — nvme gen-tls-key + nvme check-tls-key --insert on initiator
        Phase 4  — subsystem add --secure_listeners; listener add --secure
        Phase 5  — host add --psk KEY_DATA
        Phase 6  — nvme connect --transport tcp ... --tls

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1)
  node6  — nvmeof-gw (GW2)
  node10 — client / initiator

TS-06a is called from the tier-2 lifecycle. TS-06b is
suites/tentacle/nvmeof/tier-3_nvmeof_tlspsk_cross_feature.yaml.
"""

import json

from ceph.ceph import Ceph, CommandFailed
from ceph.ceph_admin.orch import Orch
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
    assert_plain_connect_refused,
    connect_with_tls,
    disconnect_nqn,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _get_spec_field(orch, service_name, field):
    """Return the value of *field* inside spec.spec for *service_name*.

    Args:
        orch (Orch): ceph orch helper.
        service_name (str): NVMeoF service name (e.g. ``nvmeof.nvmeof.group``).
        field (str): Field name inside the orch spec's ``spec`` block.

    Returns:
        The field value, or None if absent.
    """
    out, _ = orch.shell(
        args=[f"ceph orch ls --service_name {service_name} --format json"]
    )
    services = json.loads(out)
    if not services:
        raise RuntimeError(f"No orch service found for {service_name}")
    return services[0].get("spec", {}).get(field)


def _assert_spec_field(orch, service_name, field, expected):
    """Assert that an orch service spec field has *expected* value.

    Args:
        orch (Orch): ceph orch helper.
        service_name (str): NVMeoF service name.
        field (str): Field name inside spec.spec.
        expected: Expected field value.

    Raises:
        AssertionError: If the actual value differs from expected.
    """
    actual = _get_spec_field(orch, service_name, field)
    if actual != expected:
        raise AssertionError(
            f"Spec field '{field}' = {actual!r}, expected {expected!r} "
            f"on service {service_name}"
        )
    LOG.info("Spec field '%s' = %r (correct) on %s", field, actual, service_name)


def _setup_tls_psk_subsystem(nvme_service, initiator_node, config, rbd_obj, cluster):
    """Run all doc phases to create a TLS-PSK secured subsystem.

    Phases executed:
        Phase 4 — configure_subsystems (with secure_listener: true in config)
        Phase 2+3 — setup_initiator_tls_psk on initiator
        Phase 4 (listener) — add_secure_listener on each GW
        Phase 5 — add_host_with_psk

    Args:
        nvme_service (NVMeService): Deployed NVMeService instance.
        initiator_node: CephNode for the initiator.
        config (dict): Test config dict (subsystems, listener_port, etc.).
        rbd_obj: RBD utility object for namespace image pre-creation.
        cluster: Ceph cluster object.

    Returns:
        dict: ``{"hostnqn": str, "key_data": str}`` from setup_initiator_tls_psk.
    """
    nqn = config["subsystems"][0]["nqn"]
    trsvcid = config.get("listener_port", 4420)
    gateway = nvme_service.gateways[0]

    # Phase 4 — subsystem add (--secure_listeners via secure_listener: true)
    configure_subsystems(nvme_service, ceph_cluster=cluster)
    configure_namespaces(gateway, config, rbd_obj=rbd_obj)

    # Phase 2 + 3 — tlshd install + nvme gen-tls-key + nvme check-tls-key --insert
    initiator_info = setup_initiator_tls_psk(initiator_node, nqn)

    # Phase 4 (listener) — listener add --secure on every GW
    for gw in nvme_service.gateways:
        add_secure_listener(
            gateway=gw,
            nqn=nqn,
            traddr=gw.node.ip_address,
            trsvcid=trsvcid,
            host_name=gw.node.hostname,
        )

    # Phase 5 — host add --psk KEY_DATA
    add_host_with_psk(
        gateway, nqn, initiator_info["hostnqn"], initiator_info["key_data"]
    )

    return initiator_info


# ---------------------------------------------------------------------------
# TS-06a — TLS-PSK → plaintext → TLS-PSK round-trip
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_to_plain_to_tlspsk(ceph_cluster, config, nvme_service, rbd_obj):
    """TS-06a: TLS-PSK → plaintext mode → TLS-PSK re-enable.

    Steps:
        1. Deploy TLS-PSK subsystem (all 6 phases); connect --tls; verify.
        2. Delete the secure subsystem while the TLS gateway is still up.
           Redeploying a plaintext daemon against a --secure_listeners subsystem
           leaves the gateway gRPC port down.
        3. Remove TLS-PSK: set tls_psk=False, apply spec, redeploy.
        4. Recreate subsystem without --secure_listeners; add plain host.
           Plain connect (no --tls) must succeed.
        5. Delete the plain subsystem, then re-enable TLS-PSK and redeploy.
        6. Verify plain connect refused after re-enabling TLS-PSK.
        7. Connect --tls; disconnect.

    Polarion: CEPH-XXXXX
    """
    initiator_node_id = config["initiator_node"]
    initiator_node = get_node_by_id(ceph_cluster, initiator_node_id)

    gw_ip = nvme_service.gateways[0].node.ip_address
    gateway = nvme_service.gateways[0]
    nqn = config["subsystems"][0]["nqn"]
    trsvcid = config.get("listener_port", 4420)

    orch = Orch(ceph_cluster, **{})
    service_name = nvme_service.service_name

    # ── Step 1 — TLS-PSK baseline ─────────────────────────────────────────
    LOG.info("TS-06a Step 1: setting up TLS-PSK subsystem")
    _setup_tls_psk_subsystem(
        nvme_service, initiator_node, config, rbd_obj, ceph_cluster
    )
    _assert_spec_field(orch, service_name, "enable_encryption", True)

    connect_with_tls(initiator_node, gw_ip, trsvcid, nqn, expect_failure=False)
    LOG.info("TS-06a Step 1: TLS connect succeeded")
    disconnect_nqn(initiator_node, nqn)

    # Delete while the TLS gateway is still serving. A redeploy that drops
    # enable_encryption does not come back if OMAP still has this subsystem.
    LOG.info("TS-06a Step 2: deleting secure subsystem before disabling TLS-PSK")
    gateway.subsystem.delete(**{"args": {"subsystem": nqn, "force": True}})

    LOG.info("TS-06a Step 2: disabling TLS-PSK on spec (tls_psk=False)")
    nvme_service.config.pop("tls_psk", None)
    nvme_service.tls_psk = False
    nvme_service.apply_nvmeof_spec(redeploy=True, wait_sec=60)

    # ── Step 3 — plain subsystem (no secure_listeners) ────────────────────
    LOG.info("TS-06a Step 3: recreating subsystem without secure_listeners")
    plain_sub_config = dict(config["subsystems"][0])
    plain_sub_config.pop("secure_listener", None)
    plain_sub_config["secure_listener"] = False

    plain_config = dict(config)
    plain_config["subsystems"] = [plain_sub_config]

    configure_subsystems(
        nvme_service, ceph_cluster=ceph_cluster, subsystem_config=[plain_sub_config]
    )
    configure_namespaces(gateway, plain_config, rbd_obj=rbd_obj)

    # On 20.2.1+ a subsystem created with --network_mask and without
    # --secure_listeners already has a listener on each gateway address.
    # Adding it again returns "Subsystem already listens on address".
    for gw in nvme_service.gateways:
        try:
            gw.listener.add(
                **{
                    "args": {
                        "subsystem": nqn,
                        "traddr": gw.node.ip_address,
                        "trsvcid": trsvcid,
                        "host-name": gw.node.hostname,
                    }
                }
            )
        except CommandFailed as exc:
            if "already listens" not in str(exc):
                raise
            LOG.info(
                "TS-06a Step 3: listener %s:%s already present from network mask",
                gw.node.ip_address,
                trsvcid,
            )
    gateway.host.add(**{"args": {"subsystem": nqn, "host": "*"}})

    # ── Step 4 — plain connect must succeed ───────────────────────────────
    LOG.info("TS-06a Step 4: plain connect (no --tls) must succeed in plaintext mode")
    plain_cmd = (
        f"nvme connect --transport tcp --traddr {gw_ip} "
        f"--trsvcid {trsvcid} --nqn {nqn}"
    )
    initiator_node.exec_command(cmd=plain_cmd, sudo=True)
    LOG.info("TS-06a Step 4: plain connect succeeded (correct)")
    disconnect_nqn(initiator_node, nqn)

    # Same order as step 2: drop the subsystem before the spec redeploy.
    LOG.info("TS-06a Step 5: deleting plain subsystem before re-enabling TLS-PSK")
    gateway.subsystem.delete(**{"args": {"subsystem": nqn, "force": True}})

    # ── Step 5 — re-enable TLS-PSK ────────────────────────────────────────
    LOG.info("TS-06a Step 5: re-enabling TLS-PSK on spec")
    nvme_service.config["tls_psk"] = True
    nvme_service.tls_psk = True
    nvme_service.apply_nvmeof_spec(redeploy=True, wait_sec=60, generate_key=True)

    # ── Step 6 — plain connect refused ───────────────────────────────────
    LOG.info("TS-06a Step 6: re-creating TLS-PSK subsystem; plain connect must fail")
    _setup_tls_psk_subsystem(
        nvme_service, initiator_node, config, rbd_obj, ceph_cluster
    )

    assert_plain_connect_refused(
        initiator_node, gw_ip, trsvcid, nqn, context="TS-06a Step 6"
    )

    # ── Step 7 — TLS connect again ───────────────────────────────────────
    LOG.info("TS-06a Step 7: connect --tls after re-enable")
    connect_with_tls(initiator_node, gw_ip, trsvcid, nqn, expect_failure=False)
    disconnect_nqn(initiator_node, nqn)

    LOG.info("TS-06a PASSED — TLS-PSK → plain → TLS-PSK round-trip OK")


# ---------------------------------------------------------------------------
# TS-06b — mTLS and TLS-PSK together
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_with_mtls(ceph_cluster, config, nvme_service, rbd_obj):
    """TS-06b: mTLS on the CLI path and TLS-PSK on the data path together.

    The service is deployed with both flags:
      mtls: true     → spec enable_auth: true
      tls_psk: true  → spec enable_encryption: true

    enable_auth authenticates nvme CLI commands to the gateway.
    enable_encryption encrypts initiator IO in transit. Creating the
    subsystem through the CLI while encryption is on, then connecting with
    --tls, checks that one setting does not clear the other.

    Steps:
        1. Spec has enable_auth: true and enable_encryption: true.
        2. Create the secure subsystem and register the host PSK. The CLI
           commands are the mTLS path.
        3. Connect with --tls and run FIO.
        4. Connect without --tls. The data path still requires TLS-PSK.
        5. Spec still has both enable_auth and enable_encryption.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)
    gw_ip = nvme_service.gateways[0].node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    trsvcid = config.get("listener_port", 4420)

    orch = Orch(ceph_cluster, **{})
    service_name = nvme_service.service_name

    LOG.info("TS-06b: spec must have mTLS and TLS-PSK at the same time")
    _assert_spec_field(orch, service_name, "enable_auth", True)
    _assert_spec_field(orch, service_name, "enable_encryption", True)

    LOG.info("TS-06b: creating the secure subsystem through the mTLS CLI path")
    _setup_tls_psk_subsystem(
        nvme_service, initiator_node, config, rbd_obj, ceph_cluster
    )

    LOG.info("TS-06b: initiator connect --tls and FIO on the data path")
    connect_with_tls(initiator_node, gw_ip, trsvcid, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts06b-mtls-tlspsk"
    )
    disconnect_nqn(initiator_node, nqn)

    LOG.info("TS-06b: plain connect must still be refused")
    assert_plain_connect_refused(
        initiator_node, gw_ip, trsvcid, nqn, context="TS-06b"
    )

    LOG.info("TS-06b: both spec fields still set after data-path IO")
    _assert_spec_field(orch, service_name, "enable_auth", True)
    _assert_spec_field(orch, service_name, "enable_encryption", True)
    LOG.info("TS-06b PASSED — mTLS and TLS-PSK coexist")


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci test runner.

    Config keys (from suite YAML):
        install (bool): Deploy a fresh NVMeService when True.
        tls_psk (bool): Enable TLS-PSK (enable_encryption). True for both cases.
        mtls (bool): Enable mTLS (enable_auth) as well. True for TS-06b.
        run_ts06b (bool): When True run the combined mTLS + TLS-PSK case.
            Default runs the TLS-PSK ↔ plaintext round-trip.
        rbd_pool (str): RBD pool name.
        gw_group (str): Gateway group name.
        gw_nodes (list[str]): Gateway node IDs.
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Node ID for the initiator.
        subsystems (list): Subsystem config dicts (must have secure_listener: true
            for TLS-PSK subsystem creation).
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

        polarion_id = config.get("polarion-id", "")
        run_ts06b = config.get("run_ts06b", False) or "TS06b" in polarion_id

        if run_ts06b:
            test_ceph_tlspsk_with_mtls(
                ceph_cluster, config, nvme_service, rbd_obj
            )
        else:
            test_ceph_tlspsk_to_plain_to_tlspsk(
                ceph_cluster, config, nvme_service, rbd_obj
            )

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK mTLS switch test FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
