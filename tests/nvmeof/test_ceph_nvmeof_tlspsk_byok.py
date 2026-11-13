"""
TLS-PSK + BYOK/KMIP Encrypted Namespace Tests — TS-09.

TS-09 (CEPH-XXXXX): Verify that TLS-PSK transport encryption (enable_encryption)
    and BYOK (Bring-Your-Own-Key) LUKS namespace encryption (via KMIP) can be
    layered simultaneously:

      - TLS-PSK: encrypts the NVMe/TCP transport channel.
      - BYOK/LUKS: encrypts the data at rest at the RBD/block layer.
      - Combined: double-encrypted data path (transport + at-rest).
      - Verify KMIP endpoint remains reachable through the TLS-encrypted transport.
      - FIO write/read on encrypted NS over TLS-PSK transport passes with 0 errors.
      - After GW restart, KMIP passphrase is re-fetched AND TLS-PSK remains active.

Reference BYOK workflow: tests/nvmeof/workflows/kmip_utils.py
Reference: https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1)
  node6  — nvmeof-gw (GW2)
  node10 — client / initiator / KMIP server

Suite: suites/tentacle/nvmeof/tier-3_nvmeof_tlspsk_cross_feature.yaml
"""

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_namespaces,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.kmip_utils import (
    acquire_kmip_for_nvmeof,
    configure_kmip_endpoint_on_subsystem,
    register_kmip_passphrases,
    release_kmip_if_owned,
)
from tests.nvmeof.workflows.nvme_encryption import (
    add_encrypted_namespace,
    verify_all_encrypted_namespaces_listed,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import (
    add_host_with_psk,
    add_secure_listener,
    connect_with_tls_until,
    disconnect_nqn,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


# ---------------------------------------------------------------------------
# Helper: full TLS-PSK subsystem setup (Phases 2-5 from IBM doc)
# ---------------------------------------------------------------------------


def _setup_tlspsk_subsystem(
    ceph_cluster,
    config,
    nvme_service,
    initiator_node,
    rbd_obj,
    create_namespaces=True,
):
    """Run doc Phases 2-5: initiator setup + subsystem + secure listener + host PSK.

    Args:
        create_namespaces (bool): When False, the caller has already added the
            namespace (the BYOK LUKS image) and this only creates the subsystem,
            listeners, and host PSK.

    Returns:
        dict: {"hostnqn": str, "key_data": str}
    """
    gw = nvme_service.gateways[0]
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    # Phase 2+3 — initiator TLS daemon + key generation
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)

    # Phase 4 — subsystem + namespaces + secure listeners
    configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
    if create_namespaces:
        configure_namespaces(gw, config, rbd_obj=rbd_obj)
    add_secure_listener(gw, nqn, gw.node.ip_address, listener_port, gw.node.hostname)
    for extra_gw in nvme_service.gateways[1:]:
        add_secure_listener(
            extra_gw,
            nqn,
            extra_gw.node.ip_address,
            listener_port,
            extra_gw.node.hostname,
        )

    # Phase 5 — host with PSK
    add_host_with_psk(gw, nqn, psk_info["hostnqn"], psk_info["key_data"])
    return psk_info


# ---------------------------------------------------------------------------
# TS-09 — TLS-PSK + BYOK/KMIP encrypted namespace
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_byok_kmip(
    ceph_cluster, config, nvme_service, rbd_obj, custom_data=None
):
    """TS-09: TLS-PSK transport + BYOK LUKS encrypted namespace via KMIP.

    Steps:
        1. Acquire KMIP endpoint; register passphrases.
        2. Configure KMIP endpoint on subsystem.
        3. Add LUKS2/AES-256 encrypted namespace (via KMIP passphrase).
        4. Run doc Phases 2-5: initiator TLS setup + secure subsystem + host PSK
           (double-layer: TLS transport + LUKS at-rest encryption).
        5. Connect initiator with --tls.
        6. Verify encrypted NS is listed with correct encryption attributes.
        7. FIO write + read on encrypted NS over TLS-PSK transport (0 errors).
        8. Disconnect; restart GW1 daemon; verify KMIP passphrase re-fetched.
        9. Reconnect with --tls; verify encrypted NS still accessible.

    Polarion: CEPH-XXXXX
    """
    import json

    from ceph.ceph_admin.daemon import Daemon
    from ceph.ceph_admin.orch import Orch

    initiator_node_id = config["initiator_node"]
    initiator_node = get_node_by_id(ceph_cluster, initiator_node_id)
    initiator = NVMeInitiator(initiator_node)

    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    config.setdefault(
        "luks_combos",
        [
            {
                "key_id": "key-id-tlspsk-luks2-aes256",
                "format": "luks2",
                "algo": "aes256",
            }
        ],
    )
    combo = config["luks_combos"][0]

    kmip_info = None
    kmip_owned = False
    try:
        # ── Step 1 — acquire KMIP endpoint ───────────────────────────────────
        LOG.info("TS-09: Acquiring KMIP endpoint")
        kmip_info, kmip_owned = acquire_kmip_for_nvmeof(
            ceph_cluster, config, custom_data, nvme_service=nvme_service
        )
        # A server this test started already has one passphrase per luks_combo.
        # Register only when reusing a KMIP server left by an earlier test.
        if not kmip_owned:
            register_kmip_passphrases(initiator_node, config["luks_combos"], config)
        LOG.info("TS-09: Redeploying gateways so they load the KMIP certificates")
        nvme_service.redeploy(wait_sec=30)
        nvme_service.init_gateways()
        gw = nvme_service.gateways[0]

        # ── Step 2 — subsystem, then the KMIP endpoint ───────────────────────
        LOG.info("TS-09: Configuring KMIP endpoint on subsystem %s", nqn)
        configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
        configure_kmip_endpoint_on_subsystem(gw, nqn, kmip_info["kmip_cfg"])

        # ── Step 3 — add LUKS2/AES-256 encrypted namespace ───────────────────
        LOG.info("TS-09: Adding LUKS2/AES-256 encrypted namespace")
        bdev_size = config["subsystems"][0]["bdevs"][0].get("size", "4G")
        image = add_encrypted_namespace(
            gw,
            nqn,
            config["rbd_pool"],
            key_id=combo.get("key_uid", combo["key_id"]),
            luks_format=combo["format"],
            luks_algo=combo["algo"],
            size=bdev_size,
        )
        encrypted = [{"rbd_image": image}]

        # ── Step 4 — TLS-PSK listeners and host PSK, without a plain image ──
        LOG.info("TS-09: Setting up TLS-PSK (double encryption: transport + LUKS)")
        _setup_tlspsk_subsystem(
            ceph_cluster,
            config,
            nvme_service,
            initiator_node,
            rbd_obj,
            create_namespaces=False,
        )

        # ── Step 5 — connect with --tls ───────────────────────────────────────
        LOG.info("TS-09: Connecting initiator with --tls")
        psk_sync_wait = config.get("state_update_interval_sec", 30) + 15
        for addr in [gateway.node.ip_address for gateway in nvme_service.gateways]:
            connect_with_tls_until(
                initiator_node, addr, listener_port, nqn, timeout=psk_sync_wait
            )

        # ── Step 6 — verify encrypted NS listed ──────────────────────────────
        LOG.info("TS-09: Verifying encrypted NS listed with correct attrs")
        verify_all_encrypted_namespaces_listed(gw, nqn, encrypted)

        # ── Step 7 — FIO write + read on encrypted NS via TLS-PSK ────────────
        LOG.info("TS-09: FIO write on encrypted NS over TLS-PSK transport")
        initiator.start_fio(
            io_size="256m",
            serial=True,
            io_type="write",
            test_name="ts09-byok-tls-write",
        )
        LOG.info("TS-09: FIO read on encrypted NS over TLS-PSK transport")
        initiator.start_fio(
            io_size="256m",
            serial=True,
            io_type="read",
            execute_blkdiscard=False,
            test_name="ts09-byok-tls-read",
        )
        disconnect_nqn(initiator_node, nqn)

        # ── Step 8 — daemon restart; verify KMIP passphrase re-fetched ────────
        LOG.info("TS-09: Restarting GW1 daemon to verify KMIP passphrase re-fetch")
        svc = nvme_service.service_name
        orch = Orch(ceph_cluster, **{})
        daemon_obj = Daemon(ceph_cluster, **{})
        out, _ = orch.shell(args=[f"ceph orch ps --service-name {svc} --format json"])
        daemons = json.loads(out)
        if not daemons:
            raise RuntimeError(f"No daemons found for service {svc}")
        daemon_name = daemons[0]["daemon_name"]
        LOG.info("TS-09: Restarting daemon %s", daemon_name)
        daemon_obj.op("restart", {"pos_args": [daemon_name]})
        nvme_service.wait_for_gateways_ready()
        verify_all_encrypted_namespaces_listed(gw, nqn, encrypted)

        # ── Step 9 — reconnect; verify NS still accessible ─────────────────
        LOG.info(
            "TS-09: Reconnecting after GW restart; verifying encrypted NS accessible"
        )
        for addr in [gateway.node.ip_address for gateway in nvme_service.gateways]:
            connect_with_tls_until(
                initiator_node, addr, listener_port, nqn, timeout=psk_sync_wait
            )
        initiator.start_fio(
            io_size="128m",
            serial=True,
            io_type="read",
            execute_blkdiscard=False,
            test_name="ts09-after-restart",
        )
        disconnect_nqn(initiator_node, nqn)

        LOG.info(
            "TS-09 PASSED — TLS-PSK + BYOK/KMIP double encryption: "
            "FIO OK, KMIP passphrase re-fetched after restart"
        )

    finally:
        release_kmip_if_owned(kmip_info, kmip_owned)


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
        initiator_node (str): Node ID for the initiator.
        subsystems (list): Subsystem configs with secure_listener: true.
        kmip (dict): KMIP server connection details.
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

        test_ceph_tlspsk_byok_kmip(
            ceph_cluster, config, nvme_service, rbd_obj, custom_data
        )

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK + BYOK/KMIP test FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
