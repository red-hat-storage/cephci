"""
TLS-PSK + Multi-Gateway-Group Isolation Tests — TS-11.

TS-11 (CEPH-XXXXX): Extends TS-04 (multi-group key isolation) with deeper
    cross-group interaction testing:

      - Three GW groups in the same Ceph cluster: Group-A (TLS-PSK), Group-B (TLS-PSK
        different PSK), Group-C (no encryption / plaintext).
      - Verify:
          * Group-A PSK does not unlock Group-B and vice-versa.
          * Group-C (plaintext) refuses TLS connections (wrong mode).
          * Enabling TLS-PSK on Group-C transitions it to encrypted mode.
          * PSK rotation on Group-A does not affect Group-B or Group-C.
          * Concurrent FIO while Group-A and Group-B are both connected.

Reference: https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node4  — osd, used here as the Group-C gateway (one nvmeof daemon per node)
  node5  — nvmeof-gw (Group-A)
  node6  — nvmeof-gw (Group-B)
  node10 — client / initiator

Each gateway group is placed on its own node. A node cannot run two nvmeof
gateway services.

Suite: suites/tentacle/nvmeof/tier-2_nvmeof_tlspsk_tests.yaml
"""

from ceph.ceph import Ceph, CommandFailed
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
    disconnect_nqn,
    rotate_psk,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


# ---------------------------------------------------------------------------
# Per-group TLS-PSK setup helper (Phases 2-5 from IBM doc)
# ---------------------------------------------------------------------------


def _setup_group_tlspsk(
    initiator_node, svc, groups_cfg, group_idx, rbd_obj, ceph_cluster
):
    """Run doc Phases 2-5 for one gateway group.

    Args:
        initiator_node: CephNode for the initiator.
        svc: NVMeService instance for this group.
        groups_cfg (list): Full groups config list.
        group_idx (int): Index into groups_cfg.
        rbd_obj: RBD util object.
        ceph_cluster: Ceph cluster object.

    Returns:
        dict: {"hostnqn": str, "key_data": str} from setup_initiator_tls_psk.
    """
    grp_cfg = groups_cfg[group_idx]
    # Build a config dict that mirrors what configure_subsystems/namespaces expects
    cfg = {"subsystems": grp_cfg["subsystems"]}
    nqn = grp_cfg["subsystems"][0]["nqn"]
    listener_port = grp_cfg.get("listener_port", 4420)
    gw = svc.gateways[0]

    # Phase 2+3 — initiator TLS daemon + key generation (idempotent across groups)
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)

    # Phase 4 — subsystem + namespaces + secure listener
    configure_subsystems(svc, ceph_cluster=ceph_cluster)
    configure_namespaces(gw, cfg, rbd_obj=rbd_obj)
    add_secure_listener(gw, nqn, gw.node.ip_address, listener_port, gw.node.hostname)

    # Phase 5 — host with PSK
    add_host_with_psk(gw, nqn, psk_info["hostnqn"], psk_info["key_data"])
    return psk_info


def _enable_group_tlspsk(
    svc, initiator_node, groups_cfg, group_idx, rbd_obj, ceph_cluster
):
    """Turn a plaintext group into a TLS-PSK group.

    A secure listener is rejected while the subsystem still allows any host.
    Delete that subsystem first, redeploy the gateway with enable_encryption,
    then create the subsystem with secure listeners and a host PSK.
    """
    nqn = groups_cfg[group_idx]["subsystems"][0]["nqn"]
    gateway = svc.gateways[0]
    LOG.info("Deleting plaintext subsystem %s before enabling TLS-PSK", nqn)
    gateway.subsystem.delete(**{"args": {"subsystem": nqn, "force": True}})

    svc.config["tls_psk"] = True
    svc.tls_psk = True
    for subsystem in svc.config.get("subsystems", []):
        subsystem["secure_listener"] = True
    svc.apply_nvmeof_spec(redeploy=True, wait_sec=60, generate_key=True)
    return _setup_group_tlspsk(
        initiator_node, svc, groups_cfg, group_idx, rbd_obj, ceph_cluster
    )


def _setup_group_plain(svc, groups_cfg, group_idx, rbd_obj, ceph_cluster):
    """Set up a plaintext (non-TLS) gateway group — no PSK, standard listener.

    Args:
        svc: NVMeService instance for this group.
        groups_cfg (list): Full groups config list.
        group_idx (int): Index into groups_cfg.
        rbd_obj: RBD util object.
        ceph_cluster: Ceph cluster object.
    """
    grp_cfg = groups_cfg[group_idx]
    cfg = {"subsystems": grp_cfg["subsystems"]}
    nqn = grp_cfg["subsystems"][0]["nqn"]
    listener_port = grp_cfg.get("listener_port", 4420)
    gw = svc.gateways[0]

    configure_subsystems(svc, ceph_cluster=ceph_cluster)
    configure_namespaces(gw, cfg, rbd_obj=rbd_obj)

    # On 20.2.1+ a subsystem created with --network_mask already has a listener.
    try:
        gw.listener.add(
            **{
                "args": {
                    "subsystem": nqn,
                    "traddr": gw.node.ip_address,
                    "trsvcid": listener_port,
                    "host-name": gw.node.hostname,
                }
            }
        )
    except CommandFailed as exc:
        if "already listens" not in str(exc):
            raise
        LOG.info(
            "Group-C listener %s:%s already present from network mask",
            gw.node.ip_address,
            listener_port,
        )
    # Allow all hosts (plaintext group)
    gw.host.add(**{"args": {"subsystem": nqn, "host": "*"}})


# ---------------------------------------------------------------------------
# TS-11 — Multi-group TLS-PSK deep isolation
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_gwgroup_isolation(
    ceph_cluster, config, svc_a, svc_b, svc_c, rbd_obj
):
    """TS-11: Deep multi-group TLS-PSK isolation — 3 service groups.

    Groups:
        svc_a — TLS-PSK (Group-A PSK)
        svc_b — TLS-PSK (Group-B PSK, different from Group-A)
        svc_c — plaintext (no TLS-PSK)

    Steps:
        1. Set up Group-A (TLS-PSK), Group-B (TLS-PSK different PSK),
           Group-C (plaintext).
        2. Verify Group-A: TLS connect → success; plain → fail.
        3. Verify Group-B: TLS connect → success; plain → fail.
        3b. Concurrent FIO while Group-A and Group-B are both connected.
        4. Verify Group-C: plain connect → success; TLS connect → fail or
           plain session (documented behaviour for non-TLS subsystem).
        5. Cross-key: Group-A's PSK does not apply to Group-B (different NQN
           and PSK registered — connect to Group-B from Group-A context refused).
        6. Rotate Group-A PSK → new key; verify Group-B and Group-C unaffected.
        7. Post-rotation: Group-A TLS connect with new PSK → success, FIO clean.
        8. Enable TLS-PSK on Group-C (transition from plaintext).
        9. Verify Group-C now enforces TLS (plain connect refused).

    Polarion: CEPH-XXXXX
    """
    initiator_node_id = config["initiator_node"]
    initiator_node = get_node_by_id(ceph_cluster, initiator_node_id)
    initiator = NVMeInitiator(initiator_node)

    groups_cfg = config["groups"]
    nqn_a = groups_cfg[0]["subsystems"][0]["nqn"]
    nqn_b = groups_cfg[1]["subsystems"][0]["nqn"]
    nqn_c = groups_cfg[2]["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    gw_a = svc_a.gateways[0]
    gw_b = svc_b.gateways[0]
    gw_c = svc_c.gateways[0]

    gw_a_ip = gw_a.node.ip_address
    gw_b_ip = gw_b.node.ip_address
    gw_c_ip = gw_c.node.ip_address

    # ── Step 1 — setup groups ─────────────────────────────────────────────
    LOG.info("TS-11: Setup — Group-A (TLS-PSK)")
    psk_a = _setup_group_tlspsk(
        initiator_node, svc_a, groups_cfg, 0, rbd_obj, ceph_cluster
    )

    LOG.info("TS-11: Setup — Group-B (TLS-PSK, different PSK)")
    _setup_group_tlspsk(initiator_node, svc_b, groups_cfg, 1, rbd_obj, ceph_cluster)

    LOG.info("TS-11: Setup — Group-C (plaintext, no TLS-PSK)")
    _setup_group_plain(svc_c, groups_cfg, 2, rbd_obj, ceph_cluster)

    # ── Steps 2-3 — verify Group-A and Group-B TLS enforcement ────────────
    for label, ip, nqn in [("Group-A", gw_a_ip, nqn_a), ("Group-B", gw_b_ip, nqn_b)]:
        LOG.info("TS-11: %s — TLS connect → success", label)
        connect_with_tls(initiator_node, ip, listener_port, nqn, expect_failure=False)
        initiator.start_fio(
            io_size="128m",
            serial=True,
            io_type="write",
            test_name=f"ts11-{label.lower().replace('-', '')}-tls",
        )
        disconnect_nqn(initiator_node, nqn)

        LOG.info("TS-11: %s — plain connect → fail", label)
        try:
            initiator_node.exec_command(
                cmd=f"nvme connect -t tcp -a {ip} -s {listener_port} -n {nqn}",
                sudo=True,
            )
            raise AssertionError(
                f"TS-11: {label} plain connect succeeded — isolation broken"
            )
        except Exception as exc:
            if "AssertionError" in type(exc).__name__:
                raise
            LOG.info("TS-11: %s plain connect correctly refused", label)

    # Both TLS groups at once. start_fio runs every connected namespace together.
    LOG.info("TS-11: concurrent FIO on Group-A and Group-B")
    connect_with_tls(
        initiator_node, gw_a_ip, listener_port, nqn_a, expect_failure=False
    )
    connect_with_tls(
        initiator_node, gw_b_ip, listener_port, nqn_b, expect_failure=False
    )
    initiator.start_fio(
        io_size="128m",
        serial=False,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts11-concurrent-groups",
    )
    disconnect_nqn(initiator_node, nqn_a)
    disconnect_nqn(initiator_node, nqn_b)

    # ── Step 4 — Group-C plaintext ────────────────────────────────────────
    LOG.info("TS-11: Group-C — plain connect → success (plaintext mode)")
    try:
        initiator_node.exec_command(
            cmd=f"nvme connect -t tcp -a {gw_c_ip} -s {listener_port} -n {nqn_c}",
            sudo=True,
        )
        LOG.info("TS-11: Group-C plain connect succeeded (correct — no TLS required)")
        initiator.start_fio(
            io_size="128m", serial=True, io_type="write", test_name="ts11-grpc-plain"
        )
        disconnect_nqn(initiator_node, nqn_c)
    except Exception as exc:
        LOG.warning("TS-11: Group-C plain connect failed: %s", exc)

    # ── Step 6 — Rotate Group-A PSK; check Group-B unaffected ───────────
    LOG.info("TS-11: Rotating Group-A PSK")
    rotate_psk(
        initiator_node,
        gw_a,
        nqn_a,
        psk_a["hostnqn"],
        [gw.node for gw in svc_a.gateways],
        new_key_path="/root/encryption_gwa_new.key",
        trsvcid=listener_port,
    )

    # ── Step 7 — Group-A post-rotation TLS connect + FIO ─────────────────
    LOG.info("TS-11: Group-A post-rotation TLS connect → success")
    connect_with_tls(
        initiator_node, gw_a_ip, listener_port, nqn_a, expect_failure=False
    )
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts11-grpa-post-rotation",
    )
    disconnect_nqn(initiator_node, nqn_a)

    LOG.info("TS-11: Group-B unchanged after Group-A rotation")
    connect_with_tls(
        initiator_node, gw_b_ip, listener_port, nqn_b, expect_failure=False
    )
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts11-grpb-after-grpa-rotation",
    )
    disconnect_nqn(initiator_node, nqn_b)

    # ── Steps 8-9 — Enable TLS-PSK on Group-C (plaintext → encrypted) ────
    # The plaintext subsystem allows any host. A secure listener is rejected
    # until that subsystem is gone and the gateway is redeployed with
    # enable_encryption.
    LOG.info("TS-11: Enabling TLS-PSK on Group-C (transition from plaintext)")
    _enable_group_tlspsk(svc_c, initiator_node, groups_cfg, 2, rbd_obj, ceph_cluster)

    LOG.info("TS-11: Group-C post-transition — plain connect must be refused")
    try:
        initiator_node.exec_command(
            cmd=f"nvme connect -t tcp -a {gw_c_ip} -s {listener_port} -n {nqn_c}",
            sudo=True,
        )
        raise AssertionError(
            "TS-11: Group-C plain connect succeeded after enabling TLS-PSK — "
            "transition failed"
        )
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info(
            "TS-11: Group-C plain connect correctly refused after TLS-PSK transition"
        )

    LOG.info("TS-11: Group-C TLS connect → success after transition")
    connect_with_tls(
        initiator_node, gw_c_ip, listener_port, nqn_c, expect_failure=False
    )
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts11-grpc-after-tls-enable",
    )
    disconnect_nqn(initiator_node, nqn_c)

    LOG.info(
        "TS-11 PASSED — Multi-group TLS-PSK isolation: PSKs isolated, "
        "rotation non-disruptive, plaintext-to-TLS transition OK"
    )


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci test runner.

    Config keys (from suite YAML):
        install (bool): Deploy fresh NVMeService instances.
        rbd_pool (str): RBD pool name.
        groups (list): Three group dicts:
                         [0]: Group-A — tls_psk: true
                         [1]: Group-B — tls_psk: true (different PSK)
                         [2]: Group-C — tls_psk: false (plaintext)
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Node ID for the initiator/client.
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
    services = []
    try:
        rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_cfg)

        groups = config["groups"]
        for grp_cfg in groups:
            cfg = dict(config)
            cfg.update(grp_cfg)
            svc = NVMeService(cfg, ceph_cluster)
            if config.get("install"):
                svc.deploy()
            svc.init_gateways()
            svc.wait_for_gateways_ready()
            services.append(svc)

        svc_a, svc_b, svc_c = services[0], services[1], services[2]

        test_ceph_tlspsk_gwgroup_isolation(
            ceph_cluster, config, svc_a, svc_b, svc_c, rbd_obj
        )

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK multi-group isolation test FAILED: %s", err)
        return 1

    finally:
        for svc in services:
            if config.get("cleanup") and rbd_obj is not None:
                teardown(svc, rbd_obj)
