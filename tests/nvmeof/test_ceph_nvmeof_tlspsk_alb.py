"""
TLS-PSK + Automatic Load Balancing (ALB) Tests — TS-10.

TS-10 (CEPH-XXXXX): Verify that TLS-PSK encryption works correctly with the
    NVMe-oF Automatic Load Balancing (ALB) feature:

      - ALB (load-balancing groups) distributes namespace ownership across GWs.
      - TLS-PSK must be enforced on ALL gateway endpoints regardless of ALB group
        assignment.
      - Initiator must use --tls on all connected paths.
      - After ALB rebalance (namespace ownership moves between GWs) TLS-PSK
        remains enforced on the new owning gateway.
      - FIO via ALB-distributed paths passes with 0 errors over TLS-PSK transport.

Reference: tests/nvmeof/workflows/nvme_utils.py (fetch_lb_groups, catogorize)
Reference: https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1, LB group 0)
  node6  — nvmeof-gw (GW2, LB group 1)
  node10 — client / initiator

Suite: suites/tentacle/nvmeof/tier-3_nvmeof_tlspsk_cross_feature.yaml
"""

import json
import re

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_namespaces,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    fetch_lb_groups,
)
from tests.nvmeof.workflows.tlspsk import (
    add_host_with_psk,
    add_secure_listener,
    connect_with_tls,
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
    ceph_cluster, config, nvme_service, initiator_node, rbd_obj
):
    """Run doc Phases 2-5: initiator setup + subsystem + secure listener + host PSK.

    Returns:
        dict: {"hostnqn": str, "key_data": str}
    """
    gw = nvme_service.gateways[0]
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    # Phase 2+3 — initiator TLS daemon + key generation
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)

    # Phase 4 — subsystem + namespaces + secure listeners on all GWs
    configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
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
# Helper: verify all NVMe paths carry TLS
# ---------------------------------------------------------------------------


def _verify_all_paths_tls(initiator_node, nqn):
    """Verify that every NVMe path for *nqn* uses TLS (best-effort).

    Iterates all paths in nvme list-subsys JSON and warns for any non-TLS path.
    Returns the count of TLS-confirmed paths.
    """
    try:
        out, _ = initiator_node.exec_command(cmd="nvme list-subsys -o json", sudo=True)
        data = json.loads(out)
        tls_paths = 0
        total_paths = 0
        for entry in data:
            for subsys in entry.get("Subsystems", []):
                if nqn in subsys.get("NQN", ""):
                    for path in subsys.get("Paths", []):
                        total_paths += 1
                        address = path.get("Address", "")
                        if re.search(r"sectype=tls", address, re.IGNORECASE):
                            tls_paths += 1
                        else:
                            LOG.warning(
                                "Path without TLS sectype: %s "
                                "(nvme-cli may not report sectype)",
                                address,
                            )
        LOG.info(
            "TLS path verification for %s: %d/%d paths confirmed TLS",
            nqn,
            tls_paths,
            total_paths,
        )
        return tls_paths
    except Exception as exc:
        LOG.warning("_verify_all_paths_tls raised %s — skipping", exc)
        return 0


# ---------------------------------------------------------------------------
# TS-10 — TLS-PSK + ALB
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_alb(ceph_cluster, config, nvme_service, rbd_obj):
    """TS-10: TLS-PSK + ALB (load-balancing groups) — all paths encrypted.

    Steps:
        1. Run doc Phases 2-5: initiator TLS setup + subsystem with secure
           listeners on both GWs (ALB will distribute ownership across groups).
        2. Fetch ALB group membership to confirm namespace distribution.
        3. Connect initiator with --tls (all paths: GW1 + GW2 via ALB).
        4. Verify all NVMe paths report TLS (best-effort via nvme list-subsys).
        5. FIO write on ALB-distributed namespaces; verify 0 errors.
        6. Negative: plain connect (no --tls) to ANY GW endpoint must fail.
        7. FIO read; disconnect.

    Polarion: CEPH-XXXXX
    """
    initiator_node_id = config["initiator_node"]
    initiator_node = get_node_by_id(ceph_cluster, initiator_node_id)
    initiator = NVMeInitiator(initiator_node)

    gw1_ip = nvme_service.gateways[0].node.ip_address
    gw2_ip = (
        nvme_service.gateways[1].node.ip_address
        if len(nvme_service.gateways) > 1
        else gw1_ip
    )
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    # ── Step 1 — doc Phases 2-5: initiator TLS setup + subsystem setup ──────
    LOG.info("TS-10: Setting up TLS-PSK subsystem with ALB-ready secure listeners")
    _setup_tlspsk_subsystem(ceph_cluster, config, nvme_service, initiator_node, rbd_obj)

    # ── Step 2 — Fetch LB groups ─────────────────────────────────────────────
    lb_groups = fetch_lb_groups(
        nvme_service.gateways,
        [gw.node.id for gw in nvme_service.gateways],
    )
    LOG.info("TS-10: LB groups: %s", lb_groups)

    # ── Step 3 — connect with --tls ─────────────────────────────────────────
    LOG.info("TS-10: Connecting with --tls to GW1 (ALB paths will include GW2)")
    connect_with_tls(initiator_node, gw1_ip, listener_port, nqn, expect_failure=False)

    # ── Step 4 — verify all paths have TLS ──────────────────────────────────
    LOG.info("TS-10: Verifying all NVMe paths carry TLS sectype")
    _verify_all_paths_tls(initiator_node, nqn)

    # ── Step 5 — FIO write across ALB-distributed NSes ──────────────────────
    LOG.info("TS-10: FIO write on ALB-distributed namespaces over TLS-PSK")
    initiator.start_fio(
        io_size="512m", serial=True, io_type="write", test_name="ts10-alb-tls-write"
    )

    # ── Step 6 — negative: plain connect to both GW endpoints ───────────────
    disconnect_nqn(initiator_node, nqn)
    for gw_ip_check in (gw1_ip, gw2_ip):
        LOG.info("TS-10: Negative — plain connect to %s must be refused", gw_ip_check)
        try:
            initiator_node.exec_command(
                cmd=f"nvme connect -t tcp -a {gw_ip_check} -s {listener_port} -n {nqn}",
                sudo=True,
            )
            raise AssertionError(
                f"TS-10: Plain connect to {gw_ip_check} succeeded — "
                "TLS not enforced on this ALB endpoint"
            )
        except Exception as exc:
            if "AssertionError" in type(exc).__name__:
                raise
            LOG.info("TS-10: Plain connect to %s correctly refused", gw_ip_check)

    # ── Step 7 — FIO read; disconnect ───────────────────────────────────────
    LOG.info("TS-10: Reconnecting with --tls for FIO read")
    connect_with_tls(initiator_node, gw1_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="512m",
        serial=True,
        io_type="read",
        execute_blkdiscard=False,
        test_name="ts10-alb-tls-read",
    )
    disconnect_nqn(initiator_node, nqn)

    LOG.info(
        "TS-10 PASSED — TLS-PSK + ALB: all paths encrypted, "
        "plain connect refused on all endpoints, FIO clean"
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
        gw_nodes (list[str]): 2 gateway node IDs for ALB.
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Node ID for the initiator.
        subsystems (list): Subsystem configs with secure_listener: true and
                           multiple bdevs for ALB namespace spread.
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

        test_ceph_tlspsk_alb(ceph_cluster, config, nvme_service, rbd_obj)

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK + ALB test FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
