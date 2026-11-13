"""
TLS-PSK Basic Tests — TS-01 and TS-02.

TS-01 (CEPH-XXXXX): End-to-end TLS-PSK connect / FIO / disconnect following
    the exact procedure from:
    https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

TS-02 (CEPH-XXXXX): Negative — connect without --tls to a TLS-PSK-required
    subsystem must be refused; gateway remains healthy.

Procedure (from doc):
---------------------
Phase 1 — NVMeService.deploy() handles:
    openssl req -newkey rsa:512 -noenc -noout -keyout /root/encryption.key -batch
    orch spec: enable_auth: False, enable_encryption: True, encryption_key_path: ...

Phase 2 — Initiator: install ktls-utils, configure /etc/tlshd.conf, enable tlshd

Phase 3 — Initiator:
    cat /etc/nvme/hostnqn
    nvme gen-tls-key             → KEY_DATA
    modprobe nvme-tcp
    nvme check-tls-key --insert --hostnqn=HOST_NQN --subsysnqn=SUBSYSTEM_NQN
                        --keydata=KEY_DATA --identity=0

Phase 4 — Gateway CLI:
    ceph nvmeof subsystem add SUBSYSTEM_NQN --secure_listeners
    ceph nvmeof namespace add ...
    ceph nvmeof listener add --nqn SUBSYSTEM_NQN --secure

Phase 5 — Gateway CLI:
    ceph nvmeof host add --nqn SUBSYSTEM_NQN --host_nqn HOST_NQN --psk KEY_DATA

Phase 6 — Initiator:
    nvme connect --transport tcp --traddr GATEWAY_IP --trsvcid 4420
                 --nqn SUBSYSTEM_NQN --tls

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1)
  node6  — nvmeof-gw (GW2)
  node10 — client / initiator

Design: run() calls setup ONCE then passes the resulting psk_info to every test
function — no test function ever re-creates gateway resources that already exist.
"""

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
    disconnect_nqn,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


# ---------------------------------------------------------------------------
# Shared setup helper — call this ONCE from run(), pass psk_info to tests
# ---------------------------------------------------------------------------


def namespace_owner_args(gateway):
    """Place a new namespace on the connected gateway's ANA group.

    The gateway rebalancer can move it again. Callers that need a block
    device connect to every listener, because a connect to the gateway
    that no longer owns the namespace exits 0 and leaves ``nvme list`` empty.
    """
    ana_group = getattr(gateway, "ana_group_id", None)
    if ana_group is None:
        return {}
    return {"load-balancing-group": ana_group}


def _setup_tlspsk_subsystem(
    ceph_cluster, config, nvme_service, initiator_node, rbd_obj
):
    """Run doc Phases 2-5 exactly once and return psk_info.

    Creates on the gateway (must not exist yet):
      - subsystem with --secure_listeners
      - namespace(s)
      - secure listeners on all GW nodes
      - host entry with PSK

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test config dict.
        nvme_service: NVMeService instance.
        initiator_node: CephNode for the initiator.
        rbd_obj: RBD util object.

    Returns:
        dict: {"hostnqn": str, "key_data": str}
    """
    gw = nvme_service.gateways[0]
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    # Phase 2+3 — initiator
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)

    # Phase 4 — gateway: subsystem, namespaces, secure listeners
    configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
    configure_namespaces(
        gw, config, opt_args=namespace_owner_args(gw), rbd_obj=rbd_obj
    )
    add_secure_listener(gw, nqn, gw.node.ip_address, listener_port, gw.node.hostname)
    for extra_gw in nvme_service.gateways[1:]:
        add_secure_listener(
            extra_gw,
            nqn,
            extra_gw.node.ip_address,
            listener_port,
            extra_gw.node.hostname,
        )

    # Phase 5 — gateway: host with PSK
    add_host_with_psk(gw, nqn, psk_info["hostnqn"], psk_info["key_data"])
    return psk_info


# ---------------------------------------------------------------------------
# TS-01 — End-to-end TLS-PSK happy path
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_basic_connect(ceph_cluster, config, nvme_service, psk_info):
    """TS-01: Full TLS-PSK procedure from doc — connect, FIO, disconnect.

    Gateway resources (subsystem, namespaces, listeners, host) are already
    created by the caller via _setup_tlspsk_subsystem().  This function only
    runs Phase 6: connect --tls → FIO write + read → disconnect.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test config dict.
        nvme_service: NVMeService instance.
        psk_info (dict): {"hostnqn": str, "key_data": str} from setup.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)

    gw_ip = nvme_service.gateways[0].node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    # Phase 6: connect with --tls
    LOG.info("TS-01: Phase 6 — nvme connect --tls")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)

    LOG.info("TS-01: FIO write")
    initiator.start_fio(
        io_size="512m", serial=True, io_type="write", test_name="ts01-write"
    )
    LOG.info("TS-01: FIO read")
    initiator.start_fio(
        io_size="512m",
        serial=True,
        io_type="read",
        execute_blkdiscard=False,
        test_name="ts01-read",
    )

    disconnect_nqn(initiator_node, nqn)
    LOG.info("TS-01 PASSED — full TLS-PSK procedure OK")


# ---------------------------------------------------------------------------
# TS-02 — Non-TLS connect to TLS-PSK subsystem must be refused
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_plain_connect_refused(
    ceph_cluster, config, nvme_service, psk_info
):
    """TS-02: Plain connect (no --tls) to TLS-PSK subsystem must fail.

    Gateway resources already exist (shared with TS-01 when sequential).
    This function only performs the negative connect attempt and the
    health-check TLS connect that follows.

    Steps:
        1. Attempt nvme connect WITHOUT --tls → assert failure.
        2. Gateway health check: TLS connect → FIO → disconnect.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test config dict.
        nvme_service: NVMeService instance.
        psk_info (dict): {"hostnqn": str, "key_data": str} — used for nothing
            here (gateway host already registered); kept for API symmetry.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)

    gw_ip = nvme_service.gateways[0].node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    # Negative: plain connect WITHOUT --tls must be refused
    LOG.info("TS-02: Attempting plain connect (no --tls) — must fail")
    plain_cmd = (
        f"nvme connect --transport tcp --traddr {gw_ip} "
        f"--trsvcid {listener_port} --nqn {nqn}"
    )
    try:
        initiator_node.exec_command(cmd=plain_cmd, sudo=True)
        raise AssertionError(
            "TS-02 FAILED: Plain (non-TLS) connect succeeded on a "
            "--secure_listeners subsystem. TLS enforcement not working."
        )
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info("TS-02: Plain connect correctly refused: %s", exc)

    # Gateway health check: TLS connect must still work
    LOG.info("TS-02: Gateway health check — TLS connect after refused plain attempt")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts02-health-check",
    )
    disconnect_nqn(initiator_node, nqn)

    LOG.info("TS-02 PASSED — plain connect refused; gateway healthy")


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci test runner.

    Config keys (from suite YAML):
        install (bool): Deploy a fresh NVMeService when True.
        tls_psk (bool): Enable TLS-PSK in the orch spec (enable_encryption: True).
        rbd_pool (str): RBD pool name.
        gw_group (str): Gateway group name.
        gw_nodes (list[str]): Gateway node IDs.
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Node ID for the initiator/client.
        subsystems (list): Subsystem config dicts — must include secure_listener: true.
        run_ts02 (bool): When True run TS-02 only; default runs TS-01 then TS-02.
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
        run_ts02 = config.get("run_ts02", False) or "TS02" in polarion_id

        # ── One-time setup: create all GW resources (Phases 2-5) ─────────────
        initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
        psk_info = _setup_tlspsk_subsystem(
            ceph_cluster, config, nvme_service, initiator_node, rbd_obj
        )

        # ── Run test(s) — no setup inside test functions ──────────────────────
        if run_ts02:
            test_ceph_tlspsk_plain_connect_refused(
                ceph_cluster, config, nvme_service, psk_info
            )
        else:
            test_ceph_tlspsk_basic_connect(ceph_cluster, config, nvme_service, psk_info)
            test_ceph_tlspsk_plain_connect_refused(
                ceph_cluster, config, nvme_service, psk_info
            )

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK basic test FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
