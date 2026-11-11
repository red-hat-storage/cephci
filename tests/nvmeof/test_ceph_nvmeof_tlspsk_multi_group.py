"""
TLS-PSK Multi Gateway-Group Tests — TS-04.

Two independent GW groups each with its own PSK (per IBM doc procedure).
Each group runs the full 6-phase doc procedure independently.

Procedure per group (from IBM doc):
  Phase 1: openssl key + orch spec (enable_encryption: True)
  Phase 2-3: initiator — ktls-utils, tlshd, nvme gen-tls-key, check-tls-key --insert
  Phase 4: subsystem --secure_listeners, listener add --secure
  Phase 5: host add --psk KEY_DATA
  Phase 6: nvme connect --tls

Reference: https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1, Group-1)
  node6  — nvmeof-gw (GW2, Group-2)
  node10 — client / initiator
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
    generate_tls_key,
    insert_tls_key,
    load_nvme_tcp_module,
    rotate_psk,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _setup_group(ceph_cluster, config, nvme_svc, initiator_node, rbd_obj):
    """Run full doc Phase 2-5 for one GW group. Returns psk_info dict."""
    gw = nvme_svc.gateways[0]
    gw_ip = gw.node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    psk_info = setup_initiator_tls_psk(initiator_node, nqn)

    configure_subsystems(nvme_svc, ceph_cluster=ceph_cluster)
    configure_namespaces(gw, config, rbd_obj=rbd_obj)
    add_secure_listener(gw, nqn, gw_ip, listener_port, gw.node.hostname)
    for extra_gw in nvme_svc.gateways[1:]:
        add_secure_listener(
            extra_gw,
            nqn,
            extra_gw.node.ip_address,
            listener_port,
            extra_gw.node.hostname,
        )
    add_host_with_psk(gw, nqn, psk_info["hostnqn"], psk_info["key_data"])
    return psk_info


def test_ceph_tlspsk_multi_group(ceph_cluster, config, nvme_svc1, nvme_svc2, rbd_obj):
    """TS-04: Two GW groups each with independent PSK keys.

    Steps:
        1. Full doc Phase 1-6 for Group-1; FIO.
        2. Full doc Phase 1-6 for Group-2 (separate nvme gen-tls-key); FIO.
        3. Isolation: plain connect to Group-1 subsystem must fail.
        4. Isolation: plain connect to Group-2 subsystem must fail.
        5. Rotate Group-1 PSK (nvme gen-tls-key + host add --psk new_key).
        6. Group-1 reconnects with new PSK → FIO clean.
        7. Group-2 PSK unchanged — still connects → FIO clean.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)

    groups_cfg = config["groups"]
    nqn1 = groups_cfg[0]["subsystems"][0]["nqn"]
    nqn2 = groups_cfg[1]["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    gw1_ip = nvme_svc1.gateways[0].node.ip_address
    gw2_ip = nvme_svc2.gateways[0].node.ip_address

    # Step 1 — Group-1 full setup
    LOG.info("TS-04: Group-1 full doc Phase 2-5 setup")
    cfg1 = dict(config)
    cfg1.update(groups_cfg[0])
    psk1 = _setup_group(ceph_cluster, cfg1, nvme_svc1, initiator_node, rbd_obj)

    LOG.info("TS-04: Group-1 connect --tls + FIO")
    connect_with_tls(initiator_node, gw1_ip, listener_port, nqn1, expect_failure=False)
    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts04-grp1-fio"
    )
    disconnect_nqn(initiator_node, nqn1)

    # Step 2 — Group-2 full setup (fresh nvme gen-tls-key for Group-2's NQN)
    LOG.info("TS-04: Group-2 full doc Phase 2-5 setup")
    cfg2 = dict(config)
    cfg2.update(groups_cfg[1])
    psk2 = _setup_group(ceph_cluster, cfg2, nvme_svc2, initiator_node, rbd_obj)

    LOG.info("TS-04: Group-2 connect --tls + FIO")
    connect_with_tls(initiator_node, gw2_ip, listener_port, nqn2, expect_failure=False)
    initiator.start_fio(
        io_size="256m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts04-grp2-fio",
    )
    disconnect_nqn(initiator_node, nqn2)

    # Steps 3-4 — Isolation: plain connect must fail on both groups
    for label, ip, nqn in [("Group-1", gw1_ip, nqn1), ("Group-2", gw2_ip, nqn2)]:
        LOG.info("TS-04: Isolation — plain connect to %s must fail", label)
        try:
            initiator_node.exec_command(
                cmd=f"nvme connect --transport tcp --traddr {ip} "
                f"--trsvcid {listener_port} --nqn {nqn}",
                sudo=True,
            )
            raise AssertionError(
                f"TS-04: Plain connect to {label} succeeded — PSK isolation broken"
            )
        except Exception as exc:
            if "AssertionError" in type(exc).__name__:
                raise
            LOG.info("TS-04: %s plain connect correctly refused", label)

    # Step 5 — Rotate Group-1 PSK
    LOG.info("TS-04: Rotating Group-1 PSK (nvme gen-tls-key + host add --psk)")
    gw1_nodes = [gw.node for gw in nvme_svc1.gateways]
    new_key1 = rotate_psk(
        initiator_node,
        nvme_svc1.gateways[0],
        nqn1,
        psk1["hostnqn"],
        gw1_nodes,
        trsvcid=listener_port,
    )

    # Step 6 — Group-1 connects with new PSK
    LOG.info("TS-04: Group-1 connect after PSK rotation")
    connect_with_tls(initiator_node, gw1_ip, listener_port, nqn1, expect_failure=False)
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts04-grp1-post-rotation",
    )
    disconnect_nqn(initiator_node, nqn1)

    # Step 7 — Group-2 unaffected (PSK2 unchanged)
    LOG.info("TS-04: Group-2 connect after Group-1 rotation — must still work")
    connect_with_tls(initiator_node, gw2_ip, listener_port, nqn2, expect_failure=False)
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts04-grp2-after-grp1-rotation",
    )
    disconnect_nqn(initiator_node, nqn2)

    LOG.info("TS-04 PASSED — multi-group PSK isolation and independent rotation OK")


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
    nvme_svc1 = nvme_svc2 = None
    try:
        rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_cfg)
        groups = config["groups"]

        cfg1 = dict(config)
        cfg1.update(groups[0])
        cfg1["tls_psk"] = True
        nvme_svc1 = NVMeService(cfg1, ceph_cluster)
        if config.get("install"):
            nvme_svc1.deploy()
        nvme_svc1.init_gateways()
        nvme_svc1.wait_for_gateways_ready()

        cfg2 = dict(config)
        cfg2.update(groups[1])
        cfg2["tls_psk"] = True
        nvme_svc2 = NVMeService(cfg2, ceph_cluster)
        if config.get("install"):
            nvme_svc2.deploy()
        nvme_svc2.init_gateways()
        nvme_svc2.wait_for_gateways_ready()

        test_ceph_tlspsk_multi_group(
            ceph_cluster, config, nvme_svc1, nvme_svc2, rbd_obj
        )
        return 0
    except Exception as err:
        LOG.exception("TLS-PSK multi-group test FAILED: %s", err)
        return 1
    finally:
        for svc in (nvme_svc1, nvme_svc2):
            if config.get("cleanup") and svc is not None and rbd_obj is not None:
                teardown(svc, rbd_obj)
