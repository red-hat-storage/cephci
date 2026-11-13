"""
TLS-PSK Gateway Restart Recovery Tests — TS-05.

TS-05 (CEPH-XXXXX): Verify that TLS-PSK encryption is correctly restored after
    a gateway daemon restart and a full gateway group redeploy.  Specifically:
      - After daemon restart: initiator reconnects with --tls; FIO passes.
      - After full service redeploy: spec retains enable_encryption: True and
        encryption_key_path; TLS enforced immediately on daemon-up.
      - A restart must NOT silently downgrade to plaintext (no-TLS connect
        must still be refused post-restart).

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1)
  node6  — nvmeof-gw (GW2)
  node10 — client / initiator

Suite: suites/tentacle/nvmeof/tier-2_nvmeof_tlspsk_tests.yaml
"""

import json
import time

from ceph.ceph import Ceph
from ceph.ceph_admin.daemon import Daemon
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
    nvmeof_daemons_restarted,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

DEFAULT_KEY_PATH = "/root/encryption.key"


def _setup_tlspsk_subsystem(
    ceph_cluster, config, nvme_service, initiator_node, rbd_obj
):
    """Full doc Phase 2-5 setup. Returns psk_info dict."""
    gw = nvme_service.gateways[0]
    gw_ip = gw.node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    psk_info = setup_initiator_tls_psk(initiator_node, nqn)
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
    add_host_with_psk(gw, nqn, psk_info["hostnqn"], psk_info["key_data"])
    return psk_info


def _nvmeof_daemons(orch, service_name):
    """Return ``ceph orch ps`` entries for *service_name*."""
    out, _ = orch.shell(
        args=[f"ceph orch ps --service-name {service_name} --format json"]
    )
    return json.loads(out)


def _wait_until_gateways_serving(orch, nvme_service, before, timeout=180, interval=10):
    """Poll until every NVMe-oF daemon is running, then until gRPC init is done.

    ``status_desc == running`` is not enough: ``ceph orch daemon stop`` leaves
    ``user_stopped`` set, and a daemon can report running before the listener
    accepts connects. Fail if the daemons never come back.

    Args:
        orch (Orch): ceph orch helper.
        nvme_service (NVMeService): Service whose gateways must be ready.
        before (list): ``ceph orch ps`` entries captured before the restart.
        timeout (int): Seconds to wait for ``ceph orch ps`` to show serving daemons.
        interval (int): Poll interval in seconds.

    Raises:
        RuntimeError: Daemons stayed stopped or user-stopped past *timeout*.
    """
    service_name = nvme_service.service_name
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = _nvmeof_daemons(orch, service_name)
        if nvmeof_daemons_restarted(before, last):
            LOG.info("NVMe-oF daemons for %s restarted and are running", service_name)
            nvme_service.wait_for_gateways_ready()
            return
        time.sleep(interval)
    summary = [
        {
            "name": daemon.get("daemon_name"),
            "status": daemon.get("status_desc"),
            "user_stopped": daemon.get("user_stopped"),
        }
        for daemon in last
    ]
    raise RuntimeError(
        f"{service_name} daemons did not return to running within {timeout}s: "
        f"{summary}"
    )


# ---------------------------------------------------------------------------
# TS-05 — GW daemon restart: TLS-PSK survives
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_gw_restart_single_daemon(
    ceph_cluster, config, nvme_service, psk_info
):
    """TS-05a: Single daemon restart does not drop TLS enforcement.

    Gateway resources (subsystem, namespaces, listeners, host) are already
    created by the caller via _setup_tlspsk_subsystem().

    Steps:
        1. Connect --tls; FIO write.
        2. Restart one GW daemon (ceph orch daemon restart <name>).
           ``daemon stop`` sets user_stopped and does not come back on its own.
        3. Wait until every daemon is running and gateway init has finished.
        4. Verify TLS still enforced: plain connect is an NVMe/TLS refusal,
           not "connection refused".
        5. Connect with --tls; FIO read; disconnect.

    Args:
        psk_info (dict): {"hostnqn": str, "key_data": str} from _setup_tlspsk_subsystem.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)
    gw_ip = nvme_service.gateways[0].node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    LOG.info("TS-05a: Connecting for pre-restart FIO")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts05a-pre-restart"
    )
    disconnect_nqn(initiator_node, nqn)

    # Step 2 — restart one GW daemon. `daemon stop` sets user_stopped and
    # cephadm will not start it again.
    orch = Orch(ceph_cluster, **{})
    daemon_obj = Daemon(ceph_cluster, **{})
    before = _nvmeof_daemons(orch, nvme_service.service_name)
    if not before:
        raise RuntimeError(f"No daemons found for service {nvme_service.service_name}")
    daemon_name = before[0]["daemon_name"]
    LOG.info("TS-05a: Restarting daemon %s", daemon_name)
    daemon_obj.op("restart", {"pos_args": [daemon_name]})

    # Step 3 — wait until the daemon is serving again
    LOG.info("TS-05a: Waiting for daemon to restart (up to 180s)")
    _wait_until_gateways_serving(orch, nvme_service, before, timeout=180)

    # Step 4 — TLS still enforced (a closed port is not a TLS refusal)
    LOG.info("TS-05a: Verifying TLS still enforced after daemon restart")
    assert_plain_connect_refused(
        initiator_node, gw_ip, listener_port, nqn, context="TS-05a"
    )

    # Step 5 — connect with TLS; FIO read
    LOG.info("TS-05a: Connecting with --tls after daemon restart")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="256m",
        serial=True,
        io_type="read",
        execute_blkdiscard=False,
        test_name="ts05a-post-restart",
    )
    disconnect_nqn(initiator_node, nqn)
    LOG.info("TS-05a PASSED — TLS-PSK survives single daemon restart")


def test_ceph_tlspsk_gw_restart_full_redeploy(
    ceph_cluster, config, nvme_service, psk_info
):
    """TS-05b: Full service redeploy retains TLS-PSK (spec persists).

    Gateway resources (subsystem, namespaces, listeners, host) are already
    created by the caller via _setup_tlspsk_subsystem() — or left intact after
    TS-05a ran on the same subsystem.

    Steps:
        1. Connect --tls; FIO write.
        2. Trigger full service redeploy via ceph orch redeploy.
        3. Verify plain connect refused (TLS spec intact after redeploy).
        4. Connect with --tls; FIO; disconnect.

    Args:
        psk_info (dict): {"hostnqn": str, "key_data": str} from _setup_tlspsk_subsystem.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)
    gw_ip = nvme_service.gateways[0].node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)

    LOG.info("TS-05b: Connecting for pre-redeploy FIO")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="128m", serial=True, io_type="write", test_name="ts05b-pre-redeploy"
    )
    disconnect_nqn(initiator_node, nqn)

    # Step 2 — full service redeploy
    service_name = nvme_service.service_name
    LOG.info("TS-05b: ceph orch redeploy %s", service_name)
    orch = Orch(ceph_cluster, **{})
    before = _nvmeof_daemons(orch, service_name)
    orch.shell(args=[f"ceph orch redeploy {service_name}"])
    _wait_until_gateways_serving(orch, nvme_service, before, timeout=180)

    # Step 3 — TLS still enforced (a closed port is not a TLS refusal)
    LOG.info("TS-05b: Verifying TLS enforced after full redeploy")
    assert_plain_connect_refused(
        initiator_node, gw_ip, listener_port, nqn, context="TS-05b"
    )

    # Step 4 — TLS connect; FIO
    LOG.info("TS-05b: Connecting with --tls after full redeploy")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts05b-post-redeploy",
    )
    disconnect_nqn(initiator_node, nqn)
    LOG.info("TS-05b PASSED — TLS-PSK spec persists through full service redeploy")


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci test runner.

    Config keys (from suite YAML):
        install (bool): Deploy a fresh NVMeService.
        tls_psk (bool): Enable TLS-PSK.
        encryption_key_path (str): RSA key path on GW nodes.
        rbd_pool (str): RBD pool name.
        gw_group (str): Gateway group name.
        gw_nodes (list[str]): Gateway node IDs.
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Node ID for the initiator.
        subsystems (list): Subsystem config dicts.
        run_ts05b (bool): When True run only TS-05b (full redeploy standalone);
                          when False (default) run TS-05a then TS-05b sequentially —
                          both share the same subsystem created once in run().
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

        initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])

        # ONE-TIME gateway resource creation — subsystem, namespaces, listeners, host.
        # Both TS-05a and TS-05b receive the same psk_info; neither creates resources.
        psk_info = _setup_tlspsk_subsystem(
            ceph_cluster, config, nvme_service, initiator_node, rbd_obj
        )

        polarion_id = config.get("polarion-id", "")
        run_ts05b = config.get("run_ts05b", False) or "TS05b" in polarion_id

        if run_ts05b:
            # TS-05b standalone: full redeploy only (e.g. targeted Polarion run)
            test_ceph_tlspsk_gw_restart_full_redeploy(
                ceph_cluster, config, nvme_service, psk_info
            )
        else:
            # Default: TS-05a (daemon restart) then TS-05b (full redeploy) sequentially.
            # TS-05b reuses the same subsystem — no duplicate creation.
            test_ceph_tlspsk_gw_restart_single_daemon(
                ceph_cluster, config, nvme_service, psk_info
            )
            test_ceph_tlspsk_gw_restart_full_redeploy(
                ceph_cluster, config, nvme_service, psk_info
            )

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK GW restart test FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
