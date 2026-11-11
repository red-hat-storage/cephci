"""
IBMCEPH-19520: forced listener delete with live connections must not abort peers.

When GW-A deletes GW-B's listener with force while initiators still have
active I/O on that listener, peer OMAP replay used to call SPDK without
force. SPDK returned ENOTEMPTY (errno 39) and abort_server_on_update_error
took the gateway down.

Fixed by ceph-nvmeof#2190: pass force=True for listener delete on update.

Assertions mirror IBMCEPH-17197 (peer MainPID / ActiveEnterTimestamp) and
look for error-39 abort markers in peer journals.

Config:
    expect_gateway_abort (bool): When True (9.0 repro), the peer MUST abort
        with error 39. When False (default / 9.2z1), peers must stay up.
"""

import threading
import time

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.test_ceph_nvmeof_tlspsk_basic import _setup_tlspsk_subsystem
from tests.nvmeof.test_nvmeof_host_del_connected_peer_gw import (
    _gw_unit_identity,
    _snapshot_gateways,
)
from tests.nvmeof.workflows.gateway_entities import teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import connect_with_tls_until, disconnect_nqn
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

# Peer abort path for IBMCEPH-19520 (OMAP sync on ENOTEMPTY / errno 39).
ABORT_MARKERS = (
    "Got error 39 while updating gateway",
    "SystemExit: Got error 39",
    "AssertionError: Got error 39",
)

POLL_INTERVAL_SEC = 5


def _scan_peer_journals_for_abort(gateways, peer_node_id, since_seconds=180):
    """Return journal hit text from the peer, or empty string if none."""
    grep_expr = "|".join(ABORT_MARKERS)
    hits = []
    for gw in gateways:
        if gw.node.id != peer_node_id:
            continue
        unit = gw.system_unit_id
        cmd = (
            f"journalctl -u {unit} --since '{since_seconds} seconds ago' --no-pager "
            f"| grep -E '{grep_expr}' || true"
        )
        out, _ = gw.node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        text = (out or "").strip()
        if text:
            hits.append(f"[{gw.node.hostname}] journal hits:\n{text}")
    return "\n".join(hits)


def _peer_restarted(before, after, peer_node_id):
    """Return True if the peer unit restarted or left active."""
    pre = before.get(peer_node_id)
    post = after.get(peer_node_id)
    if not pre or not post:
        return True
    if post["active_state"] != "active":
        return True
    if pre["main_pid"] != post["main_pid"]:
        return True
    if pre["active_ts"] != post["active_ts"]:
        return True
    return False


def _wait_for_peer_outcome(
    gateways, before, peer_node_id, sync_wait, expect_abort
):
    """Poll until sync_wait expires; return (aborted: bool, after: dict, journal)."""
    deadline = time.time() + sync_wait
    after = before
    aborted = False
    journal = ""
    LOG.info(
        "Polling peer %s for %ss (expect_gateway_abort=%s)",
        peer_node_id,
        sync_wait,
        expect_abort,
    )
    while True:
        after = _snapshot_gateways(gateways)
        if _peer_restarted(before, after, peer_node_id):
            aborted = True
            LOG.warning(
                "Peer restart detected: before=%s after=%s",
                before.get(peer_node_id),
                after.get(peer_node_id),
            )
            break
        journal = _scan_peer_journals_for_abort(
            gateways, peer_node_id, since_seconds=sync_wait + 60
        )
        if journal:
            aborted = True
            LOG.warning("Peer abort markers found:\n%s", journal)
            break
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SEC, remaining))

    if not journal:
        journal = _scan_peer_journals_for_abort(
            gateways, peer_node_id, since_seconds=sync_wait + 60
        )
        if journal:
            aborted = True
    if _peer_restarted(before, after, peer_node_id):
        aborted = True
    return aborted, after, journal


def test_ceph_tlspsk_listener_delete_abort(
    ceph_cluster, config, nvme_service, psk_info
):
    """IBMCEPH-19520: force-delete peer listener under FIO; check peer abort.

    Gateway resources are already created by ``_setup_tlspsk_subsystem``.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Suite config. Honors expect_gateway_abort,
            state_update_interval_sec, listener_abort_fio_runtime,
            peer_sync_wait_sec.
        nvme_service: Deployed NVMeService with >= 2 gateways.
        psk_info (dict): From ``_setup_tlspsk_subsystem``.
    """
    if len(nvme_service.gateways) < 2:
        raise RuntimeError("IBMCEPH-19520 listener_abort needs two gateways")

    expect_abort = bool(config.get("expect_gateway_abort", False))
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)
    gw_a = nvme_service.gateways[0]
    gw_b = nvme_service.gateways[1]
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    psk_sync_wait = config.get("state_update_interval_sec", 30) + 15
    sync_wait = int(
        config.get(
            "peer_sync_wait_sec",
            config.get("state_update_interval_sec", 30) + 45,
        )
    )
    fio_runtime = int(config.get("listener_abort_fio_runtime", 120))

    LOG.info(
        "IBMCEPH-19520: connect --tls to both listeners (psk sync wait %ss)",
        psk_sync_wait,
    )
    for gw in nvme_service.gateways:
        connect_with_tls_until(
            initiator_node,
            gw.node.ip_address,
            listener_port,
            nqn,
            timeout=psk_sync_wait,
        )

    fio_box = {"results": None, "error": None}

    def _run_fio():
        try:
            fio_box["results"] = initiator.start_fio(
                io_size="1G",
                runtime=fio_runtime,
                serial=False,
                io_type="write",
                time_based=True,
                test_name="ibmceph-19520-listener-abort",
            )
        except Exception as exc:
            fio_box["error"] = exc

    fio_thread = threading.Thread(target=_run_fio, daemon=True)
    LOG.info("IBMCEPH-19520: starting background FIO (%ds)", fio_runtime)
    fio_thread.start()
    time.sleep(5)

    before = _snapshot_gateways(nvme_service.gateways)
    LOG.info("Gateway identities before listener del: %s", before)

    # Delete GW-B's listener via GW-A's CLI so OMAP peers (esp. GW-B) replay it.
    LOG.info(
        "IBMCEPH-19520: GW-A deleting GW-B listener %s:%s force=True",
        gw_b.node.ip_address,
        listener_port,
    )
    gw_a.listener.delete(
        **{
            "args": {
                "subsystem": nqn,
                "traddr": gw_b.node.ip_address,
                "trsvcid": listener_port,
                "host-name": gw_b.node.hostname,
                "force": True,
            }
        }
    )

    aborted, after, journal = _wait_for_peer_outcome(
        nvme_service.gateways,
        before,
        gw_b.node.id,
        sync_wait,
        expect_abort,
    )
    LOG.info("Gateway identities after listener del: %s", after)

    try:
        disconnect_nqn(initiator_node, nqn)
    except Exception as exc:
        LOG.info("Disconnect after listener_abort: %s", exc)

    fio_thread.join(timeout=fio_runtime + 60)

    if expect_abort:
        if not aborted:
            raise AssertionError(
                "IBMCEPH-19520 repro FAILED: expected peer GW abort with error 39 "
                f"on {gw_b.node.hostname}, but peer stayed up. "
                f"before={before.get(gw_b.node.id)} after={after.get(gw_b.node.id)} "
                f"journal={journal!r}"
            )
        LOG.info(
            "IBMCEPH-19520 repro CONFIRMED: peer %s aborted (error 39 path)",
            gw_b.node.hostname,
        )
        return

    if aborted:
        raise AssertionError(
            "IBMCEPH-19520 FAILED: peer gateway aborted after forced listener "
            f"delete under live I/O. peer={gw_b.node.hostname}\n"
            f"before={before.get(gw_b.node.id)}\nafter={after.get(gw_b.node.id)}\n"
            f"{journal}"
        )
    # Harden: unit identity must still match even if no journal hit.
    pre = before[gw_b.node.id]
    post = after[gw_b.node.id]
    if pre["main_pid"] != post["main_pid"] or pre["active_ts"] != post["active_ts"]:
        raise AssertionError(
            f"IBMCEPH-19520 peer {gw_b.node.hostname} unit identity changed "
            f"without abort markers: {pre} -> {post}"
        )
    LOG.info(
        "IBMCEPH-19520 PASSED: peer %s did not abort (MainPID=%s)",
        gw_b.node.hostname,
        post["main_pid"],
    )


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Standalone IBMCEPH-19520 run (deploy optional, cleanup optional).

    Returns:
        0 on success, 1 on failure.
    """
    LOG.info("Starting IBMCEPH-19520 TLS-PSK listener delete abort regression")
    config = kwargs["config"]
    custom_cfg = kwargs.get("test_data", {}).get("custom-config")
    config.update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config.get("rbd_pool", "rbd")},
        }
    )
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_cfg)
    nvme_service = None
    try:
        gw_nodes = config.get("gw_nodes") or []
        if len(gw_nodes) < 2:
            raise ValueError("IBMCEPH-19520 requires config.gw_nodes with >= 2 gateways")
        if not config.get("subsystems"):
            raise ValueError("IBMCEPH-19520 requires config.subsystems")

        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        nvme_service.wait_for_gateways_ready()

        initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
        psk_info = _setup_tlspsk_subsystem(
            ceph_cluster, config, nvme_service, initiator_node, rbd_obj
        )
        test_ceph_tlspsk_listener_delete_abort(
            ceph_cluster, config, nvme_service, psk_info
        )
        return 0
    except Exception as err:
        LOG.exception("IBMCEPH-19520 FAILED: %s", err)
        return 1
    finally:
        if config.get("cleanup") and nvme_service is not None:
            try:
                teardown(nvme_service, rbd_obj)
            except Exception as teardown_err:
                LOG.error("Teardown failed: %s", teardown_err)
