"""
TLS-PSK Key Rotation Tests — TS-03 and TS-03b.

TS-03 (CEPH-XXXXX): Standard PSK rotation.
    - Full doc procedure Phase 1-6 to establish baseline session.
    - Rotate: nvme gen-tls-key again → new KEY_DATA → insert into keyring
      → host add --psk <new_key> on gateway.
    - Verify new TLS session works; FIO passes.

TS-03b (IBMCEPH-18527): keep_connections=True regression during PSK rotation.
    - With state_update_notify=false and a long OMAP poll interval, do
      RemoveHost(keep_connections=True) then AddHost inside one poll window
      (ts03b_del_add_wait << state_update_interval_sec). Buggy gateways reset
      keep_connections on host add; peers replay False and drop the session.
    - Assert: FIO write errors = 0; initiator session stays up; GW2 journal
      shows keep_connections=True (flag not reset by host add).

Procedure reference:
    https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_2GW_1Client.yaml
  node5  — nvmeof-gw (GW1, gRPC receiver)
  node6  — nvmeof-gw (GW2, OMAP replay)
  node10 — client / initiator
"""

import json
import os
import re
import threading
import time

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
    generate_tls_key,
    insert_tls_key,
    load_nvme_tcp_module,
    rotate_psk,
    setup_initiator_tls_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _setup_tlspsk_subsystem(
    ceph_cluster, config, nvme_service, initiator_node, rbd_obj
):
    """Run full doc Phase 2-5 setup and return psk_info."""
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


# ---------------------------------------------------------------------------
# TS-03 — Standard PSK rotation
# ---------------------------------------------------------------------------


def test_ceph_tlspsk_key_rotation(ceph_cluster, config, nvme_service, psk_info):
    """TS-03: Standard TLS-PSK rotation lifecycle.

    Gateway resources (subsystem, namespaces, listeners, host with KEY1) are
    already created by the caller via _setup_tlspsk_subsystem().

    Steps:
        1. Connect --tls with KEY1 → FIO → disconnect.
        2. Rotate PSK:
           a. nvme gen-tls-key (new KEY2 on initiator)
           b. modprobe nvme-tcp
           c. nvme check-tls-key --insert ... --keydata=KEY2
           d. ceph nvmeof host add --psk KEY2  (updates gateway)
        3. Connect with --tls using KEY2 → assert success → FIO → disconnect.
        4. Verify plain (non-TLS) connect still refused after rotation.

    Args:
        psk_info (dict): {"hostnqn": str, "key_data": str} from _setup_tlspsk_subsystem.

    Returns:
        dict: Updated psk_info with new key_data after rotation.

    Polarion: CEPH-XXXXX
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)

    gw = nvme_service.gateways[0]
    gw_ip = gw.node.ip_address
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    gw_nodes = [gw.node for gw in nvme_service.gateways]

    hostnqn = psk_info["hostnqn"]

    LOG.info("TS-03: Connecting with KEY1 for baseline FIO")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="256m", serial=True, io_type="write", test_name="ts03-baseline"
    )
    disconnect_nqn(initiator_node, nqn)

    # Step 2 — rotate PSK (new nvme gen-tls-key + host add --psk)
    LOG.info("TS-03: Rotating PSK — nvme gen-tls-key + host add --psk KEY2")
    # Keep this off the service key path. A later redeploy loads
    # encryption_key_path, and this rotation only needs a new host PSK.
    rotate_psk(
        initiator_node,
        gw,
        nqn,
        hostnqn,
        gw_nodes,
        new_key_path="/root/encryption_host_psk_rotate.key",
        trsvcid=listener_port,
    )

    # Step 3 — connect with KEY2 → FIO
    LOG.info("TS-03: Connecting with KEY2 after rotation")
    connect_with_tls(initiator_node, gw_ip, listener_port, nqn, expect_failure=False)
    initiator.start_fio(
        io_size="256m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts03-after-rotation",
    )
    disconnect_nqn(initiator_node, nqn)

    # Step 4 — plain connect still refused after rotation
    LOG.info("TS-03: Verifying plain connect refused after rotation")
    plain_cmd = (
        f"nvme connect --transport tcp --traddr {gw_ip} "
        f"--trsvcid {listener_port} --nqn {nqn}"
    )
    try:
        initiator_node.exec_command(cmd=plain_cmd, sudo=True)
        raise AssertionError(
            "TS-03: Plain connect succeeded after rotation — TLS enforcement lost"
        )
    except Exception as exc:
        if "AssertionError" in type(exc).__name__:
            raise
        LOG.info("TS-03: Plain connect correctly refused after rotation")

    LOG.info("TS-03 PASSED — TLS-PSK rotation lifecycle OK")


# ---------------------------------------------------------------------------
# TS-03b — keep_connections=True rotation (bug regression)
# ---------------------------------------------------------------------------


def _fio_error_count(fio_results):
    """Extract total error count from FIO result list."""
    total_errors = 0
    for result in fio_results or []:
        try:
            data = json.loads(result) if isinstance(result, str) else result
            for job in data.get("jobs", []):
                total_errors += job.get("error", 0)
        except Exception:
            pass
    return total_errors


def test_ceph_tlspsk_keep_connections_rotation(
    ceph_cluster, config, nvme_service, psk_info
):
    """TS-03b: PSK rotation with keep_connections=True — writes must not fail.

    Regression test for IBMCEPH-18527 Bug 1: host add resets keep_connections
    to False. With state_update_notify=false, if RemoveHost(keep_connections=True)
    is followed by AddHost inside one OMAP poll window, peer GWs replay the
    reset flag and drop the live session / fail writes.

    Suite knobs (must open that window):
      - state_update_notify: false
      - state_update_interval_sec: 60 (OMAP poll)
      - ts03b_del_add_wait: << interval (e.g. 10) so del→add finishes before peers poll

    The gateway uses the PSK only while adding the host, then discards it.
    After RemoveHost the gateway has no saved PSK until the next host add.
    That is expected. The initiator keeps its own key for nvme connect.

    Gateway resources (subsystem, namespaces, listeners, host with KEY1) are
    already created by the caller via _setup_tlspsk_subsystem().

    Steps:
        1. Connect --tls with KEY1; start background FIO write.
        2. RemoveHost(force=True, keep_connections=True) on GW1.
        3. Short wait (<< state_update_interval_sec); gen KEY2; AddHost --psk KEY2.
        4. Assert FIO write errors = 0 throughout (session must survive).
        5. Assert initiator session still alive.
        6. Assert GW2 OMAP replay log shows keep_connections=True (not reset).
        7. Disconnect; reconnect with KEY2 → FIO.

    Args:
        psk_info (dict): {"hostnqn": str, "key_data": str} from _setup_tlspsk_subsystem.

    Polarion: IBMCEPH-18527
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)

    gw1 = nvme_service.gateways[0]
    gw2_node = (
        nvme_service.gateways[1].node
        if len(nvme_service.gateways) > 1
        else nvme_service.gateways[0].node
    )
    # The rebalancer moves a new namespace to another ANA group within a few
    # seconds. nvme connect to the gateway that no longer owns it exits 0 and
    # nvme list stays empty. Connect to every listener so the owning path is up.
    listener_addrs = [gw.node.ip_address for gw in nvme_service.gateways]
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    gw_nodes = [gw.node for gw in nvme_service.gateways]

    def _connect_listeners():
        # The gateway that took host add has the PSK immediately. Peers copy
        # it from OMAP on state_update_interval_sec, so a connect in that
        # window is a stale-key refusal.
        psk_sync_wait = config.get("state_update_interval_sec", 30) + 15
        for addr in listener_addrs:
            connect_with_tls_until(
                initiator_node,
                addr,
                listener_port,
                nqn,
                timeout=psk_sync_wait,
            )

    hostnqn = psk_info["hostnqn"]

    # Do NOT pass ctrl_loss_tmo or reconnect_delay here: if Bug 1 fires and GW2
    # drops the session, we want FIO to see unrecoverable errors immediately.
    # A generous ctrl_loss_tmo would mask the failure by auto-reconnecting.
    LOG.info("TS-03b: Connecting with KEY1 (no ctrl_loss_tmo — drops must be fatal)")
    _connect_listeners()

    # Scenario A — FIO runs in a real background thread so the rotation steps
    # (host del → wait → host add) happen while writes are still in flight.
    #
    # start_fio(serial=False) blocks internally (parallel() joins all threads
    # before returning), so we wrap it in a Python thread to make it truly
    # non-blocking from this function's perspective.
    #
    # Runtime: long enough to cover warm-up (5s) + del_add_wait + key-gen +
    # host-add + post-rotation assertions + margin.  Default 360s (6 min).
    fio_runtime = config.get("ts03b_fio_runtime", 360)
    # del_add_wait MUST be less than state_update_interval_sec so RemoveHost
    # (keep_connections=True) and AddHost land in the same OMAP poll on peers.
    # Buggy gateways reset keep_connections on host add; peers then replay False
    # and drop the session. Fixed builds keep the flag until an explicit remove
    # turns it off. Default 10s with suite interval 60s.
    del_add_wait = config.get("ts03b_del_add_wait", 10)

    failures = []  # collect non-fatal assertion failures; raised together at end
    fio_container = {"results": None, "error": None}

    def _run_fio():
        try:
            # Loop inside 1G. --size 100% on the 20G namespace fills the
            # image; the ENOSPC writes keep fio alive past --runtime, so the
            # process is still running after this phase has moved on.
            fio_container["results"] = initiator.start_fio(
                io_size="1G",
                runtime=fio_runtime,
                serial=False,
                io_type="write",
                time_based=True,
                test_name="ts03b-scenario-a",
            )
        except Exception as exc:
            fio_container["error"] = exc

    fio_thread = threading.Thread(target=_run_fio, daemon=True)
    LOG.info(
        "TS-03b: Scenario A — starting background FIO thread (%ds runtime)", fio_runtime
    )
    fio_thread.start()

    # Give FIO a few seconds to start issuing I/O before triggering the rotation.
    time.sleep(5)

    LOG.info("TS-03b: RemoveHost(force=True, keep_connections=True)")
    # The journal is read after FIO, which runs longer than fio_runtime because
    # of SSH and stalled IO. A relative --since from that later moment misses
    # the RemoveHost line. Use the gateway clock from before the delete.
    since_out, _ = gw2_node.exec_command(cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True)
    journal_since = since_out.strip()
    LOG.info("TS-03b: GW2 journal window starts at %s", journal_since)
    gw1.host.delete(
        **{
            "args": {
                "subsystem": nqn,
                "host": hostnqn,
                "force": True,
                "keep-connections": True,
            }
        }
    )

    # The gateway does not retain the PSK after host add. An empty gateway
    # key store between RemoveHost and the next host add is expected. The
    # open session keeps running until the initiator keepalive expires, and
    # the initiator still has the key it passes to nvme connect.

    # Wait briefly between host del and host add — both must finish before
    # peers poll OMAP (del_add_wait << state_update_interval_sec). That is the
    # IBMCEPH-18527 race: host add resets keep_connections; peers replay False.
    interval = config.get("state_update_interval_sec", 60)
    if del_add_wait >= interval:
        LOG.warning(
            "TS-03b: ts03b_del_add_wait (%ds) >= state_update_interval_sec (%ds); "
            "peers may see RemoveHost alone and miss the host-add keep_connections "
            "reset race. Prefer del_add_wait << interval.",
            del_add_wait,
            interval,
        )
    LOG.info(
        "TS-03b: waiting %ds between host del and host add "
        "(must be << state_update_interval_sec=%d)",
        del_add_wait,
        interval,
    )
    time.sleep(del_add_wait)

    # Generate new PSK and add host with new key
    LOG.info("TS-03b: nvme gen-tls-key (new KEY2)")
    new_key_data = generate_tls_key(initiator_node)
    load_nvme_tcp_module(initiator_node)
    insert_tls_key(initiator_node, hostnqn, nqn, new_key_data, identity=0)

    LOG.info("TS-03b: host add --psk KEY2")
    add_host_with_psk(gw1, nqn, hostnqn, new_key_data)

    # Wait for FIO to finish, then check errors.
    LOG.info("TS-03b: waiting for FIO thread to complete")
    # Wall clock exceeds --runtime: the 360s job in the last run took 465s.
    fio_thread.join(timeout=fio_runtime + 180)
    if fio_thread.is_alive():
        LOG.warning(
            "TS-03b: FIO still running after %ds — stopping it before continuing",
            fio_runtime + 180,
        )
        try:
            initiator.stop_fio()
        except Exception as exc:
            LOG.warning("TS-03b: stop_fio failed: %s", exc)
        fio_thread.join(timeout=60)
        msg = (
            "TS-03b Scenario A FAILED: FIO still running after "
            f"{fio_runtime + 180}s"
        )
        LOG.warning("TS-03b: %s — recording and continuing", msg)
        failures.append(msg)
    elif fio_container["error"]:
        msg = f"TS-03b Scenario A FAILED: FIO thread raised {fio_container['error']}"
        LOG.warning("TS-03b: %s — recording and continuing", msg)
        failures.append(msg)
    else:
        fio_error_count = _fio_error_count(fio_container["results"])
        if fio_error_count > 0:
            msg = (
                f"TS-03b Scenario A FAILED: FIO write errors={fio_error_count} during "
                "keep_connections=True rotation. "
                "Confirms Bug 1 (OMAP replay drops the session) is present."
            )
            LOG.warning("TS-03b: %s — recording and continuing", msg)
            failures.append(msg)
        else:
            LOG.info("TS-03b: Scenario A — FIO write errors = 0 (correct)")

    # Assert session still alive.
    # nvme list --output-format json returns device-level objects (DevicePath,
    # NameSpace, ModelNumber …) — the NQN does NOT appear in those entries.
    # A non-empty Devices list is sufficient proof: the only NVMe device on
    # this initiator is the one we connected, so any device = session alive.
    out, _ = initiator.list(**{"output-format": "json"})
    devices = (json.loads(out) or {}).get("Devices", [])
    if not devices:
        msg = f"TS-03b: Session for {nqn} terminated after RemoveHost(keep_connections=True)"
        LOG.warning("TS-03b: %s — recording and continuing", msg)
        failures.append(msg)
    else:
        LOG.info(
            "TS-03b: Scenario A — initiator session still alive (%d device(s) visible)",
            len(devices),
        )

    # Scenario C — OMAP replay log check on GW2
    # Hardened: absence of keep_connections=True in GW2 log is a hard failure
    # (was a LOG.warning before, which meant Bug 1 silently passed the test).
    LOG.info("TS-03b: Scenario C — GW2 OMAP replay log check")
    log_cmd = (
        f"journalctl -u 'ceph-*nvmeof*' --since '{journal_since}' "
        "--no-pager -q 2>/dev/null || true"
    )

    def _save_gw_log(node, log_content, label):
        """Write gateway journalctl output to the cephci run log directory."""
        log_dir = LOG.log_dir
        if log_dir and os.path.isdir(log_dir):
            log_file = os.path.join(
                log_dir, f"nvmeof_gw_{node.hostname}_ts03b_{label}.log"
            )
            try:
                with open(log_file, "w") as fh:
                    fh.write(log_content)
                LOG.info("TS-03b: GW %s journal saved to %s", node.hostname, log_file)
            except Exception as save_exc:
                LOG.warning(
                    "TS-03b: could not save GW log to %s: %s", log_file, save_exc
                )
        else:
            LOG.warning(
                "TS-03b: log_dir not set or missing, GW journal not saved to disk"
            )

    try:
        log_out, _ = gw2_node.exec_command(cmd=log_cmd, sudo=True)
        _save_gw_log(gw2_node, log_out, "scenario_c")
        # Bug 1 detection — GW2 MUST have logged keep_connections=True when it
        # replayed the RemoveHost event.  If the flag was silently overwritten to
        # False (the bug), no such log line exists → hard failure, not a warning.
        if not re.search(r"keep.connections.*[Tt]rue|keep_connections.*true", log_out):
            msg = (
                "TS-03b Scenario C FAILED: keep_connections=True not found in GW2 "
                "journal — Bug 1 (OMAP replay overwrites keep_connections to False) "
                "is confirmed. Check the saved GW2 log for details."
            )
            LOG.warning("TS-03b: %s — recording and continuing", msg)
            failures.append(msg)
        # Secondary check: GW2 must not have logged a session termination for the host.
        elif re.search(
            rf"remov.*host.*{re.escape(hostnqn[:30])}.*terminat|"
            rf"disconnect.*{re.escape(hostnqn[:30])}",
            log_out,
            re.IGNORECASE,
        ):
            msg = (
                "TS-03b Scenario C FAILED: GW2 log shows session termination "
                "during keep_connections=True (Bug 1 confirmed)"
            )
            LOG.warning("TS-03b: %s — recording and continuing", msg)
            failures.append(msg)
        else:
            LOG.info(
                "TS-03b: Scenario C PASSED — GW2 logged keep_connections=True, no termination"
            )
    except Exception as exc:
        LOG.warning("TS-03b: Scenario C log check skipped: %s", exc)

    # Collect GW journals for audit of the keep_connections replay.
    LOG.info("TS-03b: collecting GW journals for audit")
    for gw_node in gw_nodes:
        try:
            d_out, _ = gw_node.exec_command(cmd=log_cmd, sudo=True)
            _save_gw_log(gw_node, d_out, "scenario_d")
        except Exception:
            pass
    LOG.info("TS-03b: GW journals collected")

    # Scenario B — disconnect; connect with KEY2
    LOG.info("TS-03b: Scenario B — disconnecting; reconnecting with KEY2")
    disconnect_nqn(initiator_node, nqn)
    _connect_listeners()
    initiator.start_fio(
        io_size="256m",
        serial=True,
        io_type="write",
        execute_blkdiscard=False,
        test_name="ts03b-after-rotation",
    )
    disconnect_nqn(initiator_node, nqn)

    if failures:
        raise AssertionError(
            f"TS-03b FAILED — {len(failures)} assertion(s):\n"
            + "\n".join(f"  [{i+1}] {f}" for i, f in enumerate(failures))
        )
    LOG.info(
        "TS-03b PASSED — keep_connections=True rotation: no write failures, "
        "OMAP replay correct"
    )


# ---------------------------------------------------------------------------
# Gateway log collection helper
# ---------------------------------------------------------------------------


def _collect_gw_logs(nvme_service, label="final"):
    """Collect journalctl output from all gateway nodes and save to the run log dir.

    Called unconditionally from run()'s finally block so logs are always
    captured whether the test passed or failed.

    Args:
        nvme_service (NVMeService): deployed service; may be None if deploy failed.
        label (str): filename suffix to distinguish collection points.
    """
    if nvme_service is None:
        LOG.warning("_collect_gw_logs: nvme_service not initialised — skipping")
        return

    log_cmd = (
        "journalctl -u 'ceph-*nvmeof*' --since '1 hour ago' "
        "--no-pager -q 2>/dev/null || true"
    )
    log_dir = getattr(LOG, "log_dir", None)

    for gw in getattr(nvme_service, "gateways", []):
        node = gw.node
        try:
            log_out, _ = node.exec_command(cmd=log_cmd, sudo=True)
        except Exception as exc:
            LOG.warning(
                "_collect_gw_logs: could not fetch journal from %s: %s",
                node.hostname,
                exc,
            )
            continue

        if log_dir and os.path.isdir(log_dir):
            log_file = os.path.join(log_dir, f"nvmeof_gw_{node.hostname}_{label}.log")
            try:
                with open(log_file, "w") as fh:
                    fh.write(log_out)
                LOG.info(
                    "_collect_gw_logs: saved %s journal to %s",
                    node.hostname,
                    log_file,
                )
            except Exception as save_exc:
                LOG.warning(
                    "_collect_gw_logs: could not write log file %s: %s",
                    log_file,
                    save_exc,
                )
        else:
            LOG.info(
                "_collect_gw_logs: %s journal (no log_dir):\n%s",
                node.hostname,
                log_out[:2000],
            )


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci test runner.

    Config keys (from suite YAML):
        install (bool): Deploy a fresh NVMeService.
        tls_psk (bool): Enable TLS-PSK (enable_encryption: True in orch spec).
        rbd_pool (str): RBD pool name.
        gw_group (str): Gateway group name.
        gw_nodes (list[str]): Gateway node IDs.
        listener_port (int): NVMe-oF listener port (default 4420).
        initiator_node (str): Node ID for the initiator/client.
        subsystems (list): Subsystem configs (must include secure_listener: true).
        run_ts03b (bool): When True run only TS-03b (keep_connections regression);
                          when False (default) run only TS-03 (standard rotation).
                          TS-03 and TS-03b are mutually exclusive — running both
                          sequentially would require tearing down and recreating the
                          subsystem between them.
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
        # Both TS-03 and TS-03b receive the same psk_info; neither creates resources.
        psk_info = _setup_tlspsk_subsystem(
            ceph_cluster, config, nvme_service, initiator_node, rbd_obj
        )

        polarion_id = config.get("polarion-id", "")
        run_ts03b = config.get("run_ts03b", False) or "TS03b" in polarion_id

        if run_ts03b:
            # TS-03b: keep_connections=True regression — standalone scenario
            test_ceph_tlspsk_keep_connections_rotation(
                ceph_cluster, config, nvme_service, psk_info
            )
        else:
            # TS-03: standard PSK rotation lifecycle
            test_ceph_tlspsk_key_rotation(ceph_cluster, config, nvme_service, psk_info)

        return 0

    except Exception as err:
        LOG.exception("TLS-PSK key rotation test FAILED: %s", err)
        return 1

    finally:
        _collect_gw_logs(nvme_service, label="test_end")
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
