"""
IBMCEPH-17197: host del while connected must not crash peer gateways.

Precondition: initiator is TCP-connected to a subsystem across multiple GWs.
Steps:
  1. namespace del (optional, mirrors bug repro)
  2. host del while host is still connected
  3. Peer GWs syncing from OMAP must not abort/restart on gRPC error 16

Key assertion: peer gateway units keep the same MainPID / ActiveEnterTimestamp
and journals must not show OMAP-sync abort markers for gRPC error 16.
"""

import json
import time

from ceph.ceph import Ceph, CommandFailed
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

# Peer abort path for IBMCEPH-17197 (OMAP sync on error 16). Keep markers specific
# to avoid false positives on unrelated "aborting" log lines.
ABORT_MARKERS = (
    "Got error 16 while updating gateway state",
    "SystemExit: Got error 16",
)

# Primary host-del may warn while still applying OMAP; align with CEPH-83575455.
EXPECTED_HOST_DEL_MARKERS = (
    "still connected",
    "Reconnecting the host would fail",
)

POLL_INTERVAL_SEC = 5


def _gw_unit_identity(gateway):
    """Return MainPID / ActiveEnterTimestampMonotonic / ActiveState for the NVMeoF unit."""
    unit = gateway.system_unit_id
    if not unit:
        raise Exception(f"Empty system_unit_id for gateway on {gateway.node.hostname}")
    out, _ = gateway.node.exec_command(
        sudo=True,
        cmd=(
            f"systemctl show {unit} "
            "-p MainPID -p ActiveEnterTimestampMonotonic -p ActiveState"
        ),
    )
    props = {}
    for line in (out or "").strip().splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()

    main_pid = props.get("MainPID", "")
    active_ts = props.get("ActiveEnterTimestampMonotonic", "")
    active_state = props.get("ActiveState", "")
    identity = {
        "unit": unit,
        "main_pid": main_pid,
        "active_ts": active_ts,
        "active_state": active_state,
        "hostname": gateway.node.hostname,
    }
    # Empty or zero PID would make before/after equality a false pass.
    if not main_pid or main_pid == "0":
        raise Exception(
            f"Invalid MainPID={main_pid!r} for {identity['hostname']} unit={unit}"
        )
    if not active_ts or active_ts == "0":
        raise Exception(
            f"Invalid ActiveEnterTimestampMonotonic={active_ts!r} for "
            f"{identity['hostname']} unit={unit}"
        )
    if active_state != "active":
        raise Exception(
            f"Gateway {identity['hostname']} unit={unit} ActiveState={active_state!r} "
            f"(expected active) before peer-stability check"
        )
    return identity


def _snapshot_gateways(gateways):
    return {gw.node.id: _gw_unit_identity(gw) for gw in gateways}


def _assert_peers_did_not_restart(before, after, primary_node_id):
    """Fail if any non-primary gateway restarted or went inactive."""
    failures = []
    peer_count = 0
    for node_id, pre in before.items():
        if node_id == primary_node_id:
            continue
        peer_count += 1
        post = after.get(node_id)
        if not post:
            failures.append(f"Missing post-check identity for peer {node_id}")
            continue
        if pre["unit"] != post["unit"]:
            failures.append(
                f"Peer {post['hostname']} unit changed "
                f"{pre['unit']!r} -> {post['unit']!r}"
            )
        if post["active_state"] != "active":
            failures.append(
                f"Peer {post['hostname']} ActiveState={post['active_state']!r} "
                f"(expected active)"
            )
        if pre["main_pid"] != post["main_pid"]:
            failures.append(
                f"Peer {post['hostname']} MainPID changed "
                f"{pre['main_pid']} -> {post['main_pid']} (gateway restarted)"
            )
        if pre["active_ts"] != post["active_ts"]:
            failures.append(
                f"Peer {post['hostname']} ActiveEnterTimestampMonotonic changed "
                f"{pre['active_ts']} -> {post['active_ts']} (gateway restarted)"
            )
    if peer_count < 1:
        failures.append("No peer gateways found to validate (need >= 2 gateways)")
    if failures:
        raise Exception(
            "IBMCEPH-17197 peer gateway restart detected:\n- " + "\n- ".join(failures)
        )


def _scan_peer_journals(gateways, primary_node_id, since_seconds=180):
    """Fail if peer journals show OMAP-sync abort markers from the bug."""
    hits = []
    grep_expr = "|".join(ABORT_MARKERS)
    for gw in gateways:
        if gw.node.id == primary_node_id:
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
    if hits:
        raise Exception(
            "IBMCEPH-17197 abort markers found in peer gateway journals:\n"
            + "\n".join(hits)
        )


def _is_expected_host_del_error(message):
    """Return True if primary host-del failed with the known connected-host warning."""
    msg = message or ""
    msg_lower = msg.lower()
    if "still connected" in msg_lower:
        return True
    return any(marker in msg for marker in EXPECTED_HOST_DEL_MARKERS)


def _connected_path_traddrs(initiator, device):
    """Return set of live path traddrs for an NVMe device via list-subsys."""
    out, _ = initiator.list_subsys(**{"device": device, "output-format": "json"})
    data = json.loads(out)
    # nvme-cli may return a list of controllers or a single object
    entries = data if isinstance(data, list) else [data]
    traddrs = set()
    for entry in entries:
        for subsys in entry.get("Subsystems", []) or []:
            for path in subsys.get("Paths", []) or []:
                state = (path.get("State") or "").lower()
                if state and state not in ("live", "connecting"):
                    continue
                address = path.get("Address") or ""
                if "traddr=" not in address:
                    continue
                traddr = address.split("traddr=")[1].split(",")[0].strip()
                if traddr:
                    traddrs.add(traddr)
    return traddrs


def _assert_connected_across_gateways(initiator, gateways, devices):
    """Fail if initiator is not live-connected to more than one gateway IP."""
    expected_ips = {gw.node.ip_address for gw in gateways}
    if len(expected_ips) < 2:
        raise ValueError("IBMCEPH-17197 requires at least two distinct gateway IPs")

    seen = set()
    for device in devices:
        try:
            seen |= _connected_path_traddrs(initiator, device)
        except Exception as err:
            LOG.warning("list-subsys failed for %s: %s", device, err)

    missing = expected_ips - seen
    if len(seen & expected_ips) < 2:
        raise Exception(
            "IBMCEPH-17197 precondition failed: initiator not connected across "
            f"multiple gateways. expected={sorted(expected_ips)} "
            f"seen_paths={sorted(seen)} missing={sorted(missing)}"
        )
    LOG.info(
        "Initiator connected across gateways: %s",
        sorted(seen & expected_ips),
    )


def _wait_and_assert_peers_stable(gateways, before, primary_node_id, sync_wait):
    """Poll peer identity during OMAP sync window; fail fast on restart."""
    deadline = time.time() + sync_wait
    LOG.info(
        "Polling peer gateway stability for %ss (interval=%ss)",
        sync_wait,
        POLL_INTERVAL_SEC,
    )
    after = None
    while True:
        after = _snapshot_gateways(gateways)
        _assert_peers_did_not_restart(before, after, primary_node_id)
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SEC, remaining))
    return after


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Automate IBMCEPH-17197 host-del-while-connected peer stability.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: suite config (gw_nodes >= 2, subsystems, initiators, cleanup)

    Returns:
        0 on success, 1 on failure
    """
    LOG.info("Starting IBMCEPH-17197: host del while connected, peer GW stability")
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    try:
        gw_nodes = config.get("gw_nodes") or [config.get("gw_node")]
        if not isinstance(gw_nodes, list) or len(gw_nodes) < 2:
            raise ValueError(
                "IBMCEPH-17197 requires config.gw_nodes with at least 2 gateways"
            )

        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        primary = nvme_service.gateways[0]
        primary_id = primary.node.id
        subsystems = [sub["nqn"] for sub in config["subsystems"] if sub.get("nqn")]
        if not subsystems:
            raise ValueError("IBMCEPH-17197 requires at least one subsystem nqn")

        # Prefer suite initiators; if nqn is connect-all, expand to each subsystem
        initiator_cfgs = []
        for initiator_cfg in config.get("initiators", []):
            nqn = initiator_cfg.get("nqn")
            if nqn in (None, "connect-all", "discover-all"):
                for sub_nqn in subsystems:
                    cfg = dict(initiator_cfg)
                    cfg["nqn"] = sub_nqn
                    initiator_cfgs.append(cfg)
            else:
                initiator_cfgs.append(initiator_cfg)
        if not initiator_cfgs:
            raise ValueError("IBMCEPH-17197 requires config.initiators")

        client = get_node_by_id(ceph_cluster, initiator_cfgs[0]["node"])
        initiator = NVMeInitiator(client)
        host_nqn = initiator.initiator_nqn()

        # Connect each subsystem individually (nvme connect -n <subnqn>)
        for initiator_cfg in initiator_cfgs:
            LOG.info(
                "Connecting initiator on %s to subsystem %s",
                initiator_cfg["node"],
                initiator_cfg["nqn"],
            )
            initiator.connect_targets(primary, initiator_cfg)

        devices = initiator.list_devices()
        LOG.info("Initiator connected with devices: %s", devices)
        _assert_connected_across_gateways(initiator, nvme_service.gateways, devices)

        before = _snapshot_gateways(nvme_service.gateways)
        LOG.info("Gateway identities before host del: %s", before)

        # Bug repro: remove namespace first (optional but matches reported steps)
        if config.get("delete_namespace_first", True):
            for subsystem in subsystems:
                try:
                    out, _ = primary.namespace.list(
                        **{
                            "base_cmd_args": {"format": "json"},
                            "args": {"subsystem": subsystem},
                        }
                    )
                    try:
                        namespaces = (
                            json.loads(out).get("namespaces", []) if out else []
                        )
                    except (TypeError, ValueError, json.JSONDecodeError) as parse_err:
                        LOG.warning(
                            "Failed to parse namespace list for %s: %s",
                            subsystem,
                            parse_err,
                        )
                        continue
                    if not namespaces:
                        LOG.warning("No namespaces found on %s to delete", subsystem)
                        continue
                    nsid = namespaces[0]["nsid"]
                    primary.namespace.delete(
                        **{"args": {"subsystem": subsystem, "nsid": nsid}}
                    )
                    LOG.info(
                        "namespace del subsystem=%s nsid=%s completed",
                        subsystem,
                        nsid,
                    )
                except CommandFailed as err:
                    # Continue — host del is the critical step for peer abort
                    LOG.warning("namespace del on %s returned: %s", subsystem, err)

        # Host del while still connected — primary may warn but must update OMAP
        for subsystem in subsystems:
            host_args = {"args": {"subsystem": subsystem, "host": host_nqn}}
            if config.get("host_del_force"):
                host_args["args"]["force"] = True
            try:
                primary.host.delete(**host_args)
                LOG.info("host del completed without exception for %s", subsystem)
            except Exception as host_del_err:
                msg = str(host_del_err)
                if _is_expected_host_del_error(msg):
                    LOG.info(
                        "host del on %s returned expected connected-host warning: %s",
                        subsystem,
                        msg,
                    )
                else:
                    raise

        sync_wait = int(config.get("peer_sync_wait_sec", 60))
        after = _wait_and_assert_peers_stable(
            nvme_service.gateways, before, primary_id, sync_wait
        )
        LOG.info("Gateway identities after host del: %s", after)
        _scan_peer_journals(
            nvme_service.gateways,
            primary_id,
            since_seconds=sync_wait + 60,
        )

        LOG.info("IBMCEPH-17197 validation successful: peers did not restart")
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup") and nvme_service is not None:
            try:
                teardown(nvme_service, rbd_obj)
            except Exception as teardown_err:
                LOG.error("Teardown failed: %s", teardown_err)
