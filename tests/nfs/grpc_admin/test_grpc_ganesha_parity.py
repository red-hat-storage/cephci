"""
D-Bus vs gRPC parity tests for NFS-Ganesha admin APIs.

Phase 0: capture gRPC (and optional D-Bus) responses into ``golden/``.
Phase 1+: compare live responses against captured baselines.

D-Bus capture uses busctl inside the NFS container when available.
"""

import json
import os
from time import sleep

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    check_blocked_operation,
    ensure_subvolume_group,
    get_client_ids,
    get_grace_period,
    get_session_ids,
    grpc_invoke_kwargs,
    install_grpcurl,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    setup_grpc_transport,
)
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")

# D-Bus methods used for parity when D-Bus is still enabled in the build.
DBUS_CALLS = {
    "show_clients": (
        "org.ganesha.nfsd",
        "/org/ganesha/nfsd/ClientMgr",
        "org.ganesha.nfsd.clientmgr",
        "ShowClients",
    ),
    "show_exports": (
        "org.ganesha.nfsd",
        "/org/ganesha/nfsd/ExportMgr",
        "org.ganesha.nfsd.exportmgr",
        "ShowExports",
    ),
}


def _golden_path(name):
    return os.path.join(GOLDEN_DIR, f"{name}.json")


def _load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _save_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    log.info("Wrote golden file: %s", path)


def _get_nfs_container_id(installer, nfs_name):
    out = CephAdm(installer).ceph.orch.ps(service_name=f"nfs.{nfs_name}", format="json")
    processes = json.loads(out) if out else []
    if not processes:
        return None
    return processes[0].get("container_id")


def _dbus_call_in_container(nfs_node, container_id, bus_name, path, iface, method):
    """Run busctl inside the NFS container; return stdout or empty string."""
    if not container_id:
        return ""
    cmd = (
        f"podman exec {container_id} busctl call "
        f"{bus_name} {path} {iface} {method} 2>/dev/null"
    )
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return out.strip()


def _capture_grpc_snapshot(client, target, snapshot_name, kw_tls):
    """Capture implemented gRPC RPC responses for *snapshot_name*."""
    snapshot = {"snapshot": snapshot_name, "grpc": {}}
    ok, ingrace, raw = get_grace_period(client, target, **kw_tls)
    snapshot["grpc"]["get_grace_period"] = {
        "ok": ok,
        "ingrace": ingrace,
        "raw": raw,
    }
    ok, client_ids, raw = get_client_ids(client, target, **kw_tls)
    snapshot["grpc"]["get_client_ids"] = {
        "ok": ok,
        "client_ids": client_ids,
        "raw": raw,
    }
    ok, session_ids, raw = get_session_ids(client, target, **kw_tls)
    snapshot["grpc"]["get_session_ids"] = {
        "ok": ok,
        "session_ids": session_ids,
        "raw": raw,
    }
    return snapshot


def _capture_dbus_snapshot(installer, nfs_node, nfs_name, keys):
    """Capture selected D-Bus admin responses from the NFS container."""
    container_id = _get_nfs_container_id(installer, nfs_name)
    snapshot = {"dbus": {}, "container_id": container_id}
    for key in keys:
        if key not in DBUS_CALLS:
            continue
        bus_name, path, iface, method = DBUS_CALLS[key]
        snapshot["dbus"][key] = _dbus_call_in_container(
            nfs_node, container_id, bus_name, path, iface, method
        )
    return snapshot


def _compare_snapshots(baseline, current, keys):
    """Return list of human-readable diffs for *keys* under grpc/dbus sections."""
    diffs = []
    for section in ("grpc", "dbus"):
        for key in keys:
            base_val = baseline.get(section, {}).get(key)
            cur_val = current.get(section, {}).get(key)
            if base_val != cur_val:
                diffs.append(
                    f"{section}.{key}: baseline={base_val!r} current={cur_val!r}"
                )
    return diffs


def run(ceph_cluster, **kw):
    """
    Parity operations (``operation`` config key):

        capture_grpc_baseline — save gRPC RPC snapshot to golden/
        capture_dbus_baseline — save D-Bus ShowClients/ShowExports to golden/
        capture_full_baseline — both gRPC and D-Bus into one golden file
        compare_grpc_baseline — compare live gRPC vs golden (regression)
        compare_full_baseline — compare gRPC + D-Bus vs golden
        compare_admin_parity — AdminService D-Bus vs gRPC (blocked until proto)
        compare_log_parity — LogService parity (blocked until proto)
        compare_stats_parity — StatsService parity (blocked until proto)
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    golden_name = config.get("golden_name", "grpc_admin_baseline")
    golden_path = _golden_path(golden_name)
    compare_keys = config.get(
        "compare_keys",
        ["get_grace_period", "get_client_ids", "get_session_ids"],
    )
    dbus_keys = config.get("dbus_keys", ["show_clients", "show_exports"])

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    installers = ceph_cluster.get_nodes("installer")
    installer = installers[0] if installers else client
    target = resolve_grpc_target(config, nfs_ip)

    try:
        install_grpcurl(client)
        open_grpc_firewall(nfs_node)
        ensure_subvolume_group(client, fs_name, ceph_cluster=ceph_cluster)

        setup_nfs_cluster(
            clients=[client],
            nfs_server=nfs_server,
            port=nfs_port,
            version=nfs_version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs,
            ceph_cluster=ceph_cluster,
        )
        sleep(int(config.get("settle_sleep", 5)))
        transport = setup_grpc_transport(config, [client], installer, nfs_name)
        kw_tls = grpc_invoke_kwargs(transport)

        if operation == "capture_grpc_baseline":
            snapshot = _capture_grpc_snapshot(client, target, golden_name, kw_tls)
            _save_json(golden_path, snapshot)

        elif operation == "capture_dbus_baseline":
            snapshot = {
                "snapshot": golden_name,
                **_capture_dbus_snapshot(installer, nfs_node, nfs_name, dbus_keys),
            }
            _save_json(golden_path, snapshot)

        elif operation == "capture_full_baseline":
            snapshot = _capture_grpc_snapshot(client, target, golden_name, kw_tls)
            snapshot.update(
                _capture_dbus_snapshot(installer, nfs_node, nfs_name, dbus_keys)
            )
            _save_json(golden_path, snapshot)

        elif operation in (
            "compare_admin_parity",
            "compare_log_parity",
            "compare_stats_parity",
        ):
            blocked_op = {
                "compare_admin_parity": "shutdown",
                "compare_log_parity": "set_log_level",
                "compare_stats_parity": "show_exports",
            }[operation]
            skip_rc = check_blocked_operation(blocked_op, config)
            if skip_rc is not None:
                return skip_rc
            raise ConfigError(
                f"{operation} requires Admin/Log/Stats gRPC proto in nfs-ganesha"
            )

        elif operation in ("compare_grpc_baseline", "compare_full_baseline"):
            if not os.path.isfile(golden_path):
                raise OperationFailedError(
                    f"Golden file missing: {golden_path}. Run capture first."
                )
            baseline = _load_json(golden_path)
            current = _capture_grpc_snapshot(client, target, golden_name, kw_tls)
            if operation == "compare_full_baseline":
                current.update(
                    _capture_dbus_snapshot(installer, nfs_node, nfs_name, dbus_keys)
                )
            diffs = _compare_snapshots(baseline, current, compare_keys)
            if "show_clients" in dbus_keys or "show_exports" in dbus_keys:
                diffs.extend(_compare_snapshots(baseline, current, dbus_keys))
            if diffs:
                for diff in diffs:
                    log.error("Parity diff: %s", diff)
                raise OperationFailedError(
                    f"Parity check failed with {len(diffs)} difference(s)"
                )
            log.info("Parity check passed against %s", golden_path)

        else:
            raise ConfigError(f"Unknown parity operation: {operation}")

        log.info("TEST PASSED - gRPC parity operation=%s", operation)
        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Parity test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Parity test failed: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
