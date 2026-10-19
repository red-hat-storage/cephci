"""
Shared gRPC client helpers for NFS-Ganesha API tests.

Wraps grpcurl installation, plaintext/TLS invocation, and RPC helpers for
services defined in nfs-ganesha next/src/grpc_server/proto:

  nfsService        — GetClientId, GetNfsGrace, StartNfsGrace, GetSessionId,
                      nfsAdmin, ShowIdMapperService
  exportService     — ExportService, ExportStats
  cltmgrService     — ClientMgr, ClientStats
"""

import json
import re
from time import sleep

from looseversion import LooseVersion

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import get_ceph_version
from utility.log import Log

log = Log(__name__)

DEFAULT_GRPC_PORT = 50051
DEFAULT_GRPC_UDS_PATH = "/var/run/ganesha/ganesha.grpc.sock"

DEFAULT_CERT_PATHS = {
    "ca_cert": "/etc/ganesha/certs/ca.crt",
    "client_cert": "/etc/ganesha/certs/client.crt",
    "client_key": "/etc/ganesha/certs/client.key",
    "server_cert": "/etc/ganesha/certs/server.crt",
    "server_key": "/etc/ganesha/certs/server.key",
}

# Ceph mgr NFS gRPC client certs (same paths as tests/nfs/test_nfs_grpc.py).
GRPC_CERT_DIR_CLIENT = "/root/nfs_grpc_certs"
GRPC_CERT_FILES = ("ca.crt", "client.crt", "client.key")
GRPC_CERT_COPY_RETRIES = 6
GRPC_CERT_COPY_DELAY_SEC = 5
GRPC_MTLS_MIN_VERSION = "20.2.2"

# Packages / services expected when USE_GRPC=ON image includes full proto set.
EXPECTED_GRPC_SERVICES = [
    "nfsService.GetClientId",
    "nfsService.GetNfsGrace",
    "nfsService.GetSessionId",
    "nfsService.StartNfsGrace",
    "nfsService.nfsAdmin",
    "nfsService.ShowIdMapperService",
    "exportService.ExportService",
    "exportService.ExportStats",
    "cltmgrService.ClientMgr",
    "cltmgrService.ClientStats",
    "grpc.reflection.v1alpha.ServerReflection",
]

# Only Log D-Bus APIs remain without a gRPC proto on upstream next.
BLOCKED_OPERATIONS = frozenset(
    {
        "set_log_level",
        "get_log_level",
        "compare_log_parity",
    }
)


def install_grpcurl(node):
    """Install grpcurl on *node* if not already present.

    Detects the node architecture at runtime and downloads the matching
    grpcurl binary (arm64 for aarch64, x86_64 otherwise).
    """
    log.info("Installing grpcurl on %s", node.hostname)
    out, _ = node.exec_command(cmd="which grpcurl", check_ec=False)
    if "grpcurl" in (out or ""):
        log.info("grpcurl already installed on %s", node.hostname)
        return

    arch_out, _ = node.exec_command(cmd="uname -m", check_ec=False)
    arch = (arch_out or "").strip()
    if arch == "aarch64":
        tarball = "grpcurl_1.8.9_linux_arm64.tar.gz"
    else:
        tarball = "grpcurl_1.8.9_linux_x86_64.tar.gz"

    log.info(f"Detected arch={arch!r}, downloading {tarball}")
    wget_cmd = (
        "curl -LO "
        f"https://github.com/fullstorydev/grpcurl/releases/download/v1.8.9/{tarball}"
    )
    install_cmd = (
        f"{wget_cmd} && tar -xvzf {tarball} && "
        "chmod +x grpcurl && mv grpcurl /usr/local/bin/"
    )
    node.exec_command(sudo=True, cmd=install_cmd)
    out, _ = node.exec_command(cmd="grpcurl --version", check_ec=False)
    log.info("grpcurl version on %s: %s", node.hostname, (out or "").strip())


def open_grpc_firewall(nfs_node, port=DEFAULT_GRPC_PORT):
    """Open *port*/tcp on the NFS node firewall (best-effort)."""
    cmd = f"firewall-cmd --permanent --add-port={port}/tcp"
    nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)


def build_grpcurl_target(host, port=DEFAULT_GRPC_PORT):
    """Return host:port target string for grpcurl."""
    return f"{host}:{port}"


def build_grpcurl_cmd(
    target,
    method,
    data=None,
    plaintext=True,
    tls_paths=None,
):
    """
    Build a grpcurl shell command.

    Args:
        target: host:port
        method: full method e.g. nfsService.GetClientId/GetClientIds
        data: optional JSON request body
        plaintext: use -plaintext when True
        tls_paths: dict with ca_cert, client_cert, client_key for mTLS
    """
    parts = ["grpcurl"]
    if plaintext:
        parts.append("-plaintext")
    elif tls_paths:
        parts.extend(
            [
                f"-cacert {tls_paths['ca_cert']}",
                f"-cert {tls_paths['client_cert']}",
                f"-key {tls_paths['client_key']}",
            ]
        )
    if data is not None:
        parts.append(f"-d '{data}'")
    parts.append(target)
    parts.append(method)
    return " ".join(parts)


# Transport / TLS failures typically land on stderr (same idea as test_nfs_grpc.py).
GRPCURL_STDERR_FAILURE_MARKERS = (
    "failed to dial",
    "context deadline exceeded",
    "connection refused",
    "connection reset",
    "no such host",
    "x509",
    "tls: ",
    "handshake",
    "bad certificate",
    "certificate required",
    "remote error",
    "authentication handshake failed",
)


def grpcurl_exec(node, cmd):
    """Run grpcurl *cmd* on *node*; return (stdout, stderr, success)."""
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    out = out or ""
    err = err or ""
    if int(getattr(node, "exit_status", 0)) != 0:
        log.error(
            "grpcurl failed rc=%s stderr=%r stdout=%r",
            node.exit_status,
            err,
            out,
        )
        return out, err, False

    err_l = err.lower()
    combined = f"{out}\n{err}".lower()

    for marker in GRPCURL_STDERR_FAILURE_MARKERS:
        if marker in err_l:
            log.error("grpcurl transport/TLS failure (%r): stderr=%r", marker, err)
            return out, err, False

    # Client-side grpcurl failures (e.g. malformed -d JSON) before dial/RPC.
    for marker in (
        "error invoking method",
        "error getting request data",
        "invalid character",
        "failed to unmarshal",
    ):
        if marker in combined:
            log.error("grpcurl client failure (%r): %s", marker, err or out)
            return out, err, False

    failed = "rpc error" in combined or (err and "error" in err_l and "desc" in err_l)
    return out, err, not failed


def list_grpc_services(client_node, target, plaintext=True, tls_paths=None):
    """List gRPC services on *target*. Empty discovery is treated as failure."""
    cmd = build_grpcurl_cmd(target, "list", plaintext=plaintext, tls_paths=tls_paths)
    out, err, ok = grpcurl_exec(client_node, cmd)
    if not ok:
        log.error("Failed to list gRPC services: %s", err or out)
        return False, []
    services = [line.strip() for line in out.strip().split("\n") if line.strip()]
    log.info("Discovered gRPC services: %s", services)
    if not services:
        log.error(
            "grpcurl list returned no services on %s (stdout=%r stderr=%r)",
            target,
            out,
            err,
        )
        return False, []
    return True, services


def list_grpc_services_uds(nfs_node, socket_path, plaintext=True):
    """List gRPC services via Unix domain socket on the NFS node."""
    parts = ["grpcurl"]
    if plaintext:
        parts.append("-plaintext")
    parts.extend(["-unix", socket_path, "list"])
    cmd = " ".join(parts)
    out, err, ok = grpcurl_exec(nfs_node, cmd)
    if not ok:
        log.error("UDS gRPC discovery failed on %s: %s", socket_path, err or out)
        return False, []
    services = [line.strip() for line in out.strip().split("\n") if line.strip()]
    log.info("UDS gRPC services on %s: %s", socket_path, services)
    if not services:
        return False, []
    return True, services


def invoke_grpc_method(
    client_node,
    target,
    method,
    data=None,
    plaintext=True,
    tls_paths=None,
    max_time_sec=None,
):
    """
    Invoke a gRPC method via grpcurl.

    Returns:
        tuple: (success, stdout, stderr)
    """
    cmd = build_grpcurl_cmd(
        target, method, data=data, plaintext=plaintext, tls_paths=tls_paths
    )
    if max_time_sec is not None:
        cmd = cmd.replace("grpcurl", f"grpcurl -max-time {max_time_sec}", 1)
    out, err, ok = grpcurl_exec(client_node, cmd)
    return ok, out, err


# Client-side grpcurl failures (bad JSON, etc.) before a server RPC response.
GRPCURL_CLIENT_ERROR_MARKERS = (
    "rpc error",
    "code =",
    "error invoking method",
    "error getting request data",
    "invalid character",
    "failed to unmarshal",
    "proto:",
)


def expect_rpc_error(client_node, target, method, data, plaintext=True, tls_paths=None):
    """
    Return True when grpcurl reports a client or RPC error (negative helper).

    Covers server RPC failures and client-side failures such as malformed JSON
    (``error getting request data``) that never reach the gRPC server.
    """
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    combined = f"{out}\n{err}".lower()
    has_error = not ok or any(
        marker in combined for marker in GRPCURL_CLIENT_ERROR_MARKERS
    )
    log.info(
        "RPC negative check ok=%s has_error=%s combined=%s",
        ok,
        has_error,
        combined[:300],
    )
    return has_error


def _service_matches(expected, discovered):
    """
    Return True if *expected* service name is present in *discovered*.

    Matches exact equality only (grpcurl ``list`` returns fully-qualified
    names like ``nfsService.GetClientId``). Does not treat another service
    in the same package as a match.
    """
    return any(svc == expected for svc in discovered)


def verify_expected_services(discovered, require_all=False):
    """
    Validate discovered services against EXPECTED_GRPC_SERVICES.

    By default require all four core nfsService RPCs. When *require_all*
    is True (full gRPC image), every expected service must be present.
    """
    found = [e for e in EXPECTED_GRPC_SERVICES if _service_matches(e, discovered)]
    log.info(
        "Matched expected services (%s/%s): %s",
        len(found),
        len(EXPECTED_GRPC_SERVICES),
        found,
    )
    if require_all:
        missing = [e for e in EXPECTED_GRPC_SERVICES if e not in found]
        if missing:
            log.error("Missing expected gRPC services: %s", missing)
            return False
        return True
    core = (
        "nfsService.GetClientId",
        "nfsService.GetNfsGrace",
        "nfsService.StartNfsGrace",
        "nfsService.GetSessionId",
    )
    missing_core = [c for c in core if c not in found]
    if missing_core:
        log.error("Missing core nfsService RPCs: %s", missing_core)
        return False
    return True


def verify_grpc_port_listening(nfs_node, port=DEFAULT_GRPC_PORT):
    """Return True when ganesha is listening on *port*."""
    cmd = f"ss -tulnp | grep {port}"
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    if str(port) not in out:
        log.error("gRPC port %s is not listening", port)
        return False
    log.info("gRPC port %s listening: %s", port, out.strip())
    return True


def _parse_json(raw):
    try:
        return json.loads(raw) if raw and raw.strip() else {}
    except json.JSONDecodeError:
        return {}


def get_grace_period(client_node, target, plaintext=True, tls_paths=None):
    """Call GetNfsGrace.GetGracePeriod; return (ok, ingrace_or_none, raw)."""
    method = "nfsService.GetNfsGrace/GetGracePeriod"
    cmd = build_grpcurl_cmd(
        target, method, data="{}", plaintext=plaintext, tls_paths=tls_paths
    )
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("GetGracePeriod response: %s", out)
    if not ok:
        return False, None, out or err

    ingrace = None
    payload = _parse_json(out)
    if payload:
        ingrace = payload.get("ingrace", payload.get("inGrace"))
    else:
        match = re.search(r'"?ingrace"?\s*:\s*(true|false)', out, re.IGNORECASE)
        if match:
            ingrace = match.group(1).lower() == "true"
    return True, ingrace, out


def start_grace_with_event(
    client_node,
    target,
    event_id,
    node_id=1,
    ip_addr=None,
    plaintext=True,
    tls_paths=None,
):
    """Call StartNfsGrace.StartGraceWithEvent."""
    host = target.split(":")[0]
    ip_addr = ip_addr or host
    request_data = json.dumps(
        {"Event": int(event_id), "NodeId": int(node_id), "IpAddr": ip_addr}
    )
    method = "nfsService.StartNfsGrace/StartGraceWithEvent"
    cmd = build_grpcurl_cmd(
        target,
        method,
        data=request_data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("StartGraceWithEvent response: %s", out)
    if int(event_id) == 0:
        ok = ok and "graceStarted" in out and "true" in out.lower()
    return ok, out or err


def parse_id_list(raw, json_keys, regex_pattern):
    """Parse repeated ID fields from grpcurl JSON or text output."""
    payload = _parse_json(raw)
    if payload:
        for key in json_keys:
            if key in payload:
                return list(payload[key])
    return re.findall(regex_pattern, raw, re.IGNORECASE)


def get_client_ids(client_node, target, plaintext=True, tls_paths=None):
    """Call GetClientId.GetClientIds; return (ok, ids, raw)."""
    method = "nfsService.GetClientId/GetClientIds"
    cmd = build_grpcurl_cmd(
        target, method, data="{}", plaintext=plaintext, tls_paths=tls_paths
    )
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("GetClientIds response: %s", out)
    if not ok:
        return False, [], out or err
    ids = parse_id_list(
        out,
        ("clientIds", "client_ids"),
        r'"?client_?ids?"?\s*:\s*\[([^\]]*)\]|"?clientId"?\s*:\s*"?(\d+)"?',
    )
    if not ids and out.strip():
        ids = re.findall(r'"?clientId"?\s*:\s*"?(\d+)"?', out, re.IGNORECASE)
    return True, ids, out


def get_session_ids(client_node, target, plaintext=True, tls_paths=None):
    """Call GetSessionId.GetSessionIds; return (ok, ids, raw)."""
    method = "nfsService.GetSessionId/GetSessionIds"
    cmd = build_grpcurl_cmd(
        target, method, data="{}", plaintext=plaintext, tls_paths=tls_paths
    )
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("GetSessionIds response: %s", out)
    if not ok:
        return False, [], out or err
    ids = parse_id_list(
        out,
        ("sessionIds", "session_ids"),
        r'"?sessionId"?\s*:\s*"?([^",\s]+)"?',
    )
    return True, ids, out


# ---------------------------------------------------------------------------
# exportService.ExportService
# ---------------------------------------------------------------------------


def show_exports(client_node, target, plaintext=True, tls_paths=None):
    """Call ExportService.ShowExports; return (ok, exports_list, raw)."""
    method = "exportService.ExportService/ShowExports"
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data="{}",
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("ShowExports response: %s", out)
    if not ok:
        return False, [], out or err
    payload = _parse_json(out)
    exports = payload.get("exports", [])
    if payload.get("success") is False:
        return False, exports, out
    return True, exports, out


def display_export(client_node, target, export_id, plaintext=True, tls_paths=None):
    """Call ExportService.DisplayExport; return (ok, payload, raw)."""
    method = "exportService.ExportService/DisplayExport"
    data = json.dumps({"export_id": int(export_id)})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("DisplayExport(%s) response: %s", export_id, out)
    if not ok:
        return False, {}, out or err
    payload = _parse_json(out)
    if payload.get("success") is False:
        return False, payload, out
    return True, payload, out


def add_export(
    client_node,
    target,
    file_path,
    export_expression="EXPORT",
    plaintext=True,
    tls_paths=None,
):
    """Call ExportService.AddExport; return (ok, raw)."""
    method = "exportService.ExportService/AddExport"
    data = json.dumps({"file_path": file_path, "export_expression": export_expression})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("AddExport response: %s", out)
    payload = _parse_json(out)
    if payload and payload.get("success") is False:
        return False, out or err
    return ok, out or err


def remove_export(client_node, target, export_id, plaintext=True, tls_paths=None):
    """Call ExportService.RemoveExport; return (ok, raw)."""
    method = "exportService.ExportService/RemoveExport"
    data = json.dumps({"export_id": int(export_id)})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("RemoveExport(%s) response: %s", export_id, out)
    payload = _parse_json(out)
    if payload and payload.get("success") is False:
        return False, out or err
    return ok, out or err


def update_export(
    client_node,
    target,
    file_path,
    export_expression="EXPORT",
    plaintext=True,
    tls_paths=None,
):
    """Call ExportService.UpdateExport; return (ok, raw)."""
    method = "exportService.ExportService/UpdateExport"
    data = json.dumps({"file_path": file_path, "export_expression": export_expression})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("UpdateExport response: %s", out)
    payload = _parse_json(out)
    if payload and payload.get("success") is False:
        return False, out or err
    return ok, out or err


# ---------------------------------------------------------------------------
# exportService.ExportStats / cltmgrService.ClientStats
# ---------------------------------------------------------------------------


def export_stats(
    client_node,
    target,
    rpc_name,
    export_id=None,
    plaintext=True,
    tls_paths=None,
):
    """
    Call an ExportStats RPC.

    *rpc_name* examples: GetNFSv41IO, GetTotalOPS, GetNFSIO, StatusStats,
    ResetStats, ShowFDUsage, ShowMDCache.
    """
    method = f"exportService.ExportStats/{rpc_name}"
    data = "{}"
    if export_id is not None:
        data = json.dumps({"export_id": int(export_id)})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("ExportStats.%s response: %s", rpc_name, out[:500] if out else out)
    return ok, out or err


def client_stats(
    client_node,
    target,
    rpc_name,
    ipaddr,
    plaintext=True,
    tls_paths=None,
):
    """Call a ClientStats RPC that takes ClientIpRequest."""
    method = f"cltmgrService.ClientStats/{rpc_name}"
    data = json.dumps({"ipaddr": ipaddr})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info(
        "ClientStats.%s(%s) response: %s", rpc_name, ipaddr, out[:500] if out else out
    )
    return ok, out or err


# ---------------------------------------------------------------------------
# cltmgrService.ClientMgr
# ---------------------------------------------------------------------------


def show_clients(client_node, target, plaintext=True, tls_paths=None):
    """Call ClientMgr.ShowClients; return (ok, clients_list, raw)."""
    method = "cltmgrService.ClientMgr/ShowClients"
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data="{}",
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("ShowClients response: %s", out)
    if not ok:
        return False, [], out or err
    payload = _parse_json(out)
    clients = payload.get("clients", [])
    return True, clients, out


def add_client(client_node, target, ipaddr, plaintext=True, tls_paths=None):
    """Call ClientMgr.AddClient; return (ok, raw)."""
    method = "cltmgrService.ClientMgr/AddClient"
    data = json.dumps({"ipaddr": ipaddr})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("AddClient(%s) response: %s", ipaddr, out)
    payload = _parse_json(out)
    if payload and payload.get("success") is False:
        return False, out or err
    return ok, out or err


def remove_client(client_node, target, ipaddr, plaintext=True, tls_paths=None):
    """Call ClientMgr.RemoveClient; return (ok, raw)."""
    method = "cltmgrService.ClientMgr/RemoveClient"
    data = json.dumps({"ipaddr": ipaddr})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("RemoveClient(%s) response: %s", ipaddr, out)
    payload = _parse_json(out)
    if payload and payload.get("success") is False:
        return False, out or err
    return ok, out or err


def disconnect_nfsv41_client(
    client_node, target, ipaddr, plaintext=True, tls_paths=None
):
    """Call ClientMgr.DisconnectNfsv41Client; return (ok, raw)."""
    method = "cltmgrService.ClientMgr/DisconnectNfsv41Client"
    data = json.dumps({"ipaddr": ipaddr})
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("DisconnectNfsv41Client(%s) response: %s", ipaddr, out)
    payload = _parse_json(out)
    status = payload.get("status") or {}
    if isinstance(status, dict) and status.get("success") is False:
        return False, out or err
    return ok, out or err


# ---------------------------------------------------------------------------
# nfsService.nfsAdmin / ShowIdMapperService
# ---------------------------------------------------------------------------


def admin_rpc(
    client_node,
    target,
    rpc_name,
    data=None,
    plaintext=True,
    tls_paths=None,
):
    """
    Call nfsService.nfsAdmin/<rpc_name>; return (ok, raw).

    Defaults *data* to ``{}`` so EmptyRequest RPCs work reliably with grpcurl.
    Treats ActionResponse/MessageResponse ``success: false`` as failure.
    """
    method = f"nfsService.nfsAdmin/{rpc_name}"
    if data is None:
        data = "{}"
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data=data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("nfsAdmin.%s response: %s", rpc_name, out)
    payload = _parse_json(out)
    if payload.get("success") is False:
        return False, out or err
    return ok, out or err


def show_idmapper(client_node, target, plaintext=True, tls_paths=None):
    """Call ShowIdMapperService.ShowIdMapper; return (ok, entries, raw)."""
    method = "nfsService.ShowIdMapperService/ShowIdMapper"
    ok, out, err = invoke_grpc_method(
        client_node,
        target,
        method,
        data="{}",
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    log.info("ShowIdMapper response: %s", out)
    if not ok:
        return False, [], out or err
    payload = _parse_json(out)
    return True, payload.get("entries", []), out


def get_nfs_grpc_cert_dir(admin_node, nfs_name):
    """
    Resolve NFS gRPC client cert directory on the Ceph admin/installer node.

    Prefers an fsid that contains:
      /var/lib/ceph/<fsid>/nfs_grpc-client-certs/nfs.<nfs_name>/
    """
    fsids = admin_node.get_dir_list("/var/lib/ceph", sudo=True) or []
    candidates = []
    for fsid in fsids:
        path = f"/var/lib/ceph/{fsid}/nfs_grpc-client-certs/nfs.{nfs_name}"
        out, _ = admin_node.exec_command(
            sudo=True, cmd=f"test -d {path} && echo ok", check_ec=False
        )
        if "ok" in (out or ""):
            candidates.append(path)

    if not candidates:
        raise OperationFailedError(
            f"NFS gRPC client cert dir not found under "
            f"/var/lib/ceph/*/nfs_grpc-client-certs/nfs.{nfs_name} on admin node "
            f"{admin_node.hostname} (fsids={fsids})"
        )
    if len(candidates) > 1:
        log.warning("Multiple NFS gRPC cert dirs found, using first: %s", candidates)
    return candidates[0]


def copy_nfs_grpc_certs(
    admin_node, client_node, nfs_name, dest_dir=GRPC_CERT_DIR_CLIENT
):
    """
    Copy NFS gRPC mTLS client certs from the admin node to a grpcurl client.

    Retries briefly because certs may appear shortly after NFS deploy.
    """
    last_error = None
    src_dir = None

    for attempt in range(1, GRPC_CERT_COPY_RETRIES + 1):
        try:
            src_dir = get_nfs_grpc_cert_dir(admin_node, nfs_name)
            log.info(
                "Copying NFS gRPC certs from admin %s:%s to %s:%s (attempt %s)",
                admin_node.hostname,
                src_dir,
                client_node.hostname,
                dest_dir,
                attempt,
            )

            client_node.exec_command(sudo=True, cmd=f"mkdir -p {dest_dir}")

            for name in GRPC_CERT_FILES:
                content, err = admin_node.exec_command(
                    sudo=True, cmd=f"cat {src_dir}/{name}", check_ec=False
                )
                if not content or "No such file" in (err or ""):
                    raise OperationFailedError(
                        f"Failed to read {src_dir}/{name} on admin "
                        f"{admin_node.hostname}: {err}"
                    )
                cert_fp = client_node.remote_file(
                    sudo=True, file_name=f"{dest_dir}/{name}", file_mode="w+"
                )
                cert_fp.write(content)
                cert_fp.flush()
                cert_fp.close()

            client_node.exec_command(sudo=True, cmd=f"chmod 600 {dest_dir}/client.key")
            for name in GRPC_CERT_FILES:
                out, _ = client_node.exec_command(
                    sudo=True,
                    cmd=f"test -s {dest_dir}/{name} && echo ok",
                    check_ec=False,
                )
                if "ok" not in (out or ""):
                    raise OperationFailedError(
                        f"Cert file missing or empty on client: {dest_dir}/{name}"
                    )

            log.info("NFS gRPC certs ready on %s:%s", client_node.hostname, dest_dir)
            return dest_dir
        except Exception as exc:
            last_error = exc
            log.warning(
                "NFS gRPC cert copy attempt %s/%s failed: %s",
                attempt,
                GRPC_CERT_COPY_RETRIES,
                exc,
            )
            if attempt < GRPC_CERT_COPY_RETRIES:
                sleep(GRPC_CERT_COPY_DELAY_SEC)

    raise OperationFailedError(
        f"Unable to copy NFS gRPC certs for nfs.{nfs_name} "
        f"(last src={src_dir}): {last_error}"
    )


def grpc_requires_mtls(ceph_version):
    """Return True when NFS gRPC requires mTLS (Ceph >= 20.2.2)."""
    if not ceph_version:
        return False
    return LooseVersion(str(ceph_version)) >= LooseVersion(GRPC_MTLS_MIN_VERSION)


def resolve_ceph_version(client_node, admin_node=None):
    """Resolve installed Ceph version from client, falling back to admin."""
    for node, prefix in ((client_node, False), (admin_node, True)):
        if node is None:
            continue
        try:
            version = get_ceph_version(node, prefix_cephadm=prefix)
            if version:
                return version
        except Exception as exc:
            log.warning(
                "Failed to get Ceph version from %s (prefix_cephadm=%s): %s",
                getattr(node, "hostname", node),
                prefix,
                exc,
            )
    return None


def tls_paths_from_cert_dir(cert_dir):
    """Build grpcurl -cacert/-cert/-key paths from a client cert directory."""
    return {
        "ca_cert": f"{cert_dir}/ca.crt",
        "client_cert": f"{cert_dir}/client.crt",
        "client_key": f"{cert_dir}/client.key",
    }


def tls_paths_from_config(config):
    """Build TLS path dict from test config, falling back to defaults."""
    paths = dict(DEFAULT_CERT_PATHS)
    for key in paths:
        if config.get(key):
            paths[key] = config[key]
    return paths


def setup_grpc_transport(config, client_nodes, admin_node, nfs_name):
    """
    Resolve plaintext vs mTLS and copy mgr client certs when required.

    Matches tests/nfs/test_nfs_grpc.py: Ceph >= 20.2.2 uses mTLS with certs
    from nfs_grpc-client-certs on the admin node copied to each grpcurl client.

    Config overrides:
        force_plaintext (bool): always use -plaintext
        use_tls (bool): force mTLS on/off when set (otherwise version-based)

    Returns:
        dict: plaintext, tls_paths, use_mtls
    """
    if config.get("force_plaintext"):
        log.info("NFS gRPC transport: plaintext (force_plaintext)")
        return {"plaintext": True, "tls_paths": None, "use_mtls": False}

    if "use_tls" in config:
        use_mtls = bool(config["use_tls"])
        log.info(
            "NFS gRPC transport: %s (use_tls config)",
            "mTLS" if use_mtls else "plaintext",
        )
    else:
        version = resolve_ceph_version(client_nodes[0], admin_node)
        use_mtls = grpc_requires_mtls(version)
        log.info(
            "Ceph version %s -> NFS gRPC mode: %s (mTLS min %s)",
            version,
            "mTLS" if use_mtls else "plaintext",
            GRPC_MTLS_MIN_VERSION,
        )

    if not use_mtls:
        return {"plaintext": True, "tls_paths": None, "use_mtls": False}

    if admin_node is None:
        raise OperationFailedError(
            "No installer/admin node available for NFS gRPC mTLS client certs"
        )

    cert_dir = config.get("grpc_cert_dir", GRPC_CERT_DIR_CLIENT)
    seen = set()
    for node in client_nodes:
        host = getattr(node, "hostname", str(node))
        if host in seen:
            continue
        seen.add(host)
        copy_nfs_grpc_certs(admin_node, node, nfs_name, dest_dir=cert_dir)

    tls_paths = tls_paths_from_cert_dir(cert_dir)
    for key in ("ca_cert", "client_cert", "client_key"):
        if config.get(key):
            tls_paths[key] = config[key]

    return {"plaintext": False, "tls_paths": tls_paths, "use_mtls": True}


def grpc_invoke_kwargs(transport):
    """Return kwargs for grpc_client RPC helpers from setup_grpc_transport()."""
    return {
        "plaintext": transport["plaintext"],
        "tls_paths": transport["tls_paths"],
    }


def resolve_grpc_target(config, nfs_ip, port=None):
    """Resolve grpcurl target from config (supports loopback override)."""
    port = int(config.get("grpc_port", port or DEFAULT_GRPC_PORT))
    host = config.get("grpc_host", nfs_ip)
    return build_grpcurl_target(host, port)


def check_blocked_operation(operation, config):
    """
    Return -1 to skip when *operation* is blocked pending upstream proto.

    Raises ConfigError when blocked and skip_if_blocked is False.
    """
    blocked = config.get("blocked", operation in BLOCKED_OPERATIONS)
    if not blocked:
        return None
    msg = (
        f"Operation '{operation}' is blocked — Log gRPC proto not yet in "
        "upstream nfs-ganesha (or explicitly marked blocked in suite config)"
    )
    if config.get("skip_if_blocked", True):
        log.info("SKIP: %s", msg)
        return -1
    raise ConfigError(msg)


def prepare_cluster_nodes(ceph_cluster, config):
    """
    Validate and return common node handles from *ceph_cluster*.

    Returns:
        tuple: (clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip)
    """
    num_clients = int(config.get("clients", 1))
    clients = ceph_cluster.get_nodes(role="client")
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")

    if not clients:
        raise OperationFailedError("No client nodes available")
    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes available")
    if num_clients > len(clients):
        raise ConfigError(
            f"Test requires {num_clients} clients but only {len(clients)} available"
        )

    clients = clients[:num_clients]
    nfs_node = nfs_nodes[0]
    return (
        clients,
        nfs_nodes,
        clients[0],
        nfs_node,
        nfs_node.hostname,
        nfs_node.ip_address,
    )


def ensure_subvolume_group(client, fs_name, group="ganeshagroup", ceph_cluster=None):
    """Create NFS subvolume group if needed (wraps nfs_operations.ensure_ganeshagroup)."""
    from tests.nfs.nfs_operations import ensure_ganeshagroup

    ensure_ganeshagroup(client, fs_name=fs_name, group=group, ceph_cluster=ceph_cluster)


def first_export_id(exports):
    """Return the first export_id from a ShowExports list, or None."""
    if not exports:
        return None
    entry = exports[0]
    return entry.get("exportId", entry.get("export_id"))
