"""
NFS Ganesha gRPC Service Tests

This module contains tests for verifying NFS Ganesha gRPC service functionality.
It uses the 'operation' config parameter to determine which test scenario to run.

gRPC is mTLS-secured. Client certs are copied from the Ceph admin (_admin) node:
  /var/lib/ceph/<fsid>/nfs_grpc-client-certs/nfs.<nfs-cluster-name>/

Supported Operations:
    - verify_port: Verify gRPC Service Port Availability (port 50051)
    - verify_discovery: Verify gRPC Service Discovery (list available methods)
    - grace_event_0: Start Grace Period (Event ID 0)
    - grace_event_2: Release IP from Grace (Event ID 2)
    - grace_event_4: Node Takeover (Event ID 4)
    - grace_event_5: IP Takeover (Event ID 5)
    - verify_client_session_ids: Verify Client and Session IDs for multiple clients
    - verify_id_after_unmount: Verify ID Updates After Client Unmount
"""

import json
import re
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.nfs_operations import cleanup_cluster, nfs_log_parser, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

GRPC_PORT = 50051
GRPC_CERT_DIR_CLIENT = "/root/nfs_grpc_certs"
GRPC_CERT_FILES = ("ca.crt", "client.crt", "client.key")
GRPC_CERT_COPY_RETRIES = 6
GRPC_CERT_COPY_DELAY_SEC = 5

# Full names expected from `grpcurl ... list` (discovery test)
EXPECTED_GRPC_SERVICES = [
    "nfsService.GetClientId",
    "nfsService.GetNfsGrace",
    "nfsService.GetSessionId",
    "nfsService.StartNfsGrace",
    "grpc.reflection.v1alpha.ServerReflection",
]

# Transport / TLS failures typically land on stderr
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
)

# RPC failures may appear on stdout or stderr
GRPCURL_RPC_FAILURE_MARKERS = ("rpc error",)


def install_grpcurl(node):
    """Install grpcurl on the specified node (no-op if already present)."""
    log.info(f"Installing grpcurl on {node.hostname}")

    out, _ = node.exec_command(cmd="which grpcurl", check_ec=False)
    if "grpcurl" in (out or ""):
        log.info("grpcurl already installed")
        return

    wget_cmd = (
        "curl -LO "
        "https://github.com/fullstorydev/grpcurl/releases/download/v1.8.9/"
        "grpcurl_1.8.9_linux_x86_64.tar.gz"
    )
    node.exec_command(sudo=True, cmd=wget_cmd)
    node.exec_command(
        sudo=True,
        cmd=(
            "tar -xvzf grpcurl_1.8.9_linux_x86_64.tar.gz && "
            "chmod +x grpcurl && mv grpcurl /usr/local/bin/"
        ),
    )

    out, _ = node.exec_command(cmd="grpcurl --version", check_ec=False)
    log.info(f"grpcurl version: {out}")


def get_nfs_grpc_cert_dir(admin_node, nfs_name):
    """
    Resolve NFS gRPC client cert directory on the Ceph admin/installer node.

    Prefers an fsid that actually contains:
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
            f"NFS gRPC client cert dir not found under /var/lib/ceph/*/nfs_grpc-client-certs/"
            f"nfs.{nfs_name} on admin node {admin_node.hostname} (fsids={fsids})"
        )
    if len(candidates) > 1:
        log.warning(f"Multiple NFS gRPC cert dirs found, using first: {candidates}")
    return candidates[0]


def copy_nfs_grpc_certs(
    admin_node, client_node, nfs_name, dest_dir=GRPC_CERT_DIR_CLIENT
):
    """
    Copy NFS gRPC mTLS client certs from the admin/installer node to a client.

    Certs live on the _admin node at:
      /var/lib/ceph/<fsid>/nfs_grpc-client-certs/nfs.<nfs_name>/

    Retries briefly because certs may appear shortly after NFS deploy.

    Returns:
        str: Destination directory path on the client
    """
    last_error = None
    src_dir = None

    for attempt in range(1, GRPC_CERT_COPY_RETRIES + 1):
        try:
            src_dir = get_nfs_grpc_cert_dir(admin_node, nfs_name)
            log.info(
                f"Copying NFS gRPC certs from admin {admin_node.hostname}:{src_dir} "
                f"to {client_node.hostname}:{dest_dir} (attempt {attempt})"
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
                # Match SMB gRPC cert copy pattern (write/flush/close)
                cert_fp = client_node.remote_file(
                    sudo=True, file_name=f"{dest_dir}/{name}", file_mode="w+"
                )
                cert_fp.write(content)
                cert_fp.flush()
                cert_fp.close()

            client_node.exec_command(sudo=True, cmd=f"chmod 600 {dest_dir}/client.key")
            # Quick sanity: files must be non-empty on the client
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

            log.info(f"NFS gRPC certs ready on {client_node.hostname}:{dest_dir}")
            return dest_dir
        except Exception as exc:
            last_error = exc
            log.warning(
                f"NFS gRPC cert copy attempt {attempt}/{GRPC_CERT_COPY_RETRIES} "
                f"failed: {exc}"
            )
            if attempt < GRPC_CERT_COPY_RETRIES:
                sleep(GRPC_CERT_COPY_DELAY_SEC)

    raise OperationFailedError(
        f"Unable to copy NFS gRPC certs for nfs.{nfs_name} "
        f"(last src={src_dir}): {last_error}"
    )


def grpc_auth_flags(cert_dir):
    """Return grpcurl mTLS flags for the given client cert directory."""
    return (
        f"-cacert {cert_dir}/ca.crt "
        f"-cert {cert_dir}/client.crt "
        f"-key {cert_dir}/client.key"
    )


def open_grpc_firewall_port(nfs_node):
    """Open TCP 50051 on the NFS node (permanent + runtime)."""
    nfs_node.exec_command(
        sudo=True,
        cmd=f"firewall-cmd --permanent --add-port={GRPC_PORT}/tcp",
        check_ec=False,
    )
    nfs_node.exec_command(
        sudo=True,
        cmd=f"firewall-cmd --add-port={GRPC_PORT}/tcp",
        check_ec=False,
    )
    nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)


def check_grpcurl_result(out, err, require_stdout=True):
    """
    Validate grpcurl stdout/stderr.

    Only scan stderr for transport/TLS markers so a server response_msg that
    happens to mention 'certificate' cannot false-fail the call.
    """
    out = out or ""
    err = err or ""
    err_l = err.lower()
    out_l = out.lower()
    combined_l = f"{out_l}\n{err_l}"

    for marker in GRPCURL_STDERR_FAILURE_MARKERS:
        if marker in err_l:
            log.error(f"grpcurl transport/TLS failure ({marker!r}): stderr={err!r}")
            return False, err or out

    for marker in GRPCURL_RPC_FAILURE_MARKERS:
        if marker in combined_l:
            log.error(f"grpcurl RPC failure ({marker!r}): out={out!r} err={err!r}")
            return False, err or out

    if require_stdout and not out.strip():
        log.error(f"grpcurl returned empty stdout; stderr={err!r}")
        return False, err or "empty grpcurl response"

    return True, out


def _parse_id_list_response(out, snake_key, camel_key):
    """
    Parse GetClientIds / GetSessionIds JSON.

    proto3 + grpcurl omit empty repeated fields, so `{}` means an empty list.
    """
    try:
        response = json.loads(out)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc

    if not isinstance(response, dict):
        raise ValueError(f"expected JSON object, got {type(response).__name__}")

    if camel_key in response:
        return response[camel_key]
    if snake_key in response:
        return response[snake_key]
    # Empty successful response
    if response == {}:
        return []
    raise ValueError(f"missing {snake_key}/{camel_key} in response: {response}")


def verify_grpc_port_availability(nfs_node, nfs_ip):
    """
    Verify gRPC port 50051 is listening and not localhost-only.

    Returns:
        bool: True if port looks reachable for remote clients
    """
    log.info(f"Verifying gRPC port {GRPC_PORT} availability on {nfs_ip}")

    out, _ = nfs_node.exec_command(
        sudo=True, cmd=f"ss -tulnp | grep {GRPC_PORT}", check_ec=False
    )

    if str(GRPC_PORT) not in (out or ""):
        log.error(f"gRPC port {GRPC_PORT} is not listening")
        return False

    log.info(f"gRPC port {GRPC_PORT} is listening: {out}")

    listen_lines = [ln for ln in out.splitlines() if str(GRPC_PORT) in ln]
    if not listen_lines:
        log.error(f"gRPC port {GRPC_PORT} not found in ss output lines")
        return False

    def _is_remote_reachable_bind(ln):
        """True if this ss line indicates a non-localhost listener."""
        if "0.0.0.0" in ln or "*:" in ln:
            return True
        # IPv6 any-address forms used by ss
        if "[::]:" in ln and "[::1]" not in ln:
            return True
        if re.search(r"(^|[\s]):::\d", ln):
            return True
        if "127.0.0.1" in ln or "[::1]" in ln:
            return False
        # Specific NIC IP (e.g. 10.x.x.x:50051)
        return True

    if not any(_is_remote_reachable_bind(ln) for ln in listen_lines):
        log.error(f"gRPC port {GRPC_PORT} appears bound to localhost only: {out}")
        return False

    if "ganesha" in out.lower() or "nfsd" in out.lower():
        log.info("ganesha process is listening on gRPC port")
    else:
        log.warning("Port is listening but ganesha process name not confirmed")

    return True


def probe_grpc_mtls(client_node, nfs_ip, cert_dir):
    """
    Lightweight mTLS connectivity probe: `grpcurl list` must return >=1 service.

    Unlike discovery, this does not assert the full expected service set.
    """
    log.info(f"Probing mTLS gRPC connectivity on {nfs_ip}:{GRPC_PORT}")
    cmd = f"grpcurl {grpc_auth_flags(cert_dir)} {nfs_ip}:{GRPC_PORT} list"
    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    ok, detail = check_grpcurl_result(out, err, require_stdout=True)
    if not ok:
        return False, detail

    services = [ln.strip() for ln in out.strip().splitlines() if ln.strip()]
    if not services:
        return False, "grpcurl list returned no services"
    return True, services


def verify_grpc_service_discovery(client_node, nfs_ip, cert_dir):
    """Confirm expected gRPC services via mTLS grpcurl list."""
    log.info(f"Discovering gRPC services on {nfs_ip}:{GRPC_PORT}")

    ok, result = probe_grpc_mtls(client_node, nfs_ip, cert_dir)
    if not ok:
        log.error(f"Failed to discover gRPC services: {result}")
        return False, []

    discovered_services = result
    log.info(f"Discovered gRPC services: {discovered_services}")

    missing = [svc for svc in EXPECTED_GRPC_SERVICES if svc not in discovered_services]
    if missing:
        log.error(f"Missing expected gRPC services: {missing}")
        return False, discovered_services

    log.info(f"All expected gRPC services found: {EXPECTED_GRPC_SERVICES}")
    return True, discovered_services


def start_grace_period(client_node, nfs_ip, event_id, cert_dir, node_id=1):
    """
    Invoke StartGraceWithEvent for the given Event ID.

    Returns:
        tuple: (bool, str) success and response/detail
    """
    log.info(f"Starting grace period with Event ID {event_id} on {nfs_ip}")

    request_data = f'{{"Event":{event_id},"NodeId":{node_id},"IpAddr":"{nfs_ip}"}}'
    cmd = (
        f"grpcurl {grpc_auth_flags(cert_dir)} -d '{request_data}' "
        f"{nfs_ip}:{GRPC_PORT} nfsService.StartNfsGrace/StartGraceWithEvent"
    )

    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    log.info(f"gRPC response: {out}")
    if err:
        log.warning(f"gRPC stderr: {err}")

    ok, detail = check_grpcurl_result(out, err, require_stdout=True)
    if not ok:
        log.error(f"Failed to execute event {event_id}: {detail}")
        return False, detail

    try:
        response = json.loads(out)
    except json.JSONDecodeError:
        log.error(f"Grace event {event_id} response is not valid JSON: {out!r}")
        return False, out

    if not isinstance(response, dict):
        log.error(f"Grace event {event_id} response is not an object: {response!r}")
        return False, out

    if "graceStarted" not in response and "grace_started" not in response:
        log.error(f"Grace event {event_id} response missing graceStarted: {response}")
        return False, out

    if event_id == 0:
        grace_started = response.get("graceStarted", response.get("grace_started"))
        if grace_started is not True and str(grace_started).lower() != "true":
            log.error(f"Expected graceStarted=true for event 0, got: {response}")
            return False, out
        log.info("Grace period started successfully")
    else:
        log.info(f"Event {event_id} executed successfully: {response}")

    return True, out


def _resolve_nfs_daemon_name(client, nfs_name):
    """Return ceph orch daemon name for the NFS service (e.g. nfs.cephfs-nfs....)."""
    out, err = client.exec_command(
        sudo=True, cmd=f"ceph orch ps | grep {nfs_name}", check_ec=False
    )
    if not out or not out.strip():
        raise OperationFailedError(
            f"Could not resolve NFS daemon name for {nfs_name}: {err}"
        )
    return out.split()[0]


def _fetch_nfs_daemon_log(client, nfs_node, nfs_name):
    """Fetch current cephadm logs for the NFS daemon as a string."""
    daemon = _resolve_nfs_daemon_name(client, nfs_name)
    log_path = "/tmp/nfs_grpc_event.log"
    nfs_node.exec_command(
        sudo=True, cmd=f"cephadm logs --name {daemon} > {log_path}", check_ec=False
    )
    content, _ = nfs_node.exec_command(sudo=True, cmd=f"cat {log_path}", check_ec=False)
    return content or ""


def verify_grace_logs(nfs_node, nfs_name, event_id, client, log_before=""):
    """
    Verify expected log messages after grace period events.

    If log_before is provided, require that pattern occurrence count increases
    after the event (avoids matching stale startup lines).
    """
    log.info(f"Verifying logs for Event ID {event_id}")

    # Event 2 may log either recovery-event text or the release helper name.
    expected_patterns = {
        0: ["NFS Server Now IN GRACE"],
        2: ["NFS Server recovery event 2", "nfs_release_v4_clients"],
        4: ["NFS Server recovery event 4"],
        5: ["NFS Server recovery event 5"],
    }

    patterns = expected_patterns.get(event_id)
    if not patterns:
        log.error(f"No expected patterns defined for Event ID {event_id}")
        return False

    log_after = _fetch_nfs_daemon_log(client, nfs_node, nfs_name)
    if not log_after.strip():
        log.error("NFS daemon log is empty after grace event")
        return False

    for pattern in patterns:
        before_count = log_before.count(pattern) if log_before is not None else 0
        after_count = log_after.count(pattern)
        if after_count > before_count:
            log.info(
                f"Found new log occurrences of {pattern!r} for Event ID {event_id} "
                f"({before_count} -> {after_count})"
            )
            return True
        log.info(
            f"Pattern {pattern!r} did not increase ({before_count} -> {after_count})"
        )

    # Fallback for callers that did not snapshot logs: full-log search via nfs_log_parser
    if not log_before:
        for pattern in patterns:
            result = nfs_log_parser(
                client=client,
                nfs_node=nfs_node,
                nfs_name=nfs_name,
                expect_list=[pattern],
            )
            if result == 0:
                log.warning(
                    f"Matched {pattern!r} in full logs without a before-snapshot; "
                    "this may include pre-existing lines"
                )
                return True

    log.error(
        f"Expected new log patterns not found for Event ID {event_id}: {patterns}"
    )
    return False


def get_client_ids(client_node, nfs_ip, cert_dir=GRPC_CERT_DIR_CLIENT):
    """Get NFS client IDs via mTLS gRPC. Returns (ok, list)."""
    log.info(f"Getting client IDs from {nfs_ip}")

    cmd = (
        f"grpcurl {grpc_auth_flags(cert_dir)} "
        f"{nfs_ip}:{GRPC_PORT} nfsService.GetClientId/GetClientIds"
    )
    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    log.info(f"GetClientIds response: {out}")

    ok, detail = check_grpcurl_result(out, err, require_stdout=True)
    if not ok:
        log.error(f"Failed to get client IDs: {detail}")
        return False, []

    try:
        client_ids = _parse_id_list_response(out, "client_ids", "clientIds")
    except ValueError as exc:
        log.error(f"Could not parse client IDs: {exc}; raw={out!r}")
        return False, []

    if not isinstance(client_ids, list):
        log.error(f"client_ids is not a list: {client_ids!r}")
        return False, []

    log.info(f"Found client IDs: {client_ids}")
    return True, client_ids


def get_session_ids(client_node, nfs_ip, cert_dir=GRPC_CERT_DIR_CLIENT):
    """Get NFS session IDs via mTLS gRPC. Returns (ok, list)."""
    log.info(f"Getting session IDs from {nfs_ip}")

    cmd = (
        f"grpcurl {grpc_auth_flags(cert_dir)} "
        f"{nfs_ip}:{GRPC_PORT} nfsService.GetSessionId/GetSessionIds"
    )
    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    log.info(f"GetSessionIds response: {out}")

    ok, detail = check_grpcurl_result(out, err, require_stdout=True)
    if not ok:
        log.error(f"Failed to get session IDs: {detail}")
        return False, []

    try:
        session_ids = _parse_id_list_response(out, "session_ids", "sessionIds")
    except ValueError as exc:
        log.error(f"Could not parse session IDs: {exc}; raw={out!r}")
        return False, []

    if not isinstance(session_ids, list):
        log.error(f"session_ids is not a list: {session_ids!r}")
        return False, []

    log.info(f"Found session IDs: {session_ids}")
    return True, session_ids


def setup_additional_mounts(clients, nfs_server, nfs_export, nfs_mount, version, port):
    """
    Mount NFS export on additional clients (clients[1:]).

    Raises:
        OperationFailedError: If any additional mount fails
    """
    mount_points = []

    for i, client in enumerate(clients[1:], start=1):
        mount_point = f"{nfs_mount}_{i}"
        client.create_dirs(dir_path=mount_point, sudo=True)

        if Mount(client).nfs(
            mount=mount_point,
            version=version,
            port=port,
            server=nfs_server,
            export=nfs_export,
        ):
            raise OperationFailedError(f"Failed to mount NFS on {client.hostname}")

        log.info(f"NFS mounted successfully on {client.hostname} at {mount_point}")
        client.exec_command(sudo=True, cmd=f"touch {mount_point}/testfile_{i}")
        mount_points.append((client, mount_point))
        sleep(2)

    return mount_points


def cleanup_additional_mounts(mount_points):
    """Unmount additional NFS mounts."""
    for client, mount_point in mount_points or []:
        try:
            client.exec_command(
                sudo=True, cmd=f"rm -rf {mount_point}/*", check_ec=False
            )
            Unmount(client).unmount(mount_point)
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}", check_ec=False)
            log.info(f"Cleaned up mount on {client.hostname}")
        except Exception as e:
            log.warning(f"Cleanup error on {client.hostname}: {e}")


def run(ceph_cluster, **kw):
    """
    Test NFS Ganesha gRPC Service functionality.

    Returns:
        int: 0 on success, 1 on failure
    """
    config = kw.get("config", {})

    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    num_clients = int(config.get("clients", 1))
    subvolume_group = config.get("subvolume_group", "ganeshagroup")

    clients = ceph_cluster.get_nodes(role="client")
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")
    installer_nodes = ceph_cluster.get_nodes(role="installer")

    if not clients:
        raise OperationFailedError("No client nodes available")
    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes available")
    if not installer_nodes:
        raise OperationFailedError("No installer/admin node available for gRPC certs")
    if num_clients > len(clients):
        raise ConfigError(
            f"Test requires {num_clients} clients but only {len(clients)} available"
        )

    clients = clients[:num_clients]
    client = clients[0]
    nfs_node = nfs_nodes[0]
    admin_node = installer_nodes[0]
    nfs_server = nfs_node.hostname
    nfs_ip = nfs_node.ip_address

    log.info(f"Running operation: {operation}")
    log.info(f"Using NFS server: {nfs_server} ({nfs_ip})")
    log.info(f"Using admin node for gRPC certs: {admin_node.hostname}")
    log.info(f"Using {len(clients)} client(s) for testing")

    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    additional_mounts = []
    cert_dir = GRPC_CERT_DIR_CLIENT

    try:
        # Only the driver client runs grpcurl today
        install_grpcurl(client)
        open_grpc_firewall_port(nfs_node)

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
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
        )
        log.info("NFS cluster setup complete.")

        cert_dir = copy_nfs_grpc_certs(admin_node, client, nfs_name)

        if operation == "verify_port":
            log.info("=" * 60)
            log.info("TEST: Verify gRPC Service Port Availability")
            log.info("=" * 60)

            if not verify_grpc_port_availability(nfs_node, nfs_ip):
                raise OperationFailedError(
                    f"gRPC port {GRPC_PORT} is not available on {nfs_ip}"
                )

            ok, result = probe_grpc_mtls(client, nfs_ip, cert_dir)
            if not ok:
                raise OperationFailedError(
                    f"gRPC port listening but mTLS probe failed on "
                    f"{nfs_ip}:{GRPC_PORT}: {result}"
                )
            log.info(
                f"PASS: gRPC port {GRPC_PORT} listening and reachable over mTLS "
                f"({len(result)} services listed)"
            )

        elif operation == "verify_discovery":
            log.info("=" * 60)
            log.info("TEST: Verify gRPC Service Discovery")
            log.info("=" * 60)

            success, services = verify_grpc_service_discovery(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError(
                    f"Could not discover required gRPC services on {nfs_ip}:{GRPC_PORT}"
                )
            log.info(f"PASS: Discovered gRPC services: {services}")

        elif operation.startswith("grace_event_"):
            event_id = int(operation.split("_")[-1])
            event_names = {
                0: "Start Grace Period",
                2: "Release IP from Grace",
                4: "Node Takeover",
                5: "IP Takeover",
            }
            if event_id not in event_names:
                raise ConfigError(f"Unsupported grace event operation: {operation}")
            event_name = event_names[event_id]

            log.info("=" * 60)
            log.info(f"TEST: {event_name} (Event ID {event_id})")
            log.info("=" * 60)

            log_before = _fetch_nfs_daemon_log(client, nfs_node, nfs_name)

            success, response = start_grace_period(
                client, nfs_ip, event_id, cert_dir=cert_dir
            )
            if not success:
                raise OperationFailedError(
                    f"Grace event {event_id} gRPC call failed: {response}"
                )

            sleep(5)

            if not verify_grace_logs(
                nfs_node, nfs_name, event_id, client, log_before=log_before
            ):
                raise OperationFailedError(
                    f"Expected new ganesha log lines not found for grace event {event_id}"
                )

            log.info(f"PASS: {event_name} executed successfully")

        elif operation == "verify_client_session_ids":
            log.info("=" * 60)
            log.info("TEST: Verify Client and Session IDs for Multiple Clients")
            log.info("=" * 60)

            if len(clients) > 1:
                additional_mounts = setup_additional_mounts(
                    clients,
                    nfs_server,
                    f"{nfs_export}_0",
                    nfs_mount,
                    nfs_version,
                    nfs_port,
                )

            sleep(10)

            success, client_ids = get_client_ids(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError("Could not get client IDs via gRPC")

            success, session_ids = get_session_ids(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError("Could not get session IDs via gRPC")

            log.info(f"Client IDs: {client_ids}")
            log.info(f"Session IDs: {session_ids}")

            unique_client_ids = len({str(x) for x in client_ids})
            unique_session_ids = len({str(x) for x in session_ids})

            log.info(f"Unique Client IDs: {unique_client_ids}")
            log.info(f"Unique Session IDs: {unique_session_ids}")

            if unique_client_ids < num_clients:
                raise OperationFailedError(
                    f"Expected at least {num_clients} unique client IDs, "
                    f"got {unique_client_ids}: {client_ids}"
                )
            if unique_session_ids < 1:
                raise OperationFailedError(
                    f"Expected at least 1 session ID, got {unique_session_ids}: "
                    f"{session_ids}"
                )

            log.info("PASS: Client and Session IDs retrieved successfully")

        elif operation == "verify_id_after_unmount":
            log.info("=" * 60)
            log.info("TEST: Verify ID Updates After Client Unmount")
            log.info("=" * 60)

            if len(clients) < 2:
                raise ConfigError("This test requires at least 2 clients")

            additional_mounts = setup_additional_mounts(
                clients, nfs_server, f"{nfs_export}_0", nfs_mount, nfs_version, nfs_port
            )

            sleep(10)

            success, initial_client_ids = get_client_ids(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError("Could not get initial client IDs via gRPC")
            success, initial_session_ids = get_session_ids(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError("Could not get initial session IDs via gRPC")

            log.info(f"Initial Client IDs: {initial_client_ids}")
            log.info(f"Initial Session IDs: {initial_session_ids}")

            initial_clients = len({str(x) for x in initial_client_ids})
            initial_sessions = len({str(x) for x in initial_session_ids})
            if initial_clients < 2:
                raise OperationFailedError(
                    f"Expected at least 2 clients before unmount, got {initial_clients}"
                )

            if not additional_mounts:
                raise OperationFailedError("No additional mounts available to unmount")

            unmount_client, unmount_point = additional_mounts[-1]
            log.info(f"Unmounting from {unmount_client.hostname}")

            unmount_client.exec_command(
                sudo=True, cmd=f"rm -rf {unmount_point}/*", check_ec=False
            )
            Unmount(unmount_client).unmount(unmount_point)
            unmount_client.exec_command(
                sudo=True, cmd=f"rm -rf {unmount_point}", check_ec=False
            )
            additional_mounts = additional_mounts[:-1]

            sleep(15)

            success, updated_client_ids = get_client_ids(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError("Could not get updated client IDs via gRPC")
            success, updated_session_ids = get_session_ids(client, nfs_ip, cert_dir)
            if not success:
                raise OperationFailedError("Could not get updated session IDs via gRPC")

            log.info(f"Updated Client IDs: {updated_client_ids}")
            log.info(f"Updated Session IDs: {updated_session_ids}")

            updated_clients = len({str(x) for x in updated_client_ids})
            updated_sessions = len({str(x) for x in updated_session_ids})

            if not (
                updated_clients < initial_clients or updated_sessions < initial_sessions
            ):
                raise OperationFailedError(
                    "Expected client or session count to decrease after unmount; "
                    f"clients {initial_clients}->{updated_clients}, "
                    f"sessions {initial_sessions}->{updated_sessions}"
                )

            log.info("PASS: Client/session count decreased after unmount")

        else:
            raise ConfigError(f"Unknown operation: {operation}")

        return 0

    except (ConfigError, OperationFailedError) as e:
        log.error(f"Test failed: {e}")
        return 1
    except Exception as e:
        log.error(f"Test failed with unexpected error: {e}")
        import traceback

        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("=" * 60)
        log.info("CLEANUP")
        log.info("=" * 60)

        try:
            cleanup_additional_mounts(additional_mounts)
            nfs_log_parser(client=client, nfs_node=nfs_nodes, nfs_name=nfs_name)
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
            log.info("Cleanup completed")
        except Exception as cleanup_error:
            log.warning(f"Cleanup error (non-fatal): {cleanup_error}")
