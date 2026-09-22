"""
KMIP / GKLM utilities for NVMe-oF BYOK encryption tests (Ceph 9.2+).

Two KMIP backends are supported, selected by config["use_dummy_kmip"]:

  use_dummy_kmip: true  (default for CI)
    Spins up a minimal stdlib-only KMIP server on the initiator/client node.
    No pip install or external dependency required — works on Python 3.9-3.12+.
    Steps:
      1. Generate self-signed CA + server + client certs via openssl
      2. Write the minimal KMIP server script to the node
      3. Pre-populate the key store JSON with AES-256 keys for each key_id
      4. Start the server in background (port 5696) via setsid
      5. Wait for port to be listening
    Teardown kills the process and removes files.

  use_dummy_kmip: false  (for environments with a real GKLM appliance)
    Reuses existing NFS BYOK helpers from tests.nfs.byok.byok_tools and
    utility.gklm_client.  Credentials loaded from ~/.cephci.yaml gklm_config.

Public API (same regardless of backend):
  setup_kmip_for_nvmeof(ceph_cluster, config, custom_data) -> kmip_info dict
  configure_kmip_endpoint_on_subsystem(gateway, nqn, kmip_cfg)
  teardown_kmip(kmip_info)
"""

import re
import secrets
import time

from utility.log import Log

LOG = Log(__name__)

# ---------------------------------------------------------------------------
# Dummy KMIP backend — podman-based (quay.io/gdidi/kmip/kmip-server:latest)
#
# Server image : config["kmip_server_image"]  (default below)
# CLI image    : config["kmip_cli_image"]      (default below)
#
# The server container is started with --network=host so port 5696 is
# directly reachable by the gateway daemons on the same node.
# Certs live in /var/lib/kmip-data/certs/ which is the path the server
# image expects and which is bind-mounted into the CLI container.
# ---------------------------------------------------------------------------

_DUMMY_KMIP_PORT = 5696
_DUMMY_KMIP_CERT_DIR = "/var/lib/kmip-data/certs"
_KMIP_CONTAINER_NAME = "nvmeof-byok-kmip"
_DEFAULT_KMIP_SERVER_IMAGE = "quay.io/gdidi/kmip/kmip-server:latest"
_DEFAULT_KMIP_CLI_IMAGE = "quay.io/gdidi/kmip/kmip-cli:latest"

# openssl subject used for all self-signed certs
_OPENSSL_SUBJ = "/CN=nvmeof-byok-kmip/O=cephci/C=US"

# ---------------------------------------------------------------------------
# (placeholder kept so the file parses — replaced by podman approach)
# ---------------------------------------------------------------------------
_MINIMAL_KMIP_SERVER_SCRIPT = r'''
"""Minimal KMIP 1.2 server — stdlib only, Python 3.9+ compatible.

Handles Locate and Get requests over mutual-TLS.
Key store is loaded from {keystore} at startup.
"""
import json
import os
import socket
import ssl
import struct
import sys
import threading

CERT_DIR   = os.environ.get("KMIP_CERT_DIR", "/tmp/nvmeof_byok_kmip")
PORT       = int(os.environ.get("KMIP_PORT", "5696"))
KEYSTORE_F = os.path.join(CERT_DIR, "keystore.json")

# ── TTLV helpers ────────────────────────────────────────────────────────────
# Tag constants (KMIP 1.2 spec, section 9.1)
TAG_REQUEST_MESSAGE        = 0x420078
TAG_RESPONSE_MESSAGE       = 0x42007B
TAG_REQUEST_HEADER         = 0x420077
TAG_RESPONSE_HEADER        = 0x42007A
TAG_REQUEST_PAYLOAD        = 0x420079
TAG_RESPONSE_PAYLOAD       = 0x42007C
TAG_PROTOCOL_VERSION       = 0x420069
TAG_PROTOCOL_VERSION_MAJOR = 0x42006A
TAG_PROTOCOL_VERSION_MINOR = 0x42006B
TAG_OPERATION              = 0x42005C
TAG_UNIQUE_IDENTIFIER      = 0x420094
TAG_OBJECT_TYPE            = 0x420057
TAG_MANAGED_OBJECT         = 0x420069  # reused as payload wrapper
TAG_SYMMETRIC_KEY          = 0x42008F
TAG_KEY_BLOCK              = 0x420040
TAG_KEY_VALUE              = 0x420045
TAG_KEY_FORMAT_TYPE        = 0x420042
TAG_CRYPTOGRAPHIC_ALGORITHM= 0x420028
TAG_CRYPTOGRAPHIC_LENGTH   = 0x42002A
TAG_NAME                   = 0x420053
TAG_NAME_VALUE             = 0x420055
TAG_NAME_TYPE              = 0x420054
TAG_ATTRIBUTE              = 0x420008
TAG_ATTRIBUTE_NAME         = 0x42000A
TAG_ATTRIBUTE_VALUE        = 0x42000B
TAG_BATCH_COUNT            = 0x42000D
TAG_BATCH_ITEM             = 0x42000F
TAG_RESULT_STATUS          = 0x42007F
TAG_RESULT_REASON          = 0x420080
TAG_RESULT_MESSAGE         = 0x420081

TYPE_STRUCTURE  = 0x01
TYPE_INTEGER    = 0x02
TYPE_LONG_INT   = 0x03
TYPE_ENUM       = 0x05
TYPE_TEXT_STRING= 0x07
TYPE_BYTE_STRING= 0x08

OP_LOCATE = 0x0000000B
OP_GET    = 0x00000012

STATUS_SUCCESS = 0x00000000
STATUS_FAILED  = 0x00000001

REASON_ITEM_NOT_FOUND = 0x00000001

def _pack(tag, typ, value_bytes):
    length = len(value_bytes)
    # Pad to 8-byte boundary
    pad = (8 - (length % 8)) % 8
    return struct.pack(">IHH", tag, typ, length) + value_bytes + b"\x00" * pad

def pack_int(tag, value):
    return _pack(tag, TYPE_INTEGER, struct.pack(">i4x", value))

def pack_enum(tag, value):
    return _pack(tag, TYPE_ENUM, struct.pack(">i4x", value))

def pack_text(tag, text):
    b = text.encode("utf-8")
    return _pack(tag, TYPE_TEXT_STRING, b)

def pack_bytes(tag, data):
    return _pack(tag, TYPE_BYTE_STRING, data)

def pack_struct(tag, inner):
    return _pack(tag, TYPE_STRUCTURE, inner)

def read_tlv(data, offset):
    if offset + 8 > len(data):
        return None, offset
    tag, typ, length = struct.unpack_from(">IHH", data, offset)
    # tag is 3 bytes + 1 reserved; unpack gave us 4-byte tag
    offset += 8
    pad = (8 - (length % 8)) % 8
    value = data[offset:offset + length]
    offset += length + pad
    return (tag, typ, value), offset

def find_tlv(data, target_tag, typ_filter=None):
    offset = 0
    while offset < len(data):
        item, offset = read_tlv(data, offset)
        if item is None:
            break
        tag, typ, value = item
        if tag == target_tag and (typ_filter is None or typ == typ_filter):
            return value
        if typ == TYPE_STRUCTURE:
            result = find_tlv(value, target_tag, typ_filter)
            if result is not None:
                return result
    return None

def find_all_tlv(data, target_tag, typ_filter=None):
    results = []
    offset = 0
    while offset < len(data):
        item, offset = read_tlv(data, offset)
        if item is None:
            break
        tag, typ, value = item
        if tag == target_tag and (typ_filter is None or typ == typ_filter):
            results.append(value)
        if typ == TYPE_STRUCTURE:
            results.extend(find_all_tlv(value, target_tag, typ_filter))
    return results

# ── Protocol version header ─────────────────────────────────────────────────
def _proto_version():
    return pack_struct(TAG_PROTOCOL_VERSION,
        pack_int(TAG_PROTOCOL_VERSION_MAJOR, 1) +
        pack_int(TAG_PROTOCOL_VERSION_MINOR, 2))

def _response_header(batch_count=1):
    return pack_struct(TAG_RESPONSE_HEADER,
        _proto_version() +
        pack_int(TAG_BATCH_COUNT, batch_count))

# ── Request parsing ─────────────────────────────────────────────────────────
def parse_request(data):
    """Return (operation_enum, payload_bytes) from a raw KMIP request."""
    req_raw = find_tlv(data, TAG_REQUEST_MESSAGE, TYPE_STRUCTURE)
    if req_raw is None:
        req_raw = data  # already unwrapped
    op_bytes = find_tlv(req_raw, TAG_OPERATION, TYPE_ENUM)
    operation = struct.unpack(">i", op_bytes[:4])[0] if op_bytes else None
    payload = find_tlv(req_raw, TAG_REQUEST_PAYLOAD, TYPE_STRUCTURE)
    return operation, payload

# ── Response builders ────────────────────────────────────────────────────────
def success_batch_item(operation, payload_bytes):
    return pack_struct(TAG_BATCH_ITEM,
        pack_enum(TAG_OPERATION, operation) +
        pack_enum(TAG_RESULT_STATUS, STATUS_SUCCESS) +
        pack_struct(TAG_RESPONSE_PAYLOAD, payload_bytes))

def error_batch_item(operation, reason, message):
    return pack_struct(TAG_BATCH_ITEM,
        pack_enum(TAG_OPERATION, operation) +
        pack_enum(TAG_RESULT_STATUS, STATUS_FAILED) +
        pack_enum(TAG_RESULT_REASON, reason) +
        pack_text(TAG_RESULT_MESSAGE, message))

def build_response(batch_item_bytes):
    body = _response_header() + batch_item_bytes
    return pack_struct(TAG_RESPONSE_MESSAGE, body)

# ── Operation handlers ───────────────────────────────────────────────────────
def handle_locate(payload, keystore):
    """Find a key by Name attribute; return its Unique Identifier."""
    name_val = None
    # Walk all Attribute structures looking for Name
    for attr_bytes in find_all_tlv(payload, TAG_ATTRIBUTE, TYPE_STRUCTURE):
        attr_name_b = find_tlv(attr_bytes, TAG_ATTRIBUTE_NAME, TYPE_TEXT_STRING)
        if attr_name_b and attr_name_b.rstrip(b"\x00").decode() == "Name":
            name_struct = find_tlv(attr_bytes, TAG_ATTRIBUTE_VALUE, TYPE_STRUCTURE)
            if name_struct:
                nv = find_tlv(name_struct, TAG_NAME_VALUE, TYPE_TEXT_STRING)
                if nv:
                    name_val = nv.rstrip(b"\x00").decode()
    if name_val is None:
        # Also try Name structure directly in payload
        name_struct = find_tlv(payload, TAG_NAME, TYPE_STRUCTURE)
        if name_struct:
            nv = find_tlv(name_struct, TAG_NAME_VALUE, TYPE_TEXT_STRING)
            if nv:
                name_val = nv.rstrip(b"\x00").decode()

    if name_val and name_val in keystore:
        uid = keystore[name_val]["uid"]
        resp_payload = pack_text(TAG_UNIQUE_IDENTIFIER, uid)
        return success_batch_item(OP_LOCATE, resp_payload)
    return error_batch_item(OP_LOCATE, REASON_ITEM_NOT_FOUND,
                            f"Key '{name_val}' not found")

def handle_get(payload, keystore):
    """Return key bytes for the given Unique Identifier."""
    uid_b = find_tlv(payload, TAG_UNIQUE_IDENTIFIER, TYPE_TEXT_STRING)
    if uid_b is None:
        return error_batch_item(OP_GET, REASON_ITEM_NOT_FOUND, "No UID in request")
    uid = uid_b.rstrip(b"\x00").decode()
    # Find by uid
    entry = next((v for v in keystore.values() if v["uid"] == uid), None)
    if entry is None:
        return error_batch_item(OP_GET, REASON_ITEM_NOT_FOUND, f"UID {uid} not found")

    key_bytes = bytes.fromhex(entry["key_hex"])
    key_block = (
        pack_enum(TAG_KEY_FORMAT_TYPE, 1) +   # Raw
        pack_struct(TAG_KEY_VALUE, pack_bytes(TAG_KEY_VALUE, key_bytes)) +
        pack_enum(TAG_CRYPTOGRAPHIC_ALGORITHM, 3) +  # AES
        pack_int(TAG_CRYPTOGRAPHIC_LENGTH, len(key_bytes) * 8)
    )
    sym_key = pack_struct(TAG_SYMMETRIC_KEY, pack_struct(TAG_KEY_BLOCK, key_block))
    resp_payload = (
        pack_text(TAG_UNIQUE_IDENTIFIER, uid) +
        pack_enum(TAG_OBJECT_TYPE, 2) +  # SymmetricKey
        sym_key
    )
    return success_batch_item(OP_GET, resp_payload)

# ── Connection handler ───────────────────────────────────────────────────────
def handle_connection(conn, keystore):
    try:
        raw = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            raw += chunk
            # KMIP messages start with a structure TLV; length is at bytes 4-7
            if len(raw) >= 8:
                _, _, msg_len = struct.unpack_from(">IHH", raw, 0)
                pad = (8 - (msg_len % 8)) % 8
                total = 8 + msg_len + pad
                if len(raw) >= total:
                    break
        if not raw:
            return
        operation, payload = parse_request(raw)
        if operation == OP_LOCATE:
            batch_item = handle_locate(payload, keystore)
        elif operation == OP_GET:
            batch_item = handle_get(payload, keystore)
        else:
            batch_item = error_batch_item(
                operation or 0, REASON_ITEM_NOT_FOUND,
                f"Unsupported operation 0x{(operation or 0):08X}")
        response = build_response(batch_item)
        conn.sendall(response)
    except Exception as exc:
        print(f"[kmip-server] connection error: {exc}", flush=True)
    finally:
        conn.close()

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    with open(KEYSTORE_F) as f:
        keystore = json.load(f)
    print(f"[kmip-server] loaded {len(keystore)} keys from {KEYSTORE_F}", flush=True)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(
        certfile=os.path.join(CERT_DIR, "server.crt"),
        keyfile=os.path.join(CERT_DIR, "server.key"),
    )
    ctx.load_verify_locations(os.path.join(CERT_DIR, "ca.crt"))
    ctx.verify_mode = ssl.CERT_REQUIRED

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", PORT))
        sock.listen(16)
        print(f"[kmip-server] listening on 0.0.0.0:{PORT}", flush=True)
        sys.stdout.flush()
        while True:
            try:
                raw_conn, addr = sock.accept()
                tls_conn = ctx.wrap_socket(raw_conn, server_side=True)
                t = threading.Thread(
                    target=handle_connection, args=(tls_conn, keystore), daemon=True
                )
                t.start()
            except Exception as exc:
                print(f"[kmip-server] accept error: {exc}", flush=True)

if __name__ == "__main__":
    main()
'''

# Script that generates the keystore.json — run once before the server starts.
_KEYGEN_SCRIPT = """\
import json, os, secrets
key_ids = {key_ids}
store = {{}}
for i, kid in enumerate(key_ids):
    store[kid] = {{
        "uid": str(i + 1),
        "key_hex": secrets.token_hex(32),   # 256-bit AES key
    }}
with open("{keystore}", "w") as f:
    json.dump(store, f)
print("KEYSTORE_WRITTEN")
"""


def setup_dummy_kmip_server(node, config):
    """Start a KMIP server container on *node* and register all LUKS keys.

    Uses:
      Server: config["kmip_server_image"] (default quay.io/gdidi/kmip/kmip-server:latest)
      CLI:    config["kmip_cli_image"]    (default quay.io/gdidi/kmip/kmip-cli:latest)

    Steps:
      1. Generate self-signed CA + server + client certs via openssl
      2. Start the KMIP server container with --network=host
      3. Wait for port 5696 to be listening
      4. Create one AES key per luks_combo entry via the CLI container
         and record the returned UID in config["luks_combos"][i]["key_uid"]

    Args:
        node: CephNode on which the containers will run (initiator/client node).
        config (dict): Test config; must contain "luks_combos" list.

    Returns:
        dict: kmip_cfg compatible with configure_kmip_endpoint_on_subsystem().
    """
    port = _DUMMY_KMIP_PORT
    server_image = config.get("kmip_server_image", _DEFAULT_KMIP_SERVER_IMAGE)
    cli_image = config.get("kmip_cli_image", _DEFAULT_KMIP_CLI_IMAGE)
    # Host directory where certs will be copied from the container
    host_cert_dir = _DUMMY_KMIP_CERT_DIR
    container_cert_dir = "/kmip/certs"
    cli_run = (
        f"podman run --rm --network host "
        f"-v {host_cert_dir}:{host_cert_dir}:ro,Z "
        f"{cli_image} "
        f"--ca {host_cert_dir}/ca_cert.pem "
        f"--cert {host_cert_dir}/client_cert.pem "
        f"--key {host_cert_dir}/client_key.pem"
    )

    LOG.info("Setting up KMIP server container on %s (port %d)", node.hostname, port)

    # ── 1. Start KMIP server container ───────────────────────────────────────
    LOG.info("Step D-1: Starting KMIP server container %s", server_image)
    node.exec_command(
        cmd=f"podman rm -f {_KMIP_CONTAINER_NAME} 2>/dev/null || true",
        sudo=True,
    )
    node.exec_command(
        cmd=(
            f"podman run -d --name {_KMIP_CONTAINER_NAME} --network host "
            f"{server_image}"
        ),
        sudo=True,
    )

    # ── 2. Wait for port to be listening (max 60 s) ──────────────────────────
    LOG.info("Step D-2: Waiting for KMIP server on port %d", port)
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            out, _ = node.exec_command(
                cmd=f"ss -tlnp | grep :{port} || true", sudo=True
            )
            if str(port) in out:
                LOG.info("KMIP server is listening on port %d", port)
                break
        except Exception:
            pass
        time.sleep(3)
    else:
        log_out, _ = node.exec_command(
            cmd=f"podman logs {_KMIP_CONTAINER_NAME} 2>&1 | tail -30", sudo=True
        )
        raise RuntimeError(
            f"KMIP server container did not start within 60 s on {node.hostname}.\n"
            f"--- container logs ---\n{log_out}"
        )

    # ── 2b. Open port on the KMIP node in every active firewalld zone ────────
    # GW daemons on remote nodes connect to the KMIP server; we must open the
    # port in ALL active zones because the internal cluster network interface may
    # be bound to a different zone than the default public zone.
    # firewall-cmd --get-active-zones emits lines like "public (default)" — strip
    # the " (default)" annotation so the zone name is usable as a CLI argument.
    # Fall back to iptables when firewalld is not running.
    LOG.info(
        "Step D-2b: Opening firewall port %d/tcp on KMIP node %s (all zones)",
        port,
        node.hostname,
    )
    try:
        fw_active, _ = node.exec_command(
            cmd="systemctl is-active firewalld || true", sudo=True
        )
        if fw_active.strip() == "active":
            zones_out, _ = node.exec_command(
                cmd="firewall-cmd --get-active-zones | awk 'NF == 1 || / \\(/{print $1}' || true",
                sudo=True,
            )
            # Strip any residual "(default)" annotation; keep only the bare zone name
            active_zones = []
            for z in zones_out.splitlines():
                z = z.strip().split()[0] if z.strip() else ""
                if z:
                    active_zones.append(z)
            if not active_zones:
                active_zones = ["public"]
            for zone in active_zones:
                try:
                    node.exec_command(
                        cmd=f"firewall-cmd --permanent --zone={zone} --add-port={port}/tcp",
                        sudo=True,
                    )
                    LOG.info(
                        "firewalld: port %d/tcp opened in zone '%s' on KMIP node %s",
                        port,
                        zone,
                        node.hostname,
                    )
                except Exception as ze:
                    LOG.warning(
                        "firewalld: could not open port %d in zone '%s' on %s: %s",
                        port,
                        zone,
                        node.hostname,
                        ze,
                    )
            node.exec_command(cmd="firewall-cmd --reload", sudo=True)
            # Belt-and-suspenders: insert a low-level accept rule so the port is
            # reachable from any interface/zone.  Try nftables first (RHEL 10+),
            # fall back to iptables (RHEL 8/9).
            node.exec_command(
                cmd=(
                    f"nft add rule inet filter input tcp dport {port} accept 2>/dev/null || "
                    f"iptables -C INPUT -p tcp --dport {port} -j ACCEPT 2>/dev/null || "
                    f"iptables -I INPUT 1 -p tcp --dport {port} -j ACCEPT 2>/dev/null || true"
                ),
                sudo=True,
            )
            LOG.info(
                "nft/iptables: port %d/tcp accepted as fallback on KMIP node %s",
                port,
                node.hostname,
            )
        else:
            # firewalld not active — use nftables/iptables directly
            node.exec_command(
                cmd=(
                    f"nft add rule inet filter input tcp dport {port} accept 2>/dev/null || "
                    f"iptables -C INPUT -p tcp --dport {port} -j ACCEPT 2>/dev/null || "
                    f"iptables -I INPUT 1 -p tcp --dport {port} -j ACCEPT 2>/dev/null || true"
                ),
                sudo=True,
            )
            LOG.info(
                "nft/iptables: port %d/tcp accepted on KMIP node %s",
                port,
                node.hostname,
            )
    except Exception as exc:
        LOG.warning(
            "Could not open firewall port %d on %s: %s (continuing)",
            port,
            node.hostname,
            exc,
        )

    # ── 3. Copy certs from container to host ─────────────────────────────────
    LOG.info("Step D-3: Copying certs from container to %s", host_cert_dir)
    node.exec_command(cmd=f"mkdir -p {host_cert_dir}", sudo=True)
    node.exec_command(
        cmd=(
            f"podman cp "
            f"{_KMIP_CONTAINER_NAME}:{container_cert_dir}/. {host_cert_dir}/"
        ),
        sudo=True,
    )

    # ── 4. Create one passphrase per luks_combo via CLI container ────────────
    LOG.info("Step D-4: Registering LUKS passphrases via KMIP CLI")
    for combo in config.get("luks_combos", []):
        key_name = combo["key_id"]
        passphrase = secrets.token_hex(32)
        out, _ = node.exec_command(
            cmd=(
                f"{cli_run} create-passphrase "
                f"--name {key_name} --value {passphrase}"
            ),
            sudo=True,
        )
        # Output format:
        #   Created and activated passphrase 'key-id-luks1-aes128'
        #     uuid: 1
        #     name: key-id-luks1-aes128
        #     ...
        # Parse "uuid: <value>" — value may be an integer or a UUID string.
        uid_match = re.search(r"uuid:\s*(\S+)", out)
        if not uid_match:
            raise RuntimeError(
                f"Could not parse uuid from create-passphrase output "
                f"for key '{key_name}':\n{out}"
            )
        uid = uid_match.group(1).rstrip(",")
        combo["key_uid"] = uid
        LOG.info("Created KMIP passphrase name=%s uid=%s", key_name, uid)

    # ── 5. Read certs from host path (copied from container) ──────────────────
    LOG.info("Step D-5: Reading certs from %s", host_cert_dir)

    def _read_cert(filename):
        out, _ = node.exec_command(cmd=f"cat {host_cert_dir}/{filename}", sudo=True)
        return out

    return {
        "host": node.ip_address,
        "port": port,
        "server_name": node.hostname,
        "ca_cert_pem": _read_cert("ca_cert.pem"),
        "client_cert_pem": _read_cert("client_cert.pem"),
        "client_key_pem": _read_cert("client_key.pem"),
        "dummy_node": node,
        "dummy_cert_dir": host_cert_dir,
    }


def teardown_dummy_kmip_server(kmip_cfg):
    """Stop the KMIP server container and remove cert files."""
    node = kmip_cfg.get("dummy_node")
    cert_dir = kmip_cfg.get("dummy_cert_dir", _DUMMY_KMIP_CERT_DIR)
    if node is None:
        LOG.warning("teardown_dummy_kmip_server: no node in kmip_cfg, skipping")
        return
    LOG.info("Tearing down KMIP server container on %s", node.hostname)
    node.exec_command(
        cmd=f"podman rm -f {_KMIP_CONTAINER_NAME} 2>/dev/null || true ; rm -rf {cert_dir}",
        sudo=True,
    )
    LOG.info("KMIP server container stopped and cert files removed.")


# ---------------------------------------------------------------------------
# Real GKLM backend
# ---------------------------------------------------------------------------


def setup_gklm_for_nvmeof(ceph_cluster, config, custom_data):
    """Full GKLM setup for NVMe-oF BYOK tests (real GKLM/Thales appliance).

    Steps performed:
      1. Load GKLM credentials (from ~/.cephci.yaml / --custom-config).
      2. Update /etc/hosts on all GW nodes so they can resolve the GKLM hostname.
      3. Login to GKLM REST API.
      4. Fetch CA certificate from GKLM server.
      5. Generate client key + certificate for the GW node's TLS identity.
      6. Ensure a fresh GKLM KMIP client exists (removes stale state from prior runs).
      7. Assign the GW's certificate to the GKLM client.
      8. Create one symmetric AES key per luks_combo entry in config["luks_combos"].
      9. Write all cert PEM strings to the first GW node's filesystem.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test config.
        custom_data (dict): kwargs["test_data"] from the cephci runner.

    Returns:
        dict: kmip_cfg compatible with configure_kmip_endpoint_on_subsystem(),
              plus gklm_rest_client / gklm_client_name / gklm_cert_alias for teardown.
    """
    import re

    from tests.nfs.byok.byok_tools import (
        create_in_file_certs,
        ensure_fresh_gklm_kmip_client,
        load_gklm_config,
        setup_gklm_infrastructure,
    )
    from utility.gklm_client.gklm_client import build_gklm_client
    from utility.utils import get_cephci_config

    cephci_data = get_cephci_config()
    gklm_params = load_gklm_config(custom_data, config, cephci_data)
    gklm_ip = gklm_params["gklm_ip"]
    gklm_hostname = gklm_params["gklm_hostname"]
    gklm_user = gklm_params["gklm_user"]

    gw_nodes = ceph_cluster.get_nodes("nvmeof-gw")

    # Step 2 — /etc/hosts on all GW nodes
    setup_gklm_infrastructure(gw_nodes, gklm_ip, gklm_hostname)

    # Step 3 — REST login
    gklm_client = build_gklm_client(gklm_params)

    # Step 4 — CA certificate from GKLM
    ca_cert = gklm_client.certificates.get_system_certificate(gklm_hostname)

    # Step 5 — client key + certificate for the first GW node
    gw_node = gw_nodes[0]
    rsa_key, cert, _ = gklm_client.certificates.get_certificates(
        subject={"common_name": gw_node.hostname, "ip_address": gw_node.ip_address}
    )

    # Step 6 — clean up stale state, then create fresh KMIP client
    client_name = config.get("gklm_client_name", "nvmeof_byok_automation")
    cert_alias = config.get("gklm_cert_alias", "nvmeof_byok_cert")
    ensure_fresh_gklm_kmip_client(
        gklm_client, client_name, legacy_cert_aliases=(cert_alias,)
    )

    # Step 7 — assign certificate to the GKLM client
    gklm_client.clients.assign_users_to_generic_kmip_client(
        client_name, users=[gklm_user]
    )
    gklm_client.clients.assign_client_certificate(client_name, cert, cert_alias)

    # Step 8 — one AES symmetric key per LUKS combo entry
    key_id_map = {}
    for combo in config.get("luks_combos", []):
        sym = gklm_client.objects.create_symmetric_key_object(
            number_of_objects=1,
            client_name=client_name,
            alias_prefix_name=combo["key_id"],
            cryptoUsageMask="Encrypt,Decrypt",
        )
        key_id_map[combo["key_id"]] = sym["id"]
        LOG.info("Created GKLM key '%s' → uuid %s", combo["key_id"], sym["id"])

    # Step 9 — write PEM files to the first GW node
    create_in_file_certs(
        certs_dict={
            "kmip_cert": "|\n" + cert.rstrip("\\n"),
            "kmip_key": "|\n" + rsa_key.rstrip("\\n"),
            "kmip_ca_cert": "|\n" + re.sub("\r", "", ca_cert.rstrip("\\n")),
            "kmip_host_list": [gklm_hostname],
        },
        node=gw_node,
    )

    LOG.info(
        "GKLM setup complete: client=%s cert_alias=%s keys=%s",
        client_name,
        cert_alias,
        list(key_id_map.keys()),
    )
    return {
        "host": gklm_ip,
        "port": config.get("kmip_port", 5696),
        "server_name": gklm_hostname,
        "ca_cert_pem": ca_cert,
        "client_cert_pem": cert,
        "client_key_pem": rsa_key,
        # GKLM-specific teardown handles
        "gklm_rest_client": gklm_client,
        "gklm_client_name": client_name,
        "gklm_cert_alias": cert_alias,
        "key_id_map": key_id_map,
    }


# ---------------------------------------------------------------------------
# Cert distribution to GW nodes
# ---------------------------------------------------------------------------


def copy_kmip_certs_to_gw_nodes(ceph_cluster, kmip_cfg, nvme_service=None):
    """Copy CA, client cert, and client key to every GW node.

    Certs are written exclusively to ``/etc/kmip/<server_name>/`` on each GW
    node.  The NVMe-oF gateway spec must set ``kmip_cert_dir`` to point at this
    path (or a symlink to it) so the daemon container can read the files.

    Args:
        ceph_cluster: Ceph cluster object.
        kmip_cfg (dict): Must contain server_name, ca_cert_pem, client_cert_pem,
            client_key_pem.
        nvme_service: NVMeService instance whose gw_nodes will be used when
            provided; falls back to cluster nodes with role "nvmeof-gw".
    """
    server_name = kmip_cfg["server_name"]

    files = {
        "ca_cert.pem": kmip_cfg["ca_cert_pem"],
        "client_cert.pem": kmip_cfg["client_cert_pem"],
        "client_key.pem": kmip_cfg["client_key_pem"],
    }

    if nvme_service is not None and getattr(nvme_service, "gw_nodes", None):
        gw_nodes = nvme_service.gw_nodes
    else:
        gw_nodes = ceph_cluster.get_nodes("nvmeof-gw")
    cert_dir = f"/etc/kmip/{server_name}"

    for node in gw_nodes:
        LOG.info("Copying KMIP certs to %s:%s", node.hostname, cert_dir)
        node.exec_command(cmd=f"mkdir -p {cert_dir}", sudo=True)
        for filename, pem_content in files.items():
            dest = f"{cert_dir}/{filename}"
            node.exec_command(
                cmd=f"cat > {dest} << 'EOFPEM'\n{pem_content}\nEOFPEM",
                sudo=True,
            )
        node.exec_command(cmd=f"chmod 600 {cert_dir}/client_key.pem", sudo=True)
        LOG.info("KMIP certs installed on %s:%s", node.hostname, cert_dir)


# ---------------------------------------------------------------------------
# Unified public API  (used by the test module regardless of backend)
# ---------------------------------------------------------------------------


def setup_kmip_for_nvmeof(ceph_cluster, config, custom_data, nvme_service=None):
    """Set up a KMIP server for NVMe-oF BYOK tests.

    Selects the backend based on config["use_dummy_kmip"] (default: True).

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        custom_data (dict): kwargs["test_data"] from the cephci runner.
        nvme_service: Accepted for API compatibility; passed through to
            copy_kmip_certs_to_gw_nodes() but not used.

    Returns:
        dict: Unified kmip_info dict with keys:
          kmip_cfg  — {host, port, server_name, ca_cert_pem, ...}
          use_dummy — bool (so teardown_kmip() knows which path to take)
          + backend-specific teardown handles
    """
    use_dummy = config.get("use_dummy_kmip", True)
    if use_dummy:
        from ceph.utils import get_node_by_id

        server_image = config.get("kmip_server_image", _DEFAULT_KMIP_SERVER_IMAGE)
        cli_image = config.get("kmip_cli_image", _DEFAULT_KMIP_CLI_IMAGE)
        LOG.info(
            "KMIP backend: podman container (use_dummy_kmip=true) " "server=%s cli=%s",
            server_image,
            cli_image,
        )
        kmip_node = get_node_by_id(ceph_cluster, config["initiator_node"])
        kmip_cfg = setup_dummy_kmip_server(kmip_node, config)
        copy_kmip_certs_to_gw_nodes(ceph_cluster, kmip_cfg, nvme_service=nvme_service)
        return {"kmip_cfg": kmip_cfg, "use_dummy": True}
    else:
        LOG.info("KMIP backend: real GKLM appliance (use_dummy_kmip=false)")
        gklm_info = setup_gklm_for_nvmeof(ceph_cluster, config, custom_data)
        kmip_cfg = {
            k: gklm_info[k]
            for k in (
                "host",
                "port",
                "server_name",
                "ca_cert_pem",
                "client_cert_pem",
                "client_key_pem",
            )
        }
        gklm_info["kmip_cfg"] = kmip_cfg
        gklm_info["use_dummy"] = False
        copy_kmip_certs_to_gw_nodes(ceph_cluster, kmip_cfg, nvme_service=nvme_service)
        return gklm_info


def configure_kmip_endpoint_on_subsystem(gateway, nqn, kmip_cfg):
    """Register the KMIP server endpoint on a subsystem.

    Calls:
      ceph nvmeof subsystem add_kmip_server_endpoint <nqn>
          [server_name] [address] [port] ...

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        kmip_cfg (dict): kmip_info["kmip_cfg"] from setup_kmip_for_nvmeof().
    """
    LOG.info(
        "Registering KMIP endpoint %s:%s on subsystem %s",
        kmip_cfg["host"],
        kmip_cfg["port"],
        nqn,
    )
    out, err = gateway.subsystem.add_kmip_server_endpoint(
        **{
            "args": {
                "subsystem": nqn,
                "server_name": kmip_cfg["server_name"],
                "address": kmip_cfg["host"],
                "port": kmip_cfg["port"],
            }
        }
    )
    if err and "error" in err.lower():
        raise RuntimeError(f"KMIP endpoint registration failed: {err}")
    LOG.info("KMIP endpoint registered. out=%s", out)


# Key on cephci test_data for the suite-level dummy KMIP server.
SHARED_KMIP_KEY = "byok_shared_kmip"


def acquire_kmip_for_nvmeof(ceph_cluster, config, custom_data, nvme_service=None):
    """Return ``(kmip_info, owned)`` for a BYOK test.

    If the suite setup test stored a dummy KMIP handle on ``custom_data``,
    reuse it (``owned=False`` — caller must not tear it down). Otherwise start
    a private server (``owned=True``) so a single-TC run still works.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        custom_data (dict): kwargs["test_data"] from the cephci runner.
        nvme_service: Optional NVMeService for cert copy targeting.

    Returns:
        tuple: ``(kmip_info, owned)``
    """
    custom_data = custom_data or {}
    shared = custom_data.get(SHARED_KMIP_KEY)
    if shared:
        kmip_cfg = shared.get("kmip_cfg") or {}
        LOG.info(
            "Reusing suite-level KMIP server %s:%s",
            kmip_cfg.get("host"),
            kmip_cfg.get("port"),
        )
        return shared, False
    return (
        setup_kmip_for_nvmeof(
            ceph_cluster, config, custom_data, nvme_service=nvme_service
        ),
        True,
    )


def release_kmip_if_owned(kmip_info, owned):
    """Tear down KMIP only when this test started the server."""
    if owned and kmip_info:
        teardown_kmip(kmip_info)


def teardown_kmip(kmip_info):
    """Clean up KMIP resources — dummy server or GKLM objects.

    Args:
        kmip_info (dict): Returned by setup_kmip_for_nvmeof().
    """
    if kmip_info.get("use_dummy"):
        teardown_dummy_kmip_server(kmip_info["kmip_cfg"])
    else:
        from tests.nfs.byok.byok_tools import clean_up_gklm

        LOG.info(
            "Tearing down GKLM resources for client '%s'",
            kmip_info.get("gklm_client_name"),
        )
        clean_up_gklm(
            kmip_info["gklm_rest_client"],
            kmip_info["gklm_client_name"],
            kmip_info["gklm_cert_alias"],
        )


# ---------------------------------------------------------------------------
# Additional KMIP registration helpers (TC-02 and beyond)
# ---------------------------------------------------------------------------


def register_kmip_passphrases(node, key_entries, config):
    """Register named passphrases on an already-running KMIP server container.

    Uses the CLI container to call ``create-passphrase`` for each entry,
    records the returned UID back into each entry dict as ``"key_uid"``,
    and writes the raw passphrase to a temporary file on *node* so that
    ``rbd encryption format`` can consume it.

    Args:
        node: CephNode where the KMIP server container is already running
              (same as the initiator/client node).
        key_entries (list[dict]): Each dict must have a ``"key_id"`` string.
            After return each dict also has ``"key_uid"`` and
            ``"passphrase_file"`` (the remote path to the passphrase file).
        config (dict): Test config; used for ``kmip_cli_image``.

    Returns:
        list[dict]: The same ``key_entries`` list with ``key_uid`` and
            ``passphrase_file`` added to every element.
    """
    host_cert_dir = _DUMMY_KMIP_CERT_DIR
    cli_image = config.get("kmip_cli_image", _DEFAULT_KMIP_CLI_IMAGE)
    cli_run = (
        f"podman run --rm --network host "
        f"-v {host_cert_dir}:{host_cert_dir}:ro,Z "
        f"{cli_image} "
        f"--ca {host_cert_dir}/ca_cert.pem "
        f"--cert {host_cert_dir}/client_cert.pem "
        f"--key {host_cert_dir}/client_key.pem"
    )

    for entry in key_entries:
        key_name = entry["key_id"]
        passphrase = secrets.token_hex(32)

        out, _ = node.exec_command(
            cmd=(
                f"{cli_run} create-passphrase "
                f"--name {key_name} --value {passphrase}"
            ),
            sudo=True,
        )
        uid_match = re.search(r"uuid:\s*(\S+)", out)
        if not uid_match:
            raise RuntimeError(
                f"Could not parse uuid from create-passphrase output "
                f"for key '{key_name}':\n{out}"
            )
        uid = uid_match.group(1).rstrip(",")
        entry["key_uid"] = uid
        LOG.info("Registered KMIP passphrase name=%s uid=%s", key_name, uid)

        # Write the passphrase to a temp file so rbd encryption format can use it
        passphrase_file = f"/tmp/kmip-pp-{key_name}.txt"
        node.exec_command(
            cmd=f"echo -n '{passphrase}' > {passphrase_file} && chmod 600 {passphrase_file}",
            sudo=True,
        )
        entry["passphrase_file"] = passphrase_file
        LOG.info("Passphrase file written: %s", passphrase_file)

    return key_entries


# ---------------------------------------------------------------------------
# Multi-server KMIP helpers (TC-03: KMIP failover)
# ---------------------------------------------------------------------------

_KMIP_CONTAINER_2_NAME = "nvmeof-byok-kmip-2"
_DUMMY_KMIP_PORT_2 = 5697
_DUMMY_KMIP_CERT_DIR_2 = "/var/lib/kmip-data-2/certs"


def _create_kmip_passphrase(node, cli_run, key_name, passphrase):
    """Create a passphrase on a dummy KMIP server and return the assigned UID."""
    out, _ = node.exec_command(
        cmd=f"{cli_run} create-passphrase --name {key_name} --value {passphrase}",
        sudo=True,
    )
    uid_match = re.search(r"uuid:\s*(\S+)", out)
    if not uid_match:
        raise RuntimeError(
            f"Could not parse uuid from create-passphrase for key '{key_name}':\n{out}"
        )
    return uid_match.group(1).rstrip(",")


def setup_second_kmip_server(
    node, config, shared_passphrase_map, server_name=None, uid_map=None
):
    """Start a second KMIP server container on *node* and load the same key material.

    The second server runs on a different port (default 5697) and uses a
    separate cert directory so it does not conflict with KMIP-1.  The caller
    passes *shared_passphrase_map* — a ``{key_id: passphrase}`` dict — so
    that the exact same passphrases are registered on both servers.  This is
    required because the RBD image was already formatted using the passphrase
    from KMIP-1; KMIP-2 must return the identical value when the GW fails over.

    Dummy KMIP assigns sequential integer UIDs starting at 1.  When KMIP-1 is
    the suite-level shared server it may already hold other keys, so the
    failover passphrase can be UID 7 while a fresh KMIP-2 would assign UID 1.
    Pass *uid_map* so KMIP-2 is padded until each key lands on the same UID
    the gateway will request.

    Args:
        node: CephNode where the second container will run (same as KMIP-1 node).
        config (dict): Test config; used for ``kmip_server_image`` /
            ``kmip_cli_image``.
        shared_passphrase_map (dict): ``{key_id (str): passphrase (str)}``
            mapping produced when KMIP-1 was set up.  Every key present in
            KMIP-1 must appear here so the failover server holds identical
            material.
        server_name (str|None): Logical name for this server endpoint
            (used as the KMIP ``server_name`` in the subsystem config and as
            the cert sub-directory name).  Defaults to the node hostname with
            ``-kmip2`` suffix.
        uid_map (dict|None): ``{key_id (str): uid (str)}`` from KMIP-1. When
            set, dummy keys are created first so each real key gets the same
            UID on KMIP-2.

    Returns:
        dict: ``kmip_cfg`` compatible with
            ``configure_kmip_endpoint_on_subsystem()`` and
            ``copy_kmip_certs_to_gw_nodes()``.  Also contains:
            ``dummy_node``        — node reference for stop/start helpers
            ``dummy_cert_dir``    — cert directory on the node
            ``container_name``    — podman container name
    """
    port = config.get("kmip_port_2", _DUMMY_KMIP_PORT_2)
    server_image = config.get("kmip_server_image", _DEFAULT_KMIP_SERVER_IMAGE)
    cli_image = config.get("kmip_cli_image", _DEFAULT_KMIP_CLI_IMAGE)
    host_cert_dir = config.get("kmip_cert_dir_2", _DUMMY_KMIP_CERT_DIR_2)
    container_name = _KMIP_CONTAINER_2_NAME
    server_name = server_name or f"{node.hostname}-kmip2"
    container_cert_dir = "/kmip/certs"

    # Build the base CLI invocation for KMIP-2.  --hostname / --port / cert
    # flags are top-level args (before the subcommand) per kmip_cli.py usage.
    # The server is on a different node, so we must specify its IP and certs
    # explicitly (the image defaults to localhost / /kmip/certs).
    cli_run = (
        f"podman run --rm --network host "
        f"-v {host_cert_dir}:{host_cert_dir}:ro,Z "
        f"{cli_image} "
        f"--hostname {node.ip_address} --port {port} "
        f"--ca {host_cert_dir}/ca_cert.pem "
        f"--cert {host_cert_dir}/client_cert.pem "
        f"--key {host_cert_dir}/client_key.pem"
    )

    LOG.info(
        "Setting up second KMIP server container on %s (port %d, name=%s)",
        node.hostname,
        port,
        container_name,
    )

    # ── 1. Start second KMIP server container ───────────────────────────────
    node.exec_command(
        cmd=f"podman rm -f {container_name} 2>/dev/null || true",
        sudo=True,
    )
    node.exec_command(
        cmd=(
            f"podman run -d --name {container_name} --network host "
            f"-e KMIP_PORT={port} "
            f"{server_image}"
        ),
        sudo=True,
    )

    # ── 2. Wait for port to be listening (max 60 s) ──────────────────────────
    LOG.info("Waiting for second KMIP server on port %d", port)
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            out, _ = node.exec_command(
                cmd=f"ss -tlnp | grep :{port} || true", sudo=True
            )
            if str(port) in out:
                LOG.info("Second KMIP server listening on port %d", port)
                break
        except Exception:
            pass
        time.sleep(3)
    else:
        log_out, _ = node.exec_command(
            cmd=f"podman logs {container_name} 2>&1 | tail -30", sudo=True
        )
        raise RuntimeError(
            f"Second KMIP server container did not start within 60 s "
            f"on {node.hostname}.\n--- container logs ---\n{log_out}"
        )

    # ── 2b. Open port in firewalld on the KMIP node (all zones) ──────────────
    LOG.info(
        "Opening firewall port %d/tcp on KMIP node %s (second server, all zones)",
        port,
        node.hostname,
    )
    try:
        fw_active, _ = node.exec_command(
            cmd="systemctl is-active firewalld || true", sudo=True
        )
        if fw_active.strip() == "active":
            zones_out, _ = node.exec_command(
                cmd="firewall-cmd --get-active-zones | awk 'NF == 1 || / \\(/{print $1}' || true",
                sudo=True,
            )
            active_zones = []
            for z in zones_out.splitlines():
                z = z.strip().split()[0] if z.strip() else ""
                if z:
                    active_zones.append(z)
            if not active_zones:
                active_zones = ["public"]
            for zone in active_zones:
                try:
                    node.exec_command(
                        cmd=f"firewall-cmd --permanent --zone={zone} --add-port={port}/tcp",
                        sudo=True,
                    )
                    LOG.info(
                        "firewalld: port %d/tcp opened in zone '%s' on KMIP node %s (second server)",
                        port,
                        zone,
                        node.hostname,
                    )
                except Exception as ze:
                    LOG.warning(
                        "firewalld: could not open port %d in zone '%s' on %s: %s",
                        port,
                        zone,
                        node.hostname,
                        ze,
                    )
            node.exec_command(cmd="firewall-cmd --reload", sudo=True)
            node.exec_command(
                cmd=(
                    f"nft add rule inet filter input tcp dport {port} accept 2>/dev/null || "
                    f"iptables -C INPUT -p tcp --dport {port} -j ACCEPT 2>/dev/null || "
                    f"iptables -I INPUT 1 -p tcp --dport {port} -j ACCEPT 2>/dev/null || true"
                ),
                sudo=True,
            )
            LOG.info(
                "nft/iptables: port %d/tcp accepted as fallback on KMIP node %s (second server)",
                port,
                node.hostname,
            )
        else:
            node.exec_command(
                cmd=(
                    f"nft add rule inet filter input tcp dport {port} accept 2>/dev/null || "
                    f"iptables -C INPUT -p tcp --dport {port} -j ACCEPT 2>/dev/null || "
                    f"iptables -I INPUT 1 -p tcp --dport {port} -j ACCEPT 2>/dev/null || true"
                ),
                sudo=True,
            )
            LOG.info(
                "nft/iptables: port %d/tcp accepted on KMIP node %s (second server)",
                port,
                node.hostname,
            )
    except Exception as exc:
        LOG.warning(
            "Could not open firewall port %d on %s: %s (continuing)",
            port,
            node.hostname,
            exc,
        )

    # ── 3. Copy certs from container to dedicated host dir ───────────────────
    LOG.info("Copying certs from container %s to %s", container_name, host_cert_dir)
    node.exec_command(cmd=f"mkdir -p {host_cert_dir}", sudo=True)
    node.exec_command(
        cmd=(f"podman cp " f"{container_name}:{container_cert_dir}/. {host_cert_dir}/"),
        sudo=True,
    )

    # ── 4. Register the SAME passphrases so failover returns identical bytes ─
    LOG.info(
        "Registering %d shared passphrases on second KMIP server (port %d)",
        len(shared_passphrase_map),
        port,
    )
    items = list(shared_passphrase_map.items())
    if uid_map:
        items.sort(key=lambda kv: int(uid_map[kv[0]]))
    next_uid = 1
    for key_name, passphrase in items:
        desired_uid = (
            int(uid_map[key_name]) if uid_map and key_name in uid_map else None
        )
        if desired_uid is not None:
            while next_uid < desired_uid:
                pad_name = f"byok-uid-pad-{next_uid}"
                pad_uid = _create_kmip_passphrase(
                    node, cli_run, pad_name, secrets.token_hex(32)
                )
                LOG.info(
                    "KMIP-2 UID pad: name=%s uid=%s (aligning to %s)",
                    pad_name,
                    pad_uid,
                    desired_uid,
                )
                next_uid = int(pad_uid) + 1
        uid = _create_kmip_passphrase(node, cli_run, key_name, passphrase)
        if desired_uid is not None and int(uid) != desired_uid:
            raise RuntimeError(
                f"KMIP-2 assigned uid={uid} for '{key_name}', expected {desired_uid}"
            )
        next_uid = int(uid) + 1
        LOG.info(
            "KMIP-2 passphrase registered: name=%s uid=%s (port %d)",
            key_name,
            uid,
            port,
        )

    # ── 5. Read certs from host path ─────────────────────────────────────────
    def _read_cert(filename):
        out, _ = node.exec_command(cmd=f"cat {host_cert_dir}/{filename}", sudo=True)
        return out

    return {
        "host": node.ip_address,
        "port": port,
        "server_name": server_name,
        "ca_cert_pem": _read_cert("ca_cert.pem"),
        "client_cert_pem": _read_cert("client_cert.pem"),
        "client_key_pem": _read_cert("client_key.pem"),
        "dummy_node": node,
        "dummy_cert_dir": host_cert_dir,
        "container_name": container_name,
    }


def stop_kmip_container(node, container_name):
    """Pause a KMIP server container to simulate the server going offline.

    Uses ``podman stop`` rather than ``podman rm`` so the container can be
    resumed with :func:`start_kmip_container`.  Cert files on the GW nodes
    are left untouched — the subsystem endpoint registration is also
    preserved; only the TCP port becomes unreachable.

    Args:
        node: CephNode where the container is running.
        container_name (str): Podman container name to stop.
    """
    LOG.info("Stopping KMIP container '%s' on %s", container_name, node.hostname)
    node.exec_command(
        cmd=f"podman stop {container_name} 2>/dev/null || true",
        sudo=True,
    )
    LOG.info("KMIP container '%s' stopped (simulating server failure).", container_name)


def start_kmip_container(node, container_name, port):
    """Resume a previously stopped KMIP server container.

    Waits up to 60 s for the port to become reachable again before returning.

    Args:
        node: CephNode where the container is stopped.
        container_name (str): Podman container name to start.
        port (int): TCP port the server listens on (used for readiness check).

    Raises:
        RuntimeError: If the server does not become reachable within 60 s.
    """
    LOG.info(
        "Starting KMIP container '%s' on %s (port %d)",
        container_name,
        node.hostname,
        port,
    )
    node.exec_command(
        cmd=f"podman start {container_name}",
        sudo=True,
    )

    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            out, _ = node.exec_command(
                cmd=f"ss -tlnp | grep :{port} || true", sudo=True
            )
            if str(port) in out:
                LOG.info(
                    "KMIP container '%s' is listening on port %d again.",
                    container_name,
                    port,
                )
                return
        except Exception:
            pass
        time.sleep(3)

    raise RuntimeError(
        f"KMIP container '{container_name}' did not become reachable on port "
        f"{port} within 60 s after start on {node.hostname}."
    )


def extract_passphrase_map(kmip_cfg, key_entries):
    """Build a ``{key_id: passphrase}`` map from a dummy-KMIP setup result.

    After :func:`register_kmip_passphrases` is called, each entry in
    *key_entries* has a ``passphrase_file`` field pointing to a file on the
    node.  This helper reads each file back so the caller can pass the raw
    passphrase strings to :func:`setup_second_kmip_server`.

    Args:
        kmip_cfg (dict): ``kmip_cfg`` from the first KMIP setup call; used to
            access ``dummy_node``.
        key_entries (list[dict]): Updated key_entries list (with
            ``passphrase_file`` added by ``register_kmip_passphrases``).

    Returns:
        dict: ``{key_id (str): passphrase (str)}``
    """
    node = kmip_cfg["dummy_node"]
    passphrase_map = {}
    for entry in key_entries:
        key_id = entry["key_id"]
        pp_file = entry.get("passphrase_file")
        if not pp_file:
            raise ValueError(
                f"key_entries entry for '{key_id}' has no 'passphrase_file'; "
                "call register_kmip_passphrases() first."
            )
        out, _ = node.exec_command(cmd=f"cat {pp_file}", sudo=True)
        passphrase_map[key_id] = out.strip()
    LOG.info("Extracted passphrase map for %d keys.", len(passphrase_map))
    return passphrase_map


# ---------------------------------------------------------------------------
# Network-block helpers for GW nodes (TC-04: negative error paths)
# ---------------------------------------------------------------------------


def block_kmip_on_gw_nodes(ceph_cluster, kmip_host, kmip_port):
    """Block outbound KMIP traffic from every GW node.

    On RHEL 10 the GW daemons run inside cephadm containers that share the
    host network namespace (``--network host``).  Outgoing connections from
    these processes traverse the kernel's OUTPUT chain, **not** the FORWARD
    chain where ``firewall-cmd`` rich-rules are inserted.  To reliably drop
    the GW → KMIP packets we therefore insert a direct ``nft`` rule into the
    OUTPUT chain, regardless of whether firewalld is active.

    The handle assigned to the new rule is retrieved immediately after insertion
    (``nft -a list chain …``) and stored in a temp file on the node so that
    :func:`unblock_kmip_on_gw_nodes` can delete the rule precisely without
    relying on any ``comment`` syntax (which is not supported inside nft rule
    bodies on all RHEL versions).

    Args:
        ceph_cluster: Ceph cluster object.
        kmip_host (str): IP address of the KMIP server.
        kmip_port (int): TCP port of the KMIP server.
    """
    gw_nodes = ceph_cluster.get_nodes("nvmeof-gw")
    handle_file = f"/tmp/byok-nft-{kmip_host}-{kmip_port}.handle"

    # Ensure the inet filter table and output chain exist (nftables may not
    # create them by default on RHEL 10).  'add' is idempotent.
    ensure_table = "nft add table inet filter 2>/dev/null || true"
    ensure_chain = (
        "nft add chain inet filter output "
        "'{ type filter hook output priority 0; policy accept; }' "
        "2>/dev/null || true"
    )
    # Insert the drop rule (no comment — not valid nft rule syntax on RHEL 10).
    add_rule = (
        f"nft add rule inet filter output "
        f"ip daddr {kmip_host} tcp dport {kmip_port} drop"
    )
    # After insertion, retrieve the handle of the rule we just added by
    # matching on the ip daddr / tcp dport values in the -a listing.
    # The handle is the last field on each matching line.
    fetch_handle = (
        f"nft -a list chain inet filter output 2>/dev/null | "
        f"grep 'daddr {kmip_host}' | grep 'dport {kmip_port}' | "
        f"awk '{{print $NF}}' | tail -1 | tee {handle_file}"
    )

    for node in gw_nodes:
        LOG.info(
            "Blocking KMIP %s:%s on GW node %s via nft OUTPUT rule",
            kmip_host,
            kmip_port,
            node.hostname,
        )
        node.exec_command(cmd=ensure_table, sudo=True, check_ec=False)
        node.exec_command(cmd=ensure_chain, sudo=True, check_ec=False)
        node.exec_command(cmd=add_rule, sudo=True)
        out, _ = node.exec_command(cmd=fetch_handle, sudo=True, check_ec=False)
        handle = out.strip() if out else ""
        if handle:
            LOG.info(
                "nft rule handle %s saved to %s on %s",
                handle,
                handle_file,
                node.hostname,
            )
        else:
            LOG.warning(
                "Could not retrieve nft rule handle on %s — unblock may need manual cleanup",
                node.hostname,
            )

    LOG.info(
        "KMIP %s:%s blocked on %d GW node(s).", kmip_host, kmip_port, len(gw_nodes)
    )


def unblock_kmip_on_gw_nodes(ceph_cluster, kmip_host, kmip_port):
    """Remove the KMIP block installed by :func:`block_kmip_on_gw_nodes`.

    Reads the handle stored by :func:`block_kmip_on_gw_nodes` from the temp
    file on each GW node and deletes the rule by handle number.  Falls back to
    a grep-based scan of the chain when the file is absent.  Uses
    ``check_ec=False`` throughout so cleanup never raises even if the rule is
    already gone.

    Args:
        ceph_cluster: Ceph cluster object.
        kmip_host (str): IP address of the KMIP server.
        kmip_port (int): TCP port of the KMIP server.
    """
    gw_nodes = ceph_cluster.get_nodes("nvmeof-gw")
    handle_file = f"/tmp/byok-nft-{kmip_host}-{kmip_port}.handle"

    # Primary path: delete by stored handle then remove the temp file.
    delete_by_file = (
        f"handle=$(cat {handle_file} 2>/dev/null) && "
        f'[ -n "$handle" ] && '
        f"nft delete rule inet filter output handle $handle && "
        f"rm -f {handle_file}"
    )
    # Fallback: scan the chain for rules matching ip daddr / tcp dport and
    # delete every matching handle (handles the case where the file was lost).
    delete_by_scan = (
        f"nft -a list chain inet filter output 2>/dev/null | "
        f"grep 'daddr {kmip_host}' | grep 'dport {kmip_port}' | "
        f"awk '{{print $NF}}' | "
        f"xargs -r -I{{}} nft delete rule inet filter output handle {{}}"
    )

    for node in gw_nodes:
        LOG.info(
            "Unblocking KMIP %s:%s on GW node %s",
            kmip_host,
            kmip_port,
            node.hostname,
        )
        out, err = node.exec_command(cmd=delete_by_file, sudo=True, check_ec=False)
        # If the primary path failed (file absent, rule already gone, etc.)
        # run the fallback scan to be safe.
        node.exec_command(cmd=delete_by_scan, sudo=True, check_ec=False)

    LOG.info(
        "KMIP %s:%s unblocked on %d GW node(s).", kmip_host, kmip_port, len(gw_nodes)
    )


def rotate_kmip_passphrase(node, key_id, config):
    """Register a brand-new passphrase under the same *key_id* name on KMIP.

    The KMIP ``create-passphrase`` call with an existing name creates a new
    object; the server returns a new UID.  After this call, when the GW
    queries the KMIP server for the passphrase associated with ``key_id``, it
    will receive the **new** passphrase (key B).  The RBD image was formatted
    with the **old** passphrase (key A), so any attempt to open it will fail
    at the librbd LUKS header verification step.

    Args:
        node: CephNode where the KMIP server container is running.
        key_id (str): The ``key_id`` name already present on KMIP
            (same name that was used when creating the namespace).
        config (dict): Test config; used for ``kmip_cli_image``.

    Returns:
        tuple[str, str]: ``(new_uid, new_passphrase)`` — the UID assigned by
            the KMIP server for the rotated key and the new raw passphrase
            (different from the one in the LUKS header).
    """
    host_cert_dir = _DUMMY_KMIP_CERT_DIR
    cli_image = config.get("kmip_cli_image", _DEFAULT_KMIP_CLI_IMAGE)
    cli_run = (
        f"podman run --rm --network host "
        f"-v {host_cert_dir}:{host_cert_dir}:ro,Z "
        f"{cli_image} "
        f"--ca {host_cert_dir}/ca_cert.pem "
        f"--cert {host_cert_dir}/client_cert.pem "
        f"--key {host_cert_dir}/client_key.pem"
    )

    new_passphrase = secrets.token_hex(32)  # deliberately different from original
    LOG.info(
        "Rotating passphrase for key '%s' — registering new passphrase on KMIP",
        key_id,
    )
    out, _ = node.exec_command(
        cmd=(
            f"{cli_run} create-passphrase " f"--name {key_id} --value {new_passphrase}"
        ),
        sudo=True,
    )
    uid_match = re.search(r"uuid:\s*(\S+)", out)
    if not uid_match:
        raise RuntimeError(
            f"rotate_kmip_passphrase: could not parse uuid from output "
            f"for key '{key_id}':\n{out}"
        )
    new_uid = uid_match.group(1).rstrip(",")
    LOG.info(
        "Passphrase rotated for key '%s': new uid=%s (old passphrase now superseded)",
        key_id,
        new_uid,
    )
    return new_uid, new_passphrase
