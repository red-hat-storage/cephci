"""Post-upgrade CephX key rotation workflow for RBD mirror upgrade tests."""

import base64
import json

from ceph.rbd.utils import exec_cmd, getdict
from ceph.rbd.workflows.rbd_mirror import wait_for_status
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)

AES256K_CIPHER = "aes256k"
LEGACY_CIPHER = "aes"
RBD_PEER_CLIENT = "client.rbd-mirror-peer"
RBD_MIRROR_DAEMON_PREFIX = "client.rbd-mirror."


def cipher_names_from_mon_dump(mon_dump):
    """Return allowed cipher names from a ``ceph mon dump --format json`` payload."""
    ciphers = mon_dump.get("auth_allowed_ciphers", [])
    names = []
    for cipher in ciphers:
        if isinstance(cipher, dict):
            name = cipher.get("name")
            if name:
                names.append(name)
        elif cipher:
            names.append(str(cipher))
    return names


def is_rbd_cephx_entity(entity_name):
    """Return True when *entity_name* is an RBD mirror CephX identity."""
    return entity_name == RBD_PEER_CLIENT or entity_name.startswith(
        RBD_MIRROR_DAEMON_PREFIX
    )


def rbd_mirror_daemon_id_from_entity(entity_name):
    """Return the cephadm daemon id for ``client.rbd-mirror.<daemon-id>``."""
    if not entity_name.startswith(RBD_MIRROR_DAEMON_PREFIX):
        return None
    return entity_name[len(RBD_MIRROR_DAEMON_PREFIX) :]


def entity_name_from_dump_entry(entry):
    """Build a Ceph entity name from an auth dump-keys secret entry."""
    entity = entry.get("entity", {})
    type_str = entity.get("type_str", "")
    entity_id = entity.get("id", "")
    if not type_str or not entity_id:
        return None
    return f"{type_str}.{entity_id}"


def key_type_from_dump_entry(entry):
    """Return the key cipher type string for an auth dump-keys secret entry."""
    auth = entry.get("auth", {})
    key_info = auth.get("key", {})
    return key_info.get("type_str")


def parse_auth_dump_keys(dump_payload):
    """Return ``{entity_name: key_type}`` from ``ceph auth dump-keys`` JSON."""
    if not dump_payload:
        return {}

    data = dump_payload
    if isinstance(data, str):
        data = json.loads(data)

    if "data" in data:
        data = data["data"]

    secrets = data.get("secrets", [])
    entities = {}
    for entry in secrets:
        entity_name = entity_name_from_dump_entry(entry)
        if not entity_name:
            continue
        entities[entity_name] = key_type_from_dump_entry(entry)
    return entities


def legacy_rbd_entities_from_key_types(entities):
    """Classify RBD entities from dump-keys into legacy and unknown key types."""
    legacy = []
    unknown = []
    for entity_name, key_type in sorted(entities.items()):
        if not is_rbd_cephx_entity(entity_name):
            continue
        if key_type == LEGACY_CIPHER:
            legacy.append(entity_name)
        elif key_type == AES256K_CIPHER:
            continue
        else:
            unknown.append(entity_name)
            log.warning(
                "RBD CephX entity %s has unsupported/unknown key type %s; "
                "skipping automatic rotation",
                entity_name,
                key_type,
            )
    return legacy, unknown


def parse_insecure_entities_from_health_detail(health_detail):
    """Return RBD mirror entities flagged by AUTH_INSECURE_CLIENT_KEY_TYPE."""
    legacy = []
    for line in health_detail.splitlines():
        line = line.strip()
        if not line.startswith("entity "):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        entity_name = parts[1]
        if is_rbd_cephx_entity(entity_name):
            legacy.append(entity_name)
    return legacy


def _log_non_rbd_insecure_entities(health_detail):
    """Log insecure entities that are outside the RBD CephX migration scope."""
    for line in health_detail.splitlines():
        line = line.strip()
        if not line.startswith("entity "):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        entity_name = parts[1]
        if not is_rbd_cephx_entity(entity_name):
            log.info("%s is outside RBD CephX migration scope", entity_name)


def rotation_mode_enabled(mode):
    """Return True when the configured rotation mode allows automatic rotation."""
    if mode is True:
        return True
    if mode in (False, None, "off", "none", "skip", "disabled"):
        return False
    return str(mode).lower() in ("auto", "on", "enabled", "true", "yes")


def rbd_cephx_rotation_required_from_state(
    supports_aes256k, legacy_entities, mode="auto"
):
    """Pure decision helper for whether RBD CephX rotation should run."""
    if not rotation_mode_enabled(mode):
        return False
    if not supports_aes256k:
        return False
    return bool(legacy_entities)


def _run_ceph_json(client, cmd):
    """Run a ceph JSON command on a cluster client/installer node."""
    out = exec_cmd(node=client, cmd=cmd, output=True, check_ec=False)
    if client.exit_status != 0:
        raise RuntimeError(f"Command failed (rc={client.exit_status}): {cmd}\n{out}")
    return json.loads(out)


def target_supports_aes256k(client):
    """Detect whether the cluster allows the aes256k CephX cipher at runtime."""
    try:
        mon_dump = _run_ceph_json(client, "ceph mon dump --format json")
    except (RuntimeError, json.JSONDecodeError) as exc:
        log.warning("Unable to determine aes256k capability: %s", exc)
        return False

    cipher_names = cipher_names_from_mon_dump(mon_dump)
    supports = AES256K_CIPHER in cipher_names
    log.info(
        "Target supports aes256k: %s (allowed ciphers: %s)", supports, cipher_names
    )
    return supports


def _load_auth_entities_from_dump_keys(client):
    """Return entity key types from dump-keys, or None when unavailable."""
    dump_out = exec_cmd(
        node=client,
        cmd="ceph auth dump-keys --format json",
        output=True,
        check_ec=False,
    )
    if client.exit_status != 0 or not dump_out:
        return None
    try:
        return parse_auth_dump_keys(dump_out)
    except json.JSONDecodeError as exc:
        log.warning("Failed to parse auth dump-keys JSON: %s", exc)
        return None


def get_legacy_rbd_cephx_entities(client):
    """Return RBD mirror entities confirmed to use legacy ``aes`` keys."""
    health_detail = exec_cmd(node=client, cmd="ceph health detail", output=True)
    _log_non_rbd_insecure_entities(health_detail)

    entities = _load_auth_entities_from_dump_keys(client)
    if entities is not None:
        legacy, unknown = legacy_rbd_entities_from_key_types(entities)
        for entity_name, key_type in sorted(entities.items()):
            if not is_rbd_cephx_entity(entity_name):
                continue
            if key_type == AES256K_CIPHER:
                log.info("%s already uses aes256k; skipping", entity_name)
        for entity_name in legacy:
            if entity_name == RBD_PEER_CLIENT:
                log.info("client.rbd-mirror-peer uses legacy aes; rotation required")
            else:
                log.info("%s uses legacy aes; rotation required", entity_name)
        return legacy

    log.warning("auth dump-keys unavailable; using health detail fallback for RBD")
    legacy = parse_insecure_entities_from_health_detail(health_detail)
    for entity_name in legacy:
        log.info("Legacy RBD CephX credential detected (health): %s", entity_name)
    if not legacy:
        log.warning("Cannot determine RBD CephX key types; skipping rotation")
    return legacy


def rbd_cephx_rotation_required(client, mode="auto"):
    """Return True when post-upgrade RBD CephX rotation should be performed."""
    supports_aes256k = target_supports_aes256k(client)
    legacy_entities = get_legacy_rbd_cephx_entities(client) if supports_aes256k else []
    required = rbd_cephx_rotation_required_from_state(
        supports_aes256k, legacy_entities, mode
    )
    if not rotation_mode_enabled(mode):
        log.info("CephX rotation disabled by configuration (mode=%s)", mode)
    elif not supports_aes256k:
        log.info("Target does not support aes256k; skipping RBD CephX rotation")
    elif not legacy_entities:
        log.info("RBD credentials already use aes256k; skipping CephX rotation")
    else:
        log.info("RBD CephX rotation required: True")
    return required


def discover_mirrored_pools(pool_types, config):
    """Discover mirrored pool names from upgrade test configuration."""
    pools = []
    for pool_type in pool_types:
        pool_config = getdict(config.get(pool_type, {}))
        pools.extend(pool_config.keys())
    return pools


def _parse_pool_info(pool_info_raw):
    if isinstance(pool_info_raw, str):
        return json.loads(pool_info_raw)
    return pool_info_raw


def _strip_cephadm_preamble(text):
    """Drop cephadm log lines that precede JSON CLI output."""
    if not text:
        return text
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            return "\n".join(lines[idx:])
    return text


def _cephadm_noise_only(stderr):
    """Return True when stderr only contains cephadm preamble lines."""
    if not stderr or not stderr.strip():
        return True
    for line in stderr.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Inferring "):
            continue
        if stripped.startswith("Using ceph image"):
            continue
        if stripped.startswith("cp.icr.io") or stripped.startswith("preprod.icr.io"):
            continue
        return False
    return True


def capture_mirror_pool_state(rbd, pool_name):
    """Capture ``rbd mirror pool info --all`` state for a pool."""
    out, err = rbd.mirror.pool.info(pool=pool_name, format="json", all=True)
    if err and not _cephadm_noise_only(err):
        raise RuntimeError(f"Failed to capture mirror pool info for {pool_name}: {err}")
    payload = _strip_cephadm_preamble(out) or _strip_cephadm_preamble(err)
    if not payload:
        raise RuntimeError(
            f"Failed to capture mirror pool info for {pool_name}: empty output"
        )
    return _parse_pool_info(payload)


def capture_mirror_pool_states(rbd_primary, rbd_secondary, pool_names):
    """Capture mirror pool metadata from both clusters before rotation."""
    baseline = {}
    for pool_name in pool_names:
        baseline[pool_name] = {
            "primary": capture_mirror_pool_state(rbd_primary, pool_name),
            "secondary": capture_mirror_pool_state(rbd_secondary, pool_name),
        }
    return baseline


def _auth_key_value(auth_get_output):
    for line in auth_get_output.splitlines():
        stripped = line.strip()
        if stripped.startswith("key"):
            return stripped.split("=", 1)[1].strip()
    return None


def rotate_rbd_cephx_credentials(client, entities):
    """Rotate the given RBD CephX entities to ``aes256k``."""
    rotated = []
    for entity in entities:
        before = _auth_key_value(
            exec_cmd(node=client, cmd=f"ceph auth get {entity}", output=True)
        )
        log.info("Rotating %s", entity)
        exec_cmd(
            node=client,
            cmd=f"ceph auth rotate --key-type={AES256K_CIPHER} {entity}",
        )
        after = _auth_key_value(
            exec_cmd(node=client, cmd=f"ceph auth get {entity}", output=True)
        )
        if before and before == after:
            raise RuntimeError(f"CephX key for {entity} did not change after rotation")
        rotated.append(entity)
    return rotated


def _write_bootstrap_token(rbd, token, token_path):
    encoded = base64.b64encode(token.encode()).decode()
    cmd = f"echo {encoded} | base64 -d > {token_path}"
    rbd.execute_as_sudo(cmd=cmd)


def refresh_existing_rbd_mirror_peers(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    pool_names,
):
    """Refresh peer credentials with one bootstrap create/import per pool."""
    for pool_name in pool_names:
        log.info("Refreshing peer credential for pool %s", pool_name)
        token_path = f"/root/rbd_peer_bootstrap_{pool_name}"
        primary_token = rbd_primary.mirror.pool.peer.bootstrap.create(
            pool=pool_name,
        )[0].strip()
        _write_bootstrap_token(rbd_primary, primary_token, token_path)
        encoded = base64.b64encode(primary_token.encode()).decode()
        import_cmd = (
            f"echo {encoded} | base64 -d | "
            f"rbd mirror pool peer bootstrap import {pool_name} - --direction rx-tx"
        )
        rbd_secondary.execute_as_sudo(cmd=import_cmd)


def _daemon_running_after_redeploy(client, daemon_id):
    ps_out, ps_err = client.exec_command(
        cmd=f"ceph orch ps --daemon-id {daemon_id} --format json",
        sudo=True,
        check_ec=False,
    )
    if ps_err or client.exit_status != 0:
        return False
    try:
        daemons = json.loads(ps_out)
    except (json.JSONDecodeError, TypeError):
        return False
    if not daemons:
        return False
    status_desc = daemons[0].get("status_desc", "")
    return "running" in status_desc


def redeploy_rbd_mirror_daemons(client, mirror_daemon_entities):
    """Redeploy rotated rbd-mirror daemons so local keyrings pick up new keys."""
    daemon_names = []
    for entity_name in mirror_daemon_entities:
        daemon_id = rbd_mirror_daemon_id_from_entity(entity_name)
        if not daemon_id:
            continue
        daemon_names.append(f"rbd-mirror.{daemon_id}")

    if not daemon_names:
        return

    for daemon_name in daemon_names:
        log.info("Redeploying %s", daemon_name)
        client.exec_command(
            cmd=f"ceph orch daemon redeploy {daemon_name}",
            sudo=True,
        )

    for daemon_name in daemon_names:
        daemon_id = daemon_name.split(".", 1)[1]
        for waiter in WaitUntil(300, 15):
            if _daemon_running_after_redeploy(client, daemon_id):
                log.info("%s is running after redeploy", daemon_name)
                break
        else:
            raise RuntimeError(f"{daemon_name} did not reach running state")


def _peer_identity_unchanged(before_info, after_info):
    if before_info.get("site_name") != after_info.get("site_name"):
        return False, "site name changed"
    if before_info.get("mirror_uuid") != after_info.get("mirror_uuid"):
        return False, "mirror UUID changed"

    before_peers = before_info.get("peers") or []
    after_peers = after_info.get("peers") or []
    if len(before_peers) != len(after_peers):
        return False, "peer count changed"

    for before_peer, after_peer in zip(before_peers, after_peers):
        if before_peer.get("uuid") != after_peer.get("uuid"):
            return False, "peer UUID changed"
        if before_peer.get("name") != after_peer.get("name"):
            return False, "peer name changed"
        if before_peer.get("direction") != after_peer.get("direction"):
            return False, "peer direction changed"
        if not after_peer.get("client_name"):
            return False, "peer client missing"
        if not (after_peer.get("mon_host") or after_peer.get("mon_hosts")):
            return False, "peer mon host missing"
        if not after_peer.get("key"):
            return False, "peer key missing"
    return True, ""


def verify_rbd_mirroring(
    rbd_primary,
    rbd_secondary,
    cluster_primary_name,
    cluster_secondary_name,
    pool_names,
    baseline,
):
    """Verify mirror metadata and health after CephX rotation."""
    for pool_name in pool_names:
        primary_after = capture_mirror_pool_state(rbd_primary, pool_name)
        secondary_after = capture_mirror_pool_state(rbd_secondary, pool_name)
        pool_baseline = baseline[pool_name]

        ok, reason = _peer_identity_unchanged(pool_baseline["primary"], primary_after)
        if not ok:
            raise RuntimeError(
                f"Primary mirror pool {pool_name} identity check failed: {reason}"
            )

        ok, reason = _peer_identity_unchanged(
            pool_baseline["secondary"], secondary_after
        )
        if not ok:
            raise RuntimeError(
                f"Secondary mirror pool {pool_name} identity check failed: {reason}"
            )

        log.info("RBD mirror peer identity preserved for pool %s", pool_name)

        wait_for_status(
            rbd=rbd_primary,
            cluster_name=cluster_primary_name,
            poolname=pool_name,
            health_pattern="OK",
        )
        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=cluster_secondary_name,
            poolname=pool_name,
            health_pattern="OK",
        )

    log.info("RBD mirroring healthy after CephX migration")


def get_cephx_rotation_mode(config):
    """Read the configured CephX rotation mode from test config."""
    rbd_mirror_config = config.get("rbd_mirror", {})
    if isinstance(rbd_mirror_config, dict) and "cephx_rotation" in rbd_mirror_config:
        return rbd_mirror_config.get("cephx_rotation", "auto")
    return config.get("cephx_rotation", "auto")


def _rotated_mirror_daemon_entities(legacy_entities):
    return [
        entity
        for entity in legacy_entities
        if entity.startswith(RBD_MIRROR_DAEMON_PREFIX)
    ]


def post_upgrade_rbd_cephx_workflow(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    cluster_primary_name,
    cluster_secondary_name,
    pool_types,
    config,
):
    """Run post-upgrade RBD CephX rotation when required on both mirror clusters."""
    try:
        mode = get_cephx_rotation_mode(config)
        if not rotation_mode_enabled(mode):
            log.info("CephX rotation disabled by configuration (mode=%s)", mode)
            return 0

        primary_required = rbd_cephx_rotation_required(client_primary, mode=mode)
        secondary_required = rbd_cephx_rotation_required(client_secondary, mode=mode)
        if not primary_required and not secondary_required:
            return 0

        pool_names = discover_mirrored_pools(pool_types, config)
        if not pool_names:
            raise RuntimeError(
                "CephX rotation required but no mirrored pools discovered from config"
            )

        log.info("Mirrored pools for CephX rotation: %s", pool_names)
        baseline = capture_mirror_pool_states(rbd_primary, rbd_secondary, pool_names)

        peer_refresh_required = False
        if primary_required:
            legacy_primary = get_legacy_rbd_cephx_entities(client_primary)
            rotate_rbd_cephx_credentials(client_primary, legacy_primary)
            peer_refresh_required = (
                peer_refresh_required or RBD_PEER_CLIENT in legacy_primary
            )
            redeploy_rbd_mirror_daemons(
                client_primary, _rotated_mirror_daemon_entities(legacy_primary)
            )

        if secondary_required:
            legacy_secondary = get_legacy_rbd_cephx_entities(client_secondary)
            rotate_rbd_cephx_credentials(client_secondary, legacy_secondary)
            peer_refresh_required = (
                peer_refresh_required or RBD_PEER_CLIENT in legacy_secondary
            )
            redeploy_rbd_mirror_daemons(
                client_secondary, _rotated_mirror_daemon_entities(legacy_secondary)
            )

        if peer_refresh_required:
            refresh_existing_rbd_mirror_peers(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                pool_names,
            )

        verify_rbd_mirroring(
            rbd_primary,
            rbd_secondary,
            cluster_primary_name,
            cluster_secondary_name,
            pool_names,
            baseline,
        )
        return 0
    except Exception as exc:
        log.error("Post-upgrade RBD CephX rotation workflow failed: %s", exc)
        return 1
