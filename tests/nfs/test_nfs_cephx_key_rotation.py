"""
Test: NFS-Ganesha CephX Key Rotation (IBMCEPH-16471)

Verifies ``ceph nfs cluster rotate-key``: keys rotate, export config is
updated, and a client that stays mounted can still read and write.  No
manual ``ceph auth rotate``, ``ceph orch redeploy``, ``ceph nfs export
apply``, or unmount/remount.

Steps:
    1. If ``ceph nfs cluster --help`` has no rotate-key, skip (pre-CephX).
    2. Default: create ``cephfs-nfs-cephx`` and mount it (tier-1).
       Upgrade suites set ``setup: false`` and use the existing
       ``cephfs-nfs`` mount. That path does not create or delete a cluster.
    3. Write a baseline file on the live mount.
    4. Snapshot CephX keys (logged half-masked).
    5. Run ``ceph nfs cluster rotate-key <cluster> --key-type aes256k``.
    6. Assert JSON: rotated non-empty, service_redeployed=true,
       updated_exports includes the export.
    7. Wait for NFS daemons to be running again.
    8. Same mount (no umount/mount): baseline file readable, new file IO.
    9. Snapshot keys again; log masked before/after; fail if any key is
       unchanged.
    10. Assert Ganesha logs do not contain export-init errors.
    11. Cleanup only the cluster this test created. ``setup: false`` leaves
        the suite mount in place.

Conf:  conf/tentacle/nfs/1admin-7node-3client.yaml
Suite: suites/tentacle/nfs/tier1-nfs-ganesha.yaml
"""

import json
import traceback
from time import sleep

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    mount_nfs_export_with_wait,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)
from utility.log import Log

log = Log(__name__)

# ── constants ────────────────────────────────────────────────────────────────
NFS_NAME = "cephfs-nfs-cephx"
FS_NAME = "cephfs"
# Unique export prefix so subvol ``export_cephx_0`` does not collide with suite
# ``cephfs-nfs`` exports ``/export_0`` (shared ganeshagroup). See RCA #3174.
NFS_EXPORT = "/export_cephx_0"
NFS_MOUNT = "/mnt/nfs_cephx"
NFS_VERSION = "4.1"
NFS_PORT = "2049"
KEY_TYPE = "aes256k"

# Ganesha CRIT signatures of IBMCEPH-16471 (stale export key after rotation).
# Tightened vs a bare "Permission denied" grep (too broad in mixed logs).
GANESHA_ERROR_PATTERNS = [
    "Ceph Init Handle",
    "fsal_export is NULL",
]


# ── helpers ───────────────────────────────────────────────────────────────────


def _rotate_key_available(installer):
    """Return True if ``ceph nfs cluster rotate-key`` exists on this build.

    Uses ``ceph nfs cluster --help`` (same style as other NFS feature probes).
    Missing subcommand → pre-CephX build; skip. Help failure → hard error.
    """
    out, err = installer.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph nfs cluster --help",
        check_ec=False,
    )
    help_text = "\n".join(part for part in (out, err) if part)
    if "rotate-key" in help_text:
        return True
    if installer.exit_status == 0:
        return False
    raise OperationFailedError(
        "Could not run 'ceph nfs cluster --help' to detect rotate-key "
        f"(rc={installer.exit_status}): {help_text.strip() or '<empty>'}"
    )


def _mask_key(key):
    """Return a half-masked key for logs (first 4 ... last 4)."""
    if not key or len(key) < 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def _get_nfs_entities(installer, nfs_name, timeout=60, interval=6):
    """Return all CephX entities belonging to the NFS cluster as a list.

    Polls until timeout to allow the NFS daemon time to register its
    CephX keys after cluster creation.
    """
    prefix = f"client.nfs.{nfs_name}"
    for w in WaitUntil(timeout=timeout, interval=interval):
        raw = CephAdm(installer).ceph.auth.list()
        entities = []
        for line in raw.splitlines():
            line = line.strip()
            if line.startswith(prefix):
                entities.append(line.split()[0] if " " in line else line)
        if entities:
            log.info("CephX entities for %s: %s", nfs_name, entities)
            return entities
        log.info(
            "No CephX entities found yet for %r, waiting %ds ...",
            nfs_name,
            interval,
        )
    if w.expired:
        raise OperationFailedError(
            f"No CephX entities found for NFS cluster {nfs_name!r} after {timeout}s"
        )


def _get_key_value(installer, entity):
    """Return the current key string for the given CephX entity."""
    raw = CephAdm(installer).ceph.auth.get(entity)
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("key"):
            return line.split("=", 1)[1].strip()
    raise OperationFailedError(f"Could not parse key value for entity {entity!r}")


def _snapshot_keys(installer, nfs_name):
    """Return {entity: key} for all NFS CephX entities."""
    entities = _get_nfs_entities(installer, nfs_name)
    return {e: _get_key_value(installer, e) for e in entities}


def _parse_export_paths(raw):
    """Normalize ``ceph nfs export ls`` output to a list of pseudo paths."""
    if isinstance(raw, list):
        items = raw
    else:
        text = (raw or "").strip()
        try:
            items = json.loads(text)
        except json.JSONDecodeError:
            items = [line.strip() for line in text.splitlines() if line.strip()]
    if isinstance(items, dict):
        items = items.get("exports") or items.get("paths") or []
    paths = []
    for item in items:
        if isinstance(item, str) and item.startswith("/"):
            paths.append(item)
        elif isinstance(item, dict):
            path = item.get("pseudo") or item.get("path")
            if path:
                paths.append(path)
    return paths


def _export_client_entities(installer, nfs_name):
    """Return CephFS export client entities, one per distinct ``fsal.user_id``.

    These are the client keyrings stored on the export (``client.nfs.<cluster>.<fs>``).
    Daemon keyrings (``client.nfs.<cluster>.<node>.<host>``) are not included.
    ``ceph nfs cluster rotate-key`` redeploys Ganesha only when a daemon key
    is rotated, so rotating these entities leaves the NFS service running.
    """
    adm = CephAdm(installer)
    paths = _parse_export_paths(adm.ceph.nfs.export.ls(nfs_name))
    if not paths:
        raise OperationFailedError(
            f"No exports listed for NFS cluster {nfs_name!r}; cannot rotate client keys"
        )
    entities = []
    seen = set()
    for path in paths:
        info = adm.ceph.nfs.export.info(nfs_name, path)
        if isinstance(info, str):
            info = json.loads(info)
        user_id = (info.get("fsal") or {}).get("user_id")
        if not user_id:
            raise OperationFailedError(
                f"Export {path} on {nfs_name} has no fsal.user_id"
            )
        entity = user_id if str(user_id).startswith("client.") else f"client.{user_id}"
        log.info("Export %s client key %s", path, entity)
        if entity not in seen:
            seen.add(entity)
            entities.append(entity)
    return entities


def _server_entities(installer, nfs_name, client_entities):
    """NFS auth entities that are not export client users.

    These are the Ganesha daemon keyrings. Rotating one redeploys the service.
    """
    client_set = set(client_entities)
    servers = [
        entity
        for entity in _get_nfs_entities(installer, nfs_name)
        if entity not in client_set
    ]
    if not servers:
        raise OperationFailedError(
            f"No server keys found for NFS cluster {nfs_name!r}. "
            f"Client keys were {client_entities}"
        )
    log.info("Server keys: %s", servers)
    return servers


def _findmnt_field(client, mount_point, field):
    """Return one findmnt column for *mount_point*, or an empty string."""
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"findmnt -n -o {field} --target {mount_point}",
        check_ec=False,
    )
    lines = (out or "").strip().splitlines()
    if client.exit_status != 0 or not lines:
        return ""
    return lines[0].strip()


def _existing_mounts(clients, mount_point, nfs_version, nfs_port):
    """Record each client that already has *mount_point*, with its vers and port.

    Later upgrade steps reuse these mounts. Server-key rotation redeploys
    Ganesha, so each recorded client is remounted with the same source.
    """
    mounts = []
    for client in clients:
        source = _findmnt_field(client, mount_point, "SOURCE")
        if ":" not in source:
            log.info("%s has no NFS mount at %s", client.hostname, mount_point)
            continue
        version = nfs_version
        port = str(nfs_port)
        for part in _findmnt_field(client, mount_point, "OPTIONS").split(","):
            if part.startswith("vers="):
                version = part.split("=", 1)[1]
            elif part.startswith("port="):
                port = part.split("=", 1)[1]
        export = source.rsplit(":", 1)[1]
        server = source.rsplit(":", 1)[0]
        log.info(
            "Existing mount on %s: %s vers=%s port=%s",
            client.hostname,
            source,
            version,
            port,
        )
        mounts.append(
            {
                "client": client,
                "server": server,
                "export": export,
                "version": version,
                "port": port,
                "mount_point": mount_point,
            }
        )
    if not mounts:
        raise OperationFailedError(
            f"No client has an NFS mount at {mount_point} to restore after rotation"
        )
    return mounts


def _unmount_mount_point(client, mount_point):
    """Unmount *mount_point*. A second lazy unmount covers a busy mount."""
    client.exec_command(sudo=True, cmd=f"umount {mount_point}", check_ec=False)
    if _findmnt_field(client, mount_point, "SOURCE"):
        log.warning("Normal unmount left %s mounted; trying umount -l", mount_point)
        client.exec_command(sudo=True, cmd=f"umount -l {mount_point}", check_ec=False)


def _remount_existing_mounts(mounts, installer, nfs_name):
    """Remount every client that had the export before key rotation."""
    errors = []
    for mount in mounts:
        client = mount["client"]
        log.info(
            "Remounting %s: %s:%s vers=%s",
            client.hostname,
            mount["server"],
            mount["export"],
            mount["version"],
        )
        try:
            _unmount_mount_point(client, mount["mount_point"])
            mount_nfs_export_with_wait(
                client,
                mount["server"],
                mount["port"],
                mount["export"],
                mount["mount_point"],
                mount["version"],
                installer_node=installer,
                nfs_name=nfs_name,
                nfs_wait_timeout=300,
            )
            log.info(
                "Remount complete on %s at %s", client.hostname, mount["mount_point"]
            )
        except Exception as exc:
            errors.append(f"{client.hostname}: {exc}")
            log.error("Remount failed on %s: %s", client.hostname, exc)
    if errors:
        raise OperationFailedError(
            "Remount after key rotation failed:\n" + "\n".join(errors)
        )


def _assert_scoped_rotate_response(result, nfs_name, entity, forbidden):
    """Fail unless this call rotated *entity* and none of *forbidden*."""
    errors = []
    rotated = result.get("rotated") or []
    if entity not in rotated:
        errors.append(f"{entity} missing from rotated={rotated}")
    unexpected = [item for item in rotated if item in forbidden or item != entity]
    if unexpected:
        errors.append(f"rotated unexpected entities: {unexpected}")
    if result.get("cluster_id") not in (None, nfs_name):
        errors.append(
            f"cluster_id mismatch: expected {nfs_name!r}, "
            f"got {result.get('cluster_id')!r}"
        )
    if errors:
        raise OperationFailedError(
            "rotate-key response validation failed:\n" + "\n".join(errors)
        )
    log.info(
        "rotate-key %s: service_redeployed=%s updated_exports=%s daemon_keys=%s",
        entity,
        result.get("service_redeployed"),
        result.get("updated_exports"),
        result.get("daemon_keys"),
    )


def _rotate_entities(installer, nfs_name, entities, forbidden, label):
    """Rotate each entity with ``ceph nfs cluster rotate-key <cluster> <entity>``."""
    log.info("Rotating %d %s key(s): %s", len(entities), label, entities)
    for entity in entities:
        log.info(
            "Running ceph nfs cluster rotate-key %s %s --key-type %s",
            nfs_name,
            entity,
            KEY_TYPE,
        )
        result = CephAdm(installer).ceph.nfs.cluster.rotate_key(
            nfs_name, entity=entity, key_type=KEY_TYPE
        )
        log.info("rotate-key response: %s", json.dumps(result, indent=2))
        _assert_scoped_rotate_response(result, nfs_name, entity, forbidden)


def _assert_named_keys_changed(baseline_keys, after_keys, entities):
    """Fail unless every listed entity has a new key value."""
    unchanged = []
    for entity in entities:
        before = baseline_keys.get(entity)
        after = after_keys.get(entity)
        log.info(
            "  %-55s  before=[%s] after=[%s]",
            entity,
            _mask_key(before) if before else "<missing>",
            _mask_key(after) if after else "<missing>",
        )
        if not before or not after or before == after:
            unchanged.append(entity)
    if unchanged:
        raise OperationFailedError(
            "Key value did not change after rotate-key for: " + ", ".join(unchanged)
        )
    log.info("All requested keys changed.")


def _assert_keys_rotated(baseline_keys, after_keys):
    """Log masked before/after keys and fail if any shared entity is unchanged."""
    shared = set(baseline_keys) & set(after_keys)
    if not shared:
        raise OperationFailedError(
            "No overlapping NFS CephX entities before vs after rotate-key: "
            f"before={list(baseline_keys)} after={list(after_keys)}"
        )
    unchanged = []
    for entity in sorted(shared):
        before = baseline_keys[entity]
        after = after_keys[entity]
        log.info(
            "  %-55s  before=[%s] after=[%s]",
            entity,
            _mask_key(before),
            _mask_key(after),
        )
        if before == after:
            unchanged.append(entity)
    for entity in sorted(set(after_keys) - set(baseline_keys)):
        log.info(
            "  %-55s  after=[%s] (new entity)",
            entity,
            _mask_key(after_keys[entity]),
        )
    if unchanged:
        raise OperationFailedError(
            "Key value did not change after rotate-key for: " + ", ".join(unchanged)
        )
    log.info("All overlapping NFS CephX keys changed after rotate-key.")


def _assert_existing_nfs_mount(client, mount_point, export):
    """Fail unless *mount_point* is already an NFS mount of *export*."""
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"findmnt -n -o SOURCE --target {mount_point}",
        check_ec=False,
    )
    source = (out or "").strip().splitlines()
    source = source[0].strip() if source else ""
    if client.exit_status != 0 or not source.endswith(f":{export}"):
        raise OperationFailedError(
            f"Expected existing NFS mount of {export} at {mount_point}, "
            f"findmnt returned: {source or '<empty>'}"
        )
    log.info("Using existing NFS mount %s at %s", source, mount_point)


def _assert_exports_listed(installer, nfs_name, expected):
    """Assert that all expected pseudo-paths are listed by the NFS cluster."""
    all_exports = CephAdm(installer).ceph.nfs.export.ls(nfs_name)
    if isinstance(all_exports, str):
        try:
            all_exports = json.loads(all_exports)
        except json.JSONDecodeError:
            pass  # keep as plain string — "in" works for substring check too
    missing = [e for e in expected if e not in all_exports]
    if missing:
        raise OperationFailedError(
            f"Exports missing from 'ceph nfs export ls {nfs_name}': {missing}"
        )
    log.info("All expected exports listed: %s", all_exports)


def _write_test_file(client, mount_point, filename="cephx_testfile"):
    """Write a small test file to the mounted share."""
    client.exec_command(
        sudo=True, cmd=f"dd if=/dev/urandom of={mount_point}/{filename} bs=1M count=10"
    )
    log.info("Wrote test file %s/%s", mount_point, filename)


def _verify_test_file(client, mount_point, filename="cephx_testfile"):
    """Verify the test file is still present and readable."""
    out, _ = client.exec_command(sudo=True, cmd=f"ls -lh {mount_point}/{filename}")
    if filename not in out:
        raise OperationFailedError(
            f"Test file {filename} not found on {mount_point} after key rotation"
        )
    log.info("Test file verified: %s", out.strip())


def _wait_for_live_mount_io(client, mount_point, filename, timeout=180, interval=10):
    """Retry read on an existing mount after Ganesha redeploy (no remount).

    Full rotate-key sets service_redeployed=true, so the NFS server restarts.
    The customer path is to keep the mount; poll until IO returns.
    ``timeout 20`` on the remote command avoids a hard-NFS hang blocking the
    test for exec_command's default 600s.
    """
    path = f"{mount_point}/{filename}"
    cmd = f"timeout 20 ls -lh {path}"
    log.info("Waiting for live mount IO on %s (no remount) ...", path)
    for w in WaitUntil(timeout=timeout, interval=interval):
        client.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=30)
        rc = client.exit_status
        if rc == 0:
            log.info("Live mount IO recovered after rotate-key.")
            return
        log.info(
            "Live mount not ready yet (ls %s rc=%s), retrying ...",
            path,
            rc,
        )
    if w.expired:
        raise OperationFailedError(
            f"Live NFS mount did not recover IO on {path} within {timeout}s "
            "after rotate-key (no remount)"
        )


def _list_nfs_daemons(client, nfs_name, nfs_nodes):
    """Return [(node, daemon_name), ...] from a single ``ceph orch ps``.

    One orch-ps call (review: do not grep twice). Includes every NFS host
    that has a daemon, not only nfs_nodes[0].
    """
    out, _ = client.exec_command(sudo=True, cmd=f"ceph orch ps | grep {nfs_name}")
    daemons = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        daemon_name, host = parts[0], parts[1]
        for node in nfs_nodes:
            if node.hostname == host:
                daemons.append((node, daemon_name))
                break
    if not daemons:
        raise OperationFailedError(
            f"No NFS daemons found in 'ceph orch ps | grep {nfs_name}'"
        )
    log.info(
        "NFS daemons: %s",
        [(node.hostname, name) for node, name in daemons],
    )
    return daemons


def _check_ganesha_logs(nfs_name, nfs_nodes, client, patterns):
    """Fail if any Ganesha daemon log contains an export-init error pattern.

    Uses ``test -s`` so an empty capture is not treated as "pattern absent".
    grep: exit 0 = found, 1 = absent; any other rc is a hard failure.
    """
    daemons = _list_nfs_daemons(client, nfs_name, nfs_nodes)
    for nfs_node, daemon_name in daemons:
        log.info("Checking logs for daemon %s on %s", daemon_name, nfs_node.hostname)
        nfs_node.exec_command(
            sudo=True, cmd=f"cephadm logs --name {daemon_name} > /tmp/nfs_ganesha_log"
        )
        nfs_node.exec_command(
            sudo=True, cmd="test -s /tmp/nfs_ganesha_log", check_ec=False
        )
        if nfs_node.exit_status != 0:
            raise OperationFailedError(
                f"Ganesha log file empty or missing on {nfs_node.hostname} "
                f"after 'cephadm logs --name {daemon_name}'"
            )

        for pattern in patterns:
            nfs_node.exec_command(
                sudo=True,
                cmd=f'grep "{pattern}" /tmp/nfs_ganesha_log',
                check_ec=False,
            )
            rc = nfs_node.exit_status
            if rc not in (0, 1):
                raise OperationFailedError(
                    f"grep failed unexpectedly (rc={rc}) while checking Ganesha "
                    f"logs for {pattern!r} on {daemon_name}"
                )
            if rc == 0:
                log.info("  [FOUND] pattern %r in %s logs", pattern, daemon_name)
                raise OperationFailedError(
                    f"Ganesha logged export errors after "
                    f"'ceph nfs cluster rotate-key' on {daemon_name}: {pattern!r}"
                )
            log.info("  [absent] pattern %r not in %s logs", pattern, daemon_name)

    log.info("No export-init errors in Ganesha logs on any NFS daemon.")


def _assert_rotate_key_response(result, nfs_name, exports):
    """Validate the JSON from ceph nfs cluster rotate-key."""
    errors = []

    rotated = result.get("rotated", [])
    if not rotated:
        errors.append("'rotated' list is empty — no keys were rotated")

    if result.get("service_redeployed") is not True:
        errors.append(
            f"Expected service_redeployed=true, got {result.get('service_redeployed')!r}"
        )

    updated = result.get("updated_exports", [])
    missing = [e for e in exports if e not in updated]
    if missing:
        errors.append(f"Exports not in updated_exports: {missing}")

    if result.get("cluster_id") != nfs_name:
        errors.append(
            f"cluster_id mismatch: expected {nfs_name!r}, "
            f"got {result.get('cluster_id')!r}"
        )

    if errors:
        raise OperationFailedError(
            "rotate-key response validation failed:\n" + "\n".join(errors)
        )
    log.info(
        "rotate-key response valid: %d keys rotated, service_redeployed=true, "
        "%d exports updated.",
        len(rotated),
        len(updated),
    )


def _run_rotate_key_live_mount(
    installer,
    nfs_nodes,
    clients,
    nfs_name,
    export,
    mount_point,
    rotate_scope="all",
    nfs_version=NFS_VERSION,
    nfs_port=NFS_PORT,
    cluster_clients=None,
):
    """Rotate keys on a mounted export.

    ``rotate_scope=all`` rotates every key in one call and keeps the mount.
    ``rotate_scope=server_client`` rotates server keys, then client keys,
    then remounts every client that already had the export. Client rotation
    is before the remount. The default path does not unmount.
    """
    server_then_client = rotate_scope == "server_client"
    log.info(
        "=== ceph nfs cluster rotate-key (%s) ===",
        "server, then client, then remount" if server_then_client else "all keys",
    )
    client = clients[0]

    log.info("Step 1: write baseline file on the existing mount")
    _write_test_file(client, mount_point, filename="pre_rotate_file")
    _verify_test_file(client, mount_point, filename="pre_rotate_file")
    existing_mounts = (
        _existing_mounts(cluster_clients or clients, mount_point, nfs_version, nfs_port)
        if server_then_client
        else None
    )

    log.info("Step 2: snapshot NFS CephX keys (masked)")
    baseline_keys = _snapshot_keys(installer, nfs_name)
    for entity, key in baseline_keys.items():
        log.info("  baseline  %-55s  key=[%s]", entity, _mask_key(key))

    rotated_entities = []
    if server_then_client:
        client_entities = _export_client_entities(installer, nfs_name)
        server_entities = _server_entities(installer, nfs_name, client_entities)
        log.info("Step 3: rotating server keys")
        _rotate_entities(
            installer, nfs_name, server_entities, client_entities, "server"
        )
        log.info("Step 4: rotating client keys before remount")
        _rotate_entities(
            installer, nfs_name, client_entities, server_entities, "client"
        )
        rotated_entities = server_entities + client_entities
    else:
        log.info(
            "Step 3: running ceph nfs cluster rotate-key %s --key-type %s",
            nfs_name,
            KEY_TYPE,
        )
        result = CephAdm(installer).ceph.nfs.cluster.rotate_key(
            nfs_name, key_type=KEY_TYPE
        )
        log.info("rotate-key response: %s", json.dumps(result, indent=2))

        log.info("Step 4: validating rotate-key JSON response ...")
        _assert_rotate_key_response(result, nfs_name, [export])

    log.info("Step 5: waiting for NFS daemons to be running ...")
    sleep(10)
    verify_nfs_ganesha_service(installer, timeout=300)

    if server_then_client:
        log.info("Step 6: remount every existing client after key rotation ...")
        _remount_existing_mounts(existing_mounts, installer, nfs_name)
    else:
        log.info("Step 6: live-mount IO on the same mount (no remount) ...")
        _wait_for_live_mount_io(client, mount_point, "pre_rotate_file")
    _verify_test_file(client, mount_point, filename="pre_rotate_file")
    _write_test_file(client, mount_point, filename="post_rotate_file")
    _verify_test_file(client, mount_point, filename="post_rotate_file")

    log.info("Step 7: confirming key values changed (masked before/after) ...")
    after_keys = _snapshot_keys(installer, nfs_name)
    if server_then_client:
        _assert_named_keys_changed(baseline_keys, after_keys, rotated_entities)
    else:
        _assert_keys_rotated(baseline_keys, after_keys)

    log.info("Step 8: asserting Ganesha logs are clean after rotate-key ...")
    _check_ganesha_logs(
        nfs_name,
        nfs_nodes,
        client,
        patterns=GANESHA_ERROR_PATTERNS,
    )

    log.info("Step 9: asserting exports are listed ...")
    _assert_exports_listed(installer, nfs_name, [export])

    if server_then_client:
        log.info(
            "TEST PASSED - server keys, then client keys, then remount; "
            "requested keys changed and IO succeeded"
        )
    else:
        log.info(
            "TEST PASSED - ceph nfs cluster rotate-key rotated keys (values "
            "changed), updated exports, Ganesha logs stayed clean, and IO "
            "succeeded on the existing mount without unmount/remount"
        )


# ── entry point ───────────────────────────────────────────────────────────────


def run(ceph_cluster, **kw):
    """
    Test NFS CephX key rotation — IBMCEPH-16471.

    Config keys (all optional):
        nfs_version (str): NFS mount version, default "4.1"
        port (str): NFS port, default "2049"
        clients (int): number of client nodes to use, default 1
        setup (bool): True (default) creates ``cephfs-nfs-cephx``.
            False reuses the suite cluster and does not delete it.
        nfs_name (str): NFS cluster name. Default ``cephfs-nfs-cephx``,
            or ``cephfs-nfs`` when setup is false.
        nfs_export (str): Mounted pseudo path when setup is false.
            Default ``/export_0``.
        fs_name (str): CephFS filesystem name, default "cephfs"
        nfs_mount (str): Client mount point. Default ``/mnt/nfs_cephx``,
            or ``/mnt/nfs`` when setup is false.
        rotate_scope (str): ``all`` (default) rotates every key in one
            call and keeps every existing mount. ``server_client`` rotates
            server keys, then client keys, then remounts each client that
            already had the export, using that client's vers and port.
            Upgrade suites use ``server_client``.

    Returns:
        0 on success, 1 on failure, -1 if rotate-key is absent (pre-CephX).
    """
    config = kw.get("config", {})

    reuse_mount = config.get("setup", True) is False
    nfs_name = config.get("nfs_name", "cephfs-nfs" if reuse_mount else NFS_NAME)
    fs_name = config.get("fs_name", FS_NAME)
    mount_point = config.get("nfs_mount", "/mnt/nfs" if reuse_mount else NFS_MOUNT)
    nfs_version = config.get("nfs_version", NFS_VERSION)
    nfs_port = str(config.get("port", NFS_PORT))
    no_clients = int(config.get("clients", 1))

    installer = ceph_cluster.get_nodes("installer")[0]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    if no_clients > len(clients):
        raise ConfigError(
            f"Test requires {no_clients} client(s) but only {len(clients)} available"
        )
    clients = clients[:no_clients]

    if not _rotate_key_available(installer):
        log.info(
            "TEST SKIPPED - ceph nfs cluster rotate-key not in "
            "'ceph nfs cluster --help' (pre-CephX build)"
        )
        return -1

    # All NFS hostnames so CephX keys register on every NFS daemon.
    nfs_server_hostnames = [n.hostname for n in nfs_nodes]
    # setup_nfs_cluster appends _{i}; unique prefix avoids colliding with suite
    # ``/export_0`` subvolume in shared ganeshagroup (RCA #3174).
    nfs_export_base = "/export_cephx"
    export = config.get("nfs_export", "/export_0" if reuse_mount else NFS_EXPORT)
    rotate_scope = str(config.get("rotate_scope", "all")).lower()
    if rotate_scope not in ("all", "server_client"):
        raise ConfigError(
            "rotate_scope must be 'all' or 'server_client', " f"got {rotate_scope!r}"
        )

    try:
        if reuse_mount:
            log.info(
                "setup=false: rotate-key on existing cluster %s mount %s export %s",
                nfs_name,
                mount_point,
                export,
            )
            _assert_existing_nfs_mount(clients[0], mount_point, export)
        else:
            # Default path used by tier-1: create cephfs-nfs-cephx and mount it.
            setup_nfs_cluster(
                clients,
                nfs_server_hostnames,
                nfs_port,
                nfs_version,
                nfs_name,
                mount_point,
                fs_name,
                nfs_export_base,
                fs_name,
                ceph_cluster=ceph_cluster,
            )

        _run_rotate_key_live_mount(
            installer,
            nfs_nodes,
            clients,
            nfs_name,
            export,
            mount_point,
            rotate_scope=rotate_scope,
            nfs_version=nfs_version,
            nfs_port=nfs_port,
            cluster_clients=ceph_cluster.get_nodes("client"),
        )

        log.info("TEST PASSED - NFS CephX key rotation completed")
        return 0

    except Exception as exc:
        log.error("Test failed: %s", exc)
        log.error(traceback.format_exc())
        return 1

    finally:
        if reuse_mount:
            # Do not unmount or delete cephfs-nfs. Later suite steps still use it.
            log.info(
                "Leaving NFS cluster %r and mount %s in place",
                nfs_name,
                mount_point,
            )
            try:
                clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        f"rm -f {mount_point}/pre_rotate_file "
                        f"{mount_point}/post_rotate_file"
                    ),
                    check_ec=False,
                )
            except Exception as cleanup_exc:
                log.warning("Could not remove rotate-key probe files: %s", cleanup_exc)
        else:
            log.info("Cleaning up NFS cluster %r ...", nfs_name)
            try:
                cleanup_cluster(clients, mount_point, nfs_name, nfs_export_base)
                log.info("Cleanup done.")
            except Exception as cleanup_exc:
                log.warning(
                    "Cleanup of %r raised after test body finished: %s",
                    nfs_name,
                    cleanup_exc,
                )
