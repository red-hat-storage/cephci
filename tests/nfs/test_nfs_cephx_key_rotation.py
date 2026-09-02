"""
Test: NFS-Ganesha CephX Key Rotation (IBMCEPH-16471)

Verifies ``ceph nfs cluster rotate-key``: keys rotate, export config is
updated, and a client that stays mounted can still read and write.  No
manual ``ceph auth rotate``, ``ceph orch redeploy``, ``ceph nfs export
apply``, or unmount/remount.

Steps:
    1. If ``ceph nfs cluster --help`` has no rotate-key, skip (pre-CephX).
    2. Setup NFS cluster and mount (version from suite YAML).
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
    11. Cleanup (unmount is teardown only).

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
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)
from utility.log import Log

log = Log(__name__)

# ── constants ────────────────────────────────────────────────────────────────
NFS_NAME = "cephfs-nfs-cephx"
FS_NAME = "cephfs"
NFS_EXPORT = "/export_0"
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
):
    """Customer path: rotate-key while the export stays mounted."""
    log.info("=== ceph nfs cluster rotate-key (live mount) ===")
    client = clients[0]

    log.info("Step 1: write baseline file on the existing mount")
    _write_test_file(client, mount_point, filename="pre_rotate_file")
    _verify_test_file(client, mount_point, filename="pre_rotate_file")

    log.info("Step 2: snapshot NFS CephX keys (masked)")
    baseline_keys = _snapshot_keys(installer, nfs_name)
    for entity, key in baseline_keys.items():
        log.info("  baseline  %-55s  key=[%s]", entity, _mask_key(key))

    log.info(
        "Step 3: running ceph nfs cluster rotate-key %s --key-type %s",
        nfs_name,
        KEY_TYPE,
    )
    result = CephAdm(installer).ceph.nfs.cluster.rotate_key(nfs_name, key_type=KEY_TYPE)
    log.info("rotate-key response: %s", json.dumps(result, indent=2))

    log.info("Step 4: validating rotate-key JSON response ...")
    _assert_rotate_key_response(result, nfs_name, [export])

    log.info("Step 5: waiting for NFS daemons to be running ...")
    sleep(10)
    verify_nfs_ganesha_service(installer, timeout=300)

    log.info("Step 6: live-mount IO on the same mount (no remount) ...")
    _wait_for_live_mount_io(client, mount_point, "pre_rotate_file")
    _verify_test_file(client, mount_point, filename="pre_rotate_file")
    _write_test_file(client, mount_point, filename="post_rotate_file")
    _verify_test_file(client, mount_point, filename="post_rotate_file")

    log.info("Step 7: confirming key values changed (masked before/after) ...")
    after_keys = _snapshot_keys(installer, nfs_name)
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

    log.info(
        "TEST PASSED - ceph nfs cluster rotate-key rotated keys (values changed), "
        "updated exports, Ganesha logs stayed clean, and IO succeeded on the "
        "existing mount without unmount/remount"
    )


# ── entry point ───────────────────────────────────────────────────────────────


def run(ceph_cluster, **kw):
    """
    Test NFS CephX key rotation — IBMCEPH-16471.

    Config keys (all optional):
        nfs_version (str): NFS mount version, default "4.1"
        port (str): NFS port, default "2049"
        clients (int): number of client nodes to use, default 1
        nfs_name (str): NFS cluster name, default "cephfs-nfs-cephx"
        fs_name (str): CephFS filesystem name, default "cephfs"
        nfs_mount (str): client mount point, default "/mnt/nfs_cephx"

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})

    nfs_name = config.get("nfs_name", NFS_NAME)
    fs_name = config.get("fs_name", FS_NAME)
    mount_point = config.get("nfs_mount", NFS_MOUNT)
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
        return 0

    # All NFS hostnames so CephX keys register on every NFS daemon.
    nfs_server_hostnames = [n.hostname for n in nfs_nodes]
    # setup_nfs_cluster appends _{i} to form the export name, so pass the
    # base prefix "/export"; the resulting export will be "/export_0"
    nfs_export_base = "/export"
    export = NFS_EXPORT

    try:
        # Framework helper mounts using nfs_version / first NFS host from conf.
        # skip_mount=False: one mount for the whole test (customer stays mounted).
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
        )

        log.info("TEST PASSED - NFS CephX key rotation completed")
        return 0

    except Exception as exc:
        log.error("Test failed: %s", exc)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Cleaning up NFS cluster %r ...", nfs_name)
        cleanup_cluster(clients, mount_point, nfs_name, nfs_export_base)
        log.info("Cleanup done.")
