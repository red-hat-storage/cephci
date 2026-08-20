"""
Test: NFS-Ganesha CephX Key Rotation (IBMCEPH-16471)

Validates that rotating CephX keys for an NFS cluster — including the export
CephFS key — is handled correctly.

Two scenarios are covered:

  Scenario A — Manual path (regression baseline for IBMCEPH-16471 bug):
    Demonstrates that manually rotating all NFS CephX keys via
    ``ceph auth rotate`` and redeploying NFS reproduces the bug:
    Ganesha logs WILL contain ``Permission denied`` / ``fsal_export is NULL``
    because the export config still holds the old key reference.

    Recovery is performed by re-applying the export configuration via
    ``ceph nfs export apply``, after which the service is healthy and IO works.

    Steps:
      1. Mount the export and write a baseline test file to confirm IO works.
      2. Unmount cleanly.
      3. Rotate ALL NFS CephX keys individually via ``ceph auth rotate``.
      4. Redeploy the NFS service via ``ceph orch redeploy``.
      5. Assert Ganesha logs CONTAIN the expected bug errors (Permission denied /
         fsal_export is NULL) — this documents and verifies the broken path.
      6. Recover: re-apply the export config via ``ceph nfs export apply``.
      7. Wait for service to stabilise.
      8. Assert exports are still listed by the NFS cluster.
      9. Remount the export — must succeed after recovery.
      10. Assert baseline file is intact (no data loss).
      11. Write a new file post-rotation and read it back (live IO works).
      12. Assert all key values changed from baseline (rotation was effective).

  Scenario B — One-shot command (fix verification for IBMCEPH-16471 / 9.1z2):
    Verifies that ``ceph nfs cluster rotate-key`` atomically rotates all keys
    AND updates the export config, so no manual recovery is needed and exports
    remain accessible immediately after rotation.

    Steps:
      1. Mount the export and write a baseline test file.
      2. Unmount cleanly.
      3. Run ``ceph nfs cluster rotate-key <cluster> --key-type aes256k``.
      4. Assert JSON response: rotated=all, service_redeployed=true,
         updated_exports=all exports.
      5. Wait for NFS daemons to come back running.
      6. Assert Ganesha logs contain NO Permission denied / fsal_export errors.
      7. Assert exports still listed.
      8. Remount — assert mount succeeds WITHOUT any manual export apply.
      9. Assert baseline file is intact (no data loss).
      10. Write a new file post-rotation and read it back (live IO works).
      Sub-test: rotate only the export CephFS key — assert service_redeployed=false
      and all exports updated, then verify mount and IO still work.

Conf:  conf/tentacle/nfs/1admin-3node-1client.yaml
Suite: suites/tentacle/nfs/tier1-nfs-ganesha-cephx-key-rotation.yaml
"""

import json
import traceback
from time import sleep

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    mount_retry,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)
from utility.log import Log

log = Log(__name__)

# ── constants ────────────────────────────────────────────────────────────────
NFS_NAME = "cephfs-nfs"
FS_NAME = "cephfs"
NFS_EXPORT = "/export_0"
NFS_MOUNT = "/mnt/nfs_cephx"
NFS_VERSION = "4.1"
NFS_PORT = "2049"
KEY_TYPE = "aes256k"

# Ganesha error patterns indicating the export CephFS key bug.
# Scenario A asserts these ARE present (documenting the broken manual path).
# Scenario B asserts these are NOT present (verifying the fix).
GANESHA_ERROR_PATTERNS = [
    "Permission denied",
    "fsal_export is NULL",
]


# ── helpers ───────────────────────────────────────────────────────────────────


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
            f"No CephX entities found for NFS cluster {nfs_name!r} " f"after {timeout}s"
        )


def _get_key_value(installer, entity):
    """Return the current key string for the given CephX entity."""
    raw = CephAdm(installer).ceph.auth.get(entity)
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("key"):
            return line.split("=", 1)[1].strip()
    raise OperationFailedError(f"Could not parse key value for entity {entity!r}")


def _assert_exports_listed(installer, nfs_name, expected):
    """Assert that all expected pseudo-paths are listed by the NFS cluster."""
    all_exports = CephAdm(installer).ceph.nfs.export.ls(nfs_name)
    if isinstance(all_exports, str):
        try:
            all_exports = json.loads(all_exports)
        except Exception:
            pass  # keep as plain string — "in" works for substring check too
    missing = [e for e in expected if e not in all_exports]
    if missing:
        raise OperationFailedError(
            f"Exports missing from 'ceph nfs export ls {nfs_name}': {missing}"
        )
    log.info("All expected exports listed: %s", all_exports)


def _mount_and_verify(
    client, nfs_server_ip, export, mount_point, version=NFS_VERSION, port=NFS_PORT
):
    """
    Mount an NFS export and verify the mount point is accessible.
    Uses mount_retry() from nfs_operations; raises on failure.
    """
    client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
    # Unmount first in case there is a stale handle from a previous redeploy
    client.exec_command(sudo=True, cmd=f"umount -f {mount_point}", check_ec=False)
    sleep(3)
    mount_retry(client, mount_point, version, port, nfs_server_ip, export)
    out, _ = client.exec_command(sudo=True, cmd=f"ls {mount_point}")
    log.info("Mount %s:%s contents: %s", nfs_server_ip, export, out.strip())
    return out.strip()


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


def _get_active_nfs_node(client, nfs_name, nfs_nodes):
    """Return the nfs_node object where the NFS daemon is currently running.

    Runs 'ceph orch ps' on the client node to find which host the daemon
    is placed on, then matches against the nfs_nodes list.
    Falls back to nfs_nodes[0] if no match is found.
    """
    try:
        out, _ = client.exec_command(sudo=True, cmd=f"ceph orch ps | grep {nfs_name}")
        # Each line: <daemon_name> <host> <status> ...
        # e.g. nfs.cephfs-nfs.0.0.node2.abc123  node2  running ...
        for line in out.splitlines():
            parts = line.split()
            if not parts:
                continue
            host = parts[1] if len(parts) > 1 else ""
            for node in nfs_nodes:
                if node.hostname == host:
                    log.info("Active NFS daemon found on %s", node.hostname)
                    return node
    except Exception as e:
        log.warning("Could not determine active NFS node via orch ps: %s", e)
    log.warning(
        "Falling back to nfs_nodes[0] (%s) for log check", nfs_nodes[0].hostname
    )
    return nfs_nodes[0]


def _check_ganesha_logs(client, nfs_name, nfs_node, patterns, expect_present=True):
    """Check Ganesha container logs on nfs_node for the given patterns.

    Fetches logs via ``cephadm logs`` on the node where the daemon runs,
    then greps for each pattern.

    Args:
        client: node with ceph in PATH (used for ``ceph orch ps``)
        nfs_name: NFS cluster name (used to find daemon name)
        nfs_node: node object where the daemon is running (from _get_active_nfs_node)
        patterns: list of strings to search for
        expect_present (bool): if True, raise if ANY pattern is missing from logs
                               (use when documenting the bug);
                               if False, raise if ANY pattern IS found in logs
                               (use when verifying the fix).

    Returns:
        dict mapping pattern -> bool (True = found in logs)
    """
    # Get the daemon name that is running on nfs_node from ceph orch ps.
    # ceph orch ps output: <daemon_name> <host> <status> ...
    out, _ = client.exec_command(sudo=True, cmd=f"ceph orch ps | grep {nfs_name}")
    daemon_name = None
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1] == nfs_node.hostname:
            daemon_name = parts[0]
            break
    if daemon_name is None:
        # Fallback: take first daemon name if hostname match fails
        daemon_name = out.split()[0]
        log.warning(
            "Could not match daemon to %s by hostname; using %s",
            nfs_node.hostname,
            daemon_name,
        )
    log.info("Checking logs for daemon %s on %s", daemon_name, nfs_node.hostname)

    # Fetch logs into a temp file on the nfs_node
    nfs_node.exec_command(
        sudo=True, cmd=f"cephadm logs --name {daemon_name} > /tmp/nfs_ganesha_log"
    )
    # Fail loudly if capture produced nothing — do not treat that as "pattern absent"
    nfs_node.exec_command(sudo=True, cmd="test -s /tmp/nfs_ganesha_log", check_ec=False)
    if nfs_node.exit_status != 0:
        raise OperationFailedError(
            f"Ganesha log file empty or missing on {nfs_node.hostname} "
            f"after 'cephadm logs --name {daemon_name}'"
        )

    found = {}
    for pattern in patterns:
        # check_ec=False: grep exit 1 means "not found", which is a valid result.
        # exec_command returns (stdout, stderr); the exit code is on node.exit_status.
        nfs_node.exec_command(
            sudo=True,
            cmd=f'grep "{pattern}" /tmp/nfs_ganesha_log',
            check_ec=False,
        )
        rc = nfs_node.exit_status
        if rc not in (0, 1):
            raise OperationFailedError(
                f"grep failed unexpectedly (rc={rc}) while checking Ganesha logs "
                f"for {pattern!r} on {daemon_name}"
            )
        found[pattern] = rc == 0
        if found[pattern]:
            log.info("  [FOUND] pattern %r in %s logs", pattern, daemon_name)
        else:
            log.info("  [absent] pattern %r not in %s logs", pattern, daemon_name)

    if expect_present:
        missing = [p for p, v in found.items() if not v]
        if missing:
            raise OperationFailedError(
                f"Expected bug patterns not found in Ganesha logs after manual rotation "
                f"— check if cluster version supports the test: {missing}"
            )
        log.info("All expected bug patterns confirmed in Ganesha logs (manual path).")
    else:
        present = [p for p, v in found.items() if v]
        if present:
            raise OperationFailedError(
                f"Ganesha logged export errors after key rotation via "
                f"'ceph nfs cluster rotate-key' (fix should prevent this): {present}"
            )
        log.info("No bug patterns in Ganesha logs — fix is working correctly.")

    return found


# ── scenario A ───────────────────────────────────────────────────────────────


def _scenario_a_manual_rotate(
    installer,
    nfs_nodes,
    clients,
    nfs_name,
    export,
    nfs_server_ip,
    mount_point,
    nfs_version,
):
    """
    Scenario A — Manual ceph auth rotate path, documents the bug.

    Rotates all NFS CephX keys individually (including the export CephFS key)
    and redeployes the service.  This reproduces the IBMCEPH-16471 bug:
    Ganesha cannot initialise the export because it still references the old
    key, so Permission denied / fsal_export is NULL appear in the logs.

    Recovery is performed by re-applying the export config, after which IO
    is restored without any data loss.
    """
    log.info("=== Scenario A: manual ceph auth rotate for all NFS keys ===")
    client = clients[0]

    # Step 1 — baseline mount + write + unmount
    log.info("Step 1: baseline mount, write test file, unmount")
    _mount_and_verify(client, nfs_server_ip, export, mount_point, version=nfs_version)
    _write_test_file(client, mount_point, filename="pre_rotate_file")
    Unmount(client).unmount(mount_point)

    # Step 2 — capture baseline key values
    entities = _get_nfs_entities(installer, nfs_name)
    baseline_keys = {e: _get_key_value(installer, e) for e in entities}
    log.info("Step 2: baseline keys captured for %d entities.", len(baseline_keys))
    for e, k in baseline_keys.items():
        log.info("  baseline  %-55s  key=[%s...%s]", e, k[:4], k[-4:])

    # Step 3 — rotate ALL keys including the export CephFS key
    log.info("Step 3: rotating all NFS CephX keys ...")
    for entity in entities:
        log.info("  rotating %s", entity)
        CephAdm(installer).ceph.auth.rotate(entity, key_type=KEY_TYPE)
        new_key = _get_key_value(installer, entity)
        log.info("  rotated  %-55s  key=[%s...%s]", entity, new_key[:4], new_key[-4:])

    # Step 4 — redeploy and wait
    log.info("Step 4: redeploying nfs.%s ...", nfs_name)
    CephAdm(installer).ceph.orch.redeploy(f"nfs.{nfs_name}")
    sleep(10)
    verify_nfs_ganesha_service(installer, timeout=300)

    # Step 5 — Ganesha logs MUST contain the bug errors.
    # This documents that the manual rotation path reproduces the bug:
    # the export config still references the old key after raw auth rotate.
    log.info(
        "Step 5: asserting Ganesha logs contain expected bug errors "
        "(Permission denied / fsal_export is NULL) ..."
    )
    active_nfs_node = _get_active_nfs_node(client, nfs_name, nfs_nodes)
    _check_ganesha_logs(
        client,
        nfs_name,
        active_nfs_node,
        patterns=GANESHA_ERROR_PATTERNS,
        expect_present=True,
    )

    # Step 6 — recover by re-applying the export config.
    # Fetch the current export config from the client node (which has ceph in PATH)
    # and re-apply it so Ganesha picks up the new key reference.
    # Uses the same pattern as nfs_operations.permission().
    log.info("Step 6: recovering via ceph nfs export apply ...")
    export_conf_raw = Ceph(client).nfs.export.get(nfs_name, export)
    client.exec_command(
        sudo=True,
        cmd=f"echo '{export_conf_raw}' > /tmp/nfs_export_recovery.conf",
    )
    Ceph(client).nfs.export.apply(nfs_name, "/tmp/nfs_export_recovery.conf")
    sleep(10)
    verify_nfs_ganesha_service(installer, timeout=300)
    log.info("Export re-applied successfully.")

    # Step 7 — exports must still be listed after recovery
    log.info("Step 7: asserting exports are listed after recovery ...")
    _assert_exports_listed(installer, nfs_name, [export])

    # Step 8 — remount must succeed after recovery
    log.info("Step 8: remounting export after recovery ...")
    _mount_and_verify(client, nfs_server_ip, export, mount_point, version=nfs_version)

    # Step 9 — baseline file must still be there (no data loss)
    log.info("Step 9: verifying baseline file is intact ...")
    _verify_test_file(client, mount_point, filename="pre_rotate_file")

    # Step 10 — write and read a new file post-rotation (live IO works)
    log.info("Step 10: writing and reading new file post-rotation ...")
    _write_test_file(client, mount_point, filename="post_rotate_file")
    _verify_test_file(client, mount_point, filename="post_rotate_file")

    # Step 11 — every key must have a different value from baseline
    log.info("Step 11: confirming all key values changed ...")
    for entity in entities:
        new_key = _get_key_value(installer, entity)
        if new_key == baseline_keys[entity]:
            raise OperationFailedError(
                f"Key did not change after rotation for entity {entity!r}"
            )
        log.info(
            "  confirmed  %-55s  before=[%s...%s] after=[%s...%s]",
            entity,
            baseline_keys[entity][:4],
            baseline_keys[entity][-4:],
            new_key[:4],
            new_key[-4:],
        )
    log.info("All keys confirmed rotated (values differ from baseline).")

    Unmount(client).unmount(mount_point)
    log.info(
        "TEST PASSED - Scenario A: manual ceph auth rotate reproduced the "
        "IBMCEPH-16471 bug (Ganesha Permission denied / fsal_export is NULL); "
        "export apply recovered the share and IO succeeded"
    )


# ── scenario B ───────────────────────────────────────────────────────────────


def _scenario_b_rotate_key_command(
    installer,
    nfs_nodes,
    clients,
    nfs_name,
    export,
    nfs_server_ip,
    mount_point,
    nfs_version,
):
    """
    Scenario B — Verify the 9.1z2 fix: ceph nfs cluster rotate-key.

    The new command atomically rotates all CephX keys AND updates the export
    config so Ganesha can reinitialise exports with the new key.  No manual
    export re-apply is needed; exports are immediately accessible after rotation.
    """
    log.info("=== Scenario B: ceph nfs cluster rotate-key (9.1z2 fix) ===")
    client = clients[0]

    # Step 1 — baseline mount + write + unmount
    log.info("Step 1: baseline mount, write test file, unmount")
    _mount_and_verify(client, nfs_server_ip, export, mount_point, version=nfs_version)
    _write_test_file(client, mount_point, filename="pre_rotate_file")
    Unmount(client).unmount(mount_point)

    # Step 2 — one-shot rotate
    log.info(
        "Step 2: running ceph nfs cluster rotate-key %s --key-type %s",
        nfs_name,
        KEY_TYPE,
    )
    result = CephAdm(installer).ceph.nfs.cluster.rotate_key(nfs_name, key_type=KEY_TYPE)
    log.info("rotate-key response: %s", json.dumps(result, indent=2))

    # Step 3 — validate JSON response
    log.info("Step 3: validating rotate-key JSON response ...")
    _assert_rotate_key_response(result, nfs_name, [export])

    # Step 4 — wait for daemons
    log.info("Step 4: waiting for NFS daemons to be running ...")
    sleep(10)
    verify_nfs_ganesha_service(installer, timeout=300)

    # Step 5 — Ganesha logs must be CLEAN — no Permission denied / fsal_export errors.
    # This is the key assertion for the 9.1z2 fix.
    log.info("Step 5: asserting Ganesha logs are clean after rotate-key ...")
    active_nfs_node = _get_active_nfs_node(client, nfs_name, nfs_nodes)
    _check_ganesha_logs(
        client,
        nfs_name,
        active_nfs_node,
        patterns=GANESHA_ERROR_PATTERNS,
        expect_present=False,
    )

    # Step 6 — exports must still be listed
    log.info("Step 6: asserting exports are listed ...")
    _assert_exports_listed(installer, nfs_name, [export])

    # Step 7 — remount: must succeed WITHOUT any manual export apply
    # This is the fix assertion: the new command handles everything.
    log.info("Step 7: remounting export — no manual export apply should be needed ...")
    _mount_and_verify(client, nfs_server_ip, export, mount_point, version=nfs_version)

    # Step 8 — baseline file must still be there (no data loss)
    log.info("Step 8: verifying baseline file is intact ...")
    _verify_test_file(client, mount_point, filename="pre_rotate_file")

    # Step 9 — write and read a new file post-rotation (live IO works)
    log.info("Step 9: writing and reading new file post-rotation ...")
    _write_test_file(client, mount_point, filename="post_rotate_file")
    _verify_test_file(client, mount_point, filename="post_rotate_file")

    # Sub-test — rotate only the export CephFS key specifically
    log.info("--- Sub-test: rotate only the export CephFS key ---")
    export_entity = _find_export_entity(installer, nfs_name)
    result2 = CephAdm(installer).ceph.nfs.cluster.rotate_key(
        nfs_name, entity=export_entity, key_type=KEY_TYPE
    )
    log.info("rotate-key (export only) response: %s", json.dumps(result2, indent=2))

    # service_redeployed must be false — only export key rotated, no daemon restart needed
    if result2.get("service_redeployed") is not False:
        raise OperationFailedError(
            "Expected service_redeployed=false when rotating only the export key, "
            f"got: {result2.get('service_redeployed')!r}"
        )
    if not result2.get("updated_exports"):
        raise OperationFailedError(
            "Expected updated_exports to be non-empty after export-only key rotation"
        )

    # Exports still listed and IO still works after export-only rotation
    _assert_exports_listed(installer, nfs_name, [export])
    _verify_test_file(client, mount_point, filename="post_rotate_file")
    _write_test_file(client, mount_point, filename="after_export_rotate_file")
    _verify_test_file(client, mount_point, filename="after_export_rotate_file")
    log.info("Export-only rotation sub-test PASSED.")

    Unmount(client).unmount(mount_point)
    log.info(
        "TEST PASSED - Scenario B: ceph nfs cluster rotate-key rotated keys "
        "and updated exports with no Ganesha errors; mount and IO succeeded "
        "without a manual export apply"
    )


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


def _find_export_entity(installer, nfs_name):
    """Return the export CephFS entity (client.nfs.<name>.<fs>.<hash>)."""
    entities = _get_nfs_entities(installer, nfs_name)
    for e in entities:
        parts = e.split(".")
        # Pattern: client.nfs.<name>.<fs_name>.<8-char-hash>
        if len(parts) >= 5 and not parts[-1].endswith("-rgw") and len(parts[-1]) == 8:
            return e
    raise OperationFailedError(
        f"Could not identify export CephFS entity among: {entities}"
    )


# ── entry point ───────────────────────────────────────────────────────────────


def run(ceph_cluster, **kw):
    """
    Test NFS CephX key rotation — IBMCEPH-16471.

    Config keys (all optional):
        nfs_version (str): NFS mount version, default "4.1"
        clients (int): number of client nodes to use, default 1
        nfs_name (str): NFS cluster name, default "cephfs-nfs"
        fs_name (str): CephFS filesystem name, default "cephfs"
        nfs_mount (str): client mount point, default "/mnt/nfs_cephx"
        operation (str): "manual_rotate_key", "cluster_rotate_key", or "all" (default "all")

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})

    nfs_name = config.get("nfs_name", NFS_NAME)
    fs_name = config.get("fs_name", FS_NAME)
    mount_point = config.get("nfs_mount", NFS_MOUNT)
    nfs_version = config.get("nfs_version", NFS_VERSION)
    scenario = config.get("operation", "all").lower()
    valid_ops = {"manual_rotate_key", "cluster_rotate_key", "all"}
    if scenario not in valid_ops:
        log.error(
            "Invalid operation %r; expected one of %s",
            scenario,
            sorted(valid_ops),
        )
        return 1
    no_clients = int(config.get("clients", 1))

    installer = ceph_cluster.get_nodes("installer")[0]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    if no_clients > len(clients):
        raise ConfigError(
            f"Test requires {no_clients} client(s) but only {len(clients)} available"
        )
    clients = clients[:no_clients]

    nfs_server_ip = nfs_nodes[0].ip_address
    # Pass all NFS node hostnames so the cluster is created with all available
    # NFS daemons — this ensures CephX keys are registered on all nodes and
    # makes setup more reliable.
    nfs_server_hostnames = [n.hostname for n in nfs_nodes]
    # setup_nfs_cluster appends _{i} to form the export name, so pass the
    # base prefix "/export"; the resulting export will be "/export_0"
    nfs_export_base = "/export"
    export = NFS_EXPORT  # "/export_0" — used for all mount/verify calls

    try:
        # ── setup — use the standard helper (skip_mount=True so we control
        #   mount/unmount explicitly per rotation step).
        # setup_nfs_cluster already verifies the export exists before returning.
        setup_nfs_cluster(
            clients,
            nfs_server_hostnames,
            NFS_PORT,
            nfs_version,
            nfs_name,
            mount_point,
            fs_name,
            nfs_export_base,
            fs_name,
            ceph_cluster=ceph_cluster,
            skip_mount=True,
        )

        # ── manual_rotate_key ──────────────────────────────────────────────
        if scenario in ("manual_rotate_key", "all"):
            _scenario_a_manual_rotate(
                installer,
                nfs_nodes,
                clients,
                nfs_name,
                export,
                nfs_server_ip,
                mount_point,
                nfs_version,
            )

        # ── cluster_rotate_key ─────────────────────────────────────────────
        if scenario in ("cluster_rotate_key", "all"):
            _scenario_b_rotate_key_command(
                installer,
                nfs_nodes,
                clients,
                nfs_name,
                export,
                nfs_server_ip,
                mount_point,
                nfs_version,
            )

        log.info(
            "TEST PASSED - NFS CephX key rotation completed (operation=%s)",
            scenario,
        )
        return 0

    except Exception as exc:
        log.error("Test failed: %s", exc)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Cleaning up NFS cluster %r ...", nfs_name)
        cleanup_cluster(clients, mount_point, nfs_name, nfs_export_base)
        log.info("Cleanup done.")
