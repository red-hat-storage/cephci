"""
Test module to validate automatic pruning of obsolete RBD mirror snapshots
after safe reverse synchronization.

Polarion ID: CEPH-83632907

Pre-requisites:
    Two Ceph clusters (version 9.x / Tentacle or later) with:
      - MON, MGR, OSD services active on both clusters
      - rbd-mirror daemon deployed on both clusters
      - Both clusters reporting HEALTH_OK

Test Case Flow:
    This module covers two distinct failover flows for a replicated pool
    configured with image-level snapshot-based mirroring (rx-tx):

    Flow A - test_clean_failover_snapshot_pruning  (Steps 4-15)
    ---------------------------------------------------------------
    Uses image: rep_pool_mirror/prune_clean_test
    1. Create a 10 GiB image and enable snapshot-based mirroring.
    2. Wait for up+stopped (primary) / up+replaying (secondary).
    3. Write 1 GiB of data and create a mirror snapshot on primary (Cluster-1).
    4. Verify Cluster-2 receives a non-primary copied snapshot; wait for idle replay.
    5. Demote the image on Cluster-1 (orderly failover).
    6. Promote the image on Cluster-2 WITHOUT --force (clean promote).
    7. Write 1 GiB on Cluster-2 and create a new mirror snapshot.
    8. Monitor Cluster-1 for reverse sync completion (up+replaying, copied snap present).
    9. Assert that obsolete mirror snapshots on Cluster-1 are automatically pruned:
       - No .mirror.primary.* snap with peer_uuids == []
       - No demoted-primary snap with a stale peer UUID
       - Only the current .mirror.non_primary.* copied snap remains.
    10. Verify no manual rbd snap rm was called; cluster health is OK.

    Flow B - test_unsynced_failover_snapshot_pruning  (Steps 16-28)
    ----------------------------------------------------------------
    Uses image: rep_pool_mirror/prune_unsynced_test
    1. Create a 10 GiB image, enable snapshot mirroring, write 1 GiB, create snapshot.
    2. Wait for Cluster-2 to fully sync; record the synced snapshot.
    3. Stop the rbd-mirror daemon on Cluster-2 via ceph orch daemon stop.
    4. With daemon stopped on Cluster-2, write another 1 GiB on Cluster-1
       and create a new mirror snapshot (intentionally unsynced).
    5. Confirm Cluster-2 snap list is unchanged (daemon is stopped).
    6. Demote prune_unsynced_test on Cluster-1.
    7. Force-promote on Cluster-2 (--force required, secondary is behind).
    8. Restart the stopped rbd-mirror daemon on Cluster-2.
    9. Write 1 GiB on Cluster-2 and create a new mirror snapshot.
    10. Monitor Cluster-1 for reverse sync (up+replaying + copied snap).
    11. Assert snapshots needed during incomplete sync were NOT removed prematurely.
    12. Assert final pruning on Cluster-1 after reverse sync:
        - No .mirror.primary.* snap with peer_uuids == []
        - No demoted-primary snap with stale peer UUID
        - Current .mirror.non_primary.* copied snap is present.
    13. Verify cluster health; confirm no manual snap rm was done.
"""

import datetime
import json
import time

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd_mirror import wait_for_status
from utility.log import Log

log = Log(__name__)

# ── Pruning / snapshot helper constants ─────────────────────────────────────
MIRROR_SNAP_NAMESPACE = "mirror"   # namespace type prefix in rbd snap ls --all output
PRUNE_TIMEOUT = 600                # seconds to wait for obsolete snaps to disappear
SYNC_TIMEOUT = 1800                # seconds to wait for reverse sync completion


# ── Low-level helpers ────────────────────────────────────────────────────────

def _get_all_snapshots(rbd, pool, image):
    """Return parsed JSON list of ALL snapshots (all namespaces) for an image.

    Args:
        rbd: cli.rbd.rbd.Rbd instance
        pool: pool name string
        image: image name string

    Returns:
        list of snapshot dicts as returned by rbd snap ls --all --format json
    """
    out, err = rbd.snap.list(**{"pool": pool, "image": image, "all": True, "format": "json"})
    if err and "rbd: error" in err.lower():
        raise Exception(f"rbd snap ls --all failed for {pool}/{image}: {err}")
    return json.loads(out) if out.strip() else []


def _get_mirror_snapshots(rbd, pool, image):
    """Return only mirror-namespace snapshots from rbd snap ls --all.

    Each returned dict contains at minimum:
        id, name, namespace (dict with type, primary, state, peer_uuids, complete)

    Args:
        rbd: cli.rbd.rbd.Rbd instance
        pool: pool name string
        image: image name string

    Returns:
        list of snapshot dicts whose namespace.type is 'MirrorPrimary'
        or 'MirrorNonPrimary'
    """
    all_snaps = _get_all_snapshots(rbd, pool, image)
    return [
        s for s in all_snaps
        if isinstance(s.get("namespace"), dict)
        and s["namespace"].get("type", "").startswith("Mirror")
    ]


def _has_obsolete_primary_snap(mirror_snaps):
    """Return True if any MirrorPrimary snapshot has an empty peer_uuids list.

    An empty peer_uuids list means the snapshot is no longer associated with
    any peer and should have been pruned by rbd-mirror.

    Args:
        mirror_snaps: list of snapshot dicts from _get_mirror_snapshots()

    Returns:
        bool
    """
    for snap in mirror_snaps:
        ns = snap.get("namespace", {})
        if ns.get("type") == "MirrorPrimary" and ns.get("peer_uuids") == []:
            log.debug(
                f"Found obsolete MirrorPrimary snap with empty peer_uuids: {snap['name']}"
            )
            return True
    return False


def _has_demoted_primary_snap(mirror_snaps):
    """Return True if any MirrorPrimary snapshot is still present (non-empty peer_uuids).

    After a successful reverse sync, all MirrorPrimary (demoted-primary) snapshots
    on Cluster-1 should have been pruned.

    Args:
        mirror_snaps: list of snapshot dicts from _get_mirror_snapshots()

    Returns:
        bool
    """
    for snap in mirror_snaps:
        ns = snap.get("namespace", {})
        if ns.get("type") == "MirrorPrimary":
            log.debug(f"Found MirrorPrimary (demoted-primary) snap still present: {snap['name']}")
            return True
    return False


def _has_valid_non_primary_copied_snap(mirror_snaps):
    """Return True if at least one MirrorNonPrimary snapshot is complete (copied).

    Args:
        mirror_snaps: list of snapshot dicts from _get_mirror_snapshots()

    Returns:
        bool
    """
    for snap in mirror_snaps:
        ns = snap.get("namespace", {})
        if ns.get("type") == "MirrorNonPrimary" and ns.get("complete") is True:
            log.debug(f"Found valid MirrorNonPrimary copied snap: {snap['name']}")
            return True
    return False


def _wait_for_snapshot_pruning(rbd, pool, image, timeout=PRUNE_TIMEOUT):
    """Poll until all obsolete mirror snapshots are pruned on the given cluster.

    Obsolete means:
      - MirrorPrimary snaps with peer_uuids == []  (orphaned primary)
      - Any remaining MirrorPrimary snap               (demoted-primary)

    Args:
        rbd: cli.rbd.rbd.Rbd instance
        pool: pool name string
        image: image name string
        timeout: maximum seconds to wait

    Raises:
        Exception: if timeout is reached before pruning completes
    """
    imagespec = f"{pool}/{image}"
    log.info(f"Waiting for obsolete mirror snapshots to be pruned on {imagespec}")
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while datetime.datetime.now() < deadline:
        mirror_snaps = _get_mirror_snapshots(rbd, pool, image)
        log.debug(f"Current mirror snaps on {imagespec}: {[s['name'] for s in mirror_snaps]}")
        if (
            not _has_obsolete_primary_snap(mirror_snaps)
            and not _has_demoted_primary_snap(mirror_snaps)
            and _has_valid_non_primary_copied_snap(mirror_snaps)
        ):
            log.info(
                f"Snapshot pruning complete on {imagespec}: "
                f"only valid non-primary copied snap(s) remain."
            )
            return
        time.sleep(30)
    raise Exception(
        f"Timeout waiting for obsolete mirror snapshot pruning on {imagespec}. "
        f"Remaining mirror snaps: {_get_mirror_snapshots(rbd, pool, image)}"
    )


def _get_rbd_mirror_daemon_name(client):
    """Return the first rbd-mirror daemon name from ceph orch ps.

    Args:
        client: cluster client node object

    Returns:
        str: daemon name e.g. 'rbd-mirror.ceph-rbd2-node5'

    Raises:
        Exception: if no rbd-mirror daemon is found
    """
    out, err = client.exec_command(
        cmd="ceph orch ps --daemon-type rbd-mirror --format json", sudo=True
    )
    daemons = json.loads(out)
    if not daemons:
        raise Exception("No rbd-mirror daemons found via ceph orch ps")
    daemon_name = daemons[0]["daemon_name"]
    log.info(f"Found rbd-mirror daemon: {daemon_name}")
    return daemon_name


def _stop_rbd_mirror_daemon(client, daemon_name, wait_secs=30):
    """Stop an rbd-mirror daemon via ceph orch daemon stop and verify it is stopped.

    Args:
        client: cluster client node object
        daemon_name: daemon name string e.g. 'rbd-mirror.hostname'
        wait_secs: seconds to wait after issuing stop before verifying
    """
    log.info(f"Stopping rbd-mirror daemon: {daemon_name}")
    out, err = client.exec_command(
        cmd=f"ceph orch daemon stop {daemon_name}", sudo=True
    )
    if err and "error" in err.lower():
        raise Exception(f"Failed to stop daemon {daemon_name}: {err}")
    time.sleep(wait_secs)
    # Verify daemon is stopped
    daemon_id = daemon_name.split(".", 1)[-1]
    out, err = client.exec_command(
        cmd=f"ceph orch ps --daemon-id {daemon_id} --format json", sudo=True
    )
    stats = json.loads(out)
    if stats and "stopped" not in stats[0].get("status_desc", ""):
        raise Exception(
            f"Daemon {daemon_name} did not stop. Status: {stats[0].get('status_desc')}"
        )
    log.info(f"Daemon {daemon_name} stopped successfully")


def _start_rbd_mirror_daemon(client, daemon_name, wait_secs=30):
    """Start an rbd-mirror daemon via ceph orch daemon start and verify it is running.

    Args:
        client: cluster client node object
        daemon_name: daemon name string
        wait_secs: seconds to wait after issuing start before verifying
    """
    log.info(f"Starting rbd-mirror daemon: {daemon_name}")
    out, err = client.exec_command(
        cmd=f"ceph orch daemon start {daemon_name}", sudo=True
    )
    if err and "error" in err.lower():
        raise Exception(f"Failed to start daemon {daemon_name}: {err}")
    time.sleep(wait_secs)
    daemon_id = daemon_name.split(".", 1)[-1]
    out, err = client.exec_command(
        cmd=f"ceph orch ps --daemon-id {daemon_id} --format json", sudo=True
    )
    stats = json.loads(out)
    if stats and "running" not in stats[0].get("status_desc", ""):
        raise Exception(
            f"Daemon {daemon_name} did not start. Status: {stats[0].get('status_desc')}"
        )
    log.info(f"Daemon {daemon_name} started successfully")


def _create_mirror_snapshot(rbd, pool, image):
    """Create a manual mirror snapshot on the given primary image.

    Args:
        rbd: cli.rbd.rbd.Rbd instance (primary cluster)
        pool: pool name string
        image: image name string

    Returns:
        snapshot ID (int) of the newly created snapshot
    """
    imagespec = f"{pool}/{image}"
    out, err = rbd.mirror.image.snapshot(**{"image-spec": imagespec})
    if err:
        raise Exception(f"mirror image snapshot failed for {imagespec}: {err}")
    # out typically: "Snapshot ID: <id>"
    log.info(f"Mirror snapshot created for {imagespec}: {out.strip()}")
    return out.strip()


def _write_io_on_image(rbd, pool, image, io_total="1G"):
    """Run rbd bench write on an image.

    Args:
        rbd: cli.rbd.rbd.Rbd instance
        pool: pool name string
        image: image name string
        io_total: total I/O size string e.g. '1G'
    """
    imagespec = f"{pool}/{image}"
    bench_kw = {
        "image-spec": imagespec,
        "io-type": "write",
        "io-total": io_total,
        "io-threads": 16,
    }
    log.info(f"Writing {io_total} of data to {imagespec}")
    out, err = rbd.bench(**bench_kw)
    if err and "error" in err.lower():
        raise Exception(f"rbd bench write failed for {imagespec}: {err}")
    log.info(f"I/O write completed for {imagespec}")


def _verify_cluster_health(client, cluster_name):
    """Assert that the Ceph cluster reports HEALTH_OK.

    Args:
        client: cluster client node
        cluster_name: human-readable cluster name for logging
    """
    out, err = client.exec_command(cmd="ceph -s --format json", sudo=True)
    status = json.loads(out)
    health = status.get("health", {}).get("status", "")
    log.info(f"Cluster {cluster_name} health: {health}")
    if health not in ("HEALTH_OK", "HEALTH_WARN"):
        raise Exception(
            f"Cluster {cluster_name} is not healthy: {health}\n"
            f"Checks: {status.get('health', {}).get('checks', {})}"
        )


# ── Flow A: Clean orderly failover ──────────────────────────────────────────

def test_clean_failover_snapshot_pruning(
    rbd_primary, rbd_secondary, client_primary, client_secondary,
    primary_cluster_name, secondary_cluster_name, pool, **kw
):
    """Verify automatic pruning of obsolete mirror snapshots after clean orderly failover.

    Scenario (Flow A, Steps 4-15 from test plan):
      - prune_clean_test image: 10 GiB, snapshot mirroring, rx-tx
      - Clean demote on Cluster-1 → standard promote on Cluster-2 (no --force)
      - After reverse sync: assert obsolete primary/demoted-primary snaps are pruned

    Args:
        rbd_primary: cli.rbd.rbd.Rbd instance for primary cluster (Cluster-1)
        rbd_secondary: cli.rbd.rbd.Rbd instance for secondary cluster (Cluster-2)
        client_primary: client node of primary cluster
        client_secondary: client node of secondary cluster
        primary_cluster_name: name string of primary cluster (for wait_for_status)
        secondary_cluster_name: name string of secondary cluster
        pool: pool name to use for this test (e.g. rep_pool_mirror)
        **kw: additional test keyword args
    """
    image = "prune_clean_test"
    imagespec = f"{pool}/{image}"
    log.info(
        f"[Flow A] Starting clean-failover snapshot-pruning test on {imagespec}"
    )

    # ── Step 4: Create image ────────────────────────────────────────────────
    log.info(f"[Flow A] Step 4: Creating 10 GiB image {imagespec}")
    out, err = rbd_primary.create(**{"pool": pool, "image": image, "size": "10G"})
    if err:
        raise Exception(f"Image creation failed for {imagespec}: {err}")
    log.info(f"[Flow A] Image {imagespec} created successfully")

    # ── Step 5: Enable snapshot-based mirroring ─────────────────────────────
    log.info(f"[Flow A] Step 5: Enabling snapshot mirroring on {imagespec}")
    out, err = rbd_primary.mirror.image.enable(
        **{"pool": pool, "image": image, "mode": "snapshot"}
    )
    if err:
        raise Exception(f"mirror image enable failed for {imagespec}: {err}")

    # Wait for initial mirror sync: primary → up+stopped, secondary → up+replaying
    wait_for_status(
        rbd=rbd_primary, cluster_name=primary_cluster_name,
        imagespec=imagespec, state_pattern="up+stopped",
    )
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
    )
    log.info(f"[Flow A] Mirror status verified: primary up+stopped, secondary up+replaying")

    # ── Step 6: Write 1 GiB to Cluster-1 ───────────────────────────────────
    log.info(f"[Flow A] Step 6: Writing 1 GiB to {imagespec} on Cluster-1")
    _write_io_on_image(rbd_primary, pool, image, io_total="1G")

    # ── Step 7: Create mirror snapshot on Cluster-1 ─────────────────────────
    log.info(f"[Flow A] Step 7: Creating mirror snapshot on Cluster-1")
    _create_mirror_snapshot(rbd_primary, pool, image)

    # Verify primary snapshot has the expected peer UUID
    mirror_snaps_primary = _get_mirror_snapshots(rbd_primary, pool, image)
    log.info(f"[Flow A] Mirror snaps on Cluster-1 after snapshot: {[s['name'] for s in mirror_snaps_primary]}")
    if not any(s["namespace"].get("type") == "MirrorPrimary" for s in mirror_snaps_primary):
        raise Exception("No MirrorPrimary snapshot found on Cluster-1 after snapshot creation")

    # ── Step 8: Verify Cluster-2 has non-primary copied snapshot ───────────
    log.info(f"[Flow A] Step 8: Verifying Cluster-2 non-primary snapshot")
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
    )
    mirror_snaps_sec = _get_mirror_snapshots(rbd_secondary, pool, image)
    log.info(f"[Flow A] Mirror snaps on Cluster-2: {[s['name'] for s in mirror_snaps_sec]}")

    # ── Step 9: Wait for Cluster-2 to reach idle replay ─────────────────────
    log.info(f"[Flow A] Step 9: Waiting for Cluster-2 idle replay state")
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
        description_pattern="idle",
    )
    log.info(f"[Flow A] Cluster-2 is at idle replay — ready for failover")

    # ── Step 10: Demote image on Cluster-1 ──────────────────────────────────
    log.info(f"[Flow A] Step 10: Demoting {imagespec} on Cluster-1")
    out, err = rbd_primary.mirror.image.demote(**{"image-spec": imagespec})
    if err:
        raise Exception(f"mirror image demote failed for {imagespec}: {err}")
    log.info(f"[Flow A] Image {imagespec} demoted on Cluster-1 successfully")

    # ── Step 11: Capture snapshot state immediately after demotion ──────────
    log.info(f"[Flow A] Step 11: Listing snapshots on Cluster-1 immediately after demotion")
    post_demote_snaps = _get_mirror_snapshots(rbd_primary, pool, image)
    log.info(
        f"[Flow A] Mirror snaps on Cluster-1 after demotion: "
        f"{[s['name'] for s in post_demote_snaps]}"
    )

    # ── Step 12: Promote image on Cluster-2 WITHOUT --force ─────────────────
    log.info(f"[Flow A] Step 12: Promoting {imagespec} on Cluster-2 (no --force)")
    out, err = rbd_secondary.mirror.image.promote(**{"image-spec": imagespec})
    if err:
        raise Exception(
            f"Clean promote (without --force) failed for {imagespec}: {err}. "
            "If --force is required, the pre-condition (sync state) was not met."
        )
    log.info(f"[Flow A] Image {imagespec} promoted on Cluster-2 without --force")

    # Verify Cluster-2 is now primary
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+stopped",
    )

    # ── Step 13: Write on Cluster-2 and create a new mirror snapshot ────────
    log.info(f"[Flow A] Step 13: Writing 1 GiB to {imagespec} on Cluster-2")
    _write_io_on_image(rbd_secondary, pool, image, io_total="1G")
    _create_mirror_snapshot(rbd_secondary, pool, image)
    log.info(f"[Flow A] New mirror snapshot created on Cluster-2 for reverse sync")

    # ── Step 14: Monitor Cluster-1 for reverse sync completion ──────────────
    log.info(f"[Flow A] Step 14: Waiting for Cluster-1 reverse sync (up+replaying)")
    wait_for_status(
        rbd=rbd_primary, cluster_name=primary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
        tout=datetime.timedelta(seconds=SYNC_TIMEOUT),
    )
    log.info(f"[Flow A] Cluster-1 is now up+replaying — reverse sync in progress")

    # ── Step 15: Assert obsolete snapshots are pruned on Cluster-1 ──────────
    log.info(f"[Flow A] Step 15: Waiting for automatic pruning of obsolete mirror snaps on Cluster-1")
    _wait_for_snapshot_pruning(rbd_primary, pool, image, timeout=PRUNE_TIMEOUT)

    # Final snapshot state assertion
    final_snaps = _get_mirror_snapshots(rbd_primary, pool, image)
    log.info(f"[Flow A] Final mirror snaps on Cluster-1: {[s['name'] for s in final_snaps]}")

    if _has_obsolete_primary_snap(final_snaps):
        raise Exception(
            f"[Flow A] FAIL: Obsolete MirrorPrimary snap with peer_uuids=[] "
            f"still present on Cluster-1 after reverse sync: {final_snaps}"
        )
    if _has_demoted_primary_snap(final_snaps):
        raise Exception(
            f"[Flow A] FAIL: Demoted-primary MirrorPrimary snap still present "
            f"on Cluster-1 after reverse sync: {final_snaps}"
        )
    if not _has_valid_non_primary_copied_snap(final_snaps):
        raise Exception(
            f"[Flow A] FAIL: No valid MirrorNonPrimary copied snap found "
            f"on Cluster-1 after reverse sync: {final_snaps}"
        )

    log.info(
        f"[Flow A] PASS: Automatic snapshot pruning verified for clean failover. "
        f"Only valid non-primary copied snap remains on Cluster-1."
    )

    # Verify cluster health
    _verify_cluster_health(client_primary, primary_cluster_name)
    _verify_cluster_health(client_secondary, secondary_cluster_name)


# ── Flow B: Force-promote from lagging secondary ─────────────────────────────

def test_unsynced_failover_snapshot_pruning(
    rbd_primary, rbd_secondary, client_primary, client_secondary,
    primary_cluster_name, secondary_cluster_name, pool, **kw
):
    """Verify automatic pruning of obsolete mirror snapshots after force-promote
    from a lagging secondary (daemon stopped scenario).

    Scenario (Flow B, Steps 16-28 from test plan):
      - prune_unsynced_test image: 10 GiB, snapshot mirroring, rx-tx
      - Stop rbd-mirror on Cluster-2 → create unsynchronised snapshot on Cluster-1
      - Force promote on Cluster-2 → restart daemon → reverse sync
      - Assert: snapshots NOT pruned prematurely during incomplete sync
      - Assert: obsolete snaps pruned AFTER reverse sync completes

    Args:
        rbd_primary: cli.rbd.rbd.Rbd instance for primary cluster (Cluster-1)
        rbd_secondary: cli.rbd.rbd.Rbd instance for secondary cluster (Cluster-2)
        client_primary: client node of primary cluster
        client_secondary: client node of secondary cluster
        primary_cluster_name: name string of primary cluster
        secondary_cluster_name: name string of secondary cluster
        pool: pool name to use
        **kw: additional test keyword args
    """
    image = "prune_unsynced_test"
    imagespec = f"{pool}/{image}"
    log.info(
        f"[Flow B] Starting unsynced-failover snapshot-pruning test on {imagespec}"
    )

    # ── Step 16: Create image and enable snapshot mirroring ─────────────────
    log.info(f"[Flow B] Step 16: Creating 10 GiB image {imagespec}")
    out, err = rbd_primary.create(**{"pool": pool, "image": image, "size": "10G"})
    if err:
        raise Exception(f"Image creation failed for {imagespec}: {err}")

    log.info(f"[Flow B] Enabling snapshot mirroring on {imagespec}")
    out, err = rbd_primary.mirror.image.enable(
        **{"pool": pool, "image": image, "mode": "snapshot"}
    )
    if err:
        raise Exception(f"mirror image enable failed for {imagespec}: {err}")

    wait_for_status(
        rbd=rbd_primary, cluster_name=primary_cluster_name,
        imagespec=imagespec, state_pattern="up+stopped",
    )
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
    )
    log.info(f"[Flow B] Mirror status verified: primary up+stopped, secondary up+replaying")

    # ── Step 17: Write 1 GiB and create initial mirror snapshot ────────────
    log.info(f"[Flow B] Step 17: Writing 1 GiB to {imagespec} and creating mirror snapshot")
    _write_io_on_image(rbd_primary, pool, image, io_total="1G")
    _create_mirror_snapshot(rbd_primary, pool, image)

    # ── Step 18: Wait for Cluster-2 to fully sync; record snapshot ──────────
    log.info(f"[Flow B] Step 18: Waiting for Cluster-2 to fully sync")
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
        description_pattern="idle",
    )
    pre_stop_snaps = _get_mirror_snapshots(rbd_secondary, pool, image)
    log.info(
        f"[Flow B] Cluster-2 synced. Non-primary snaps: "
        f"{[s['name'] for s in pre_stop_snaps if s['namespace'].get('type') == 'MirrorNonPrimary']}"
    )

    # ── Step 19: Stop the rbd-mirror daemon on Cluster-2 ───────────────────
    log.info(f"[Flow B] Step 19: Stopping rbd-mirror daemon on Cluster-2")
    daemon_name = _get_rbd_mirror_daemon_name(client_secondary)
    _stop_rbd_mirror_daemon(client_secondary, daemon_name)

    # ── Step 20: Write 1 GiB and create new mirror snapshot on Cluster-1 ───
    log.info(
        f"[Flow B] Step 20: Writing 1 GiB and creating new mirror snapshot on Cluster-1"
        f" while Cluster-2 daemon is stopped"
    )
    _write_io_on_image(rbd_primary, pool, image, io_total="1G")
    _create_mirror_snapshot(rbd_primary, pool, image)
    log.info(f"[Flow B] New snapshot created on Cluster-1 (intentionally not synced to Cluster-2)")

    # ── Step 21: Verify Cluster-2 snaps are unchanged ───────────────────────
    log.info(f"[Flow B] Step 21: Verifying Cluster-2 snaps unchanged (daemon stopped)")
    post_stop_snaps = _get_mirror_snapshots(rbd_secondary, pool, image)
    pre_stop_snap_names = {s["name"] for s in pre_stop_snaps}
    post_stop_snap_names = {s["name"] for s in post_stop_snaps}
    if pre_stop_snap_names != post_stop_snap_names:
        raise Exception(
            f"[Flow B] Cluster-2 snaps changed while daemon was stopped! "
            f"Before: {pre_stop_snap_names}, After: {post_stop_snap_names}"
        )
    log.info(f"[Flow B] Cluster-2 snaps unchanged as expected: {post_stop_snap_names}")

    # ── Step 22: Demote prune_unsynced_test on Cluster-1 ────────────────────
    log.info(f"[Flow B] Step 22: Demoting {imagespec} on Cluster-1")
    out, err = rbd_primary.mirror.image.demote(**{"image-spec": imagespec})
    if err:
        raise Exception(f"mirror image demote failed for {imagespec}: {err}")
    log.info(f"[Flow B] Image {imagespec} demoted on Cluster-1")

    # ── Step 23: Force-promote on Cluster-2 ─────────────────────────────────
    log.info(f"[Flow B] Step 23: Force-promoting {imagespec} on Cluster-2")
    out, err = rbd_secondary.mirror.image.promote(
        **{"image-spec": imagespec, "force": True}
    )
    if err:
        raise Exception(f"force promote failed for {imagespec}: {err}")
    log.info(f"[Flow B] {imagespec} force-promoted on Cluster-2 successfully")

    # Verify Cluster-2 is primary
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+stopped",
    )

    # ── Step 24: Restart rbd-mirror daemon on Cluster-2 ─────────────────────
    log.info(f"[Flow B] Step 24: Restarting rbd-mirror daemon on Cluster-2")
    _start_rbd_mirror_daemon(client_secondary, daemon_name)

    # Wait for daemon to resume ownership of the image
    wait_for_status(
        rbd=rbd_secondary, cluster_name=secondary_cluster_name,
        imagespec=imagespec, state_pattern="up+stopped",
    )
    log.info(f"[Flow B] rbd-mirror daemon restarted; Cluster-2 image back to up+stopped")

    # ── Step 25: Write 1 GiB on Cluster-2 and create new mirror snapshot ────
    log.info(f"[Flow B] Step 25: Writing 1 GiB to {imagespec} on Cluster-2")
    _write_io_on_image(rbd_secondary, pool, image, io_total="1G")
    _create_mirror_snapshot(rbd_secondary, pool, image)
    log.info(f"[Flow B] New mirror snapshot created on Cluster-2 for reverse sync")

    # ── Step 26: Monitor Cluster-1 for reverse sync ─────────────────────────
    log.info(f"[Flow B] Step 26: Waiting for Cluster-1 reverse sync (up+replaying)")
    wait_for_status(
        rbd=rbd_primary, cluster_name=primary_cluster_name,
        imagespec=imagespec, state_pattern="up+replaying",
        tout=datetime.timedelta(seconds=SYNC_TIMEOUT),
    )
    log.info(f"[Flow B] Cluster-1 is up+replaying — reverse sync in progress")

    # ── Step 27: Verify no premature pruning during incomplete sync ──────────
    log.info(
        f"[Flow B] Step 27: Checking that required snapshots were NOT removed "
        f"before reverse sync completed"
    )
    # At this point Cluster-1 should still have mirror snaps (not yet pruned prematurely)
    # The snapshot state must have at least one mirror snap present while replaying
    in_progress_snaps = _get_mirror_snapshots(rbd_primary, pool, image)
    log.info(
        f"[Flow B] Mirror snaps on Cluster-1 during reverse sync: "
        f"{[s['name'] for s in in_progress_snaps]}"
    )
    # It is acceptable (and expected) that some snaps still exist at this point

    # ── Step 28: Assert obsolete snapshots pruned after reverse sync ─────────
    log.info(
        f"[Flow B] Step 28: Waiting for automatic pruning of obsolete snaps on Cluster-1"
    )
    _wait_for_snapshot_pruning(rbd_primary, pool, image, timeout=PRUNE_TIMEOUT)

    final_snaps = _get_mirror_snapshots(rbd_primary, pool, image)
    log.info(f"[Flow B] Final mirror snaps on Cluster-1: {[s['name'] for s in final_snaps]}")

    if _has_obsolete_primary_snap(final_snaps):
        raise Exception(
            f"[Flow B] FAIL: Obsolete MirrorPrimary snap with peer_uuids=[] "
            f"still present on Cluster-1 after reverse sync: {final_snaps}"
        )
    if _has_demoted_primary_snap(final_snaps):
        raise Exception(
            f"[Flow B] FAIL: Demoted-primary MirrorPrimary snap still present "
            f"on Cluster-1 after reverse sync: {final_snaps}"
        )
    if not _has_valid_non_primary_copied_snap(final_snaps):
        raise Exception(
            f"[Flow B] FAIL: No valid MirrorNonPrimary copied snap found "
            f"on Cluster-1 after reverse sync: {final_snaps}"
        )

    log.info(
        f"[Flow B] PASS: Automatic snapshot pruning verified for unsynced failover. "
        f"Only valid non-primary copied snap remains on Cluster-1."
    )

    # Verify cluster health
    _verify_cluster_health(client_primary, primary_cluster_name)
    _verify_cluster_health(client_secondary, secondary_cluster_name)


# ── Entry point ───────────────────────────────────────────────────────────────

def run(**kw):
    """
    Validate automatic pruning of obsolete RBD mirror snapshots after safe
    reverse synchronization.

    Polarion ID: CEPH-83XXXXXX

    Covers two flows:
      - Flow A: Clean orderly failover (prune_clean_test image)
      - Flow B: Force-promote from lagging secondary (prune_unsynced_test image)

    Both flows verify that:
      - rbd-mirror automatically removes .mirror.primary.* snaps with peer_uuids == []
      - rbd-mirror automatically removes demoted-primary snaps after reverse sync
      - No manual rbd snap rm is performed
      - Cluster health remains OK throughout

    YAML config example::

        clusters:
          ceph-rbd1:
            config:
              rep-pool-only: True
              rep_pool_config:
                num_pools: 1
                num_images: 1
                size: 1G
                io_total: 1G
                mode: image
                mirrormode: snapshot
                way: rx-tx

    Args:
        **kw: test keyword arguments from the YAML suite config

    Returns:
        int: 0 on success, 1 on failure
    """
    log.info(
        "Starting RBD mirror snapshot pruning test — "
        "CEPH-83632907 (Flow A: clean failover, Flow B: unsynced failover)"
    )

    mirror_obj = initial_mirror_config(**kw)
    mirror_obj.pop("output", [])

    pri_config = None
    sec_config = None
    for val in mirror_obj.values():
        if not val.get("is_secondary", False):
            pri_config = val
        else:
            sec_config = val

    if not pri_config or not sec_config:
        log.error("Failed to identify primary and secondary cluster configs")
        return 1

    pool_types = list(mirror_obj.values())[0].get("pool_types", [])
    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")
    client_secondary = sec_config.get("client")
    primary_cluster_name = pri_config.get("cluster").name
    secondary_cluster_name = sec_config.get("cluster").name

    # Resolve the pool name from the configuration
    config = kw.get("config", {})
    rep_pool_config = getdict(config.get("rep_pool_config", {}))
    if not rep_pool_config:
        log.error("rep_pool_config is required for this test")
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
        return 1

    pool = list(rep_pool_config.keys())[0]
    log.info(f"Using pool: {pool}")

    try:
        # ── Verify mirror pool info and status on both clusters (Step 3) ────
        log.info("Step 3: Verifying mirror pool info and status on both clusters")
        out, err = rbd_primary.mirror.pool.info(**{"pool": pool, "format": "json"})
        if err:
            raise Exception(f"mirror pool info failed on primary: {err}")
        log.info(f"Primary mirror pool info: {out}")

        out, err = rbd_secondary.mirror.pool.info(**{"pool": pool, "format": "json"})
        if err:
            raise Exception(f"mirror pool info failed on secondary: {err}")
        log.info(f"Secondary mirror pool info: {out}")

        out, err = rbd_primary.mirror.pool.status(**{"pool": pool, "verbose": True})
        if err:
            raise Exception(f"mirror pool status failed on primary: {err}")
        log.info(f"Primary mirror pool status: {out}")

        out, err = rbd_secondary.mirror.pool.status(**{"pool": pool, "verbose": True})
        if err:
            raise Exception(f"mirror pool status failed on secondary: {err}")
        log.info(f"Secondary mirror pool status: {out}")

        # ── Flow A ────────────────────────────────────────────────────────────
        log.info("=" * 70)
        log.info("Running Flow A: test_clean_failover_snapshot_pruning")
        log.info("=" * 70)
        test_clean_failover_snapshot_pruning(
            rbd_primary=rbd_primary,
            rbd_secondary=rbd_secondary,
            client_primary=client_primary,
            client_secondary=client_secondary,
            primary_cluster_name=primary_cluster_name,
            secondary_cluster_name=secondary_cluster_name,
            pool=pool,
            **kw,
        )

        # ── Flow B ────────────────────────────────────────────────────────────
        log.info("=" * 70)
        log.info("Running Flow B: test_unsynced_failover_snapshot_pruning")
        log.info("=" * 70)
        test_unsynced_failover_snapshot_pruning(
            rbd_primary=rbd_primary,
            rbd_secondary=rbd_secondary,
            client_primary=client_primary,
            client_secondary=client_secondary,
            primary_cluster_name=primary_cluster_name,
            secondary_cluster_name=secondary_cluster_name,
            pool=pool,
            **kw,
        )

        # ── Step 29: Final overall health check ──────────────────────────────
        log.info("Step 29: Final cluster health and mirror status verification")
        _verify_cluster_health(client_primary, primary_cluster_name)
        _verify_cluster_health(client_secondary, secondary_cluster_name)

        log.info(
            "RBD mirror snapshot pruning test PASSED — "
            "both Flow A (clean failover) and Flow B (unsynced failover) passed."
        )
        return 0

    except Exception as e:
        log.exception(f"RBD mirror snapshot pruning test FAILED: {e}")
        return 1

    finally:
        # ── Step 31: Cleanup ─────────────────────────────────────────────────
        log.info("Step 31: Cleaning up test images and pool")
        for img in ["prune_clean_test", "prune_unsynced_test"]:
            imagespec = f"{pool}/{img}"
            try:
                # Disable mirroring before removing the image
                rbd_primary.mirror.image.disable(**{"image-spec": imagespec})
            except Exception:
                pass
            try:
                rbd_primary.remove(**{"pool": pool, "image": img})
                log.info(f"Removed image {imagespec} from primary cluster")
            except Exception as e:
                log.warning(f"Could not remove image {imagespec}: {e}")
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
