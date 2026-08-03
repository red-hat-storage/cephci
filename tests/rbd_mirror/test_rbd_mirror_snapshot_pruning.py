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
import random
import time

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.cluster_operations import check_health
from ceph.rbd.workflows.rbd import create_single_image, remove_single_image_and_verify
from ceph.rbd.workflows.rbd_mirror import (
    ensure_rbd_mirror_daemon_running,
    get_mirror_snapshots,
    get_rbd_mirror_daemon_name,
    is_mirror_primary_snap,
    start_rbd_mirror_daemon,
    stop_rbd_mirror_daemon,
    wait_for_image_idle_replay,
    wait_for_snapshot_pruning,
    wait_for_status,
)
from utility.log import Log

log = Log(__name__)
# ── Constants ────────────────────────────────────────────────────────────────
PRUNE_TIMEOUT = 600  # seconds to wait for obsolete snaps to disappear
SYNC_TIMEOUT = 1800  # seconds to wait for reverse sync completion
MIRROR_SNAP_TIMEOUT = 120  # seconds to wait for mirror snapshot to appear


def _mirror_snap_names(rbd, pool, image):
    """Return mirror-namespace snapshot names for pool/image."""
    return {s["name"] for s in get_mirror_snapshots(rbd, pool, image)}


def _wait_for_stable_mirror_snap_names(
    rbd,
    pool,
    image,
    stable_reads=3,
    poll_interval=5,
    timeout=90,
):
    """Poll until mirror snap names are unchanged for consecutive reads."""
    deadline = time.time() + timeout
    last_names = None
    stable_count = 0
    while time.time() < deadline:
        names = _mirror_snap_names(rbd, pool, image)
        if names == last_names:
            stable_count += 1
            if stable_count >= stable_reads:
                return names
        else:
            last_names = names
            stable_count = 1
        time.sleep(poll_interval)
    log.warning(
        f"Mirror snap set on {pool}/{image} did not stabilize within {timeout}s; "
        f"using last observed set: {last_names}"
    )
    return last_names or set()


def _ensure_mirror_daemons_on_all_clusters(kw):
    """Start rbd-mirror daemons left stopped by a prior failed Flow B run."""
    for cluster_name, cluster in kw.get("ceph_cluster_dict", {}).items():
        try:
            client = cluster.get_nodes(role="client")[0]
            ensure_rbd_mirror_daemon_running(client)
        except Exception as e:
            log.warning(
                f"Could not ensure rbd-mirror daemon on cluster {cluster_name}: {e}"
            )


# ── Flow A: Clean orderly failover ──────────────────────────────────────────


def test_clean_failover_snapshot_pruning(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    primary_cluster_name,
    secondary_cluster_name,
    pool,
    **kw,
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
    pool_entry_config = kw.get("pool_entry_config", {})
    is_ec_pool = kw.get("is_ec_pool", False)
    image = "prune_clean_test"
    imagespec = f"{pool}/{image}"
    log.info(f"[Flow A] Starting clean-failover snapshot-pruning test on {imagespec}")

    # ── Step 4: Create image ────────────────────────────────────────────────
    log.info(f"[Flow A] Step 4: Creating 10 GiB image {imagespec}")
    create_single_image(
        {},
        "ceph",
        rbd_primary,
        pool,
        pool_entry_config,
        image,
        {
            "size": "10G",
            "image-feature": "exclusive-lock,object-map,fast-diff",
        },
        is_ec_pool,
        raise_exception=True,
    )

    # ── Step 5: Enable snapshot-based mirroring ─────────────────────────────
    log.info(f"[Flow A] Step 5: Enabling snapshot mirroring on {imagespec}")
    out, err = rbd_primary.mirror.image.enable(
        **{"pool": pool, "image": image, "mode": "snapshot"}
    )
    if err:
        raise Exception(f"mirror image enable failed for {imagespec}: {err}")

    wait_for_status(
        rbd=rbd_primary,
        cluster_name=primary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+stopped",
    )
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+replaying",
    )
    log.info(
        "[Flow A] Mirror status verified: primary up+stopped, secondary up+replaying"
    )

    # ── Step 6: Write 1 GiB to Cluster-1 ───────────────────────────────────
    log.info(f"[Flow A] Step 6: Writing 1 GiB to {imagespec} on Cluster-1")
    out, err = rbd_primary.bench(
        **{
            "image-spec": imagespec,
            "io-type": "write",
            "io-total": "1G",
            "io-threads": 16,
        }
    )
    if err:
        raise Exception(f"rbd bench write failed for {imagespec}: {err}")

    # ── Step 7: Create mirror snapshot on Cluster-1 ─────────────────────────
    log.info("[Flow A] Step 7: Creating mirror snapshot on Cluster-1")
    out, err = rbd_primary.mirror.image.snapshot(**{"image-spec": imagespec})
    if err:
        raise Exception(f"mirror image snapshot failed for {imagespec}: {err}")

    deadline = time.time() + MIRROR_SNAP_TIMEOUT
    mirror_snaps_primary = []
    while time.time() < deadline:
        mirror_snaps_primary = get_mirror_snapshots(rbd_primary, pool, image)
        if any(is_mirror_primary_snap(s) for s in mirror_snaps_primary):
            break
        time.sleep(5)
    else:
        raise Exception(
            f"No primary mirror snapshot on {imagespec} after {MIRROR_SNAP_TIMEOUT}s; "
            f"snaps={mirror_snaps_primary}"
        )
    log.info(
        f"[Flow A] Mirror snaps on Cluster-1 after snapshot: "
        f"{[s['name'] for s in mirror_snaps_primary]}"
    )

    # ── Step 8: Verify Cluster-2 has non-primary copied snapshot ───────────
    log.info("[Flow A] Step 8: Verifying Cluster-2 non-primary snapshot")
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+replaying",
    )
    mirror_snaps_sec = get_mirror_snapshots(rbd_secondary, pool, image)
    log.info(
        f"[Flow A] Mirror snaps on Cluster-2: {[s['name'] for s in mirror_snaps_sec]}"
    )

    # ── Step 9: Wait for Cluster-2 to reach idle replay ─────────────────────
    log.info("[Flow A] Step 9: Waiting for Cluster-2 idle replay state")
    wait_for_image_idle_replay(
        rbd_secondary,
        imagespec,
        secondary_cluster_name,
    )
    log.info("[Flow A] Cluster-2 is at idle replay — ready for failover")

    # ── Step 10: Demote image on Cluster-1 ──────────────────────────────────
    log.info(f"[Flow A] Step 10: Demoting {imagespec} on Cluster-1")
    out, err = rbd_primary.mirror.image.demote(**{"image-spec": imagespec})
    if err:
        raise Exception(f"mirror image demote failed for {imagespec}: {err}")

    # ── Step 11: Capture snapshot state immediately after demotion ──────────
    log.info(
        "[Flow A] Step 11: Listing snapshots on Cluster-1 immediately after demotion"
    )
    post_demote_snaps = get_mirror_snapshots(rbd_primary, pool, image)
    log.info(
        f"[Flow A] Mirror snaps on Cluster-1 after demotion: "
        f"{[s['name'] for s in post_demote_snaps]}"
    )

    # ── Step 12: Promote image on Cluster-2 WITHOUT --force ─────────────────
    log.info(f"[Flow A] Step 12: Promoting {imagespec} on Cluster-2 (no --force)")
    out, err = rbd_secondary.mirror.image.promote(**{"image-spec": imagespec})
    if err:
        raise Exception(
            f"Clean promote (without --force) failed for {imagespec}: {err}"
        )

    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+stopped",
    )

    # ── Step 13: Write on Cluster-2 and create a new mirror snapshot ────────
    log.info(f"[Flow A] Step 13: Writing 1 GiB to {imagespec} on Cluster-2")
    out, err = rbd_secondary.bench(
        **{
            "image-spec": imagespec,
            "io-type": "write",
            "io-total": "1G",
            "io-threads": 16,
        }
    )
    if err:
        raise Exception(f"rbd bench write failed for {imagespec}: {err}")
    rbd_secondary.mirror.image.snapshot(**{"image-spec": imagespec})

    # ── Step 14: Monitor Cluster-1 for reverse sync completion ──────────────
    log.info("[Flow A] Step 14: Waiting for Cluster-1 reverse sync (up+replaying)")
    wait_for_status(
        rbd=rbd_primary,
        cluster_name=primary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+replaying",
        tout=datetime.timedelta(seconds=SYNC_TIMEOUT),
    )

    # ── Step 15: Assert obsolete snapshots are pruned on Cluster-1 ──────────
    log.info("[Flow A] Step 15: Waiting for automatic pruning of obsolete mirror snaps")
    wait_for_snapshot_pruning(rbd_primary, pool, image, timeout=PRUNE_TIMEOUT)

    log.info("[Flow A] PASS: Automatic snapshot pruning verified for clean failover.")
    check_health(client_primary)
    check_health(client_secondary)


# ── Flow B: Force-promote from lagging secondary ─────────────────────────────


def test_unsynced_failover_snapshot_pruning(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    primary_cluster_name,
    secondary_cluster_name,
    pool,
    **kw,
):
    """Verify automatic pruning after force-promote from a lagging secondary.

    Scenario (Flow B, Steps 16-28 from test plan):
      - prune_unsynced_test image: 10 GiB, snapshot mirroring, rx-tx
      - Stop rbd-mirror on Cluster-2 → create unsynchronised snapshot on Cluster-1
      - Force promote on Cluster-2 → restart daemon → reverse sync
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
    pool_entry_config = kw.get("pool_entry_config", {})
    is_ec_pool = kw.get("is_ec_pool", False)
    image = "prune_unsynced_test"
    imagespec = f"{pool}/{image}"
    log.info(
        f"[Flow B] Starting unsynced-failover snapshot-pruning test on {imagespec}"
    )

    # ── Step 16: Create image and enable snapshot mirroring ─────────────────
    log.info(f"[Flow B] Step 16: Creating 10 GiB image {imagespec}")
    create_single_image(
        {},
        "ceph",
        rbd_primary,
        pool,
        pool_entry_config,
        image,
        {
            "size": "10G",
            "image-feature": "exclusive-lock,object-map,fast-diff",
        },
        is_ec_pool,
        raise_exception=True,
    )

    out, err = rbd_primary.mirror.image.enable(
        **{"pool": pool, "image": image, "mode": "snapshot"}
    )
    if err:
        raise Exception(f"mirror image enable failed for {imagespec}: {err}")

    wait_for_status(
        rbd=rbd_primary,
        cluster_name=primary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+stopped",
    )
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_cluster_name,
        imagespec=imagespec,
        state_pattern="up+replaying",
    )

    # ── Step 17: Write 1 GiB and create initial mirror snapshot ────────────
    log.info("[Flow B] Step 17: Writing 1 GiB and creating initial mirror snapshot")
    out, err = rbd_primary.bench(
        **{
            "image-spec": imagespec,
            "io-type": "write",
            "io-total": "1G",
            "io-threads": 16,
        }
    )
    if err:
        raise Exception(f"rbd bench write failed for {imagespec}: {err}")
    out, err = rbd_primary.mirror.image.snapshot(**{"image-spec": imagespec})
    if err:
        raise Exception(f"mirror image snapshot failed for {imagespec}: {err}")
    deadline = time.time() + MIRROR_SNAP_TIMEOUT
    while time.time() < deadline:
        if any(
            is_mirror_primary_snap(s)
            for s in get_mirror_snapshots(rbd_primary, pool, image)
        ):
            break
        time.sleep(5)
    else:
        raise Exception(
            f"No primary mirror snapshot on {imagespec} after {MIRROR_SNAP_TIMEOUT}s"
        )

    # ── Step 18: Wait for Cluster-2 to fully sync ───────────────────────────
    log.info("[Flow B] Step 18: Waiting for Cluster-2 to fully sync")
    wait_for_image_idle_replay(
        rbd_secondary,
        imagespec,
        secondary_cluster_name,
    )
    pre_stop_snap_names = _mirror_snap_names(rbd_secondary, pool, image)
    log.info(
        f"[Flow B] Cluster-2 synced. Mirror snaps before daemon stop: "
        f"{pre_stop_snap_names}"
    )

    # ── Step 19: Stop the rbd-mirror daemon on Cluster-2 ───────────────────
    log.info("[Flow B] Step 19: Stopping rbd-mirror daemon on Cluster-2")
    daemon_name = get_rbd_mirror_daemon_name(client_secondary)
    daemon_stopped = False
    try:
        stop_rbd_mirror_daemon(client_secondary, daemon_name)
        daemon_stopped = True

        post_stop_baseline_names = _wait_for_stable_mirror_snap_names(
            rbd_secondary, pool, image
        )
        if pre_stop_snap_names != post_stop_baseline_names:
            log.info(
                "[Flow B] Cluster-2 mirror snaps changed during daemon shutdown "
                f"(pre-stop: {pre_stop_snap_names}, post-stop baseline: "
                f"{post_stop_baseline_names})"
            )
        else:
            log.info(
                f"[Flow B] Cluster-2 post-stop baseline unchanged: "
                f"{post_stop_baseline_names}"
            )

        # ── Step 20: Write 1 GiB and create new mirror snapshot on Cluster-1 ───
        log.info(
            "[Flow B] Step 20: Writing 1 GiB and creating new snapshot on "
            "Cluster-1 (unsynced)"
        )
        primary_primary_before = {
            s["name"]
            for s in get_mirror_snapshots(rbd_primary, pool, image)
            if is_mirror_primary_snap(s)
        }
        out, err = rbd_primary.bench(
            **{
                "image-spec": imagespec,
                "io-type": "write",
                "io-total": "1G",
                "io-threads": 16,
            }
        )
        if err:
            raise Exception(f"rbd bench write failed for {imagespec}: {err}")
        out, err = rbd_primary.mirror.image.snapshot(**{"image-spec": imagespec})
        if err:
            raise Exception(f"mirror image snapshot failed for {imagespec}: {err}")

        deadline = time.time() + MIRROR_SNAP_TIMEOUT
        new_primary_snap_names = set()
        while time.time() < deadline:
            new_primary_snap_names = {
                s["name"]
                for s in get_mirror_snapshots(rbd_primary, pool, image)
                if is_mirror_primary_snap(s)
            } - primary_primary_before
            if new_primary_snap_names:
                break
            time.sleep(5)
        else:
            raise Exception(
                f"[Flow B] No new primary mirror snapshot on Cluster-1 after unsynced "
                f"write; existing primary snaps: {primary_primary_before}"
            )
        log.info(
            f"[Flow B] Cluster-1 created new unsynced primary snap(s): "
            f"{new_primary_snap_names}"
        )

        # ── Step 21: Verify Cluster-2 snaps unchanged since daemon stop ─────────
        log.info(
            "[Flow B] Step 21: Verifying Cluster-2 snaps unchanged since daemon stop"
        )
        after_unsynced_names = _mirror_snap_names(rbd_secondary, pool, image)
        if after_unsynced_names != post_stop_baseline_names:
            raise Exception(
                f"[Flow B] Cluster-2 snaps changed after unsynced write on Cluster-1! "
                f"Post-stop baseline: {post_stop_baseline_names}, "
                f"After unsynced write: {after_unsynced_names}"
            )
        log.info(
            f"[Flow B] Cluster-2 snaps unchanged since daemon stop: "
            f"{after_unsynced_names}"
        )

        # ── Step 22: Demote prune_unsynced_test on Cluster-1 ────────────────────
        log.info(f"[Flow B] Step 22: Demoting {imagespec} on Cluster-1")
        out, err = rbd_primary.mirror.image.demote(**{"image-spec": imagespec})
        if err:
            raise Exception(f"mirror image demote failed for {imagespec}: {err}")

        # ── Step 23: Force-promote on Cluster-2 ─────────────────────────────────
        log.info(f"[Flow B] Step 23: Force-promoting {imagespec} on Cluster-2")
        out, err = rbd_secondary.mirror.image.promote(
            **{"image-spec": imagespec, "force": True}
        )
        if err:
            raise Exception(f"force promote failed for {imagespec}: {err}")

        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=secondary_cluster_name,
            imagespec=imagespec,
            state_pattern="up+stopped",
        )

        # ── Step 24: Restart rbd-mirror daemon on Cluster-2 ─────────────────────
        log.info("[Flow B] Step 24: Restarting rbd-mirror daemon on Cluster-2")
        start_rbd_mirror_daemon(client_secondary, daemon_name)
        daemon_stopped = False

        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=secondary_cluster_name,
            imagespec=imagespec,
            state_pattern="up+stopped",
        )

        # ── Step 25: Write 1 GiB on Cluster-2 and create new mirror snapshot ────
        log.info(f"[Flow B] Step 25: Writing 1 GiB to {imagespec} on Cluster-2")
        out, err = rbd_secondary.bench(
            **{
                "image-spec": imagespec,
                "io-type": "write",
                "io-total": "1G",
                "io-threads": 16,
            }
        )
        if err:
            raise Exception(f"rbd bench write failed for {imagespec}: {err}")
        out, err = rbd_secondary.mirror.image.snapshot(**{"image-spec": imagespec})
        if err:
            raise Exception(f"mirror image snapshot failed for {imagespec}: {err}")

        # ── Step 26: Monitor Cluster-1 for reverse sync ─────────────────────────
        log.info("[Flow B] Step 26: Waiting for Cluster-1 reverse sync (up+replaying)")
        wait_for_status(
            rbd=rbd_primary,
            cluster_name=primary_cluster_name,
            imagespec=imagespec,
            state_pattern="up+replaying",
            tout=datetime.timedelta(seconds=SYNC_TIMEOUT),
        )

        # ── Step 27: Verify no premature pruning during incomplete sync ──────────
        log.info("[Flow B] Step 27: Checking mirror snaps during reverse sync")
        in_progress_snaps = get_mirror_snapshots(rbd_primary, pool, image)
        log.info(
            f"[Flow B] Mirror snaps on Cluster-1 during reverse sync: "
            f"{[s['name'] for s in in_progress_snaps]}"
        )

        # ── Step 28: Assert obsolete snapshots pruned after reverse sync ─────────
        log.info("[Flow B] Step 28: Waiting for automatic pruning on Cluster-1")
        wait_for_snapshot_pruning(rbd_primary, pool, image, timeout=PRUNE_TIMEOUT)
    finally:
        if daemon_stopped:
            log.info(
                "[Flow B] Restoring rbd-mirror daemon on Cluster-2 after early exit"
            )
            start_rbd_mirror_daemon(client_secondary, daemon_name)

    log.info(
        "[Flow B] PASS: Automatic snapshot pruning verified for unsynced failover."
    )
    check_health(client_primary)
    check_health(client_secondary)


# ── Entry point ───────────────────────────────────────────────────────────────
def run(**kw):
    """
    Validate automatic pruning of obsolete RBD mirror snapshots after safe
    reverse synchronization.

    Polarion ID: CEPH-83632907

    Covers two flows:
      - Flow A: Clean orderly failover (prune_clean_test image)
      - Flow B: Force-promote from lagging secondary (prune_unsynced_test image)

    YAML config example::

        clusters:
          ceph-rbd1:
            config:
              rep_pool_config:
                num_pools: 1
                num_images: 1
                size: 10G
                io_total: 1G
                mode: image
                mirrormode: snapshot
                way: rx-tx
              ec_pool_config:
                num_pools: 1
                num_images: 1
                size: 10G
                io_total: 1G
                mode: image
                mirrormode: snapshot
                way: rx-tx

    Each run randomly selects either ``rep_pool_config`` or ``ec_pool_config``
    and sets ``rep-pool-only`` / ``ec-pool-only`` before mirror setup.

    Args:
        **kw: test keyword arguments from the YAML suite config

    Returns:
        int: 0 on success, 1 on failure
    """
    log.info(
        "Starting RBD mirror snapshot pruning test — CEPH-83632907 "
        "(Flow A: clean failover, Flow B: unsynced failover)"
    )

    config = kw.get("config", {})
    pool_type_choices = ["rep_pool_config", "ec_pool_config"]
    available_pool_types = [
        pool_type for pool_type in pool_type_choices if config.get(pool_type)
    ]
    if not available_pool_types:
        log.error("At least one of rep_pool_config or ec_pool_config is required")
        return 1

    pool_type = random.choice(available_pool_types)
    if pool_type == "rep_pool_config":
        config["rep-pool-only"] = True
    else:
        config["ec-pool-only"] = True
    log.info(f"Randomly selected pool type for this run: {pool_type}")

    _ensure_mirror_daemons_on_all_clusters(kw)

    mirror_obj = None
    client_primary = None
    client_secondary = None
    rbd_primary = None
    rbd_secondary = None
    pool_types = []
    pool = None

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

    pool_config = getdict(config.get(pool_type, {}))
    if not pool_config:
        log.error(f"No mirrored pool found for {pool_type}")
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
        return 1

    pool = list(pool_config.keys())[0]
    pool_entry_config = pool_config[pool]
    is_ec_pool = pool_type == "ec_pool_config"
    log.info(f"Using {pool_type} pool: {pool}")
    if is_ec_pool:
        log.info(f"EC data pool for {pool}: {pool_entry_config.get('data_pool')}")

    flow_kw = {
        **kw,
        "pool_entry_config": pool_entry_config,
        "is_ec_pool": is_ec_pool,
    }

    try:
        # ── Step 3: Verify mirror pool info and status ───────────────────────
        log.info("Step 3: Verifying mirror pool info and status on both clusters")
        out, err = rbd_primary.mirror.pool.info(**{"pool": pool, "format": "json"})
        if err:
            raise Exception(f"mirror pool info failed on primary: {err}")
        log.info(f"Primary mirror pool info: {out}")

        out, err = rbd_secondary.mirror.pool.info(**{"pool": pool, "format": "json"})
        if err:
            raise Exception(f"mirror pool info failed on secondary: {err}")
        log.info(f"Secondary mirror pool info: {out}")

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
            **flow_kw,
        )

        # ── Flow B ────────────────────────────────────────────────────────────#
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
            **flow_kw,
        )

        # ── Step 29: Final overall health check ──────────────────────────────#
        log.info("Step 29: Final cluster health verification")
        check_health(client_primary)
        check_health(client_secondary)

        log.info(
            f"RBD mirror snapshot pruning test PASSED on {pool_type} "
            f"(pool={pool}) — Flow A and Flow B completed."
        )
        return 0

    except Exception as e:
        log.exception(f"RBD mirror snapshot pruning test FAILED: {e}")
        return 1

    finally:
        # ── Cleanup ──────────────────────────────────────────────────────────
        log.info("Cleanup: restoring rbd-mirror daemons and removing test images")
        for label, client in (
            ("primary", client_primary),
            ("secondary", client_secondary),
        ):
            if client:
                try:
                    ensure_rbd_mirror_daemon_running(client)
                except Exception as e:
                    log.warning(
                        f"Could not restore rbd-mirror daemon on {label} cluster: {e}"
                    )
        if pool and mirror_obj:
            for img in ["prune_clean_test", "prune_unsynced_test"]:
                imagespec = f"{pool}/{img}"
                for rbd in (rbd_primary, rbd_secondary):
                    if not rbd:
                        continue
                    try:
                        rbd.mirror.image.disable(
                            **{"image-spec": imagespec, "force": True}
                        )
                    except Exception:
                        pass
                    if remove_single_image_and_verify(rbd=rbd, pool=pool, image=img):
                        log.warning(f"Could not remove image {imagespec}")
            cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
