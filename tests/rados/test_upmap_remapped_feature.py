"""
Automation for upmap-remapped feature validation.

Validates usage of CERN upmap-remapped.py to resolve prolonged remapped /
backfilling PG states after an OSD topology change with norebalance set.
"""

import json
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

UPMAP_SCRIPT_URL = (
    "https://raw.githubusercontent.com/cernceph/ceph-scripts/master/"
    "tools/upmap/upmap-remapped.py"
)
UPMAP_SCRIPT_NAME = "upmap-remapped.py"

# Ceph release ordering used to compare require_min_compat_client
CEPH_RELEASE_ORDER = {
    "argonaut": 1,
    "bobtail": 2,
    "cuttlefish": 3,
    "dumpling": 4,
    "emperor": 5,
    "firefly": 6,
    "giant": 7,
    "hammer": 8,
    "infernalis": 9,
    "jewel": 10,
    "kraken": 11,
    "luminous": 12,
    "mimic": 13,
    "nautilus": 14,
    "octopus": 15,
    "pacific": 16,
    "quincy": 17,
    "reef": 18,
    "squid": 19,
    "tentacle": 20,
}

def run(ceph_cluster, **kw):
    """
    Verify upmap-remapped workflow for prolonged remapped/backfill scenarios.

    Steps:
      1. Verify cluster is 100% active+clean
      2. Verify require_min_compat_client >= luminous
      3. Confirm no recovery / backfilling / remapping activity
      4. Set norebalance and verify the flag
      5. Remove (mark out) an existing OSD to trigger remapping
      6. Confirm remapped+backfilling / remapped+backfill_wait
      7. Verify balancer pauses when target_max_misplaced_ratio is exceeded
      8. Classify remaining recovery as long backfill
      9. Dry-run upmap-remapped.py (default and --ignore-backfilling)
     10. Apply upmap-remapped.py | sh only for long-backfill classification
     11. Verify cluster returns to active+clean
     12. Unset norebalance and confirm balancer resumes
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]

    pool_name = config.get("pool_name", "upmap_remapped_pool")
    delete_pool = config.get("delete_pool", True)
    target_max_misplaced_ratio = config.get("target_max_misplaced_ratio", 0.01)
    long_backfill_seconds = config.get("long_backfill_seconds", 120)
    remapped_wait_timeout = config.get("remapped_wait_timeout", 300)
    clean_timeout = config.get("clean_timeout", 3600)
    bench_duration = config.get("rados_write_duration", 120)
    script_url = config.get("upmap_script_url", UPMAP_SCRIPT_URL)

    osd_id = None
    norebalance_set = False
    recovery_threads_set = False
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")

    try:
        # ------------------------------------------------------------------
        # Step 1-3: Pre-checks
        # ------------------------------------------------------------------
        log.info("Step 1: Verify cluster is 100% active+clean")
        if not wait_for_clean_pg_sets(
            rados_obj, timeout=clean_timeout, recovery_thread=True
        ):
            raise Exception("Cluster is not in active+clean state before test start")
        log.info("Cluster is active+clean")

        log.info(
            "Step 2: Verify require_min_compat_client >= luminous "
            "(pg-upmap-items compatibility)"
        )
        osd_dump = rados_obj.run_ceph_command(cmd="ceph osd dump")
        min_compat = osd_dump.get("require_min_compat_client", "")
        log.info(f"require_min_compat_client = {min_compat}")
        if not _is_compat_client_at_least(min_compat, "luminous"):
            raise Exception(
                f"require_min_compat_client '{min_compat}' is below luminous; "
                "pg-upmap / upmap-remapped requires luminous or newer"
            )
        log.info("Cluster is compatible with luminous+ upmap features")

        log.info("Step 3: Confirm no recovery / backfilling / remapping activity")
        if _has_recovery_activity(rados_obj):
            raise Exception(
                "Cluster already has recovery, backfilling, or remapping activity"
            )
        log.info("No recovery / backfill / remap activity present")

        # Create pool and write data so OSD removal causes meaningful remapping
        log.info(f"Creating pool {pool_name} and writing data")
        method_should_succeed(
            rados_obj.create_pool,
            **{
                "pool_name": pool_name,
                "pg_num": config.get("pg_num", 64),
                "app_name": "rados",
            },
        )
        method_should_succeed(
            rados_obj.bench_write,
            **{
                "pool_name": pool_name,
                "byte_size": config.get("byte_size", "4M"),
                "rados_write_duration": bench_duration,
                "max_objs": config.get("max_objs", 500),
            },
        )
        if not wait_for_clean_pg_sets(
            rados_obj, timeout=clean_timeout, test_pool=pool_name, recovery_thread=True
        ):
            raise Exception("Pool PGs did not reach active+clean after data write")

        # Throttle recovery so remapping is prolonged after OSD removal
        log.info("Throttling recovery/backfill threads to prolong remapping")
        rados_obj.change_recovery_threads(
            config={"osd_max_backfills": 1, "osd_recovery_max_active": 1},
            action="set",
        )
        recovery_threads_set = True

        # Configure balancer with a low misplaced ratio so it pauses under load
        log.info(
            f"Enabling balancer in upmap mode with "
            f"target_max_misplaced_ratio={target_max_misplaced_ratio}"
        )
        if not rados_obj.enable_balancer(
            balancer_mode="upmap",
            target_max_misplaced_ratio=target_max_misplaced_ratio,
        ):
            raise Exception("Failed to enable/configure the balancer")

        # ------------------------------------------------------------------
        # Step 4: Set norebalance
        # ------------------------------------------------------------------
        log.info("Step 4: Set norebalance flag")
        rados_obj.client.exec_command(cmd="ceph osd set norebalance", sudo=True)
        time.sleep(5)
        if not _is_osd_flag_set(rados_obj, "norebalance"):
            raise Exception("norebalance flag was not applied successfully")
        norebalance_set = True
        log.info("norebalance flag successfully applied")

        # ------------------------------------------------------------------
        # Step 5: Remove / mark out an existing OSD
        # ------------------------------------------------------------------
        acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        if not acting_set:
            raise Exception(f"Unable to fetch acting set for pool {pool_name}")
        osd_id = int(acting_set[0])
        log.info(
            f"Step 5: Removing OSD.{osd_id} from cluster topology "
            f"(mark out) to trigger constrained remapping"
        )
        rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
        method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id)
        log.info(f"OSD.{osd_id} marked out")

        # ------------------------------------------------------------------
        # Step 6-8: Remapped states, balancer pause, long-backfill classify
        # ------------------------------------------------------------------
        log.info(
            "Step 6: Verify PGs enter remapped+backfilling and/or "
            "remapped+backfill_wait"
        )
        if not _wait_for_remapped_backfill(
            rados_obj, timeout=remapped_wait_timeout, pool_name=pool_name
        ):
            raise Exception(
                "PGs did not enter remapped+backfilling / remapped+backfill_wait "
                "after OSD removal"
            )
        remapped_pgs = _get_remapped_pg_states(rados_obj)
        log.info(f"Remapped PG states observed: {remapped_pgs}")

        log.info(
            "Step 7: Verify balancer pauses when target_max_misplaced_ratio "
            "is exceeded / remapping is constrained"
        )
        if not _verify_balancer_constrained(rados_obj, target_max_misplaced_ratio):
            raise Exception(
                "Balancer did not appear paused/constrained under "
                "elevated misplaced ratio"
            )

        log.info(
            "Step 8: Classify remaining recovery as long backfill "
            f"(threshold > {long_backfill_seconds}s)"
        )
        is_long_backfill = _classify_long_backfill(
            rados_obj, min_seconds=long_backfill_seconds
        )
        if is_long_backfill:
            log.info(
                "Cluster classified as LONG backfill — upmap-remapped apply "
                "will be executed"
            )
        else:
            log.warning(
                "Cluster classified as short/minutes-scale backfill — "
                "upmap-remapped apply will be SKIPPED (dry-run only)"
            )

        # ------------------------------------------------------------------
        # Step 9: Dry-run upmap-remapped.py
        # ------------------------------------------------------------------
        log.info("Step 9: Download and dry-run upmap-remapped.py")
        _download_upmap_script(client, script_url)

        upmap_before = _get_pg_upmap_items(rados_obj)
        dry_run_out = _run_upmap_script(client, ignore_backfilling=False)
        log.info(f"Dry-run output:\n{dry_run_out}")
        if not _validate_upmap_commands(dry_run_out):
            raise Exception(
                "Dry-run did not produce valid 'ceph osd pg-upmap-items' commands"
            )
        log.info("Dry-run generated valid upmap commands")

        ignore_out = _run_upmap_script(client, ignore_backfilling=True)
        log.info(f"--ignore-backfilling dry-run output:\n{ignore_out}")
        if not _validate_upmap_commands(ignore_out, allow_empty=True):
            raise Exception(
                "--ignore-backfilling dry-run produced invalid upmap commands"
            )
        log.info("--ignore-backfilling dry-run validated")

        upmap_after_dry = _get_pg_upmap_items(rados_obj)
        if upmap_before != upmap_after_dry:
            raise Exception(
                "Dry-run unexpectedly modified pg_upmap_items on the cluster"
            )
        if not _is_osd_flag_set(rados_obj, "norebalance"):
            raise Exception("norebalance flag was cleared during dry-run")
        log.info("Dry-run made no unintended cluster changes")

        # ------------------------------------------------------------------
        # Step 10: Apply upmap-remapped only for long backfill
        # ------------------------------------------------------------------
        if is_long_backfill:
            log.info("Step 10: Applying upmap-remapped.py | sh")
            apply_out, apply_err = client.exec_command(
                sudo=True,
                cmd=f"./{UPMAP_SCRIPT_NAME} | sh",
                timeout=600,
            )
            log.info(f"Apply stdout:\n{apply_out}")
            if apply_err:
                log.debug(f"Apply stderr:\n{apply_err}")

            # Second pass recommended by CERN docs for minimal misplaced objects
            apply_out2, _ = client.exec_command(
                sudo=True,
                cmd=f"./{UPMAP_SCRIPT_NAME} | sh",
                timeout=600,
            )
            log.info(f"Second apply pass stdout:\n{apply_out2}")

            time.sleep(20)
            remapped_after = _get_remapped_pg_states(rados_obj)
            log.info(f"Remapped PGs after apply: {remapped_after}")
            log.info("upmap-remapped commands applied")
        else:
            log.info(
                "Step 10: SKIPPED apply — recovery is not classified as long backfill. "
                "Unsetting norebalance so natural recovery can complete."
            )
            # With norebalance set, remapped PGs will not clear without upmap apply.
            # For short backfill, allow natural recovery path instead.
            rados_obj.client.exec_command(cmd="ceph osd unset norebalance", sudo=True)
            time.sleep(5)
            if _is_osd_flag_set(rados_obj, "norebalance"):
                raise Exception(
                    "Failed to unset norebalance for short-backfill recovery path"
                )
            norebalance_set = False

        # ------------------------------------------------------------------
        # Step 11: Verify cluster recovery to active+clean
        # ------------------------------------------------------------------
        log.info("Step 11: Monitor cluster until active+clean")
        if recovery_threads_set:
            rados_obj.change_recovery_threads(config={}, action="rm")
            recovery_threads_set = False

        if not wait_for_clean_pg_sets(
            rados_obj, timeout=clean_timeout, recovery_thread=True
        ):
            raise Exception(
                "Cluster did not reach 100% active+clean after upmap-remapped workflow"
            )

        if _has_unexpected_pg_states(rados_obj):
            raise Exception(
                "Unexpected PGs remain in remapped / backfilling / peering states"
            )
        log.info("Cluster reached 100% active+clean with no unexpected PG states")

        # ------------------------------------------------------------------
        # Step 12: Unset norebalance (if still set) and verify balancer resumes
        # ------------------------------------------------------------------
        log.info("Step 12: Unset norebalance and verify balancer resumes")
        if norebalance_set or _is_osd_flag_set(rados_obj, "norebalance"):
            rados_obj.client.exec_command(cmd="ceph osd unset norebalance", sudo=True)
            time.sleep(5)
            if _is_osd_flag_set(rados_obj, "norebalance"):
                raise Exception("Failed to unset norebalance flag")
            norebalance_set = False
            log.info("norebalance flag unset successfully")
        else:
            log.info("norebalance already unset (short-backfill path)")

        rados_obj.client.exec_command(cmd="ceph balancer on", sudo=True)
        time.sleep(10)
        balancer_status = rados_obj.run_ceph_command(cmd="ceph balancer status")
        log.info(f"Balancer status after unset norebalance: {balancer_status}")
        if not balancer_status.get("active"):
            raise Exception("Balancer did not resume after unsetting norebalance")
        log.info("Balancer resumed normal gradual rebalancing")

        # Brief monitor window for unexpected errors
        monitor_secs = config.get("post_unset_monitor_seconds", 60)
        log.info(f"Monitoring cluster for {monitor_secs}s after unset norebalance")
        time.sleep(monitor_secs)
        rados_obj.log_cluster_health()
        peering = _pgs_in_states(rados_obj, ("peering", "incomplete", "stale"))
        if peering:
            raise Exception(f"Unexpected PG states after balancer resume: {peering}")
        log.info("Post-unset monitoring completed without unexpected errors")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        try:
            if recovery_threads_set:
                rados_obj.change_recovery_threads(config={}, action="rm")
        except Exception as err:
            log.error(f"Failed to reset recovery threads: {err}")

        try:
            if norebalance_set:
                rados_obj.client.exec_command(
                    cmd="ceph osd unset norebalance", sudo=True
                )
                log.info("Unset norebalance in finally block")
        except Exception as err:
            log.error(f"Failed to unset norebalance in finally: {err}")

        try:
            if osd_id is not None:
                utils.set_osd_in(ceph_cluster, osd_id=osd_id)
                log.info(f"Marked OSD.{osd_id} back in")
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
        except Exception as err:
            log.error(f"Failed to restore OSD/service managed state: {err}")

        try:
            if delete_pool and pool_name:
                rados_obj.delete_pool(pool=pool_name)
        except Exception as err:
            log.error(f"Failed to delete pool {pool_name}: {err}")

        try:
            client.exec_command(sudo=True, cmd=f"rm -f ./{UPMAP_SCRIPT_NAME}")
        except Exception:
            pass

        rados_obj.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, "
            f"End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Verification complete for upmap-remapped feature")
    return 0


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _is_compat_client_at_least(current: str, minimum: str) -> bool:
    """Return True if current require_min_compat_client >= minimum release."""
    cur = CEPH_RELEASE_ORDER.get(str(current).lower())
    mn = CEPH_RELEASE_ORDER.get(str(minimum).lower())
    if cur is None or mn is None:
        log.warning(
            f"Unknown release for comparison current={current}, minimum={minimum}; "
            "accepting as compatible"
        )
        return True
    return cur >= mn


def _is_osd_flag_set(rados_obj: RadosOrchestrator, flag: str) -> bool:
    """Check whether a given OSDMap flag is set."""
    osd_dump = rados_obj.run_ceph_command(cmd="ceph osd dump")
    flags_set = osd_dump.get("flags_set") or osd_dump.get("flags") or []
    if isinstance(flags_set, str):
        flags_list = [f.strip() for f in flags_set.replace(",", " ").split()]
    else:
        flags_list = list(flags_set)
    log.debug(f"OSD flags currently set: {flags_list}")
    return flag in flags_list


def _has_recovery_activity(rados_obj: RadosOrchestrator) -> bool:
    """Return True if any PG is remapped / backfilling / recovering."""
    states = _pgs_in_states(
        rados_obj,
        (
            "remapped",
            "backfilling",
            "backfill_wait",
            "recovering",
            "recovery_wait",
            "degraded",
        ),
    )
    if states:
        log.error(f"Recovery-related PG states found: {states}")
        return True
    return False


def _get_pg_stats(rados_obj: RadosOrchestrator) -> list:
    """Return pg_stats list from ceph pg dump."""
    pg_dump = rados_obj.run_ceph_command(cmd="ceph pg dump pgs")
    return pg_dump.get("pg_stats", []) if pg_dump else []


def _pgs_in_states(rados_obj: RadosOrchestrator, needles: tuple) -> dict:
    """Return {pgid: state} for PGs whose state contains any needle."""
    matched = {}
    for pg in _get_pg_stats(rados_obj):
        state = pg.get("state", "")
        if any(n in state for n in needles):
            matched[pg.get("pgid")] = state
    return matched


def _get_remapped_pg_states(rados_obj: RadosOrchestrator) -> dict:
    """Return remapped PG id -> state mapping."""
    return _pgs_in_states(rados_obj, ("remapped",))


def _wait_for_remapped_backfill(
    rados_obj: RadosOrchestrator, timeout: int = 300, pool_name: str = None
) -> bool:
    """
    Wait until at least one PG is in remapped+backfilling or remapped+backfill_wait.
    """
    end = time.time() + timeout
    while time.time() < end:
        remapped = _get_remapped_pg_states(rados_obj)
        if pool_name:
            log.debug(f"Checking remapped states (pool focus: {pool_name})")
        for pgid, state in remapped.items():
            normalized = state.replace(" ", "")
            if any(
                s in normalized
                for s in ("remapped+backfilling", "remapped+backfill_wait")
            ):
                log.info(f"Found constrained remapped PG {pgid} in state: {state}")
                return True
            # Also accept any remapped PG that includes backfill*
            if "remapped" in state and "backfill" in state:
                log.info(f"Found remapped/backfill PG {pgid} in state: {state}")
                return True
        log.debug(
            f"Waiting for remapped+backfill states; current remapped PGs: {remapped}"
        )
        time.sleep(15)
    return False


def _verify_balancer_constrained(
    rados_obj: RadosOrchestrator, target_ratio: float
) -> bool:
    """
    Verify balancer is constrained / paused under high misplaced ratio.

    Checks:
      - norebalance is set (already required by workflow), and/or
      - misplaced objects ratio approaches/exceeds target, and/or
      - balancer status shows inactive plan progress / active with no new optimizes
    """
    status = rados_obj.run_ceph_command(cmd="ceph status", client_exec=True)
    pgmap = status.get("pgmap", {}) if status else {}
    num_objects = pgmap.get("num_objects", 0) or 0
    misplaced = pgmap.get("misplaced_objects", 0) or 0
    misplaced_ratio = (misplaced / num_objects) if num_objects else 0.0
    log.info(
        f"Misplaced objects={misplaced}, total objects={num_objects}, "
        f"misplaced_ratio={misplaced_ratio:.4f}, target={target_ratio}"
    )

    balancer = rados_obj.run_ceph_command(cmd="ceph balancer status")
    log.info(f"Balancer status during remapping: {balancer}")

    # With norebalance set, automatic rebalancing must not proceed freely
    if not _is_osd_flag_set(rados_obj, "norebalance"):
        log.error("norebalance not set while verifying balancer constraint")
        return False

    # If misplaced ratio exceeds target, balancer is expected to pause optimize
    if misplaced_ratio >= target_ratio:
        log.info(
            "Misplaced ratio exceeds target_max_misplaced_ratio; "
            "balancer optimize is constrained"
        )
        return True

    # Even below ratio, remapped PGs + norebalance means rebalancing is constrained
    remapped = _get_remapped_pg_states(rados_obj)
    if remapped:
        log.info(
            "Remapped PGs present with norebalance set — "
            "rebalancing is constrained as expected"
        )
        return True

    log.error("Could not confirm balancer/rebalance constraint")
    return False


def _classify_long_backfill(rados_obj: RadosOrchestrator, min_seconds: int = 120) -> bool:
    """
    Classify whether remaining recovery is prolonged (hours / > min_seconds).

    Uses ceph status recovery rates and misplaced object counts. If estimate
    cannot be computed, observes remapped state persistence for min_seconds.
    """
    status = rados_obj.run_ceph_command(cmd="ceph status", client_exec=True) or {}
    pgmap = status.get("pgmap", {})
    misplaced = pgmap.get("misplaced_total", pgmap.get("misplaced_objects", 0)) or 0
    recovering_bytes_per_sec = pgmap.get("recovering_bytes_per_sec", 0) or 0
    misplaced_bytes = pgmap.get("misplaced_bytes", 0) or 0

    # progress events sometimes include estimated remaining time
    progress = status.get("progress_events", {}) or {}
    for event_id, event in progress.items():
        message = str(event.get("message", ""))
        # Look for hour-scale hints in progress messages
        if re.search(r"\b(\d+)\s*h(our)?s?\b", message, re.IGNORECASE):
            log.info(f"Progress event indicates hour-scale recovery: {message}")
            return True
        remaining = event.get("remaining") or event.get("time_remaining")
        if isinstance(remaining, (int, float)) and remaining > min_seconds:
            log.info(f"Progress remaining={remaining}s exceeds {min_seconds}s")
            return True

    if misplaced_bytes and recovering_bytes_per_sec:
        eta = misplaced_bytes / float(recovering_bytes_per_sec)
        log.info(
            f"Estimated recovery ETA from bytes: {eta:.1f}s "
            f"(misplaced_bytes={misplaced_bytes}, "
            f"rate={recovering_bytes_per_sec} B/s)"
        )
        if eta > min_seconds:
            return True

    # Fallback: if significant misplaced objects remain with throttled recovery,
    # observe that remapped state persists beyond a short window.
    if misplaced > 0:
        log.info(
            f"Observing remapped persistence for {min_seconds}s to classify "
            "long backfill (throttled recovery)"
        )
        time.sleep(min_seconds)
        still_remapped = _get_remapped_pg_states(rados_obj)
        if still_remapped:
            log.info(
                f"Remapped PGs still present after {min_seconds}s "
                f"({len(still_remapped)} PGs) — classifying as LONG backfill"
            )
            return True
        log.info("Remapped PGs cleared quickly — classifying as short backfill")
        return False

    log.info("No misplaced objects found for long-backfill classification")
    return False


def _download_upmap_script(client, script_url: str) -> None:
    """Download upmap-remapped.py onto the client node."""
    log.info(f"Downloading {UPMAP_SCRIPT_NAME} from {script_url}")
    client.exec_command(
        sudo=True,
        cmd=f"curl -fsSL {script_url} -o {UPMAP_SCRIPT_NAME}",
        timeout=120,
    )
    client.exec_command(sudo=True, cmd=f"chmod 755 {UPMAP_SCRIPT_NAME}")
    # Ensure shebang execution works
    out, _ = client.exec_command(sudo=True, cmd=f"head -1 {UPMAP_SCRIPT_NAME}")
    log.debug(f"Script header: {out}")


def _run_upmap_script(client, ignore_backfilling: bool = False) -> str:
    """
    Run upmap-remapped.py in dry-run mode (print commands only).
    Returns stdout.
    """
    cmd = f"./{UPMAP_SCRIPT_NAME}"
    if ignore_backfilling:
        cmd += " --ignore-backfilling"
    out, err = client.exec_command(sudo=True, cmd=cmd, timeout=300)
    if err:
        log.debug(f"upmap-remapped stderr: {err}")
    return out or ""


def _validate_upmap_commands(output: str, allow_empty: bool = False) -> bool:
    """
    Validate dry-run output contains only expected pg-upmap-items / rm commands.
    allow_empty: True when no remapped PGs may remain (e.g. ignore-backfilling).
    """
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    # Filter noise / eprint lines that are not ceph commands
    cmd_lines = [ln for ln in lines if ln.startswith("ceph osd")]
    if not cmd_lines:
        if allow_empty:
            log.info("No upmap commands generated (acceptable for this dry-run mode)")
            return True
        # Script may exit early with 'There are no remapped PGs'
        if any("no remapped" in ln.lower() for ln in lines):
            log.warning("Script reported no remapped PGs during dry-run")
            return False
        log.error(f"No ceph osd commands found in dry-run output: {output}")
        return False

    pattern = re.compile(
        r"^ceph osd (pg-upmap-items|rm-pg-upmap-items)\s+\S+"
    )
    for ln in cmd_lines:
        # Commands are printed with trailing '&' for background shell apply
        check = ln.rstrip("&").strip()
        if not pattern.search(check):
            log.error(f"Unexpected upmap command line: {ln}")
            return False
    log.info(f"Validated {len(cmd_lines)} upmap command(s)")
    return True


def _get_pg_upmap_items(rados_obj: RadosOrchestrator) -> list:
    """Return current pg_upmap_items from osd dump for dry-run change detection."""
    osd_dump = rados_obj.run_ceph_command(cmd="ceph osd dump") or {}
    items = osd_dump.get("pg_upmap_items", [])
    # Normalize for comparison
    return json.loads(json.dumps(items, sort_keys=True))


def _has_unexpected_pg_states(
    rados_obj: RadosOrchestrator, allow_remapped: bool = False
) -> bool:
    """
    Return True if unexpected remapped/backfilling/peering PGs remain.
    When allow_remapped=True, remapped alone is tolerated (gradual balancer).
    """
    needles = ["backfilling", "backfill_wait", "peering", "recovering", "recovery_wait"]
    if not allow_remapped:
        needles.append("remapped")
    bad = _pgs_in_states(rados_obj, tuple(needles))
    if bad:
        log.error(f"Unexpected PG states remaining: {bad}")
        return True
    return False
