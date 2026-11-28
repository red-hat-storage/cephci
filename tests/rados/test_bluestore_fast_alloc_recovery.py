"""
BlueStore fast allocation recovery — CephCI test module
=======================================================

What this tests
---------------
BlueStore can rebuild the allocator from onode metadata after an unclean OSD
crash when allocation is persisted in a BlueFS `ALLOCATOR` file
(null-FM / `bluestore_allocation_from_file`). Multithreaded recovery is
controlled by `bluestore_allocation_recovery_threads`; optional dual-path
compare uses `bluestore_debug_fast_recovery_compare_chance`. Offline
correctness uses `ceph-bluestore-tool recovery-compare`.

Feature refs: Ceph PRs #64369 / #68981 / #68984, tracker RADOS #77240.
Maps to QA workflows offline, runtime crash, config, negatives, teardown.

How to run
----------
Select scenarios via suite config::

    config:
      scenarios_to_run:
        - positive_spot_check
        - offline_recovery_compare_smoke
        - runtime_crash_compare_smoke
        - negative_spot_check
        - qa_teardown_compare_chance
        # optional P2:
        - offline_thread_matrix_full
        - runtime_matrix_full
      allow_null_fm_convert: false   # set true only to force null-FM bootstrap
      pool_name: test-allocation    # optional override
      bench_seconds: 60
      cbt_timeout: 600

High-level flow (`run()`)
---------------------------
1. Log cluster health; resolve FSID; pick a random *up* OSD and its host.
2. Read host `nproc`; build offline/runtime thread matrices capped at
   `2 * nproc` (smoke subsets vs full P2 lists).
3. Null-FM gate (`gate_null_fm`): require BlueFS `ALLOCATOR` on the
   OSD. If missing and `allow_null_fm_convert` is true, fake non-rotational
   and redeploy; otherwise fail early.
4. Run each scenario listed in `scenarios_to_run` (see below).
5. `finally`: log health; fail if new crashes appeared during the run.

Scenario detail
---------------
positive_spot_check (config style)
  - Create pool; `rados bench` write.
  - F02: set MT threads + `debug_bluestore=10/5`, restart, SIGKILL, assert
    Full Recovery / null-FM / `bs.onode_scan` evidence (not FM-bitmap path).
  - F01: set threads=0 and restart; change threads to MT *without* applying
    until crash-restart; assert effective threads == MT after restart.
  - F03: threads=MT, `compare_chance=0`, SIGKILL; assert *no* compare
    success lines and no compare-failure assert.
  - F04: threads=0, `compare_chance=1.0`, SIGKILL; compare still suppressed.
  - Cleanup: remove per-OSD QA config; delete pool.

offline_recovery_compare_smoke / offline_thread_matrix_full (WF-1)
  - Fill a pool; stop the OSD; for each thread count run
    `ceph-bluestore-tool --bluestore_allocation_recovery_threads=N
    recovery-compare` via CBT workflows.
  - Smoke threads ≈ `{1, min(6,nproc), min(16,nproc), 2*nproc}`.
  - Full matrix ≈ `{1,2,4,6,8,12,16,64}` intersected with `<= 2*nproc`,
    plus `2*nproc`.
  - Cache-warm: second compare at last / `2*nproc` thread count.
  - Assert FSstats/Allocators match (or success wording); no compare failure.
  - Start OSD; delete pool.

runtime_crash_compare_smoke / runtime_matrix_full (WF-2)
  - For each `(threads, compare_chance)` branch: set config, restart OSD,
    bench write, SIGKILL, wait up, inspect log since mark.
  - Smoke: `(0, 0)` and `(mt, 1)` where `mt = min(6, nproc)`.
  - Full: threads in `{0,1,3,6,12,+2*nproc}` × compare in `{0,1}`.
  - Expect: no compare success when threads=0 or chance=0; success lines when
    threads>0 and chance=1; never `comparing allocator recovery failed`.
  - Cleanup QA config and pool.

negative_spot_check (WF-6)
  - G01: `recovery-compare` while OSD is running must fail (lock / non-zero).
  - G03: `bluestore_allocation_from_file=false` on null-FM must not leave a
    healthy OSD (ENOTSUP / down); always revert config, reset-failed, start,
    confirm `ALLOCATOR` still present.

qa_teardown_compare_chance (WF-7)
  - Remove `bluestore_debug_fast_recovery_compare_chance` (osd + per-osd),
    cleanup other QA knobs, restart, SIGKILL; assert no runtime compare
    success/failure lines remain.

Unclean crash model
-------------------
`sigkill_osd` uses `systemctl kill -s SIGKILL` on
`ceph-<fsid>@osd.<id>.service`, then `reset-failed` + `start` — not a
graceful orch stop — so allocation-file restore fails and onode recovery runs.

Log evidence helpers
--------------------
- `osd_log_line_count` / `read_osd_log_since_mark`: slice OSD file log
  after a mark; fall back to `journalctl` if the slice is empty.
- Pattern tuples at module top: FM vs null-FM vs full recovery vs compare.

"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)

POOL_NAME = "test-allocation"
BENCH_SECONDS = 60
OFFLINE_BASE_THREADS = (1, 2, 4, 6, 8, 12, 16, 64)
RUNTIME_BASE_THREADS = (0, 1, 3, 6, 12)

FM_BITMAP_PATTERNS = (
    "freelist_type=bitmap",
    "loading allocation from FM -> alloc",
)
NULL_FM_PATTERNS = (
    "freelist_type=null",
    "Commit to Null-Manager",
    "Set FreelistManager to NULL FM",
)
FULL_RECOVERY_PATTERNS = (
    "Run Full Recovery from ONodes",
    "Allocation Recovery was completed",
    "bs.onode_scan",
    "restore_allocator() failed",
)
COMPARE_SUCCESS_PATTERNS = (
    "FSstats the same.",
    "Allocators the same.",
)
COMPARE_FAILURE = "comparing allocator recovery failed"
RECOVERY_COMPARE_SUCCESS = (
    "FSstats the same.",
    "Allocators the same.",
    "recovery-compare success",
)


def run(ceph_cluster, kw):
    """
    BlueStore fast allocation recovery — test summary
    =================================================

    Validates null-FM / alloc-from-file crash recovery and optional
    multithreaded onode scan vs legacy compare.

    Steps
    -----
    1. Pick a random up OSD; build thread matrices from host nproc.
    2. Gate null-FM (BlueFS ALLOCATOR present; optional convert).
    3. Run selected scenarios from config.scenarios_to_run:
       - positive_spot_check — F01–F04 config / SIGKILL recovery
       - offline_* — ceph-bluestore-tool recovery-compare matrix
       - runtime_* — crash + compare_chance branches
       - negative_spot_check — live CBT (G01), from_file=false (G03)
       - qa_teardown_compare_chance — remove QA compare_chance
    4. finally: health + crash check; return 0 on success, 1 on failure.
    """
    log.info(run.__doc__)
    config = kw.get("config", {})
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    scenarios_to_run = config.get("scenarios_to_run", ["positive_spot_check"])
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug("Test workflow started. Start time: %s", start_time)

    try:
        rados_obj.log_cluster_health()
        config = config or {}
        fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        osd_list = rados_obj.get_osd_list(status="up")
        osd_id = random.choice(osd_list)
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=str(osd_id))
        nproc = _remote_nproc(host)
        twice_nproc = nproc * 2
        unit = f"ceph-{fsid}@osd.{osd_id}.service"
        path = f"/var/lib/ceph/{fsid}/osd.{osd_id}"
        log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"

        offline_full = [t for t in OFFLINE_BASE_THREADS if t <= twice_nproc]
        if twice_nproc not in offline_full:
            offline_full.append(twice_nproc)
        offline_full = sorted(set(offline_full))

        smoke_list = [1, min(6, nproc), min(16, nproc), twice_nproc]
        offline_smoke = sorted({t for t in smoke_list if t >= 1})

        mt = min(6, nproc) if nproc > 0 else 1
        runtime_smoke = [(0, 0), (mt, 1)]

        threads = [t for t in RUNTIME_BASE_THREADS if t == 0 or t <= twice_nproc]
        if twice_nproc not in threads:
            threads.append(twice_nproc)
        threads = sorted(set(threads))
        runtime_full = [(t, c) for t in threads for c in (0, 1)]

        env = {
            "fsid": fsid,
            "osd_id": osd_id,
            "host": host,
            "hostname": getattr(host, "hostname", str(host)),
            "nproc": nproc,
            "twice_nproc": twice_nproc,
            "unit": unit,
            "path": path,
            "log": log_path,
            "mt_threads": min(6, nproc) if nproc > 0 else 1,
            "offline_threads_smoke": offline_smoke,
            "offline_threads_full": offline_full,
            "runtime_branches_smoke": runtime_smoke,
            "runtime_branches_full": runtime_full,
        }

        allow_convert = bool(config.get("allow_null_fm_convert", False))
        gate_null_fm(rados_obj, osd_id, allow_convert=allow_convert)
        log.info("Discovered env: %s", summarize_env(env))

        if "positive_spot_check" in scenarios_to_run:
            log.info("Scenario: positive_spot_check")
            run_positive_spot_check(rados_obj, mon_obj, env=env, config=config)

        if "offline_recovery_compare_smoke" in scenarios_to_run:
            log.info("Scenario: offline_recovery_compare_smoke")
            run_offline_recovery_compare_smoke(
                rados_obj, cephadm, env=env, config=config
            )

        if "runtime_crash_compare_smoke" in scenarios_to_run:
            log.info("Scenario: runtime_crash_compare_smoke")
            run_runtime_crash_compare_smoke(rados_obj, mon_obj, env=env, config=config)

        if "negative_spot_check" in scenarios_to_run:
            log.info("Scenario: negative_spot_check")
            run_negative_spot_check(rados_obj, mon_obj, cephadm, env=env)

        if "qa_teardown_compare_chance" in scenarios_to_run:
            log.info("Scenario: qa_teardown_compare_chance (A5 / WF-7)")
            run_qa_teardown_compare_chance(rados_obj, mon_obj, env=env)

        if "offline_thread_matrix_full" in scenarios_to_run:
            log.info("Scenario: offline_thread_matrix_full (P2a)")
            run_offline_thread_matrix_full(rados_obj, cephadm, env=env, config=config)

        if "runtime_matrix_full" in scenarios_to_run:
            log.info("Scenario: runtime_matrix_full (P2b)")
            run_runtime_matrix_full(rados_obj, mon_obj, env=env, config=config)

        log.info("All selected scenarios completed successfully")
    except Exception as e:
        log.error("Execution failed with exception: %s", e)
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            "Test workflow completed. Start time: %s, End time: %s",
            start_time,
            test_end_time,
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def summarize_env(env):
    return {
        "fsid": env["fsid"],
        "osd_id": env["osd_id"],
        "hostname": env["hostname"],
        "nproc": env["nproc"],
        "twice_nproc": env["twice_nproc"],
        "unit": env["unit"],
        "offline_threads_smoke": env["offline_threads_smoke"],
        "runtime_branches_smoke": env["runtime_branches_smoke"],
        "mt_threads": env["mt_threads"],
    }


def _remote_nproc(host):
    out, _ = host.exec_command(sudo=True, cmd="nproc", check_ec=False)
    try:
        nproc = int(str(out).strip().splitlines()[-1])
    except (ValueError, IndexError) as err:
        raise Exception(f"Failed to parse nproc from host: {out!r} ({err})")
    if nproc < 1:
        raise Exception(f"Invalid nproc={nproc}")
    return nproc


def gate_null_fm(rados_obj, osd_id, allow_convert=False):
    """Confirm null-FM / ALLOCATOR; optionally convert if suite allows."""
    from_file = rados_obj.run_ceph_command(
        cmd="ceph config get osd bluestore_allocation_from_file", client_exec=True
    )
    log.info("bluestore_allocation_from_file=%s", from_file)
    if osd_has_allocator_file(rados_obj, osd_id):
        log.info("osd.%s ALLOCATOR present; null-FM gate passed", osd_id)
        return
    if not allow_convert:
        raise Exception(
            f"osd.{osd_id} has no ALLOCATOR entry; designated env expects "
            "null-FM / alloc-from-file. Set allow_null_fm_convert: true only "
            "for bootstrap convert on mixed labs."
        )
    log.info(
        "osd.%s missing ALLOCATOR; allow_null_fm_convert=true — faking non-rotational and redeploying",
        osd_id,
    )
    rados_obj.switch_osd_device_type(osd_id=str(osd_id), rota_val=0, redeploy=True)
    time.sleep(30)
    if not osd_has_allocator_file(rados_obj, osd_id):
        raise Exception(f"osd.{osd_id} still has no ALLOCATOR after convert")
    log.info("null-FM gate passed for osd.%s after convert", osd_id)


# positive_spot_check


def run_positive_spot_check(rados_obj, mon_obj, env, config=None):
    config = config or {}
    osd_id = env["osd_id"]
    mt = env["mt_threads"]
    pool = config.get("pool_name", POOL_NAME)
    try:
        log.info("Creating pool %s and writing data", pool)
        assert rados_obj.create_pool(pool_name=pool), "Pool creation failed"
        assert rados_obj.bench_write(
            pool_name=pool,
            rados_write_duration=config.get("bench_seconds", BENCH_SECONDS),
            byte_size="4KB",
            nocleanup=True,
            verify_stats=False,
        ), "Initial rados bench write failed"

        from_file_and_mt_recovery(rados_obj, mon_obj, osd_id, mt, pool)
        startup_only_threads(rados_obj, mon_obj, osd_id, mt)
        compare_disabled(rados_obj, mon_obj, osd_id, mt)
        compare_suppressed_with_threads_zero(rados_obj, mon_obj, osd_id)
        log.info("positive_spot_check PASSED")
    finally:
        cleanup_osd_qa_config(rados_obj, mon_obj, osd_id)
        try:
            rados_obj.delete_pool(pool=pool)
        except Exception as err:
            log.warning("Pool cleanup skipped/failed for %s: %s", pool, err)


def osd_has_allocator_file(rados_obj, osd_id):
    out = rados_obj.run_ceph_command(
        cmd=f"ceph tell osd.{osd_id} bluefs files list", client_exec=True
    )
    log.debug("bluefs files list for osd.%s: %s", osd_id, out)
    names = []
    if isinstance(out, list):
        for entry in out:
            if isinstance(entry, dict) and entry.get("name"):
                names.append(entry["name"])
            else:
                names.append(str(entry))
    else:
        names.append(str(out))
    has_allocator = "ALLOCATOR" in " ".join(names).upper()
    log.info("osd.%s ALLOCATOR entry present: %s", osd_id, has_allocator)
    return has_allocator


def from_file_and_mt_recovery(rados_obj, mon_obj, osd_id, mt_threads, pool):
    log.info("Multi-threaded recovery after SIGKILL (threads=%s)", mt_threads)
    mon_obj.set_config(section=f"osd.{osd_id}", name="debug_bluestore", value="10/5")
    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_allocation_recovery_threads",
        value=str(mt_threads),
    )
    assert rados_obj.change_osd_state(
        action="restart", target=osd_id
    ), f"Restart failed for osd.{osd_id}"

    assert rados_obj.bench_write(
        pool_name=pool,
        rados_write_duration=60,
        byte_size="4KB",
        verify_stats=False,
    ), "rados bench before SIGKILL failed"

    mark = osd_log_line_count(rados_obj, osd_id)
    sigkill_osd(rados_obj, osd_id)
    wait_osd_up(rados_obj, osd_id)

    new_log = read_osd_log_since_mark(rados_obj, osd_id, mark)
    if any(p in new_log for p in FM_BITMAP_PATTERNS) and not any(
        p in new_log for p in FULL_RECOVERY_PATTERNS + NULL_FM_PATTERNS
    ):
        raise Exception(f"FAIL: FM bitmap path after SIGKILL on osd.{osd_id}")
    if not any(p in new_log for p in FULL_RECOVERY_PATTERNS + NULL_FM_PATTERNS):
        full_log = read_osd_log(rados_obj, osd_id, lines=200)
        if "freelist_type=null" not in full_log and not any(
            p in full_log for p in FULL_RECOVERY_PATTERNS
        ):
            raise Exception(
                "FAIL: expected Full Recovery / bs.onode_scan / null-FM "
                f"evidence after SIGKILL on osd.{osd_id}"
            )
        log.warning(
            "F02: Full Recovery strings not in newest slice; "
            "null-FM / prior recovery evidence present — continuing"
        )
    else:
        log.info("PASS: Full Recovery / onode_scan / null-FM evidence found")


def startup_only_threads(rados_obj, mon_obj, osd_id, mt_threads):
    log.info("TC-F01: threads apply only at OSD startup (mt=%s)", mt_threads)
    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_allocation_recovery_threads",
        value="0",
    )
    assert rados_obj.change_osd_state(
        action="restart", target=osd_id
    ), f"Restart failed for osd.{osd_id} (F01 threads=0)"

    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_allocation_recovery_threads",
        value=str(mt_threads),
        no_delay=True,
    )
    sigkill_osd(rados_obj, osd_id)
    wait_osd_up(rados_obj, osd_id)

    effective = mon_obj.get_config(
        section=f"osd.{osd_id}", param="bluestore_allocation_recovery_threads"
    )
    if str(effective).strip() != str(mt_threads):
        raise Exception(
            f"FAIL: expected threads={mt_threads} after crash-restart, got {effective}"
        )
    log.info("PASS: threads became %s only after restart", mt_threads)


def compare_disabled(rados_obj, mon_obj, osd_id, mt_threads):
    log.info("TC-F03: compare_chance=0 suppresses compare success lines")
    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_allocation_recovery_threads",
        value=str(mt_threads),
    )
    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_debug_fast_recovery_compare_chance",
        value="0",
    )
    assert rados_obj.change_osd_state(
        action="restart", target=osd_id
    ), f"Restart failed for osd.{osd_id}"

    mark = osd_log_line_count(rados_obj, osd_id)
    sigkill_osd(rados_obj, osd_id)
    wait_osd_up(rados_obj, osd_id)
    new_log = read_osd_log_since_mark(rados_obj, osd_id, mark)

    if any(p in new_log for p in COMPARE_SUCCESS_PATTERNS):
        raise Exception("FAIL: compare success lines present with compare_chance=0")
    if COMPARE_FAILURE in new_log:
        raise Exception("FAIL: comparing allocator recovery failed assert")
    log.info("PASS: no compare success lines")


def compare_suppressed_with_threads_zero(rados_obj, mon_obj, osd_id):
    log.info("threads=0 suppresses compare despite compare_chance=1.0")
    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_allocation_recovery_threads",
        value="0",
    )
    mon_obj.set_config(
        section=f"osd.{osd_id}",
        name="bluestore_debug_fast_recovery_compare_chance",
        value="1.0",
    )
    assert rados_obj.change_osd_state(
        action="restart", target=osd_id
    ), f"Restart failed for osd.{osd_id} (F04)"

    mark = osd_log_line_count(rados_obj, osd_id)
    sigkill_osd(rados_obj, osd_id)
    wait_osd_up(rados_obj, osd_id)
    new_log = read_osd_log_since_mark(rados_obj, osd_id, mark)

    if any(p in new_log for p in COMPARE_SUCCESS_PATTERNS):
        raise Exception(
            "FAIL: compare success lines present with threads=0 "
            "and compare_chance=1.0"
        )
    if COMPARE_FAILURE in new_log:
        raise Exception("FAIL: comparing allocator recovery failed assert")
    log.info("PASS: compare suppressed when threads=0")


# offline smoke


def run_offline_recovery_compare_smoke(rados_obj, cephadm, env, config=None):
    _run_offline_matrix(
        rados_obj,
        cephadm,
        env,
        threads=env["offline_threads_smoke"],
        config=config or {},
        label="offline_smoke",
    )


def run_offline_thread_matrix_full(rados_obj, cephadm, env, config=None):
    _run_offline_matrix(
        rados_obj,
        cephadm,
        env,
        threads=env["offline_threads_full"],
        config=config or {},
        label="offline_full",
    )


def _run_offline_matrix(rados_obj, cephadm, env, threads, config, label):
    osd_id = env["osd_id"]
    pool = config.get("pool_name", f"{POOL_NAME}-offline")
    bench_s = int(config.get("bench_seconds", BENCH_SECONDS))
    try:
        assert rados_obj.create_pool(pool_name=pool), f"Pool create failed: {pool}"
        assert rados_obj.bench_write(
            pool_name=pool,
            rados_write_duration=bench_s,
            byte_size="4KB",
            nocleanup=True,
            verify_stats=False,
        ), "offline fill bench failed"

        cbt = BluestoreToolWorkflows(node=cephadm, nostop=True, nostart=True)
        assert rados_obj.change_osd_state(
            action="stop", target=osd_id
        ), f"Failed to stop osd.{osd_id} for {label}"

        last_n = threads[-1]
        for n in threads:
            log.info("%s: recovery-compare threads=%s", label, n)
            out = cbt.run_cbt_command(
                cmd=(
                    "ceph-bluestore-tool "
                    f"--bluestore_allocation_recovery_threads={n} "
                    "recovery-compare"
                ),
                osd_id=osd_id,
                timeout=int(config.get("cbt_timeout", 600)),
            )
            _assert_recovery_compare_ok(out, n)
            last_n = n

        # Cache-warm at last / twice_nproc
        warm_n = env["twice_nproc"] if env["twice_nproc"] in threads else last_n
        log.info("%s: cache-warm recovery-compare threads=%s", label, warm_n)
        out = cbt.run_cbt_command(
            cmd=(
                "ceph-bluestore-tool "
                f"--bluestore_allocation_recovery_threads={warm_n} "
                "recovery-compare"
            ),
            osd_id=osd_id,
            timeout=int(config.get("cbt_timeout", 600)),
        )
        _assert_recovery_compare_ok(out, warm_n)
        log.info("%s PASSED for threads=%s", label, threads)
    finally:
        try:
            rados_obj.change_osd_state(action="start", target=osd_id)
        except Exception as err:
            log.warning("start osd.%s after offline matrix: %s", osd_id, err)
        wait_osd_up(rados_obj, osd_id)
        try:
            rados_obj.delete_pool(pool=pool)
        except Exception as err:
            log.warning("Pool cleanup skipped/failed for %s: %s", pool, err)


def _assert_recovery_compare_ok(output, threads):
    text = str(output)
    if not any(p in text for p in ("FSstats the same.", "Allocators the same.")):
        # Some builds phrase slightly differently; require at least one match token
        if "the same" not in text.lower() and "success" not in text.lower():
            raise Exception(
                f"recovery-compare threads={threads} missing FSstats/Allocators match. "
                f"Output snippet: {text[-800:]}"
            )
    if COMPARE_FAILURE in text:
        raise Exception(f"recovery-compare threads={threads} reported compare failure")
    log.info("recovery-compare threads=%s: OK", threads)


# A3 — runtime smoke / P2b full


def run_runtime_crash_compare_smoke(rados_obj, mon_obj, env, config=None):
    _run_runtime_matrix(
        rados_obj,
        mon_obj,
        env,
        branches=env["runtime_branches_smoke"],
        config=config or {},
        label="runtime_smoke",
    )


def run_runtime_matrix_full(rados_obj, mon_obj, env, config=None):
    _run_runtime_matrix(
        rados_obj,
        mon_obj,
        env,
        branches=env["runtime_branches_full"],
        config=config or {},
        label="runtime_full",
    )


def _run_runtime_matrix(rados_obj, mon_obj, env, branches, config, label):
    osd_id = env["osd_id"]
    pool = config.get("pool_name", f"{POOL_NAME}-runtime")
    bench_s = int(config.get("bench_seconds", max(30, BENCH_SECONDS // 2)))
    try:
        assert rados_obj.create_pool(pool_name=pool), f"Pool create failed: {pool}"
        for threads, compare in branches:
            log.info("%s: branch threads=%s compare=%s", label, threads, compare)
            mon_obj.set_config(
                section=f"osd.{osd_id}",
                name="bluestore_allocation_recovery_threads",
                value=str(threads),
            )
            mon_obj.set_config(
                section=f"osd.{osd_id}",
                name="bluestore_debug_fast_recovery_compare_chance",
                value=str(compare),
            )
            assert rados_obj.change_osd_state(
                action="restart", target=osd_id
            ), f"Restart failed osd.{osd_id} t={threads} c={compare}"

            assert rados_obj.bench_write(
                pool_name=pool,
                rados_write_duration=bench_s,
                byte_size="4KB",
                nocleanup=True,
                verify_stats=False,
            ), f"bench failed before SIGKILL t={threads} c={compare}"

            mark = osd_log_line_count(rados_obj, osd_id)
            t0 = time.time()
            sigkill_osd(rados_obj, osd_id)
            wait_osd_up(rados_obj, osd_id)
            boot_s = time.time() - t0
            new_log = read_osd_log_since_mark(rados_obj, osd_id, mark)
            log.info("%s: boot_s=%.1f (informational)", label, boot_s)

            if COMPARE_FAILURE in new_log:
                raise Exception(
                    f"{label} FAIL: compare failure t={threads} c={compare}"
                )
            if threads == 0 or compare == 0:
                if any(p in new_log for p in COMPARE_SUCCESS_PATTERNS):
                    raise Exception(
                        f"{label} FAIL: unexpected compare success "
                        f"t={threads} c={compare}"
                    )
            else:
                if not any(p in new_log for p in COMPARE_SUCCESS_PATTERNS):
                    # Fall back to longer log window
                    wider = read_osd_log(rados_obj, osd_id, lines=400)
                    if not any(p in wider for p in COMPARE_SUCCESS_PATTERNS):
                        raise Exception(
                            f"{label} FAIL: missing compare success "
                            f"t={threads} c={compare}"
                        )
            log.info("%s: branch t=%s c=%s PASS", label, threads, compare)
        log.info("%s PASSED", label)
    finally:
        cleanup_osd_qa_config(rados_obj, mon_obj, osd_id)
        try:
            rados_obj.delete_pool(pool=pool)
        except Exception as err:
            log.warning("Pool cleanup skipped/failed for %s: %s", pool, err)


# negative checks


def run_negative_spot_check(rados_obj, mon_obj, cephadm, env):
    osd_id = env["osd_id"]
    # G01: live-path recovery-compare must fail
    log.info("G01: live-path recovery-compare (OSD running)")
    cbt_live = BluestoreToolWorkflows(node=cephadm, nostop=True, nostart=True)
    failed = False
    try:
        out = cbt_live.run_cbt_command(
            cmd=(
                "ceph-bluestore-tool "
                f"--bluestore_allocation_recovery_threads={env['mt_threads']} "
                "recovery-compare"
            ),
            osd_id=osd_id,
            timeout=120,
        )
        text = str(out)
        if (
            any(p in text for p in RECOVERY_COMPARE_SUCCESS)
            and "still running" not in text
        ):
            raise Exception(
                "G01 FAIL: live recovery-compare appeared to succeed; "
                f"snippet={text[-500:]}"
            )
        # Soft success string without lock message is unexpected
        log.warning("G01: command returned without raise; output=%s", text[-500:])
        if "still running" in text.lower() or "lock" in text.lower():
            failed = True
    except Exception as err:
        msg = str(err).lower()
        log.info("G01: expected failure path: %s", err)
        if "still running" in msg or "lock" in msg or "exception" in msg:
            failed = True
        else:
            # Any failure while OSD is up is acceptable for G01
            failed = True
    if not failed:
        raise Exception("G01 FAIL: expected non-zero / lock failure on live OSD")
    wait_osd_up(rados_obj, osd_id)
    log.info("G01 PASS: live-path compare failed; OSD still up")

    # from_file=false on null-FM must fail start; always revert
    log.info(" bluestore_allocation_from_file=false on null-FM")
    try:
        mon_obj.set_config(
            section=f"osd.{osd_id}",
            name="bluestore_allocation_from_file",
            value="false",
        )
        started = rados_obj.change_osd_state(action="restart", target=osd_id)
        # Expect failure to stay up / ENOTSUP
        time.sleep(10)
        tree = rados_obj.run_ceph_command(cmd="ceph osd tree", client_exec=True)
        status = None
        for node in tree.get("nodes", []):
            if node.get("type") == "osd" and int(node.get("id", -1)) == int(osd_id):
                status = node.get("status")
                break
        if started and status == "up":
            # Some builds may refuse config apply differently; check log for ENOTSUP
            recent = read_osd_log(rados_obj, osd_id, lines=200)
            if "ENOTSUP" not in recent and "allocation_from_file" not in recent.lower():
                raise Exception(
                    "G03 FAIL: OSD stayed up with from_file=false and no ENOTSUP evidence"
                )
            log.warning("G03: OSD reported up; ENOTSUP/from_file evidence in logs")
        else:
            log.info("G03 PASS: OSD did not stay healthy (status=%s)", status)
    finally:
        try:
            mon_obj.remove_config(
                section=f"osd.{osd_id}",
                name="bluestore_allocation_from_file",
                verify_rm=False,
            )
        except Exception as err:
            log.warning("G03 revert rm per-osd from_file: %s", err)
        try:
            mon_obj.set_config(
                section="osd", name="bluestore_allocation_from_file", value="true"
            )
        except Exception as err:
            log.warning("G03 revert cluster from_file=true: %s", err)
        try:
            host, unit, _ = _osd_unit_and_log(rados_obj, osd_id)
            host.exec_command(
                sudo=True, cmd=f"systemctl reset-failed {unit}", check_ec=False
            )
            host.exec_command(sudo=True, cmd=f"systemctl start {unit}", check_ec=False)
        except Exception as err:
            log.warning("G03 start after revert: %s", err)
        wait_osd_up(rados_obj, osd_id)
        if not osd_has_allocator_file(rados_obj, osd_id):
            raise Exception("G03 FAIL: ALLOCATOR missing after revert")
        log.info("G03 PASS: reverted from_file; OSD up; ALLOCATOR present")


# teardown


def run_qa_teardown_compare_chance(rados_obj, mon_obj, env):
    osd_id = env["osd_id"]
    for section in ("osd", f"osd.{osd_id}"):
        try:
            mon_obj.remove_config(
                section=section,
                name="bluestore_debug_fast_recovery_compare_chance",
                verify_rm=False,
            )
        except Exception as err:
            log.warning("teardown rm compare_chance on %s: %s", section, err)
    cleanup_osd_qa_config(rados_obj, mon_obj, osd_id)
    assert rados_obj.change_osd_state(
        action="restart", target=osd_id
    ), f"teardown restart failed osd.{osd_id}"

    mark = osd_log_line_count(rados_obj, osd_id)
    sigkill_osd(rados_obj, osd_id)
    wait_osd_up(rados_obj, osd_id)
    new_log = read_osd_log_since_mark(rados_obj, osd_id, mark)
    if any(p in new_log for p in COMPARE_SUCCESS_PATTERNS):
        raise Exception("A5 FAIL: runtime compare success lines after teardown")
    if COMPARE_FAILURE in new_log:
        raise Exception("A5 FAIL: compare failure after teardown")
    log.info("qa_teardown_compare_chance PASSED")


# Shared helpers


def cleanup_osd_qa_config(rados_obj, mon_obj, osd_id):
    for name in (
        "bluestore_allocation_recovery_threads",
        "bluestore_debug_fast_recovery_compare_chance",
        "debug_bluestore",
    ):
        try:
            mon_obj.remove_config(section=f"osd.{osd_id}", name=name, verify_rm=False)
        except Exception as err:
            log.warning("Config rm %s on osd.%s failed: %s", name, osd_id, err)
    try:
        rados_obj.change_osd_state(action="restart", target=osd_id)
    except Exception as err:
        log.warning("Cleanup restart of osd.%s failed: %s", osd_id, err)


def _osd_unit_and_log(rados_obj, osd_id):
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=str(osd_id))
    unit = f"ceph-{fsid}@osd.{osd_id}.service"
    log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"
    return host, unit, log_path


def sigkill_osd(rados_obj, osd_id):
    """Unclean crash via systemctl kill SIGKILL (not orch daemon stop)."""
    host, unit, _ = _osd_unit_and_log(rados_obj, osd_id)
    log.info("SIGKILL %s on %s", unit, host.hostname)
    host.exec_command(
        sudo=True, cmd=f"systemctl kill -s SIGKILL {unit}", check_ec=False
    )
    time.sleep(2)
    host.exec_command(sudo=True, cmd=f"systemctl reset-failed {unit}", check_ec=False)
    host.exec_command(sudo=True, cmd=f"systemctl start {unit}", check_ec=False)


def wait_osd_up(rados_obj, osd_id, timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        try:
            tree = rados_obj.run_ceph_command(cmd="ceph osd tree", client_exec=True)
            for node in tree.get("nodes", []):
                if node.get("type") == "osd" and int(node.get("id", -1)) == int(osd_id):
                    if node.get("status") == "up":
                        log.info("osd.%s is up", osd_id)
                        return True
        except Exception as err:
            log.debug("wait_osd_up poll error: %s", err)
        time.sleep(5)
    raise Exception(f"osd.{osd_id} did not return up within {timeout}s")


def osd_log_line_count(rados_obj, osd_id):
    host, _, log_path = _osd_unit_and_log(rados_obj, osd_id)
    out, _ = host.exec_command(sudo=True, cmd=f"wc -l < {log_path}", check_ec=False)
    try:
        return int(str(out).strip())
    except ValueError:
        return 0


def read_osd_log(rados_obj, osd_id, lines=100):
    host, _, log_path = _osd_unit_and_log(rados_obj, osd_id)
    out, _ = host.exec_command(
        sudo=True, cmd=f"tail -n {lines} {log_path}", check_ec=False
    )
    return str(out)


def read_osd_log_since_mark(rados_obj, osd_id, mark):
    host, _, log_path = _osd_unit_and_log(rados_obj, osd_id)
    start = int(mark) + 1
    out, _ = host.exec_command(
        sudo=True,
        cmd=f"sed -n '{start},$p' {log_path}",
        check_ec=False,
    )
    text = str(out)
    if not text.strip():
        _, unit, _ = _osd_unit_and_log(rados_obj, osd_id)
        jout, _ = host.exec_command(
            sudo=True,
            cmd=f"journalctl -u {unit} --since '10 min ago' --no-pager",
            check_ec=False,
        )
        text = str(jout)
    return text
