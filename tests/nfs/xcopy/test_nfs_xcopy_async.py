"""NFS-Ganesha ASYNC COPY / offload lifecycle cases (RHEL 10 / NFSv4.2)."""

import time

from cli.exceptions import OperationFailedError
from tests.nfs.lib.xcopy.async_helpers import (
    NFS_OPCODE_OFFLOAD_CANCEL,
    NFS_OPCODE_OFFLOAD_STATUS,
    assert_async_progress_in_logs,
    assert_cancel_evidence,
    block_nfs_to_server,
    collect_async_log_window,
    count_nfs_opcodes,
    create_large_async_file,
    poll_copy_progress,
    signal_process,
    start_pcap_if_enabled,
    stop_pcap_if_started,
    unblock_nfs_rule,
    wait_size_stable,
)
from tests.nfs.lib.xcopy.helpers import (
    announce,
    case_begin,
    case_pass,
    ensure_background_cp_stopped,
    get_file_size_bytes,
    start_background_cp,
    wait_background_cp,
)
from tests.nfs.xcopy import test_nfs_xcopy_intra_export_sync as base
from utility.log import Log

log = Log(__name__)


def _async_size_gb(ctx):
    return float(ctx["config"].get("async_size_gb", 2))


def _poll_interval(ctx):
    return float(ctx["config"].get("async_poll_interval_sec", 5))


def _cancel_ratio(ctx):
    return float(ctx["config"].get("async_cancel_ratio", 0.30))


def _cancel_poll_interval(ctx):
    """Poll interval for ASYNC-02/03 mid-copy interrupt (default 1s)."""
    return float(ctx["config"].get("async_cancel_poll_interval_sec", 1))


def _cancel_max_ratio(ctx):
    """If dst already past this when tripwire fires, cancel window was missed."""
    return float(ctx["config"].get("async_cancel_max_ratio", 0.90))


def _disconnect_wait(ctx):
    return int(ctx["config"].get("async_disconnect_wait_sec", 90))


def _assert_ganesha_up(ctx):
    base.assert_ganesha_up(ctx, label="ASYNC scenario")


def _paths(ctx, src_name, dst_name):
    mnt = ctx["nfs_mount_a"]
    return f"{mnt}/{src_name}", f"{mnt}/{dst_name}"


def _chunk_bytes(ctx):
    return int(ctx["chunk_size_mb"]) * 1024 * 1024


# ASYNC-01


def case_offload_status_polling(ctx):
    """ASYNC-01: Large ASYNC copy with regular progress polling."""
    case_begin("offload_status_polling (ASYNC-01)")
    size_gb = _async_size_gb(ctx)
    src, dst = _paths(ctx, "xcopy_async01_src.bin", "xcopy_async01_dst.bin")
    total = create_large_async_file(ctx["client"], src, size_gb)
    base._mark_logs(ctx)
    pcap_pid = start_pcap_if_enabled(ctx)
    pid = rc_file = None
    cp_rc = samples = final_sz = None
    waited = False

    try:
        pid, rc_file = start_background_cp(ctx["client"], src, dst, tag="xcopy_async01")
        samples, final_sz = poll_copy_progress(
            ctx["client"],
            dst,
            total_bytes=total,
            poll_interval_sec=_poll_interval(ctx),
            timeout_sec=int(ctx["config"].get("async_copy_timeout_sec", 7200)),
        )
        cp_rc = wait_background_cp(
            ctx["client"],
            pid,
            rc_file,
            timeout=int(ctx["config"].get("async_copy_timeout_sec", 7200)),
        )
        waited = True
    finally:
        if pid and not waited:
            ensure_background_cp_stopped(
                ctx["client"], pid, rc_file, tag="xcopy_async01"
            )
        stop_pcap_if_started(ctx, pcap_pid)

    log_text = collect_async_log_window(ctx)
    dst_sz = get_file_size_bytes(ctx["client"], dst)
    log.info(
        "ASYNC-01 done: cp_rc=%s samples=%s final_poll=%s dst=%s total=%s",
        cp_rc,
        len(samples or []),
        final_sz,
        dst_sz,
        total,
    )

    if cp_rc != 0:
        raise OperationFailedError(f"ASYNC-01 cp failed rc={cp_rc}")
    if dst_sz < total:
        raise OperationFailedError(f"ASYNC-01 incomplete: dst={dst_sz} < total={total}")
    if len(samples) < 2:
        raise OperationFailedError(
            "ASYNC-01 need >=2 progress samples (copy finished too fast; "
            "increase async_size_gb)"
        )
    # Monotonic non-decreasing sizes
    sizes = [s for _, s in samples]
    if any(sizes[i] < sizes[i - 1] for i in range(1, len(sizes))):
        raise OperationFailedError(f"ASYNC-01 progress not monotonic: {sizes[:20]}...")
    # Must have seen intermediate progress (not 0 → full in one sample only)
    if (
        sizes[0] == 0
        and sizes[-1] >= total
        and len([s for s in sizes if 0 < s < total]) < 1
    ):
        log.warning(
            "ASYNC-01: no mid-progress sample observed (copy very fast); "
            "still checking logs/pcap"
        )

    if not assert_async_progress_in_logs(log_text):
        raise OperationFailedError(
            "ASYNC-01 missing ASYNC worker/chunk evidence in Ganesha logs"
        )

    if ctx.get("pcap_validate", True):
        ops = count_nfs_opcodes(
            ctx["client"],
            ctx["pcap_path"],
            (NFS_OPCODE_OFFLOAD_STATUS, "60"),
        )
        status_n = ops.get(NFS_OPCODE_OFFLOAD_STATUS, 0)
        copy_n = ops.get("60", 0)
        log.info(
            "ASYNC-01 pcap: OFFLOAD_STATUS(%s)=%s COPY(60)=%s",
            NFS_OPCODE_OFFLOAD_STATUS,
            status_n,
            copy_n,
        )
        if copy_n < 1:
            raise OperationFailedError("ASYNC-01 pcap missing OP_COPY (60)")
        if status_n < 1:
            # Kernel may complete via CB_OFFLOAD without client OFFLOAD_STATUS.
            log.warning(
                "OFFLOAD_STATUS (%s) not seen in pcap — progress validated via "
                "dst size polling + Ganesha ASYNC logs (RHEL client may not "
                "emit STATUS RPCs for every poll)",
                NFS_OPCODE_OFFLOAD_STATUS,
            )

    _assert_ganesha_up(ctx)
    case_pass("offload_status_polling")


# ASYNC-02


def case_offload_cancel_abort(ctx):
    """ASYNC-02: Abort ASYNC copy around 30% (SIGINT ≈ Ctrl+C / OFFLOAD_CANCEL)."""
    case_begin("offload_cancel_abort (ASYNC-02)")
    size_gb = _async_size_gb(ctx)
    ratio = _cancel_ratio(ctx)
    src, dst = _paths(ctx, "xcopy_async02_src.bin", "xcopy_async02_dst.bin")
    total = create_large_async_file(ctx["client"], src, size_gb)
    base._mark_logs(ctx)
    pcap_pid = start_pcap_if_enabled(ctx)
    chunk = _chunk_bytes(ctx)

    max_ratio = _cancel_max_ratio(ctx)
    cancel_poll = _cancel_poll_interval(ctx)
    pid = rc_file = None
    samples = at_cancel = size_at_signal = cp_rc = stable = interrupted = None
    waited = False
    try:
        pid, rc_file = start_background_cp(ctx["client"], src, dst, tag="xcopy_async02")
        samples, at_cancel = poll_copy_progress(
            ctx["client"],
            dst,
            total_bytes=total,
            poll_interval_sec=cancel_poll,
            timeout_sec=int(ctx["config"].get("async_copy_timeout_sec", 7200)),
            stop_when_ratio=ratio,
            max_stop_ratio=max_ratio,
            # Size can jump 28%→90% between SSH polls; don't wait for ~30%.
            stop_on_first_progress=True,
        )
        if at_cancel <= 0:
            raise OperationFailedError(
                "ASYNC-02 never reached cancel ratio "
                f"(ratio={ratio}, dst={at_cancel}, total={total}); "
                "increase async_size_gb"
            )
        # Copy finished (or nearly) before SIGINT — not a product cancel bug.
        if at_cancel >= int(total * max_ratio):
            raise OperationFailedError(
                "ASYNC-02 harness: missed mid-copy cancel window "
                f"(dst={at_cancel}/{total} >= {100 * max_ratio:.0f}% before "
                f"SIGINT; ratio={ratio}, poll={cancel_poll}s). "
                "Lower async_cancel_poll_interval_sec or raise async_size_gb"
            )
        announce(
            f"Cancel mid-copy (dst={at_cancel}/{total}, "
            f"~{100.0 * at_cancel / total:.1f}%; tripwire≈{100 * ratio:.0f}%) "
            f"via SIGINT"
        )
        interrupted = signal_process(
            ctx["client"], pid, sig="INT", escalate=True, tag="xcopy_async02"
        )
        if not interrupted:
            raise OperationFailedError(
                "ASYNC-02 harness: background cp session still alive after "
                "SIGINT/TERM/KILL — cannot validate OFFLOAD_CANCEL"
            )
        # Size when interrupt harness finished — cancel RPC should be in flight
        # or already processed. Pre-signal at_cancel is too stale: signal script
        # + SSH latency let many fast chunks land before OFFLOAD_CANCEL.
        size_at_signal = get_file_size_bytes(ctx["client"], dst)
        cp_rc = wait_background_cp(ctx["client"], pid, rc_file, timeout=300)
        waited = True
        # Allow current chunk to finish, then size should stabilize below total.
        stable = wait_size_stable(
            ctx["client"], dst, stable_sec=20, poll=2, timeout=180
        )
    finally:
        if pid and not waited:
            ensure_background_cp_stopped(
                ctx["client"], pid, rc_file, tag="xcopy_async02"
            )
        stop_pcap_if_started(ctx, pcap_pid)

    log_text = collect_async_log_window(ctx)
    ops = {}
    if ctx.get("pcap_validate", True):
        ops = count_nfs_opcodes(
            ctx["client"],
            ctx["pcap_path"],
            (NFS_OPCODE_OFFLOAD_CANCEL, NFS_OPCODE_OFFLOAD_STATUS, "60"),
        )

    cancel_rpc = int(ops.get(NFS_OPCODE_OFFLOAD_CANCEL, 0)) if ops else 0
    log.info(
        "ASYNC-02 result: cp_rc=%s at_cancel=%s size_at_signal=%s stable=%s "
        "total=%s chunk=%s OFFLOAD_CANCEL=%s interrupted=%s",
        cp_rc,
        at_cancel,
        size_at_signal,
        stable,
        total,
        chunk,
        cancel_rpc,
        interrupted,
    )
    _assert_ganesha_up(ctx)

    # Successful uninterrupted cp must not happen after a real mid-copy cancel.
    if cp_rc == 0 and stable >= total and cancel_rpc < 1:
        raise OperationFailedError(
            "ASYNC-02 harness: cp finished rc=0 with no OFFLOAD_CANCEL "
            f"(stable={stable} at_cancel={at_cancel}) — interrupt did not "
            "reach NFS client; check background cp trap/signal path"
        )

    # Must not complete the full file after cancel. Baseline is size when the
    # interrupt harness returned (not the earlier poll tripwire). Ganesha may
    # finish the current chunk after OFFLOAD_CANCEL ("stop at next chunk").
    max_allowed = min(total - 1, size_at_signal + chunk)
    if stable >= total:
        raise OperationFailedError(
            f"ASYNC-02 BUG: copy reached full size after cancel "
            f"(stable={stable} total={total} at_cancel={at_cancel} "
            f"size_at_signal={size_at_signal} cp_rc={cp_rc} "
            f"OFFLOAD_CANCEL={cancel_rpc})"
        )
    if stable > max_allowed:
        raise OperationFailedError(
            "ASYNC-02 dst grew beyond post-signal size + one chunk: "
            f"stable={stable} size_at_signal={size_at_signal} "
            f"at_cancel={at_cancel} chunk={chunk} max_allowed={max_allowed}"
        )

    if not assert_cancel_evidence(log_text, ops):
        # Still require incomplete copy; warn if RPC/log cancel string missing.
        log.warning(
            "No explicit OFFLOAD_CANCEL/log cancel string — incomplete dst "
            "after SIGINT is still required evidence of abort"
        )

    case_pass("offload_cancel_abort")


# ASYNC-03


def case_client_disconnect_mid_async(ctx):
    """ASYNC-03: Sever client→server NFS path mid-ASYNC copy."""
    case_begin("client_disconnect_mid_async (ASYNC-03)")
    size_gb = _async_size_gb(ctx)
    src, dst = _paths(ctx, "xcopy_async03_src.bin", "xcopy_async03_dst.bin")
    total = create_large_async_file(ctx["client"], src, size_gb)
    base._mark_logs(ctx)
    chunk = _chunk_bytes(ctx)
    rule = None

    max_ratio = _cancel_max_ratio(ctx)
    mid_poll = _cancel_poll_interval(ctx)
    pid = rc_file = None
    mid = cp_rc = stable = None
    waited = False
    try:
        pid, rc_file = start_background_cp(ctx["client"], src, dst, tag="xcopy_async03")
        _samples, mid = poll_copy_progress(
            ctx["client"],
            dst,
            total_bytes=total,
            poll_interval_sec=mid_poll,
            timeout_sec=int(ctx["config"].get("async_copy_timeout_sec", 7200)),
            stop_when_ratio=0.15,
            max_stop_ratio=max_ratio,
            stop_on_first_progress=True,
        )
        if mid <= 0:
            raise OperationFailedError(
                "ASYNC-03 no progress before disconnect; increase async_size_gb"
            )
        if mid >= int(total * max_ratio):
            raise OperationFailedError(
                "ASYNC-03 harness: missed mid-copy disconnect window "
                f"(dst={mid}/{total} >= {100 * max_ratio:.0f}% before iptables "
                f"DROP; poll={mid_poll}s). Lower async_cancel_poll_interval_sec "
                "or raise async_size_gb"
            )

        try:
            rule = block_nfs_to_server(
                ctx["client"], ctx["server_ip"], port=ctx["port"]
            )
            wait_s = _disconnect_wait(ctx)
            announce(f"Hold disconnect for {wait_s}s (session/worker timeout window)")
            time.sleep(wait_s)
        finally:
            if rule:
                unblock_nfs_rule(ctx["client"], rule)

        # Give client/server a moment to notice path restore; cp may hang or die.
        cp_rc = wait_background_cp(ctx["client"], pid, rc_file, timeout=600)
        waited = True
        stable = wait_size_stable(
            ctx["client"], dst, stable_sec=20, poll=2, timeout=180
        )
    finally:
        if pid and not waited:
            ensure_background_cp_stopped(
                ctx["client"], pid, rc_file, tag="xcopy_async03"
            )

    log_text = collect_async_log_window(ctx)

    log.info(
        "ASYNC-03 result: cp_rc=%s mid=%s stable=%s total=%s",
        cp_rc,
        mid,
        stable,
        total,
    )
    _assert_ganesha_up(ctx)

    if stable >= total:
        raise OperationFailedError(
            f"ASYNC-03 BUG: full copy completed despite disconnect "
            f"(stable={stable} total={total})"
        )
    # May finish current chunk after the cut.
    if stable > mid + (2 * chunk):
        raise OperationFailedError(
            "ASYNC-03 dst grew more than ~2 chunks after disconnect: "
            f"mid={mid} stable={stable} chunk={chunk}"
        )

    if log_text and not assert_async_progress_in_logs(log_text):
        log.warning("ASYNC-03: limited ASYNC log evidence in window")

    case_pass("client_disconnect_mid_async")


CASES = {
    "offload_status_polling": case_offload_status_polling,
    "offload_cancel_abort": case_offload_cancel_abort,
    "client_disconnect_mid_async": case_client_disconnect_mid_async,
}


def _async_iptables_sweep(ctx):
    """Remove any leftover DROP rule matching NFS port."""
    try:
        ctx["client"].exec_command(
            sudo=True,
            cmd=(
                f"iptables -S OUTPUT 2>/dev/null | "
                f"grep -F '{ctx['server_ip']}' | grep -F 'dport {ctx['port']}' "
                f"| grep DROP | sed 's/^-A /-D /' | "
                f"while read r; do iptables $r; done"
            ),
            check_ec=False,
        )
    except Exception as cleanup_exc:
        log.warning("iptables cleanup best-effort failed: %s", cleanup_exc)


def _prepare_async(config, _case_names):
    if "pcap_validate" not in config:
        config = dict(config)
        config["pcap_validate"] = True
    return config


def run(ceph_cluster, **kw):
    """Run one or more ASYNC-01..03 cases."""
    return base.run_selected(
        ceph_cluster,
        kw,
        CASES,
        defaults=[
            "offload_status_polling",
            "offload_cancel_abort",
            "client_disconnect_mid_async",
        ],
        label="ASYNC",
        prepare_ctx=_prepare_async,
        extra_finally=_async_iptables_sweep,
    )
