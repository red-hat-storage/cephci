"""NFS-Ganesha XCOPY Suite 5: Locking, Concurrency & Negative Resiliency (RES-01..04)."""

import re

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.tsm.share_open import ShareOpenSession, ensure_open_share_script
from tests.nfs.lib.xcopy import create_test_file
from tests.nfs.lib.xcopy.async_helpers import (
    assert_async_progress_in_logs,
    collect_async_log_window,
    create_large_async_file,
)
from tests.nfs.lib.xcopy.helpers import (
    announce,
    case_begin,
    case_pass,
    ensure_background_cp_stopped,
    get_file_size_bytes,
    primary_nfs_export_pseudo,
    start_background_cp,
    wait_background_cp,
    wait_file_min_size,
)
from tests.nfs.lib.xcopy.resiliency_helpers import (
    clear_cephfs_byte_quota,
    cp_stderr_suggests_nospc,
    dir_used_bytes,
    hold_exclusive_fcntl_lock,
    probe_open_share_once,
    release_fcntl_lock,
    set_cephfs_byte_quota,
    sha256_file,
    start_active_writer,
    stop_background,
    timed_cp_capture,
)
from tests.nfs.xcopy import test_nfs_xcopy_intra_export_sync as base
from utility.log import Log

log = Log(__name__)

DUAL_CLIENT_CASES = {
    "concurrent_lock_conflict_src",
    "dst_share_deny_write",
}


def _assert_ganesha_up(ctx):
    base.assert_ganesha_up(ctx, label="resiliency scenario")


def _paths(ctx, src_name, dst_name):
    mnt = ctx["nfs_mount_a"]
    return f"{mnt}/{src_name}", f"{mnt}/{dst_name}"


def _async_size_gb(ctx):
    return float(ctx["config"].get("res_async_size_gb", 8))


def _conflict_mode(ctx):
    return str(ctx["config"].get("res_src_conflict_mode", "lock")).lower()


def _ensure_second_client_mount(ctx):
    """Verify the same export is mounted on clients[1]."""
    if len(ctx["clients"]) < 2:
        raise ConfigError(
            "RES dual-client case needs config.clients >= 2 "
            f"(have {len(ctx['clients'])})"
        )
    if ctx.get("client2"):
        return
    client2 = ctx["clients"][1]
    announce(f"Verify export mount on second client {client2.hostname}")
    out, _ = client2.exec_command(
        sudo=True,
        cmd=f"mountpoint -q {ctx['nfs_mount']} && echo mounted || echo missing",
        check_ec=False,
    )
    if "mounted" not in (out or ""):
        raise ConfigError(
            f"Second client {client2.hostname} missing mount {ctx['nfs_mount']}"
        )
    ctx["client2"] = client2


# RES-01


def case_concurrent_lock_conflict_src(ctx):
    """RES-01: Exclusive lock (or active write) on source while peer runs XCOPY."""
    case_begin("concurrent_lock_conflict_src (RES-01)")
    _ensure_second_client_mount(ctx)
    mode = _conflict_mode(ctx)
    size_mb = int(ctx["config"].get("file_size_mb", 256))
    mnt = ctx["nfs_mount"]
    src = f"{mnt}/xcopy_res01_src.bin"
    dst = f"{mnt}/xcopy_res01_dst.bin"
    create_test_file(ctx["client"], src, size_mb=size_mb)
    orig = get_file_size_bytes(ctx["client"], src)
    base._mark_logs(ctx)

    lock_pid = writer_pid = None
    stop = None
    try:
        if mode == "write":
            writer_pid, stop = start_active_writer(
                ctx["client"], src, hold_sec=180, tag="xcopy_res01_w"
            )
            announce("Active write held on source; peer starts XCOPY")
        else:
            lock_pid, _ready, stop = hold_exclusive_fcntl_lock(
                ctx["client"], src, hold_sec=180, tag="xcopy_res01_lk"
            )
            announce("Exclusive F_WRLCK held on source; peer starts XCOPY")

        rc, stderr, elapsed = timed_cp_capture(ctx["client2"], src, dst, timeout=900)
        dst_sz = get_file_size_bytes(ctx["client2"], dst)
        log.info(
            "RES-01 result: mode=%s cp_rc=%s elapsed=%.3fs dst=%s/%s stderr=%r",
            mode,
            rc,
            elapsed,
            dst_sz,
            orig,
            (stderr or "")[:240],
        )
        _assert_ganesha_up(ctx)

        full_ok = rc == 0 and dst_sz >= orig
        if mode == "write":
            # Active mutation: full success under concurrent overwrite is wrong.
            if full_ok:
                raise OperationFailedError(
                    "RES-01 BUG: XCOPY succeeded fully while source was under "
                    f"active write (rc=0 dst={dst_sz} orig={orig} stderr={stderr!r})"
                )
            log.info(
                "RES-01 write-mode: XCOPY did not fully succeed under writer "
                "(rc=%s dst=%s/%s)",
                rc,
                dst_sz,
                orig,
            )
        else:
            # Lock mode: allow COPY under F_WRLCK (not share_deny). Share
            # reservation conflicts are covered by RES-02.
            if full_ok:
                log.info(
                    "RES-01 lock-mode: XCOPY completed under exclusive flock "
                    "(rc=0 dst=%s) — expected for byte-range locks; share_deny "
                    "is RES-02",
                    dst_sz,
                )
            else:
                log.info(
                    "RES-01 lock-mode: XCOPY rejected/deferred under flock "
                    "(rc=%s dst=%s/%s stderr=%r)",
                    rc,
                    dst_sz,
                    orig,
                    (stderr or "")[:160],
                )
    finally:
        if lock_pid is not None and stop:
            release_fcntl_lock(ctx["client"], stop, lock_pid)
        if writer_pid is not None and stop:
            stop_background(ctx["client"], stop, writer_pid)

    case_pass("concurrent_lock_conflict_src")


# RES-02


def case_dst_share_deny_write(ctx):
    """RES-02: Client1 OPEN dst with share_deny=WRITE; Client2 XCOPY → that dst."""
    case_begin("dst_share_deny_write (RES-02)")
    _ensure_second_client_mount(ctx)
    size_mb = min(int(ctx["config"].get("file_size_mb", 128)), 128)
    mnt = ctx["nfs_mount"]
    src_name = "xcopy_res02_src.bin"
    dst_name = "xcopy_res02_dst.bin"
    src = f"{mnt}/{src_name}"
    dst = f"{mnt}/{dst_name}"

    create_test_file(ctx["client"], src, size_mb=size_mb)
    # Seed destination with known content (different from src).
    create_test_file(ctx["client"], dst, size_mb=1)
    ctx["client"].exec_command(
        sudo=True,
        cmd=f"dd if=/dev/urandom of={dst} bs=1M count=1 conv=notrunc status=none",
    )
    before_sha = sha256_file(ctx["client"], dst)
    before_sz = get_file_size_bytes(ctx["client"], dst)
    if not before_sha:
        raise OperationFailedError("RES-02 could not hash seeded destination")

    if ensure_open_share_script(ctx["client"]):
        raise OperationFailedError("RES-02 failed to upload open_share.py")
    if ensure_open_share_script(ctx["client2"]):
        raise OperationFailedError("RES-02 failed to upload open_share.py on client2")

    export = primary_nfs_export_pseudo(ctx)
    server = ctx["server_ip"]
    sess = ShareOpenSession(
        ctx["client"],
        server=server,
        port=ctx["port"],
        export=export,
        filename=dst_name,
        tag="res02",
    )
    base._mark_logs(ctx)
    try:
        announce(
            f"Client1 OPEN hold access=rw deny=w on {dst_name} "
            f"(export={export} server={server})"
        )
        outcome = sess.open_hold(access="rw", deny="w", create=False, timeout=90)
        if outcome != "success":
            raise OperationFailedError(
                f"RES-02 Client1 share_deny OPEN failed: {outcome}"
            )

        # Prove the reservation is live at the OPEN layer (peer WRITE OPEN).
        probe, probe_txt = probe_open_share_once(
            ctx["client2"],
            server=server,
            port=ctx["port"],
            export=export,
            filename=dst_name,
            access="w",
            deny="n",
            tag="res02_peer",
        )
        if probe == "success":
            raise OperationFailedError(
                "RES-02 BUG: peer OPEN(access=WRITE) succeeded while "
                f"share_deny=WRITE held (probe={probe}). Reservation not "
                f"enforced at OPEN.\n{probe_txt[-500:]}"
            )
        if probe != "denied":
            log.warning(
                "RES-02: peer OPEN probe unexpected outcome=%s "
                "(continuing to overwrite cp; expect deny/fail)",
                probe,
            )
        else:
            log.info("RES-02: peer OPEN correctly NFS4ERR_SHARE_DENIED")

        announce(
            "Client2 initiates XCOPY overwrite of share-denied destination "
            "(no unlink — must hit existing inode)"
        )
        rc, stderr, elapsed = timed_cp_capture(
            ctx["client2"], src, dst, timeout=300, remove_dst=False
        )
        after_sha = sha256_file(ctx["client"], dst)
        after_sz = get_file_size_bytes(ctx["client"], dst)
        log.info(
            "RES-02 result: probe=%s cp_rc=%s elapsed=%.3fs "
            "before=%s/%s after=%s/%s stderr=%r",
            probe,
            rc,
            elapsed,
            before_sz,
            before_sha,
            after_sz,
            after_sha,
            (stderr or "")[:240],
        )
        _assert_ganesha_up(ctx)

        if rc == 0:
            raise OperationFailedError(
                "RES-02 BUG: XCOPY overwrite succeeded against share_deny=WRITE "
                f"destination (rc=0 after_sz={after_sz} after_sha={after_sha} "
                f"probe={probe})"
            )
        if after_sha != before_sha or after_sz != before_sz:
            raise OperationFailedError(
                "RES-02 BUG: destination tampered despite share_deny "
                f"before={before_sz}/{before_sha} after={after_sz}/{after_sha}"
            )
        log.info(
            "RES-02: XCOPY rejected and dst untampered " "(probe=%s rc=%s stderr=%r)",
            probe,
            rc,
            (stderr or "")[:160],
        )
    finally:
        try:
            sess.stop()
        except Exception as exc:
            log.warning("RES-02 share session stop: %s", exc)

    case_pass("dst_share_deny_write")


# RES-03


def case_storage_quota_mid_copy(ctx):
    """RES-03: Near-full CephFS quota; XCOPY that would exceed available bytes."""
    case_begin("storage_quota_mid_copy (RES-03)")
    mnt = ctx["nfs_mount_a"]
    src = f"{mnt}/xcopy_res03_src.bin"
    dst = f"{mnt}/xcopy_res03_dst.bin"
    # Source larger than remaining quota headroom.
    src_mb = int(ctx["config"].get("res_quota_src_mb", 64))
    headroom_mb = int(ctx["config"].get("res_quota_headroom_mb", 8))
    create_test_file(ctx["client"], src, size_mb=src_mb)
    used = dir_used_bytes(ctx["client"], mnt)
    # Quota = current used + small headroom (less than src).
    quota = used + (headroom_mb * 1024 * 1024)
    log.info(
        "RES-03 used=%s quota_target=%s src_mb=%s headroom_mb=%s",
        used,
        quota,
        src_mb,
        headroom_mb,
    )
    if src_mb * 1024 * 1024 <= headroom_mb * 1024 * 1024:
        raise ConfigError(
            "res_quota_src_mb must be > res_quota_headroom_mb so copy exceeds quota"
        )

    # Quotas must be set on the CephFS subvolume — NFS rejects ceph.quota xattrs.
    set_cephfs_byte_quota(ctx["client"], mnt, quota, ctx=ctx)
    base._mark_logs(ctx)
    try:
        rc, stderr, elapsed = timed_cp_capture(ctx["client"], src, dst, timeout=600)
        dst_sz = get_file_size_bytes(ctx["client"], dst)
        log.info(
            "RES-03 result: cp_rc=%s elapsed=%.3fs dst=%s stderr=%r",
            rc,
            elapsed,
            dst_sz,
            (stderr or "")[:240],
        )
        _assert_ganesha_up(ctx)

        if rc == 0:
            raise OperationFailedError(
                "RES-03 BUG: XCOPY succeeded despite quota that should block "
                f"(rc=0 dst={dst_sz} quota={quota})"
            )
        if not cp_stderr_suggests_nospc(stderr):
            # Some clients only return generic I/O error — warn but still require
            # failure + Ganesha up (do not mask a success).
            log.warning(
                "RES-03: cp failed but stderr lacks explicit NOSPC/EDQUOT "
                "(stderr=%r) — failure still required",
                (stderr or "")[:200],
            )
        # Best-effort cleanup of partial destination.
        ctx["client"].exec_command(sudo=True, cmd=f"rm -f {dst}", check_ec=False)
    finally:
        clear_cephfs_byte_quota(ctx["client"], mnt, ctx=ctx)
        ctx["client"].exec_command(sudo=True, cmd=f"rm -f {src} {dst}", check_ec=False)

    case_pass("storage_quota_mid_copy")


# RES-04


def case_src_unlink_truncate_mid_async(ctx):
    """RES-04: Truncate (and optionally unlink) source mid large ASYNC copy."""
    case_begin("src_unlink_truncate_mid_async (RES-04)")
    size_gb = _async_size_gb(ctx)
    action = str(ctx["config"].get("res_src_mid_action", "truncate")).lower()
    src, dst = _paths(ctx, "xcopy_res04_src.bin", "xcopy_res04_dst.bin")
    total = create_large_async_file(ctx["client"], src, size_gb)
    base._mark_logs(ctx)

    pid = rc_file = None
    mid = cp_rc = None
    waited = False
    try:
        pid, rc_file = start_background_cp(ctx["client"], src, dst, tag="xcopy_res04")
        # Wait for real mid-copy progress (at least one chunk).
        min_bytes = min(64 * 1024 * 1024, max(total // 20, 1024 * 1024))
        wait_file_min_size(ctx["client"], dst, min_bytes=min_bytes, timeout=600)
        mid = get_file_size_bytes(ctx["client"], dst)
        announce(f"Mid-copy action={action} at dst={mid}/{total}")

        if action == "unlink":
            ctx["client"].exec_command(sudo=True, cmd=f"rm -f {src}")
        else:
            ctx["client"].exec_command(sudo=True, cmd=f"truncate -s 0 {src}")
            if ctx["config"].get("res_src_mid_also_unlink"):
                ctx["client"].exec_command(sudo=True, cmd=f"rm -f {src}")

        cp_rc = wait_background_cp(ctx["client"], pid, rc_file, timeout=7200)
        waited = True
    finally:
        if pid and not waited:
            ensure_background_cp_stopped(ctx["client"], pid, rc_file, tag="xcopy_res04")

    src_sz = get_file_size_bytes(ctx["client"], src)
    dst_sz = get_file_size_bytes(ctx["client"], dst)
    log_text = collect_async_log_window(ctx)
    has_async = assert_async_progress_in_logs(log_text) if log_text else False
    # CB_OFFLOAD OK with short byte count is the classic silent-short signature.
    cb_ok_short = False
    if log_text:
        for m in re.finditer(r"CB_OFFLOAD:\s*sending OK bytes=(\d+)", log_text, re.I):
            cb_bytes = int(m.group(1))
            log.info("RES-04 CB_OFFLOAD OK bytes=%s (orig=%s)", cb_bytes, total)
            if 0 < cb_bytes < total:
                cb_ok_short = True
    log.info(
        "RES-04 result: action=%s cp_rc=%s orig=%s src=%s dst=%s mid=%s "
        "async_logs=%s cb_ok_short=%s",
        action,
        cp_rc,
        total,
        src_sz,
        dst_sz,
        mid,
        has_async,
        cb_ok_short,
    )
    _assert_ganesha_up(ctx)

    # Product bug: client sees success but dst is short.
    # Do NOT soften this — truncate mid-ASYNC must not yield silent short OK.
    if cp_rc == 0 and dst_sz > 0 and dst_sz < total:
        raise OperationFailedError(
            "RES-04 BUG: silent short COPY after src "
            f"{action}: cp_rc=0 orig={total} dst={dst_sz} src={src_sz} "
            f"mid={mid} cb_ok_short={cb_ok_short} "
            "(Ganesha reported success with incomplete offload)"
        )
    if cp_rc == 0 and dst_sz >= total:
        log.info("RES-04: full-size dst (snapshot completed before invalidate)")
    else:
        log.info(
            "RES-04: clean non-success or empty dst after %s "
            "(cp_rc=%s dst=%s) — no panic",
            action,
            cp_rc,
            dst_sz,
        )
    case_pass("src_unlink_truncate_mid_async")


CASES = {
    "concurrent_lock_conflict_src": case_concurrent_lock_conflict_src,
    "dst_share_deny_write": case_dst_share_deny_write,
    "storage_quota_mid_copy": case_storage_quota_mid_copy,
    "src_unlink_truncate_mid_async": case_src_unlink_truncate_mid_async,
}


def _prepare_resiliency(config, case_names):
    if (
        any(n in DUAL_CLIENT_CASES for n in case_names)
        and int(config.get("clients", 1)) < 2
    ):
        config = dict(config)
        config["clients"] = 2
        log.info("Raising clients=2 for dual-client resiliency cases")
    return config


def run(ceph_cluster, **kw):
    """Run one or more RES-01..04 cases."""
    return base.run_selected(
        ceph_cluster,
        kw,
        CASES,
        defaults=[
            "concurrent_lock_conflict_src",
            "dst_share_deny_write",
            "storage_quota_mid_copy",
            "src_unlink_truncate_mid_async",
        ],
        label="RESILIENCY",
        prepare_ctx=_prepare_resiliency,
    )
