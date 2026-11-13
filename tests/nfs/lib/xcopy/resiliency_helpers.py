"""Helpers for XCOPY locking / concurrency / negative resiliency (RES-01..04)."""

import time

from tests.nfs.lib.xcopy.helpers import announce, get_file_size_bytes
from utility.log import Log

log = Log(__name__)


def hold_exclusive_fcntl_lock(client, path, hold_sec=120, tag="xcopy_res_lock"):
    """Hold an exclusive POSIX flock (F_WRLCK / LOCK_EX) on ``path`` in background."""
    ready = f"/tmp/{tag}.ready"
    stop = f"/tmp/{tag}.stop"
    logf = f"/tmp/{tag}.log"
    pidf = f"/tmp/{tag}.pid"
    script = f"/tmp/{tag}.py"
    announce(f"Hold exclusive flock on {path} ({hold_sec}s max) on {client.hostname}")
    client.exec_command(
        sudo=True,
        cmd=f"rm -f {ready} {stop} {logf} {pidf} {script}",
        check_ec=False,
    )
    # Write script remotely (avoid nested quoting through SSH).
    client.exec_command(
        sudo=True,
        cmd=(
            f"cat > {script} <<'PY'\n"
            "import fcntl, os, time\n"
            f"path = {path!r}\n"
            f"ready = {ready!r}\n"
            f"stop = {stop!r}\n"
            f"hold = {int(hold_sec)}\n"
            "f = open(path, 'rb+', buffering=0)\n"
            "fcntl.flock(f.fileno(), fcntl.LOCK_EX)\n"
            "open(ready, 'w').write('1')\n"
            "t0 = time.time()\n"
            "while time.time() - t0 < hold:\n"
            "    if os.path.exists(stop):\n"
            "        break\n"
            "    time.sleep(0.2)\n"
            "fcntl.flock(f.fileno(), fcntl.LOCK_UN)\n"
            "f.close()\n"
            "PY"
        ),
    )
    from tests.nfs.lib.xcopy.helpers import _wait_remote_pid

    client.exec_command(
        sudo=True,
        cmd=(f"nohup python3 {script} >{logf} 2>&1 </dev/null & " f"echo $! > {pidf}"),
    )
    pid = _wait_remote_pid(client, pidf, retries=50, sleep_sec=0.2, log_file=logf)
    for _ in range(50):
        out, _ = client.exec_command(
            sudo=True, cmd=f"test -f {ready} && echo ready", check_ec=False
        )
        if "ready" in (out or ""):
            log.info("Exclusive flock held: pid=%s path=%s", pid, path)
            return pid, ready, stop
        time.sleep(0.2)
    out, _ = client.exec_command(sudo=True, cmd=f"cat {logf}", check_ec=False)
    raise RuntimeError(
        f"fcntl lock not acquired (no ready marker); log:\n{(out or '').strip()}"
    )


def release_fcntl_lock(client, stop_file, pid, timeout=30):
    """Drop stop marker and wait for lock holder to exit."""
    announce(f"Release flock (stop={stop_file} pid={pid})")
    stop_background(client, stop_file, pid, timeout=timeout)


def start_active_writer(client, path, hold_sec=120, tag="xcopy_res_writer"):
    """Continuously rewrite the first 1 MiB of ``path`` (active write conflict)."""
    stop = f"/tmp/{tag}.stop"
    logf = f"/tmp/{tag}.log"
    pidf = f"/tmp/{tag}.pid"
    announce(f"Start active writer on {path} ({hold_sec}s max)")
    client.exec_command(sudo=True, cmd=f"rm -f {stop} {logf} {pidf}", check_ec=False)
    client.exec_command(
        sudo=True,
        cmd=(
            f"nohup bash -c '"
            f"t0=$(date +%s); "
            f"while [ $(($(date +%s)-t0)) -lt {int(hold_sec)} ]; do "
            f"  [ -f {stop} ] && break; "
            f"  dd if=/dev/urandom of={path} bs=1M count=1 conv=notrunc "
            f"    status=none 2>/dev/null || true; "
            f"  sleep 0.5; "
            f"done"
            f"' >{logf} 2>&1 </dev/null & echo $! > {pidf}"
        ),
    )
    out, _ = client.exec_command(sudo=True, cmd=f"cat {pidf}", check_ec=False)
    pid = int((out or "0").strip() or "0")
    log.info("Active writer pid=%s path=%s", pid, path)
    return pid, stop


def stop_background(client, stop_file, pid, timeout=30):
    """Touch stop_file and wait for pid (or SIGTERM)."""
    client.exec_command(sudo=True, cmd=f"touch {stop_file}", check_ec=False)
    deadline = time.time() + float(timeout)
    while time.time() < deadline:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"kill -0 {int(pid)} 2>/dev/null && echo alive || echo dead",
            check_ec=False,
        )
        if "dead" in (out or ""):
            return
        time.sleep(0.5)
    client.exec_command(sudo=True, cmd=f"kill -TERM {int(pid)}", check_ec=False)


def set_cephfs_byte_quota(client, directory, max_bytes, ctx=None):
    """Set a byte quota that applies to the NFS export."""
    from tests.nfs.lib.xcopy.helpers import cephfs_subvol_fuse_mount

    if ctx is None:
        raise RuntimeError(
            "set_cephfs_byte_quota requires ctx (NFS rejects ceph.quota.*)"
        )
    max_bytes = int(max_bytes)
    with cephfs_subvol_fuse_mount(client, ctx, fuse_mnt="/mnt/cephfs_quota_check") as (
        _fuse_mnt,
        subvol_path,
    ):
        announce(f"Set ceph.quota.max_bytes={max_bytes} via fuse at {subvol_path}")
        client.exec_command(
            sudo=True,
            cmd=(f"setfattr -n ceph.quota.max_bytes -v {max_bytes} " f"{subvol_path}"),
            timeout=60,
        )
        out, _ = client.exec_command(
            sudo=True,
            cmd=(
                f"getfattr --only-values -n ceph.quota.max_bytes "
                f"{subvol_path} 2>/dev/null || echo unset"
            ),
            check_ec=False,
        )
        log.info("Quota now: %s", (out or "").strip())


def clear_cephfs_byte_quota(client, directory, ctx=None):
    """Remove byte quota via fuse setfattr 0 on the export subvolume."""
    from tests.nfs.lib.xcopy.helpers import cephfs_subvol_fuse_mount

    if ctx is None:
        log.warning("clear_cephfs_byte_quota called without ctx; skipping")
        return
    try:
        with cephfs_subvol_fuse_mount(
            client, ctx, fuse_mnt="/mnt/cephfs_quota_check"
        ) as (_fuse_mnt, subvol_path):
            announce(f"Clear ceph.quota.max_bytes via fuse at {subvol_path}")
            client.exec_command(
                sudo=True,
                cmd=f"setfattr -n ceph.quota.max_bytes -v 0 {subvol_path}",
                check_ec=False,
                timeout=60,
            )
    except RuntimeError as exc:
        log.warning("Skip quota clear (no fuse/getpath): %s", exc)


def dir_used_bytes(client, directory):
    """Best-effort used bytes under directory (``du -sb``)."""
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"du -sb {directory} 2>/dev/null | awk '{{print $1}}'",
        check_ec=False,
    )
    try:
        return int((out or "0").strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 0


def sha256_file(client, path):
    """Return sha256 hex digest or empty string if missing."""
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"sha256sum {path} 2>/dev/null | awk '{{print $1}}'",
        check_ec=False,
    )
    return (out or "").strip().split()[0] if (out or "").strip() else ""


def cp_stderr_suggests_nospc(text):
    """True if cp/NFS error text looks like ENOSPC / quota / no space."""
    t = (text or "").lower()
    needles = (
        "no space",
        "enospc",
        "edquot",
        "disk quota",
        "quota exceeded",
        "nospc",
    )
    return any(n in t for n in needles)


def probe_open_share_once(
    client,
    server,
    port,
    export,
    filename,
    access="w",
    deny="n",
    script="/tmp/open_share.py",
    tag="probe",
):
    """Run ``open_share.py --once`` and return outcome string."""
    from tests.nfs.lib.tsm.share_open import RESULT_MAP

    outf = f"/tmp/xcopy_share_probe_{tag}.out"
    announce(
        f"Probe OPEN once access={access} deny={deny} "
        f"file={filename} on {client.hostname}"
    )
    client.exec_command(sudo=True, cmd=f"rm -f {outf}", check_ec=False)
    client.exec_command(
        sudo=True,
        cmd=(
            f"python3 {script} --server {server} --port {int(port)} "
            f"--export {export} --file {filename} "
            f"--access {access} --deny {deny} --once "
            f"> {outf} 2>&1"
        ),
        check_ec=False,
        timeout=120,
    )
    text, _ = client.exec_command(
        sudo=True, cmd=f"cat {outf} 2>/dev/null", check_ec=False
    )
    text = text or ""
    log.info("open_share --once output:\n%s", text.strip() or "<empty>")
    outcome = "error"
    for ln in text.splitlines():
        if ln.startswith("RESULT="):
            outcome = RESULT_MAP.get(ln.split("=", 1)[1].strip(), "error")
    log.info("open_share --once outcome=%s", outcome)
    return outcome, text


def timed_cp_capture(client, src, dst, timeout=600, remove_dst=True):
    """Run ``cp src dst``; return (rc, stderr_text, elapsed_sec)."""
    announce(f"cp {src} -> {dst} (capture stderr, remove_dst={remove_dst})")
    errf = f"/tmp/xcopy_cp_err_{int(time.time())}.txt"
    if remove_dst:
        client.exec_command(sudo=True, cmd=f"rm -f {dst} {errf}", check_ec=False)
    else:
        client.exec_command(sudo=True, cmd=f"rm -f {errf}", check_ec=False)
        # Ensure destination exists — overwrite path required for share_deny.
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"test -e {dst} && echo exists || echo missing",
            check_ec=False,
        )
        if "exists" not in (out or ""):
            raise RuntimeError(
                f"timed_cp_capture remove_dst=False but dst missing: {dst}"
            )
    out, _ = client.exec_command(
        sudo=True,
        cmd=(
            f"start=$(date +%s.%N); "
            f"cp {src} {dst} 2>{errf}; rc=$?; "
            f"end=$(date +%s.%N); "
            f"echo RC=$rc; "
            f'awk -v s="$start" -v e="$end" '
            f"'BEGIN {{ printf \"ELAPSED=%.3f\\n\", e - s }}'; "
            f"echo '---stderr---'; cat {errf} 2>/dev/null || true"
        ),
        check_ec=False,
        timeout=timeout,
    )
    text = out or ""
    rc = 1
    elapsed = 0.0
    for line in text.splitlines():
        if line.startswith("RC="):
            try:
                rc = int(line.split("=", 1)[1].strip())
            except ValueError:
                rc = 1
        if line.startswith("ELAPSED="):
            try:
                elapsed = float(line.split("=", 1)[1].strip())
            except ValueError:
                elapsed = 0.0
    stderr = ""
    if "---stderr---" in text:
        stderr = text.split("---stderr---", 1)[1].strip()
    log.info(
        "cp finished: rc=%s elapsed=%.3fs stderr=%r dst_size=%s",
        rc,
        elapsed,
        stderr[:200],
        get_file_size_bytes(client, dst),
    )
    return rc, stderr, elapsed
