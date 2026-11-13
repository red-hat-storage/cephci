"""Helpers for ASYNC COPY / OFFLOAD_STATUS / OFFLOAD_CANCEL scenarios."""

import re
import time

from tests.nfs.lib.xcopy.helpers import (
    announce,
    collect_nfs_logs,
    get_file_size_bytes,
    start_xcopy_pcap_capture,
    stop_xcopy_pcap_capture,
)
from utility.log import Log

log = Log(__name__)

# Linux nfs4.h / tshark -e nfs.opcode (NFSv4.2):
#   OP_COPY=60, OP_OFFLOAD_CANCEL=66, OP_OFFLOAD_STATUS=67
# (Not 75/77 — those are wrong and miss real cancel/status in pcaps.)
NFS_OPCODE_OFFLOAD_CANCEL = "66"
NFS_OPCODE_OFFLOAD_STATUS = "67"

# Ganesha formats vary: "req=/got=" (short) or "src_off=/len=/progress=" (full).
RE_ASYNC_CHUNK = re.compile(
    r"COPY ASYNC (?:short )?chunk #(\d+):\s*"
    r"(?:req=(\d+)\s+got=(\d+)|src_off=(\d+)\s+len=(\d+)\s+progress=(\d+)/(\d+))",
    re.I,
)
RE_CANCEL = re.compile(
    r"OFFLOAD_CANCEL|offload.?cancel|copy.?cancel|CANCEL",
    re.I,
)
RE_WORKER_STOP = re.compile(
    r"stopping|worker.*stop|copy_do_work.*done|async.*complete|CANCELLED|canceled",
    re.I,
)


def create_large_async_file(client, path, size_gb):
    """Create a large file for ASYNC COPY (fast path)."""
    size_gb = float(size_gb)
    nbytes = int(size_gb * 1024 * 1024 * 1024)
    announce(
        f"Create large ASYNC source {path} ({size_gb} GiB / {nbytes} bytes) "
        f"on {client.hostname}"
    )
    client.exec_command(sudo=True, cmd=f"mkdir -p $(dirname {path})")
    client.exec_command(sudo=True, cmd=f"rm -f {path}")
    client.exec_command(
        sudo=True,
        cmd=f"fallocate -l {nbytes} {path}",
        check_ec=False,
        timeout=600,
    )
    if client.exit_status != 0:
        log.warning("fallocate failed; using truncate -s %s", nbytes)
        client.exec_command(sudo=True, cmd=f"truncate -s {nbytes} {path}")
        # Touch one block so some servers materialize the inode cleanly.
        client.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/zero of={path} bs=1M count=1 " f"conv=notrunc status=none"
            ),
            timeout=120,
        )
    sz = get_file_size_bytes(client, path)
    log.info("Large source ready: path=%s size=%s", path, sz)
    if sz < nbytes:
        raise RuntimeError(f"Large file size {sz} < expected {nbytes} for {path}")
    return sz


def poll_copy_progress(
    client,
    dst,
    total_bytes,
    poll_interval_sec=5,
    timeout_sec=3600,
    stop_when_ratio=None,
    max_stop_ratio=None,
    stop_on_first_progress=False,
):
    """Poll destination size until complete, timeout, or a mid-copy stop."""
    announce(
        f"Poll copy progress dst={dst} total={total_bytes} "
        f"interval={poll_interval_sec}s"
        + (f" stop_when_ratio={stop_when_ratio}" if stop_when_ratio is not None else "")
        + (f" max_stop_ratio={max_stop_ratio}" if max_stop_ratio is not None else "")
        + (" stop_on_first_progress=True" if stop_on_first_progress else "")
    )
    samples = []
    t0 = time.time()
    last = -1
    stagnant = 0
    max_bytes = (
        int(total_bytes * float(max_stop_ratio)) if max_stop_ratio is not None else None
    )

    def _missed_window(sz):
        return max_bytes is not None and sz >= max_bytes

    while True:
        elapsed = time.time() - t0
        sz = get_file_size_bytes(client, dst)
        samples.append((elapsed, sz))
        log.info(
            "progress t=%.1fs dst=%s / %s (%.2f%%)",
            elapsed,
            sz,
            total_bytes,
            (100.0 * sz / total_bytes) if total_bytes else 0.0,
        )
        # Cancel/disconnect: act on first mid-copy size sample before the
        # next SSH poll can jump past max_stop_ratio (seen: 28% → 90%).
        if stop_on_first_progress and sz > 0:
            if _missed_window(sz):
                log.warning(
                    "first progress already >= max_stop_ratio=%s "
                    "(dst=%s / %s) — cancel/disconnect window missed",
                    max_stop_ratio,
                    sz,
                    total_bytes,
                )
            else:
                log.info(
                    "First mid-copy progress (dst=%s); stopping for interrupt",
                    sz,
                )
            return samples, sz
        if stop_when_ratio is not None and sz >= int(
            total_bytes * float(stop_when_ratio)
        ):
            if _missed_window(sz):
                log.warning(
                    "stop_when_ratio=%s hit but dst already >= max_stop_ratio=%s "
                    "(dst=%s / %s) — cancel/disconnect window likely missed",
                    stop_when_ratio,
                    max_stop_ratio,
                    sz,
                    total_bytes,
                )
            else:
                log.info("Reached stop_when_ratio=%s (dst=%s)", stop_when_ratio, sz)
            return samples, sz
        if sz >= total_bytes:
            return samples, sz
        if sz == last:
            stagnant += 1
        else:
            stagnant = 0
            last = sz
        if elapsed >= float(timeout_sec):
            log.warning("Progress poll timed out after %.1fs", elapsed)
            return samples, sz
        # Allow some stagnant polls early (ASYNC may buffer) but not forever.
        if stagnant > 60 and sz > 0:
            log.warning("Progress stagnant for %s polls at size=%s", stagnant, sz)
            return samples, sz
        time.sleep(float(poll_interval_sec))


def signal_process(client, pid, sig="INT", escalate=True, tag=None):
    """Interrupt a ``start_background_cp`` session (default SIGINT ≈ Ctrl+C)."""
    pid = int(pid)
    announce(f"Signal {sig} -> process group -{pid}, pid {pid}, and cp children")
    cp_pid_file = f"/tmp/{tag}.cp_pid" if tag else ""
    esc = "1" if escalate else "0"
    # Heredoc keeps quoting simple across SSH/sudo.
    cmd = f"""cat > /tmp/xcopy_signal.sh <<'EOS'
#!/bin/bash
pid="$1"
sig="$2"
cp_pid_file="$3"
escalate="$4"
children=$(pgrep -P "$pid" 2>/dev/null || true)
if [ -n "$cp_pid_file" ] && [ -f "$cp_pid_file" ]; then
  cpf=$(cat "$cp_pid_file" 2>/dev/null || true)
  case "$cpf" in
    (*[!0-9]*|"") ;;
    (*) children="$children $cpf" ;;
  esac
fi
echo "BEFORE:"
ps -o pid=,pgid=,stat=,comm=,args= -p "$pid" 2>/dev/null || true
for c in $children; do
  ps -o pid=,pgid=,stat=,comm=,args= -p "$c" 2>/dev/null || true
done
kill_tree() {{
  local s="$1"
  kill -s "$s" -- -"$pid" 2>/dev/null || kill -s "$s" -"$pid" 2>/dev/null || true
  kill -s "$s" -- "$pid" 2>/dev/null || kill -s "$s" "$pid" 2>/dev/null || true
  for c in $children; do
    kill -s "$s" -- "$c" 2>/dev/null || kill -s "$s" "$c" 2>/dev/null || true
  done
}}
kill_tree "$sig"
# Give NFS client time to turn SIGINT into OFFLOAD_CANCEL before escalating.
sleep 1
refresh_alive() {{
  children=$(pgrep -P "$pid" 2>/dev/null || true; echo $children)
  alive_cp=
  for c in $children; do
    if kill -0 "$c" 2>/dev/null; then alive_cp="$alive_cp $c"; fi
  done
}}
refresh_alive
if [ "$escalate" = "1" ]; then
  if [ -n "$alive_cp" ] || kill -0 "$pid" 2>/dev/null; then
    echo "ESCALATE TERM"
    kill_tree TERM
    sleep 1
    refresh_alive
  fi
  if [ -n "$alive_cp" ] || kill -0 "$pid" 2>/dev/null; then
    echo "ESCALATE KILL"
    kill_tree KILL
    sleep 0.5
  fi
fi
echo "AFTER:"
if kill -0 "$pid" 2>/dev/null; then
  ps -o pid=,pgid=,stat=,comm=,args= -p "$pid" 2>/dev/null || true
  pgrep -a -P "$pid" 2>/dev/null || true
  echo "INTERRUPTED=0"
else
  echo "session gone"
  echo "INTERRUPTED=1"
fi
EOS
bash /tmp/xcopy_signal.sh {pid} {sig} '{cp_pid_file}' {esc}
"""
    out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    text = (out or "").strip()
    log.info("Signal %s result:\n%s", sig, text or "<empty>")
    interrupted = "INTERRUPTED=1" in text
    if not interrupted:
        log.warning(
            "Session/cp still alive after %s (escalate=%s) — cancel may be ineffective",
            sig,
            escalate,
        )
    return interrupted


def wait_size_stable(client, path, stable_sec=15, poll=2, timeout=180):
    """Wait until file size stops changing for stable_sec."""
    announce(f"Wait for size stable on {path} ({stable_sec}s)")
    t0 = time.time()
    last = get_file_size_bytes(client, path)
    stable_t0 = time.time()
    while time.time() - t0 < float(timeout):
        time.sleep(float(poll))
        cur = get_file_size_bytes(client, path)
        if cur != last:
            last = cur
            stable_t0 = time.time()
            log.info("size changed -> %s; reset stable timer", cur)
            continue
        if time.time() - stable_t0 >= float(stable_sec):
            log.info("Size stable at %s for >=%ss", cur, stable_sec)
            return cur
    log.warning("Size not fully stable; last=%s", last)
    return last


def _ensure_iptables(client):
    """Install iptables on client if missing. Raise RuntimeError if unavailable."""
    out, _ = client.exec_command(sudo=True, cmd="command -v iptables", check_ec=False)
    if (out or "").strip():
        return
    log.info("iptables missing on %s; installing", client.hostname)
    client.exec_command(
        sudo=True,
        cmd=(
            "timeout 120 dnf install -y iptables >/dev/null 2>&1 || "
            "timeout 120 yum install -y iptables >/dev/null 2>&1"
        ),
        check_ec=False,
        timeout=180,
    )
    out, _ = client.exec_command(sudo=True, cmd="command -v iptables", check_ec=False)
    if not (out or "").strip():
        raise RuntimeError(
            f"iptables not available on {client.hostname} after install attempt"
        )


def block_nfs_to_server(client, server_ip, port="2049"):
    """DROP client→server NFS TCP (OUTPUT)."""
    announce(f"Block NFS to {server_ip}:{port} on {client.hostname}")
    _ensure_iptables(client)
    rule = f"OUTPUT -d {server_ip} -p tcp --dport {port} -j DROP"
    client.exec_command(sudo=True, cmd=f"iptables -I {rule}", check_ec=False)
    rc = client.exit_status
    log.info("iptables -I %s (rc=%s)", rule, rc)
    # Confirm the rule is actually present (rc=0 alone is not enough on some hosts).
    client.exec_command(sudo=True, cmd=f"iptables -C {rule}", check_ec=False)
    if rc != 0 or client.exit_status != 0:
        raise RuntimeError(
            f"ASYNC-03 harness: failed to insert iptables DROP "
            f"({rule}) insert_rc={rc} check_rc={client.exit_status}"
        )
    return rule


def unblock_nfs_rule(client, rule):
    """Remove iptables rule previously inserted by block_nfs_to_server."""
    announce(f"Unblock NFS: iptables -D {rule}")
    client.exec_command(sudo=True, cmd=f"iptables -D {rule}", check_ec=False)
    # Best-effort flush of any duplicate DROP rules to same dport.
    log.info("iptables -D rc=%s", client.exit_status)


def count_nfs_opcodes(client, pcap_path, opcodes):
    """Return {opcode: count} from tshark nfs.opcode fields (compound-aware)."""
    from tests.nfs.lib.xcopy.helpers import _parse_opcode_histogram

    out, _ = client.exec_command(
        sudo=True,
        cmd=(
            f"tshark -r {pcap_path} -Y nfs -T fields -e nfs.opcode "
            f"2>/dev/null | tr ',' '\\n' | sort | uniq -c"
        ),
        check_ec=False,
        timeout=300,
    )
    hist = _parse_opcode_histogram((out or "").strip())
    counts = {str(o): int(hist.get(str(o), 0)) for o in opcodes}
    log.info("Resolved opcode counts: %s", counts)
    return counts


def collect_async_log_window(ctx):
    """Return grepped podman NFS logs since ctx log mark (downstream/cephadm)."""
    return collect_nfs_logs(
        ctx["nfs_nodes_for_logs"],
        nfs_name=ctx["nfs_name"],
        since=ctx.get("log_since"),
        client=ctx.get("client"),
    )


def assert_async_progress_in_logs(log_text):
    """Require ASYNC worker activity (START and/or chunk lines)."""
    if not log_text:
        log.warning("Empty log window; cannot assert ASYNC progress in logs")
        return False
    has_start = bool(re.search(r"COPY ASYNC worker START", log_text, re.I))
    chunks = list(RE_ASYNC_CHUNK.finditer(log_text))
    log.info(
        "ASYNC log evidence: worker_START=%s chunk_lines=%s",
        has_start,
        len(chunks),
    )
    for m in chunks[:8]:
        log.info("  chunk match: %s", m.group(0)[:120])
    return has_start or bool(chunks)


def assert_cancel_evidence(log_text, opcode_counts=None):
    """Look for cancel evidence in Ganesha logs and/or pcap OFFLOAD_CANCEL."""
    log_hit = bool(log_text and RE_CANCEL.search(log_text))
    op_hit = False
    if opcode_counts:
        op_hit = int(opcode_counts.get(NFS_OPCODE_OFFLOAD_CANCEL, 0)) > 0
    log.info(
        "Cancel evidence: ganesha_log=%s OFFLOAD_CANCEL(%s)=%s",
        log_hit,
        NFS_OPCODE_OFFLOAD_CANCEL,
        op_hit,
    )
    if log_text:
        for m in RE_WORKER_STOP.finditer(log_text):
            log.info("worker-stop-ish log: %s", m.group(0)[:120])
            break
    return log_hit or op_hit


def start_pcap_if_enabled(ctx):
    """Start tcpdump when ctx['pcap_validate'] is true; return pid or None."""
    if not ctx.get("pcap_validate", True):
        return None
    return start_xcopy_pcap_capture(
        ctx["client"],
        ctx["server_ip"],
        port=ctx["port"],
        pcap_path=ctx["pcap_path"],
    )


def stop_pcap_if_started(ctx, pid):
    if pid is None:
        return
    stop_xcopy_pcap_capture(ctx["client"], ctx["pcap_path"], pid)
