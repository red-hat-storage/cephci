"""NFS TSM grace-period IO conflict workflows.

Grace hold: spare open + iptables -I on spare client + spare NFS restart
→ ~90s cluster grace → C1/C2 conflict checks → unblock → after-grace TSM.

C01..C03: C1 WRITE_ONLY; C2 RO/WO/RW blocked then completes after grace.
C04..C06: C1 READ_ONLY; C2 RO succeeds during grace; C2 WO/RW blocked then
          completes after grace.
C07..C09: C1 READ_WRITE; C2 RO/WO/RW blocked then completes after grace.
C10: C1 WO+nwlock; C2 RW blocked at open → after grace open ok; nrlock+nwlock denied.
C11: C1 RO+nrlock; C2 RO+nrlock both succeed during grace (compatible).
C12: C1 RO+nrlock; C2 RW blocked → after grace open ok; nwlock denied then nrlock ok.
C13: deleg export type5; C1 WO+nwlock; C2 RW blocked → recall; nrlock+nwlock denied.
C14: deleg export type4; C1 RO+nrlock; C2 RW blocked → nwlock denied then nrlock ok.
C15: deleg export type4; C1 RO+nrlock; C2 RO+nrlock both succeed (deleg 1→2).
C16: pre-grace WO+nwlock on wf0; during grace C1 WO + C2 RO conflict on wf1; after open ok.
C17: pre-grace WO+nwlock on wf0; during grace C1 RO + C2 RO succeed on wf1 (compatible).
C18: pre-grace WO+nwlock on wf0; during grace C1 WO + C2 RW blocked on wf1;
     after open ok; nrlock+nwlock denied (pre-grace lock preserved).
C19: pre-grace RO+nrlock on wf0; during grace C1 WO + C2 RW blocked on wf1;
     after open ok; nwlock denied then nrlock success.
C20: pre-grace WO+nwlock on wf1; during grace C2 RO blocked on same file;
     after open ok; nrlock+nwlock denied (reclaim vs pre-grace open).
C21: deleg export; pre-grace WO+nwlock on wf0; during C1 WO+nwlock + C2 RW on wf1;
     after recall; nrlock+nwlock denied (sticky deleg preserved).
C22: pre-grace C1 WO+nwlock on wf0 + C2 RO+nrlock on wf2; during C1 WO + C2 RO
     blocked on wf1; after open ok (multi-client floor preserved).
R01–R06 chain: keep fds across primary restarts; cumulative reclaim then
  grace-window compatible + conflicting opens + write-deleg conflict.
  R01–R03: plant + primary restart only (no iptables); reclaim exact TSM.
  R04/R05: restart primary before each access op (spare+iptables on first).
  R04: during grace C2 RO on wf0 (compatible) → open=4 lock=3
  R05: restart again; C2 RW on wf1 blocked → after grace open=5
  R06: deleg export; plant WO+nwlock wf3 → restart → conflict RW blocked →
       after recall open=7 lock=4 deleg=0; nrlock+nwlock denied
"""

import re
import time

from tests.nfs.lib.common_lib import get_nfs_container_id, get_node_time
from tests.nfs.lib.tsm.grace_hold import (
    GraceCtx,
    enter_grace,
    grace_has_exited,
    hold_cluster_grace,
    release_cluster_grace,
    unblock_cluster_grace,
    wait_grace_exit,
)
from tests.nfs.lib.tsm.helpers import (
    announce,
    check_coredumps,
    cleanup_grace_suite,
    close_interactive,
    ensure_deleg_export,
    file_base,
    fmt_tsm,
    log_workflow_summary,
)
from tests.nfs.lib.tsm.helpers import mount_clients as _mount_clients
from tests.nfs.lib.tsm.helpers import (
    mount_spare_export,
    peer_summary,
    require_tsm_node_ids,
    seed_wf_files,
    start_spare_hold,
    wait_peer_counts,
    workflow_peers,
)
from tests.nfs.lib.tsm.interactive import InteractiveSession, ensure_interactive_binary
from tests.nfs.tsm.test_nfs_tsm_basic import deploy_step, safe_cleanup
from utility.log import Log

# Re-export for integration (TC-07) and external importers.
__all__ = ["GraceCtx", "enter_grace", "run"]

log = Log(__name__)

TSM_PORT = 36369
DEFAULT_NFS_COUNT = 4
MIN_CLIENTS = 3  # C1 + C2 + spare hold client
SPARE_FILE_IDX = 0  # spare mount: spare0.txt
PRE_FILE_IDX = 0  # primary wf mount: wf0.txt (pre-grace sticky records)
WF_FILE_IDX = 1  # primary wf mount: wf1.txt (grace conflict)
PRE_C2_FILE_IDX = 2  # C22: C2 pre-grace sticky on wf2.txt
DELEG_FILE_IDX = 3  # R06: write-deleg plant/conflict on wf3.txt

WO, RO, RW = "WRITE_ONLY", "READ_ONLY", "READ_WRITE"
SHARE_ACCESS = {RO: 1, WO: 2, RW: 3}

CONFLICT_SA_RE = re.compile(r"Open conflict existing_sa=(\d+) new_sa=(\d+)", re.I)
CONFLICT_GRACE_RE = re.compile(r"Conflicting open\.\s*Return NFS4ERR_GRACE", re.I)

# c2_expect: "blocked" → hang + after-grace complete; "success" → ok during grace.
WORKFLOWS = {
    "TC-TSM-G-C01": {
        "desc": "C1 WRITE_ONLY; C2 READ_ONLY blocked then completes after grace",
        "c1_mode": WO,
        "c2_mode": RO,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C02": {
        "desc": "C1 WRITE_ONLY; C2 WRITE_ONLY blocked then completes after grace",
        "c1_mode": WO,
        "c2_mode": WO,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C03": {
        "desc": "C1 WRITE_ONLY; C2 READ_WRITE blocked then completes after grace",
        "c1_mode": WO,
        "c2_mode": RW,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C04": {
        "desc": "C1 READ_ONLY; C2 READ_ONLY succeeds during grace (compatible)",
        "c1_mode": RO,
        "c2_mode": RO,
        "c2_expect": "success",
    },
    "TC-TSM-G-C05": {
        "desc": "C1 READ_ONLY; C2 WRITE_ONLY blocked then completes after grace",
        "c1_mode": RO,
        "c2_mode": WO,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C06": {
        "desc": "C1 READ_ONLY; C2 READ_WRITE blocked then completes after grace",
        "c1_mode": RO,
        "c2_mode": RW,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C07": {
        "desc": "C1 READ_WRITE; C2 READ_ONLY blocked then completes after grace",
        "c1_mode": RW,
        "c2_mode": RO,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C08": {
        "desc": "C1 READ_WRITE; C2 WRITE_ONLY blocked then completes after grace",
        "c1_mode": RW,
        "c2_mode": WO,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C09": {
        "desc": "C1 READ_WRITE; C2 READ_WRITE blocked then completes after grace",
        "c1_mode": RW,
        "c2_mode": RW,
        "c2_expect": "blocked",
    },
    # C1 WO + nwlock; C2 RW open blocked → after grace open ok; nrlock then nwlock denied
    "TC-TSM-G-C10": {
        "desc": (
            "C1 WO+nwlock; C2 RW blocked at open, after grace open ok; "
            "nrlock then nwlock denied"
        ),
        "c1_mode": WO,
        "c1_lock": "nwlock",
        "c2_mode": RW,
        "c2_expect": "blocked",
        "c2_locks": [("nrlock", "denied"), ("nwlock", "denied")],
    },
    # C1 RO + nrlock; C2 RO + nrlock both success during grace
    "TC-TSM-G-C11": {
        "desc": "C1 RO+nrlock; C2 RO+nrlock both succeed during grace (compatible)",
        "c1_mode": RO,
        "c1_lock": "nrlock",
        "c2_mode": RO,
        "c2_expect": "success",
        "c2_lock_during": "nrlock",
    },
    # C1 RO + nrlock; C2 RW blocked → after grace open; nwlock denied, nrlock success
    "TC-TSM-G-C12": {
        "desc": (
            "C1 RO+nrlock; C2 RW blocked at open, after grace open ok; "
            "nwlock denied then nrlock success"
        ),
        "c1_mode": RO,
        "c1_lock": "nrlock",
        "c2_mode": RW,
        "c2_expect": "blocked",
        "c2_locks": [("nwlock", "denied"), ("nrlock", "success")],
    },
    # --- Deleg export (delegations=rw): C13 write-deleg, C14/C15 read-deleg ---
    "TC-TSM-G-C13": {
        "desc": (
            "deleg type5: C1 WO+nwlock; C2 RW blocked; after grace recall; "
            "nrlock then nwlock denied"
        ),
        "use_deleg_export": True,
        "c1_mode": WO,
        "c1_lock": "nwlock",
        "c2_mode": RW,
        "c2_expect": "blocked",
        # After C2 open: recall → C1's client-local lock lands in TSM
        "after_open_lock_delta": 1,
        "after_open_deleg_delta": -1,  # recall 1→0
        "c2_locks": [("nrlock", "denied"), ("nwlock", "denied")],
    },
    "TC-TSM-G-C14": {
        "desc": (
            "deleg type4: C1 RO+nrlock; C2 RW blocked; after grace open ok; "
            "nwlock denied then nrlock success"
        ),
        "use_deleg_export": True,
        "c1_mode": RO,
        "c1_lock": "nrlock",
        "c2_mode": RW,
        "c2_expect": "blocked",
        # After C2 open: recall → C1's client-local lock lands in TSM
        "after_open_lock_delta": 1,
        "after_open_deleg_delta": -1,  # recall 1→0
        "c2_locks": [("nwlock", "denied"), ("nrlock", "success")],
    },
    "TC-TSM-G-C15": {
        "desc": (
            "deleg type4: C1 RO+nrlock; C2 RO+nrlock both succeed during grace "
            "(deleg 1→2)"
        ),
        "use_deleg_export": True,
        "c1_mode": RO,
        "c1_lock": "nrlock",
        "c2_mode": RO,
        "c2_expect": "success",
        "c2_lock_during": "nrlock",
    },
    # Pre-grace sticky records on wf0; C01-style conflict on wf1 during/after grace
    "TC-TSM-G-C16": {
        "desc": (
            "pre-grace C1 WO+nwlock on wf0; during grace C1 WO + C2 RO blocked on wf1; "
            "after grace open ok; pre-grace lock preserved"
        ),
        "pre_grace": {
            "c1_mode": WO,
            "c1_lock": "nwlock",
            "file_idx": PRE_FILE_IDX,
        },
        "c1_mode": WO,
        "c2_mode": RO,
        "c2_expect": "blocked",
    },
    "TC-TSM-G-C17": {
        "desc": (
            "pre-grace C1 WO+nwlock on wf0; during grace C1 RO + C2 RO succeed on wf1; "
            "pre-grace lock preserved"
        ),
        "pre_grace": {
            "c1_mode": WO,
            "c1_lock": "nwlock",
            "file_idx": PRE_FILE_IDX,
        },
        "c1_mode": RO,
        "c2_mode": RO,
        "c2_expect": "success",
    },
    "TC-TSM-G-C18": {
        "desc": (
            "pre-grace C1 WO+nwlock on wf0; during grace C1 WO + C2 RW blocked on wf1; "
            "after grace open ok; nrlock then nwlock denied; pre-grace lock preserved"
        ),
        "pre_grace": {
            "c1_mode": WO,
            "c1_lock": "nwlock",
            "file_idx": PRE_FILE_IDX,
        },
        "c1_mode": WO,
        "c2_mode": RW,
        "c2_expect": "blocked",
        "c2_locks": [("nrlock", "denied"), ("nwlock", "denied")],
    },
    "TC-TSM-G-C19": {
        "desc": (
            "pre-grace C1 RO+nrlock on wf0; during grace C1 WO + C2 RW blocked on wf1; "
            "after grace open ok; nwlock denied then nrlock success"
        ),
        "pre_grace": {
            "c1_mode": RO,
            "c1_lock": "nrlock",
            "file_idx": PRE_FILE_IDX,
        },
        "c1_mode": WO,
        "c2_mode": RW,
        "c2_expect": "blocked",
        "c2_locks": [("nwlock", "denied"), ("nrlock", "success")],
    },
    "TC-TSM-G-C20": {
        "desc": (
            "pre-grace C1 WO+nwlock on wf1; during grace C2 RO blocked on same file; "
            "after grace open ok; nrlock then nwlock denied"
        ),
        "pre_grace": {
            "c1_mode": WO,
            "c1_lock": "nwlock",
            "file_idx": WF_FILE_IDX,
        },
        "skip_c1_during": True,  # C1 already holds wf1 from pre-grace
        "c1_mode": WO,  # existing_sa for conflict logs
        "c2_mode": RO,
        "c2_expect": "blocked",
        "c2_locks": [("nrlock", "denied"), ("nwlock", "denied")],
    },
    "TC-TSM-G-C21": {
        "desc": (
            "deleg: pre-grace C1 WO+nwlock on wf0; during grace C1 WO+nwlock + C2 RW "
            "blocked on wf1; after recall; nrlock then nwlock denied"
        ),
        "use_deleg_export": True,
        "pre_grace": {
            "c1_mode": WO,
            "c1_lock": "nwlock",
            "file_idx": PRE_FILE_IDX,
        },
        "c1_mode": WO,
        "c1_lock": "nwlock",
        "c2_mode": RW,
        "c2_expect": "blocked",
        "after_open_lock_delta": 1,
        "after_open_deleg_delta": -1,
        "c2_locks": [("nrlock", "denied"), ("nwlock", "denied")],
    },
    "TC-TSM-G-C22": {
        "desc": (
            "pre-grace C1 WO+nwlock on wf0 + C2 RO+nrlock on wf2; during grace "
            "C1 WO + C2 RO blocked on wf1; after grace open ok; multi-client floor"
        ),
        "pre_grace": {
            "c1_mode": WO,
            "c1_lock": "nwlock",
            "file_idx": PRE_FILE_IDX,
        },
        "pre_grace_c2": {
            "c2_mode": RO,
            "c2_lock": "nrlock",
            "file_idx": PRE_C2_FILE_IDX,
        },
        "c1_mode": WO,
        "c2_mode": RO,
        "c2_expect": "blocked",
    },
    # Primary restart chain: R01–R03 plant; R04/R05 access; R06 write-deleg
    "TC-TSM-G-R01": {
        "desc": (
            "primary restart chain R01–R05 sticky reclaim + grace opens, "
            "then R06 write-deleg conflict/recall; keep fds; exact TSM"
        ),
        "runner": "recover_primary",
        "phases": [
            {
                "name": "R01",
                "who": "c1",
                "mode": WO,
                "lock": "nwlock",
                "file_idx": WF_FILE_IDX,
                "want_open": 1,
                "want_lock": 1,
            },
            {
                "name": "R02",
                "who": "c1",
                "mode": RO,
                "lock": "nrlock",
                "file_idx": PRE_FILE_IDX,
                "want_open": 2,
                "want_lock": 2,
            },
            {
                "name": "R03",
                "who": "c2",
                "mode": RO,
                "lock": "nrlock",
                "file_idx": PRE_C2_FILE_IDX,
                "want_open": 3,
                "want_lock": 3,
            },
        ],
        # Restart primary before each of R04/R05; R06 plants then restarts
        "access_phases": [
            {
                "name": "R04",
                "kind": "compatible",
                "who": "c2",
                "mode": RO,
                "file_idx": PRE_FILE_IDX,  # C1 already RO here
                "want_open": 4,
                "want_lock": 3,
            },
            {
                "name": "R05",
                "kind": "conflict",
                "who": "c2",
                "mode": RW,
                "file_idx": WF_FILE_IDX,  # C1 WO+nwlock
                "existing_mode": WO,
                "want_open_blocked": 4,  # no bump while blocked
                "want_open_after": 5,
                "want_lock": 3,
            },
            {
                "name": "R06",
                "kind": "deleg_conflict",
                "plant": {
                    "who": "c1",
                    "mode": WO,
                    "lock": "nwlock",
                    "file_idx": DELEG_FILE_IDX,
                    "want_open": 6,
                    "want_lock": 3,  # write-deleg: lock client-local until recall
                    "want_deleg": 1,
                },
                "who": "c2",
                "mode": RW,
                "file_idx": DELEG_FILE_IDX,
                "existing_mode": WO,
                "want_open_blocked": 6,
                "want_open_after": 7,
                "want_lock_after": 4,  # recall lands C1 lock in TSM
                "want_deleg_after": 0,
                "c2_locks": [("nrlock", "denied"), ("nwlock", "denied")],
            },
        ],
    },
}


def _file_base(mount, kind="wf"):
    return file_base(mount, kind)


def _seed_wf_file(client, mount, indices=None):
    """Create wf{i}.txt for each index (needed for O_RDONLY/O_WRONLY/O_RDWR)."""
    seed_wf_files(client, mount, indices if indices is not None else [WF_FILE_IDX])


def _workflow_peers(ctx):
    """Peers for TSM summary (exclude spare under partition)."""
    return workflow_peers(ctx)


def _close_and_stop(sess, indexes=None, unlock_indexes=None):
    """Close held indexes (optional unlock first), then stop the session."""
    close_interactive(sess, indexes=indexes, unlock_indexes=unlock_indexes)


def _backdate_since(since, skew_sec):
    """Return a copy of log-since stamps moved back by ``skew_sec`` seconds."""
    if not since or skew_sec <= 0:
        return since
    from datetime import datetime, timedelta

    delta = timedelta(seconds=skew_sec)

    def _one(ts):
        return (datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") - delta).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    if isinstance(since, dict):
        return {h: _one(ts) for h, ts in since.items()}
    return _one(since)


def _wait_tsm(
    nodes,
    ctx,
    since,
    want_open,
    want_lock,
    label,
    node_id,
    timeout=60,
    want_deleg=None,
    since_skew_sec=60,
):
    """Wait for exact TSM open/lock/(optional deleg) on node_id. 0 ok, 1 fail.

    ``since_skew_sec``: backdate ``since`` so host-clock ``date`` vs ganesha
    container log stamps (often a few seconds behind after restart) still match.
    """
    announce(
        f"[{label}] expect {fmt_tsm(want_open, want_lock, want_deleg)}"
        + (" (primary)" if nodes == [ctx.server] else "")
    )
    since = _backdate_since(since, since_skew_sec) if since_skew_sec else since
    if (
        wait_peer_counts(
            nodes,
            ctx.nfs_name,
            since,
            want_open,
            want_lock,
            label,
            expect_deleg=want_deleg,
            timeout=timeout,
            node_id=node_id,
        )
        is None
    ):
        return 1
    return 0


def _track_idx(lst, idx):
    if idx not in lst:
        lst.append(idx)


def _end_grace_hold(ctx, grace_since, tag, timeout=150):
    """Unblock spare iptables and wait NOT IN GRACE. 0 ok, 1 fail."""
    announce(f"[{tag}] unblock spare TCP (end grace hold)")
    unblock_cluster_grace(ctx)
    announce(f"[{tag}] wait NOT IN GRACE")
    if wait_grace_exit(ctx, grace_since, timeout=timeout):
        log.error("[%s] grace did not end in time", tag)
        return 1
    return 0


def _open_then_lock(sess, idx, mode, lock, label, close_list=None, unlock_list=None):
    """Open (+ optional lock). Track idxs on success. Return 0 ok, 1 fail."""
    if sess.open_file(idx, mode, timeout=30) != "success":
        log.error("[%s] open failed", label)
        return 1
    if close_list is not None:
        _track_idx(close_list, idx)
    if not lock:
        return 0
    announce(f"[{label}] {lock} expect=success")
    if sess.lock_file(idx, lock, timeout=15) != "success":
        log.error("[%s] %s failed", label, lock)
        return 1
    if unlock_list is not None:
        _track_idx(unlock_list, idx)
    return 0


def _hold_expect(ctx, baseline, label, since, use_deleg, locked):
    """TSM after hang open (+optional lock). 0 ok, 1 fail."""
    if use_deleg and locked:
        return _tsm_expect(
            ctx,
            baseline,
            label,
            since,
            True,
            open_delta=1,
            lock_delta=0,
            deleg_delta=1,
        )
    if locked:
        return _tsm_expect(
            ctx, baseline, label, since, True, open_delta=1, lock_delta=1
        )
    return _tsm_expect(ctx, baseline, label, since, True, open_delta=1)


def _accept_grace_exited_open(
    ctx,
    grace_since,
    label,
    reason,
    watch,
    open_since,
    want_open,
    want_lock,
    node_id,
    close_list,
    idx,
    want_deleg=None,
):
    """If primary already NOT IN GRACE, wait after-grace TSM. 0 ok, 1 fail."""
    if not grace_has_exited(ctx, grace_since):
        log.error("[%s] %s (still IN GRACE)", label, reason)
        return 1
    log.info("[%s] %s; primary NOT IN GRACE — accept", label, reason)
    _track_idx(close_list, idx)
    if _wait_tsm(
        watch,
        ctx,
        open_since,
        want_open,
        want_lock,
        f"{label} after-grace",
        node_id,
        want_deleg=want_deleg,
    ):
        return 1
    log.info("[%s] → open completed (grace already exited)", label)
    return 0


def _mount_spare(ctx, nfs_version):
    """Create spare export + mount once; reuse across TCs if still mounted."""
    return mount_spare_export(ctx, nfs_version)


def _start_spare_hold(ctx):
    """Open-hold spare0.txt via common_interactive (rwc)."""
    return start_spare_hold(ctx, file_idx=SPARE_FILE_IDX)


def _validate_conflict_logs(ctx, c1_mode, c2_mode, since, label):
    """Require Open conflict existing_sa/new_sa + NFS4ERR_GRACE on mount server."""
    existing_sa, new_sa = SHARE_ACCESS[c1_mode], SHARE_ACCESS[c2_mode]
    announce(
        f"[{label}] validate conflict existing_sa={existing_sa} "
        f"new_sa={new_sa} + NFS4ERR_GRACE"
    )
    node = ctx.server
    cid = get_nfs_container_id(node, nfs_name=ctx.nfs_name)
    if not cid:
        log.error("[%s] no NFS container on %s", label, node.hostname)
        return 1
    node_since = since.get(node.hostname) if isinstance(since, dict) else since
    since_arg = f' --since "{node_since}"' if node_since else ""
    out, _ = node.exec_command(
        sudo=True, cmd=f"podman logs{since_arg} {cid} 2>&1", check_ec=False
    )
    lines = (out or "").splitlines()
    for i, ln in enumerate(lines):
        m = CONFLICT_SA_RE.search(ln)
        if not m or int(m.group(1)) != existing_sa or int(m.group(2)) != new_sa:
            continue
        window = lines[i + 1 : i + 4]
        grace_ln = next((x for x in window if CONFLICT_GRACE_RE.search(x)), None)
        if grace_ln:
            log.info("[%s] conflict log ok:\n%s\n%s", label, ln, grace_ln)
            return 0
    log.error(
        "[%s] missing Open conflict existing_sa=%s new_sa=%s + NFS4ERR_GRACE",
        label,
        existing_sa,
        new_sa,
    )
    return 1


def _tsm_expect(
    ctx,
    baseline,
    label,
    since,
    must_match,
    open_delta=0,
    lock_delta=0,
    deleg_delta=None,
):
    """Check Node-id open/lock/(optional deleg) vs baseline+deltas.

    must_match=True  → wait until counts match
    must_match=False → fail if the bumped field(s) already match (blocked/denied)
    deleg_delta=None → do not assert deleg (C01–C12); int → assert base+delta
    """
    peers = _workflow_peers(ctx)
    node_id = ctx.server_node_id
    base_open = int((baseline or {}).get("open", 0))
    base_lock = int((baseline or {}).get("lock", 0))
    base_deleg = int((baseline or {}).get("deleg", 0))
    want_open = base_open + open_delta
    want_lock = base_lock + lock_delta
    want_deleg = None if deleg_delta is None else base_deleg + deleg_delta

    if must_match:
        announce(
            f"[{label}] validate TSM Node-id {node_id}",
            fmt_tsm(want_open, want_lock, want_deleg),
        )
        if (
            wait_peer_counts(
                peers,
                ctx.nfs_name,
                since,
                want_open,
                want_lock,
                label,
                expect_deleg=want_deleg,
                timeout=60,
                node_id=node_id,
            )
            is None
        ):
            return 1
        return 0

    announce(
        f"[{label}] TSM Node-id {node_id} must NOT match "
        f"{fmt_tsm(want_open if open_delta else base_open, want_lock if lock_delta else base_lock, want_deleg)}"
    )
    time.sleep(2)
    snap = peer_summary(peers, ctx.nfs_name, since=since, node_id=node_id)
    if not snap.get("seen"):
        log.info("[%s] no TSM summary yet — ok for blocked/denied", label)
        return 0
    if open_delta and int(snap.get("open", 0)) == want_open:
        log.error(
            "[%s] unexpected TSM open=%s on Node-id %s", label, want_open, node_id
        )
        return 1
    if lock_delta and int(snap.get("lock", 0)) == want_lock:
        log.error(
            "[%s] unexpected TSM lock=%s on Node-id %s", label, want_lock, node_id
        )
        return 1
    if deleg_delta is not None and int(snap.get("deleg", 0)) == want_deleg:
        log.error(
            "[%s] unexpected TSM deleg=%s on Node-id %s", label, want_deleg, node_id
        )
        return 1
    log.info(
        "[%s] no TSM bump match (open=%s lock=%s deleg=%s) — ok",
        label,
        snap.get("open"),
        snap.get("lock"),
        snap.get("deleg"),
    )
    return 0


def _normalize_c2_locks(spec):
    """Return [(kind, expect), ...] from c2_locks / legacy c2_lock + c2_lock_expect."""
    locks = spec.get("c2_locks")
    if locks:
        out = []
        default = spec.get("c2_lock_expect", "denied")
        for item in locks:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((item[0], item[1]))
            else:
                out.append((item, default))
        return out
    kind = spec.get("c2_lock")
    if kind:
        return [(kind, spec.get("c2_lock_expect", "denied"))]
    return []


def _run_c2_locks(ctx, c2_sess, peers, node_id, tag, locks, file_idx=WF_FILE_IDX):
    """Apply C2 lock steps after open is held. Return 0 ok, 1 fail."""
    for kind, expect in locks:
        label = f"{tag} C2 {kind} after open"
        announce(f"[{label}] expect={expect}")
        lock_since, _ = get_node_time([ctx.server] + peers)
        lock_base = peer_summary(peers, ctx.nfs_name, node_id=node_id)
        got = c2_sess.lock_file(file_idx, kind, timeout=15)
        if got != expect:
            log.error("[%s] want=%s got=%s", label, expect, got)
            return 1
        must = expect == "success"
        if _tsm_expect(ctx, lock_base, label, lock_since, must, lock_delta=1):
            return 1
        log.info(
            "[%s] → %s (TSM lock %s)",
            label,
            expect,
            "+1" if must else "not bumped",
        )
    return 0


def tc_conflict_generic(ctx, spec, nfs_version="4.2"):
    """One TC: optional pre-grace records; C1/C2 during grace; after-grace as needed."""
    tag = spec["id"]
    use_deleg = bool(spec.get("use_deleg_export"))
    pre_grace = spec.get("pre_grace")  # {c1_mode, c1_lock?, file_idx}
    pre_grace_c2 = spec.get("pre_grace_c2")  # {c2_mode, c2_lock?, file_idx}
    skip_c1_during = bool(spec.get("skip_c1_during"))
    c1_mode = spec.get("c1_mode", WO)
    c1_lock = spec.get("c1_lock")  # lock during grace (C10–C15); not pre-grace
    c2_mode = spec["c2_mode"]
    c2_expect = spec.get("c2_expect", "blocked")
    c2_lock_during = spec.get("c2_lock_during")
    c2_locks = _normalize_c2_locks(spec)
    after_open_deleg_delta = spec.get("after_open_deleg_delta")
    after_open_lock_delta = spec.get("after_open_lock_delta", 0)
    c1, c2 = ctx.client_a, ctx.client_b
    if not c2 or ctx.server_node_id is None:
        log.error("[%s] need C2 client and server_node_id", tag)
        return 1
    if ensure_interactive_binary(c1) or ensure_interactive_binary(c2):
        return 1

    if use_deleg:
        if ensure_deleg_export(ctx, nfs_version):
            return 1
        mount = ctx.deleg_mount
    else:
        mount = ctx.mount

    seed_idxs = [WF_FILE_IDX]
    pre_idx = None
    pre_lock = None
    pre_mode = None
    if pre_grace:
        pre_idx = int(pre_grace.get("file_idx", PRE_FILE_IDX))
        pre_lock = pre_grace.get("c1_lock")
        pre_mode = pre_grace.get("c1_mode", WO)
        if pre_idx not in seed_idxs:
            seed_idxs.append(pre_idx)
    pre_c2_idx = None
    pre_c2_lock = None
    pre_c2_mode = None
    if pre_grace_c2:
        pre_c2_idx = int(pre_grace_c2.get("file_idx", PRE_C2_FILE_IDX))
        pre_c2_lock = pre_grace_c2.get("c2_lock")
        pre_c2_mode = pre_grace_c2.get("c2_mode", RO)
        if pre_c2_idx not in seed_idxs:
            seed_idxs.append(pre_c2_idx)
    _seed_wf_file(c1, mount, indices=seed_idxs)
    time.sleep(3 if use_deleg else 1)
    if _mount_spare(ctx, nfs_version):
        return 1

    spare_sess = _start_spare_hold(ctx)
    if not spare_sess:
        return 1
    ctx._spare_session = spare_sess

    c1_sess = None
    c2_sess = None
    c2_held_lock = False
    c1_close_idxs = [WF_FILE_IDX]
    c1_unlock_idxs = []
    c2_close_idxs = [WF_FILE_IDX]
    c2_unlock_idxs = []
    grace_since = None
    # Share-access for conflict logs: pre-grace open when C1 skips during-grace open
    conflict_existing_mode = pre_mode if (skip_c1_during and pre_mode) else c1_mode
    try:
        peers = _workflow_peers(ctx)
        if not peers:
            log.error("[%s] need ≥1 peer for TSM summary", tag)
            return 1
        node_id = ctx.server_node_id
        base = _file_base(mount)

        # --- Optional pre-grace sticky records (C1) ---
        if pre_grace:
            c1_sess = InteractiveSession(c1, base, tag="c1")
            if not c1_sess.start():
                return 1
            if pre_idx not in c1_close_idxs:
                c1_close_idxs.insert(0, pre_idx)
            label = f"{tag} C1 pre-grace {pre_mode}" + (
                f"+{pre_lock}" if pre_lock else ""
            )
            announce(f"[{label}] open expect=success (before grace)")
            baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)
            open_since, _ = get_node_time(peers)
            if _open_then_lock(
                c1_sess, pre_idx, pre_mode, pre_lock, label, unlock_list=c1_unlock_idxs
            ):
                return 1
            if _hold_expect(
                ctx, baseline, label, open_since, use_deleg, bool(pre_lock)
            ):
                return 1
            log.info("[%s] → pre-grace records ok (held through grace)", label)

        # --- Optional pre-grace sticky records (C2 / multi-client) ---
        if pre_grace_c2:
            c2_sess = InteractiveSession(c2, base, tag="c2pre")
            if not c2_sess.start():
                return 1
            if pre_c2_idx not in c2_close_idxs:
                c2_close_idxs.insert(0, pre_c2_idx)
            label = f"{tag} C2 pre-grace {pre_c2_mode}" + (
                f"+{pre_c2_lock}" if pre_c2_lock else ""
            )
            announce(f"[{label}] open expect=success (before grace)")
            baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)
            open_since, _ = get_node_time(peers)
            if _open_then_lock(
                c2_sess,
                pre_c2_idx,
                pre_c2_mode,
                pre_c2_lock,
                label,
                unlock_list=c2_unlock_idxs,
            ):
                return 1
            if pre_c2_lock:
                c2_held_lock = True
            if _hold_expect(ctx, baseline, label, open_since, False, bool(pre_c2_lock)):
                return 1
            log.info("[%s] → C2 pre-grace records ok (held through grace)", label)

        announce(f"[{tag}] hold cluster grace (spare + iptables -I)")
        rc, grace_since = hold_cluster_grace(ctx, spare_sess)
        if rc:
            return 1

        if not c1_sess:
            c1_sess = InteractiveSession(c1, base, tag="c1")
            if not c1_sess.start():
                return 1

        # --- C1 during grace (skipped when pre-grace already holds conflict file) ---
        if not skip_c1_during:
            label = f"{tag} C1 during {c1_mode}" + (f"+{c1_lock}" if c1_lock else "")
            announce(f"[{label}] open expect=success")
            baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)
            open_since, _ = get_node_time(peers)
            if _open_then_lock(
                c1_sess,
                WF_FILE_IDX,
                c1_mode,
                c1_lock,
                label,
                unlock_list=c1_unlock_idxs,
            ):
                return 1
            if c1_lock:
                if _hold_expect(
                    ctx, baseline, label, open_since, use_deleg, locked=True
                ):
                    return 1
            else:
                # With pre-grace lock held: open+1, lock unchanged (floor preserved)
                if _tsm_expect(
                    ctx, baseline, label, open_since, True, open_delta=1, lock_delta=0
                ):
                    return 1
            log.info("[%s] → success (TSM Node-id %s ok)", label, node_id)
        else:
            announce(
                f"[{tag}] skip C1 during-grace open "
                f"(pre-grace holds index {pre_idx})"
            )

        # --- C2 during grace ---
        label = f"{tag} C2 during {c2_mode}"
        announce(f"[{label}] open expect={c2_expect}")
        open_since, _ = get_node_time([ctx.server] + peers)
        baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)
        if not c2_sess:
            c2_sess = InteractiveSession(
                c2, base, tag=f"probe{int(time.time()) % 100000}"
            )
            if not c2_sess.start():
                return 1
        timeout = 8 if c2_expect == "blocked" else 30
        got = c2_sess.open_file(WF_FILE_IDX, c2_mode, timeout=timeout)
        if got != c2_expect:
            log.error("[%s] open want=%s got=%s", label, c2_expect, got)
            return 1

        if c2_expect == "blocked":
            if _validate_conflict_logs(
                ctx, conflict_existing_mode, c2_mode, open_since, label
            ):
                return 1
            if _tsm_expect(ctx, baseline, label, open_since, False, open_delta=1):
                return 1
            # Pre-grace floor still present
            if pre_grace or pre_grace_c2:
                if use_deleg:
                    if int(baseline.get("open", 0)) < 1:
                        log.error(
                            "[%s] pre-grace open missing during blocked open", label
                        )
                        return 1
                    if int(baseline.get("deleg", 0)) < 1:
                        log.error(
                            "[%s] pre-grace deleg missing during blocked open", label
                        )
                        return 1
                else:
                    min_open = 2 if pre_grace_c2 else 1
                    if int(baseline.get("open", 0)) < min_open:
                        log.error(
                            "[%s] pre-grace open floor missing " "(want≥%s got=%s)",
                            label,
                            min_open,
                            baseline.get("open"),
                        )
                        return 1
                    if int(baseline.get("lock", 0)) < 1:
                        log.error(
                            "[%s] pre-grace lock missing during blocked open", label
                        )
                        return 1
            log.info("[%s] → blocked (session kept for after-grace)", label)

            after_since, _ = get_node_time([ctx.server] + peers)

            if _end_grace_hold(ctx, grace_since, tag):
                return 1

            label = f"{tag} C2 after {c2_mode} (same open)"
            announce(f"[{label}] wait hanging open; TSM open +1 (no fresh open)")
            if c2_sess.wait_opened(WF_FILE_IDX, timeout=90) != "success":
                log.error("[%s] hanging open did not complete", label)
                return 1
            if _tsm_expect(
                ctx,
                baseline,
                label,
                after_since,
                True,
                open_delta=1,
                lock_delta=after_open_lock_delta,
                deleg_delta=after_open_deleg_delta,
            ):
                return 1
            log.info("[%s] → open success (TSM Node-id %s +1)", label, node_id)
        else:
            if c2_lock_during:
                announce(f"[{label}] {c2_lock_during} expect=success")
                if (
                    c2_sess.lock_file(WF_FILE_IDX, c2_lock_during, timeout=15)
                    != "success"
                ):
                    log.error("[%s] C2 %s failed", label, c2_lock_during)
                    return 1
                c2_held_lock = True
                c2_unlock_idxs.append(WF_FILE_IDX)
                if _hold_expect(
                    ctx, baseline, label, open_since, use_deleg, locked=True
                ):
                    return 1
            elif _hold_expect(ctx, baseline, label, open_since, False, locked=False):
                return 1
            log.info("[%s] → success (TSM Node-id %s ok)", label, node_id)

            if _end_grace_hold(ctx, grace_since, tag):
                return 1

        if c2_locks and _run_c2_locks(ctx, c2_sess, peers, node_id, tag, c2_locks):
            return 1
        if any(exp == "success" for _, exp in c2_locks):
            c2_held_lock = True
            if WF_FILE_IDX not in c2_unlock_idxs:
                c2_unlock_idxs.append(WF_FILE_IDX)

        log.info("[%s] PASSED", tag)
        return 0
    finally:
        # Always end grace before CLOSE (even if try returned early).
        try:
            unblock_cluster_grace(ctx)
            if grace_since is not None:
                if wait_grace_exit(ctx, grace_since, timeout=150):
                    log.warning("[%s] grace exit wait timed out before close", tag)
        except Exception as exc:
            log.warning("[%s] unblock/wait grace before close: %s", tag, exc)
        unlock_c2 = list(c2_unlock_idxs) if c2_held_lock or c2_unlock_idxs else []
        _close_and_stop(
            c2_sess,
            indexes=c2_close_idxs,
            unlock_indexes=unlock_c2,
        )
        _close_and_stop(
            c1_sess,
            indexes=c1_close_idxs,
            unlock_indexes=c1_unlock_idxs,
        )
        release_cluster_grace(ctx)


def tc_recover_primary(ctx, spec, nfs_version="4.2"):
    """Primary-restart recover chain: R01→R03 plant + R04–R06 access.

    Plant phases: sticky open+lock, primary restart only (no iptables),
    reclaim exact counts (fds idle).
    Access phases: restart primary before each access op (spare+iptables
    hold on first, enter_grace on later). R06 plants write-deleg on the
    deleg export then conflicts; unblock after last access; no CLOSE until end.
    """
    tag = spec["id"]
    phases = spec.get("phases")
    access_phases = list(spec.get("access_phases") or [])
    if not phases:
        phases = [
            {
                "name": "R01",
                "who": "c1",
                "mode": spec.get("c1_mode", WO),
                "lock": spec.get("c1_lock", "nwlock"),
                "file_idx": WF_FILE_IDX,
                "want_open": 1,
                "want_lock": 1,
            }
        ]

    c1 = ctx.client_a
    c2 = ctx.client_b
    need_c2 = any(p.get("who") == "c2" for p in phases) or any(
        p.get("who") == "c2" or (p.get("plant") or {}).get("who") == "c2"
        for p in access_phases
    )
    if ctx.server_node_id is None:
        log.error("[%s] need server_node_id", tag)
        return 1
    if ensure_interactive_binary(c1):
        return 1
    if need_c2:
        if not c2:
            log.error("[%s] need C2 client", tag)
            return 1
        if ensure_interactive_binary(c2):
            return 1

    mount = ctx.mount
    seed_idxs = sorted(
        {
            int(p["file_idx"])
            for p in list(phases) + access_phases
            if p.get("file_idx") is not None and p.get("kind") != "deleg_conflict"
        }
    )
    _seed_wf_file(c1, mount, indices=seed_idxs)
    time.sleep(1)

    c1_sess = None
    c2_sess = None
    c1d_sess = None
    c2d_sess = None
    c1_close_idxs = []
    c1_unlock_idxs = []
    c2_close_idxs = []
    c2_unlock_idxs = []
    c1d_close_idxs = []
    c1d_unlock_idxs = []
    c2d_close_idxs = []
    c2d_unlock_idxs = []
    grace_since = None
    try:
        if not ctx.server:
            log.error("[%s] need primary server for TSM scrape", tag)
            return 1
        node_id = ctx.server_node_id
        base = _file_base(mount)
        watch = [ctx.server]
        floor_open = 0
        floor_lock = 0
        floor_deleg = None  # None = do not assert deleg until R06

        def _sess_for(who, *, deleg=False):
            """Lazy InteractiveSession + close/unlock lists for primary or deleg mount."""
            nonlocal c1_sess, c2_sess, c1d_sess, c2d_sess
            if deleg:
                dbase = _file_base(ctx.deleg_mount)
                if who == "c1":
                    if not c1d_sess:
                        c1d_sess = InteractiveSession(c1, dbase, tag="c1d")
                        if not c1d_sess.start():
                            return None, None, None
                    return c1d_sess, c1d_close_idxs, c1d_unlock_idxs
                if not c2d_sess:
                    c2d_sess = InteractiveSession(c2, dbase, tag="c2d")
                    if not c2d_sess.start():
                        return None, None, None
                return c2d_sess, c2d_close_idxs, c2d_unlock_idxs
            if who == "c1":
                if not c1_sess:
                    c1_sess = InteractiveSession(c1, base, tag="c1")
                    if not c1_sess.start():
                        return None, None, None
                return c1_sess, c1_close_idxs, c1_unlock_idxs
            if not c2_sess:
                c2_sess = InteractiveSession(c2, base, tag="c2")
                if not c2_sess.start():
                    return None, None, None
            return c2_sess, c2_close_idxs, c2_unlock_idxs

        for phase in phases:
            pname = phase["name"]
            who = phase.get("who", "c1")
            mode = phase["mode"]
            lock = phase.get("lock")
            idx = int(phase["file_idx"])
            want_open = int(phase["want_open"])
            want_lock = int(phase["want_lock"])

            sess, close_list, unlock_list = _sess_for(who)
            if not sess:
                return 1

            label = f"{tag} {pname} plant {who} {mode}" + (f"+{lock}" if lock else "")
            announce(
                f"[{label}] open expect=success; TSM open={want_open} "
                f"lock={want_lock} (primary)"
            )
            plant_since, _ = get_node_time(watch)
            if _open_then_lock(sess, idx, mode, lock, label, close_list, unlock_list):
                return 1
            if _wait_tsm(watch, ctx, plant_since, want_open, want_lock, label, node_id):
                return 1
            log.info(
                "[%s] → planted cumulative open=%s lock=%s (fds held idle)",
                label,
                want_open,
                want_lock,
            )

            announce(f"[{tag} {pname}] restart primary; keep planted fds idle")
            rc, grace_since = enter_grace(ctx, target_node=ctx.server)
            if rc:
                return 1
            if _wait_tsm(
                watch,
                ctx,
                grace_since,
                want_open,
                want_lock,
                f"{tag} {pname} TSM after reclaim",
                node_id,
                timeout=120,
            ):
                return 1
            log.info(
                "[%s] → reclaim ok (open=%s lock=%s)",
                f"{tag} {pname}",
                want_open,
                want_lock,
            )

            announce(f"[{tag} {pname}] wait NOT IN GRACE")
            if wait_grace_exit(ctx, grace_since, timeout=150):
                log.error("[%s %s] grace did not end in time", tag, pname)
                return 1
            if _wait_tsm(
                watch,
                ctx,
                grace_since,
                want_open,
                want_lock,
                f"{tag} {pname} TSM after grace exit",
                node_id,
            ):
                return 1
            floor_open, floor_lock = want_open, want_lock
            log.info("[%s] → phase %s ok", tag, pname)

        if access_phases:
            announce(
                f"[{tag}] access window: restart primary before each of "
                f"{[p['name'] for p in access_phases]}; "
                f"floor open={floor_open} lock={floor_lock}; keep fds idle"
            )
            if _mount_spare(ctx, nfs_version):
                return 1
            spare_sess = _start_spare_hold(ctx)
            if not spare_sess:
                return 1
            ctx._spare_session = spare_sess

            hanging = None
            grace_since = None
            post_locks = None  # (sess, file_idx, locks) after hanging completes
            for i, ap in enumerate(access_phases):
                aname = ap["name"]
                kind = ap.get("kind", "compatible")

                if kind == "deleg_conflict":
                    plant = ap.get("plant") or {}
                    p_who = plant.get("who", "c1")
                    p_mode = plant["mode"]
                    p_lock = plant.get("lock")
                    p_idx = int(plant["file_idx"])
                    want_open = int(plant["want_open"])
                    want_lock = int(plant["want_lock"])
                    want_deleg = int(plant["want_deleg"])
                    who = ap.get("who", "c2")
                    mode = ap["mode"]
                    idx = int(ap["file_idx"])
                    existing_mode = ap.get("existing_mode", WO)
                    want_blocked = int(ap.get("want_open_blocked", want_open))
                    want_after = int(ap["want_open_after"])
                    want_lock_after = int(ap.get("want_lock_after", want_lock))
                    want_deleg_after = int(ap.get("want_deleg_after", 0))
                    locks = _normalize_c2_locks(ap)

                    announce(
                        f"[{tag} {aname}] write-deleg plant then conflict "
                        f"(floor {fmt_tsm(floor_open, floor_lock, floor_deleg or 0)})"
                    )
                    if ensure_deleg_export(ctx, nfs_version):
                        return 1
                    # Seed can leave sticky write-deleg; wait until TSM is back
                    # at the R05 floor before the hang plant open.
                    seed_since, _ = get_node_time(watch)
                    _seed_wf_file(c1, ctx.deleg_mount, indices=[p_idx])
                    seed_deleg = 0 if floor_deleg is None else int(floor_deleg)
                    if _wait_tsm(
                        watch,
                        ctx,
                        seed_since,
                        floor_open,
                        floor_lock,
                        f"{tag} {aname} seed clear",
                        node_id,
                        timeout=120,
                        want_deleg=seed_deleg,
                    ):
                        return 1

                    p_sess, p_close, p_unlock = _sess_for(p_who, deleg=True)
                    if not p_sess:
                        return 1
                    c_sess, c_close, _cul = _sess_for(who, deleg=True)
                    if not c_sess:
                        return 1

                    label = (
                        f"{tag} {aname} plant {p_who} {p_mode}"
                        + (f"+{p_lock}" if p_lock else "")
                        + f" idx={p_idx}"
                    )
                    announce(
                        f"[{label}] open expect=success; "
                        f"{fmt_tsm(want_open, want_lock, want_deleg)}"
                    )
                    plant_since, _ = get_node_time(watch)
                    if _open_then_lock(
                        p_sess, p_idx, p_mode, p_lock, label, p_close, p_unlock
                    ):
                        return 1
                    if _wait_tsm(
                        watch,
                        ctx,
                        plant_since,
                        want_open,
                        want_lock,
                        label,
                        node_id,
                        want_deleg=want_deleg,
                    ):
                        return 1
                    floor_open, floor_lock, floor_deleg = (
                        want_open,
                        want_lock,
                        want_deleg,
                    )
                    log.info("[%s] → plant ok", label)

                    announce(
                        f"[{tag} {aname}] restart primary before conflict "
                        f"(floor {fmt_tsm(floor_open, floor_lock, floor_deleg)})"
                    )
                    rc, grace_since = enter_grace(ctx, target_node=ctx.server)
                    if rc:
                        return 1
                    if _wait_tsm(
                        watch,
                        ctx,
                        grace_since,
                        floor_open,
                        floor_lock,
                        f"{tag} {aname} reclaim floor",
                        node_id,
                        timeout=120,
                        want_deleg=floor_deleg,
                    ):
                        return 1

                    label = f"{tag} {aname} conflict {who} {mode} idx={idx}"
                    announce(
                        f"[{label}] during grace open expect=blocked; "
                        f"TSM stay open={want_blocked} "
                        f"lock={floor_lock} deleg={floor_deleg}"
                    )
                    open_since, _ = get_node_time(watch)
                    got = c_sess.open_file(idx, mode, timeout=8)

                    def _deleg_early(reason):
                        nonlocal floor_open, floor_lock, floor_deleg, hanging
                        nonlocal post_locks
                        if _accept_grace_exited_open(
                            ctx,
                            grace_since,
                            label,
                            reason,
                            watch,
                            open_since,
                            want_after,
                            want_lock_after,
                            node_id,
                            c_close,
                            idx,
                            want_deleg=want_deleg_after,
                        ):
                            return 1
                        floor_open = want_after
                        floor_lock = want_lock_after
                        floor_deleg = want_deleg_after
                        hanging = None
                        post_locks = (c_sess, idx, locks) if locks else None
                        return 0

                    if got == "success":
                        if _deleg_early("open want=blocked got=success"):
                            return 1
                        continue
                    if got != "blocked":
                        log.error("[%s] open want=blocked got=%s", label, got)
                        return 1
                    if _validate_conflict_logs(
                        ctx, existing_mode, mode, open_since, label
                    ):
                        return 1
                    time.sleep(2)
                    snap = peer_summary(
                        watch, ctx.nfs_name, since=open_since, node_id=node_id
                    )
                    if snap.get("seen") and int(snap.get("open", 0)) == want_after:
                        if c_sess.wait_opened(idx, timeout=90) != "success":
                            log.error("[%s] hanging open did not complete", label)
                            return 1
                        if _deleg_early(f"TSM open already {want_after} while blocked"):
                            return 1
                        continue
                    _track_idx(c_close, idx)
                    hanging = {
                        "sess": c_sess,
                        "idx": idx,
                        "want_open": want_after,
                        "want_lock": want_lock_after,
                        "want_deleg": want_deleg_after,
                        "open_since": open_since,
                        "locks": locks,
                    }
                    log.info("[%s] → blocked (hanging for after-grace)", label)
                    continue

                # --- R04/R05 compatible / conflict ---
                who = ap.get("who", "c2")
                mode = ap["mode"]
                idx = int(ap["file_idx"])
                sess, close_list, _ul = _sess_for(who)
                if not sess:
                    return 1

                announce(
                    f"[{tag} {aname}] restart primary before access "
                    f"(floor open={floor_open} lock={floor_lock})"
                )
                if i == 0:
                    rc, grace_since = hold_cluster_grace(
                        ctx, spare_sess, restart_node=ctx.server
                    )
                else:
                    rc, grace_since = enter_grace(ctx, target_node=ctx.server)
                if rc:
                    return 1
                if _wait_tsm(
                    watch,
                    ctx,
                    grace_since,
                    floor_open,
                    floor_lock,
                    f"{tag} {aname} reclaim floor",
                    node_id,
                    timeout=120,
                ):
                    return 1

                if kind == "compatible":
                    want_open = int(ap["want_open"])
                    want_lock = int(ap["want_lock"])
                    label = f"{tag} {aname} compatible {who} {mode} idx={idx}"
                    announce(
                        f"[{label}] during grace open expect=success; "
                        f"TSM open={want_open} lock={want_lock}"
                    )
                    open_since, _ = get_node_time(watch)
                    if sess.open_file(idx, mode, timeout=30) != "success":
                        log.error("[%s] compatible open failed", label)
                        return 1
                    _track_idx(close_list, idx)
                    if _wait_tsm(
                        watch,
                        ctx,
                        open_since,
                        want_open,
                        want_lock,
                        label,
                        node_id,
                    ):
                        return 1
                    floor_open, floor_lock = want_open, want_lock
                    log.info("[%s] → ok", label)
                    continue

                if kind != "conflict":
                    log.error("[%s] unknown access kind %r", tag, kind)
                    return 1

                existing_mode = ap.get("existing_mode", WO)
                want_blocked = int(ap.get("want_open_blocked", floor_open))
                want_after = int(ap["want_open_after"])
                want_lock = int(ap.get("want_lock", floor_lock))
                label = f"{tag} {aname} conflict {who} {mode} idx={idx}"
                announce(
                    f"[{label}] during grace open expect=blocked; "
                    f"TSM stay open={want_blocked} lock={want_lock}"
                )
                open_since, _ = get_node_time(watch)
                got = sess.open_file(idx, mode, timeout=8)

                def _early(reason):
                    nonlocal floor_open, floor_lock, hanging
                    if _accept_grace_exited_open(
                        ctx,
                        grace_since,
                        label,
                        reason,
                        watch,
                        open_since,
                        want_after,
                        want_lock,
                        node_id,
                        close_list,
                        idx,
                    ):
                        return 1
                    floor_open, floor_lock = want_after, want_lock
                    hanging = None
                    return 0

                if got == "success":
                    if _early("open want=blocked got=success"):
                        return 1
                    continue

                if got != "blocked":
                    log.error("[%s] open want=blocked got=%s", label, got)
                    return 1
                if _validate_conflict_logs(ctx, existing_mode, mode, open_since, label):
                    return 1
                time.sleep(2)
                snap = peer_summary(
                    watch, ctx.nfs_name, since=open_since, node_id=node_id
                )
                if snap.get("seen") and int(snap.get("open", 0)) == want_after:
                    if sess.wait_opened(idx, timeout=90) != "success":
                        log.error("[%s] hanging open did not complete", label)
                        return 1
                    if _early(f"TSM open already {want_after} while blocked"):
                        return 1
                    continue

                _track_idx(close_list, idx)
                hanging = {
                    "sess": sess,
                    "idx": idx,
                    "want_open": want_after,
                    "want_lock": want_lock,
                    "want_deleg": None,
                    "open_since": open_since,
                    "locks": [],
                }
                log.info("[%s] → blocked (hanging for after-grace)", label)

            if _end_grace_hold(ctx, grace_since, f"{tag} access window"):
                return 1

            if hanging:
                hs = hanging
                label = f"{tag} conflict after grace " f"(same open idx={hs['idx']})"
                announce(
                    f"[{label}] wait hanging open; "
                    f"{fmt_tsm(hs['want_open'], hs['want_lock'], hs.get('want_deleg'))}"
                )
                if hs["sess"].wait_opened(hs["idx"], timeout=90) != "success":
                    log.error("[%s] hanging open did not complete", label)
                    return 1
                if _wait_tsm(
                    watch,
                    ctx,
                    hs["open_since"],
                    hs["want_open"],
                    hs["want_lock"],
                    label,
                    node_id,
                    want_deleg=hs.get("want_deleg"),
                ):
                    return 1
                log.info("[%s] → open completed", label)
                if hs.get("locks"):
                    post_locks = (hs["sess"], hs["idx"], hs["locks"])

            if post_locks:
                sess, fidx, locks = post_locks
                peers = _workflow_peers(ctx)
                if _run_c2_locks(
                    ctx, sess, peers, node_id, f"{tag} R06", locks, file_idx=fidx
                ):
                    return 1

            log.info(
                "[%s] PASSED (phases %s + access %s)",
                tag,
                [p["name"] for p in phases],
                [p["name"] for p in access_phases],
            )
            return 0

        log.info("[%s] PASSED (phases %s)", tag, [p["name"] for p in phases])
        return 0
    finally:
        try:
            unblock_cluster_grace(ctx)
            if grace_since is not None:
                if wait_grace_exit(ctx, grace_since, timeout=150):
                    log.warning("[%s] grace exit wait timed out before close", tag)
        except Exception as exc:
            log.warning("[%s] unblock/wait grace before close: %s", tag, exc)
        _close_and_stop(
            c2d_sess,
            indexes=c2d_close_idxs,
            unlock_indexes=c2d_unlock_idxs,
        )
        _close_and_stop(
            c1d_sess,
            indexes=c1d_close_idxs,
            unlock_indexes=c1d_unlock_idxs,
        )
        _close_and_stop(
            c2_sess,
            indexes=c2_close_idxs,
            unlock_indexes=c2_unlock_idxs,
        )
        _close_and_stop(
            c1_sess,
            indexes=c1_close_idxs,
            unlock_indexes=c1_unlock_idxs,
        )
        release_cluster_grace(ctx)


def _log_tc_overview(ctx):
    primary = ctx.server.hostname if ctx.server else "N/A"
    spare = ctx.spare_node.hostname if ctx.spare_node else "N/A"
    ids = (
        ", ".join(f"{h}={nid}" for h, nid in sorted(ctx.tsm_node_ids.items())) or "N/A"
    )
    deleg = getattr(ctx, "deleg_export", None) or "N/A"
    log.info(
        "\n========== OVERVIEW =========================\n"
        "Spare node - %s\n"
        "Spare Client - %s\n"
        "Spare Export - %s\n"
        "Deleg Export - %s\n"
        "Client-1/2 Mounts to - %s (TSM Node-id %s)\n"
        "TSM Node-id map - %s\n"
        "============ OVERVIEW =======================",
        spare,
        ctx.spare_client.hostname if ctx.spare_client else "N/A",
        ctx.spare_export or "N/A",
        deleg,
        primary,
        ctx.server_node_id,
        ids,
    )


def run(ceph_cluster, **kw):
    """Deploy 4-node TSM cluster, run registered grace conflict WORKFLOWS."""
    config = kw.get("config") or {}
    selected = config.get("workflows")
    nfs_version = str(config.get("nfs_version", "4.2"))
    nfs_count = int(config.get("nfs_count", DEFAULT_NFS_COUNT))

    nfs_nodes = sorted(ceph_cluster.get_nodes("nfs"), key=lambda n: n.hostname)
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]

    if len(nfs_nodes) < nfs_count:
        log.error("Need ≥%s NFS nodes (have %s)", nfs_count, len(nfs_nodes))
        return log_workflow_summary(
            {"precheck": "failed"}, title="TSM GRACE IO SUMMARY"
        )
    if len(clients) < MIN_CLIENTS:
        log.error("Need ≥%s clients (have %s)", MIN_CLIENTS, len(clients))
        return log_workflow_summary(
            {"precheck": "failed"}, title="TSM GRACE IO SUMMARY"
        )

    nodes = nfs_nodes[:nfs_count]
    use_clients = clients[:MIN_CLIENTS]
    name = "tsmg-grace"
    nfs_name = f"cephfs-nfs-{name}"
    export = f"/export_{name}"
    mount = f"/mnt/nfs_{name}"
    spare_export = f"/export_{name}_spare"
    spare_mount = f"/mnt/nfs_{name}_spare"
    deleg_export = f"/export_{name}_deleg"
    deleg_mount = f"/mnt/nfs_{name}_deleg"
    results = {}
    ctx = None

    try:
        result = deploy_step(
            ceph_cluster,
            installer,
            use_clients[0],
            nodes,
            {"nfs_count": nfs_count, "tsm_port": TSM_PORT},
            nfs_name,
            name,
        )
        if result is None:
            raise RuntimeError("deploy failed")
        _, active, nfs_port, _ = result
        if len(active) < nfs_count:
            raise RuntimeError(
                f"Expected {nfs_count} active NFS daemons, got {len(active)}"
            )

        announce("Collect TSM Node-ids from podman logs (Ganesha ID)")
        tsm_node_ids = require_tsm_node_ids(active, nfs_name)

        if _mount_clients(
            use_clients[:-1],
            nfs_name,
            export,
            mount,
            nfs_port,
            active[0].hostname,
            nfs_version=nfs_version,
        ):
            raise RuntimeError("primary mount failed")

        ctx = GraceCtx(
            clients=use_clients[:-1],
            active=active,
            peers=active[1:],
            server=active[0],
            nfs_name=nfs_name,
            export=export,
            mount=mount,
            nfs_port=nfs_port,
            spare_node=active[-1],
            spare_client=use_clients[-1],
            spare_export=spare_export,
            spare_mount=spare_mount,
            tsm_port=TSM_PORT,
            tsm_node_ids=tsm_node_ids,
        )
        ctx.deleg_export = deleg_export
        ctx.deleg_mount = deleg_mount
        ctx._deleg_ready = False

        cases = selected or list(WORKFLOWS.keys())
        _, coredump_since = get_node_time(nodes)
        for i, tc_id in enumerate(cases, 1):
            spec = WORKFLOWS.get(tc_id)
            if not spec:
                log.error("Unknown workflow %s", tc_id)
                results[tc_id] = "failed"
                continue
            log.info(
                "\n==============================\n"
                "Test %s: %s: %s\n"
                "==============================",
                i,
                tc_id,
                spec["desc"],
            )
            _log_tc_overview(ctx)
            announce("Do workflow [%s] — %s" % (tc_id, spec["desc"]))
            try:
                runner = spec.get("runner", "conflict")
                if runner == "recover_primary":
                    rc = tc_recover_primary(
                        ctx, dict(spec, id=tc_id), nfs_version=nfs_version
                    )
                else:
                    rc = tc_conflict_generic(
                        ctx, dict(spec, id=tc_id), nfs_version=nfs_version
                    )
                results[tc_id] = "passed" if rc == 0 else "failed"
            except Exception as exc:
                log.error("[%s] FAILED: %s", tc_id, exc)
                results[tc_id] = "failed"
            if results[tc_id] == "passed" and check_coredumps(
                nodes, coredump_since, tc_id
            ):
                results[tc_id] = "failed"
            _, coredump_since = get_node_time(nodes)
    except Exception as exc:
        log.error("Grace IO suite FAILED: %s", exc)
        results["suite"] = "failed"
    finally:
        cleanup_grace_suite(
            ctx, use_clients, mount, nfs_name, export, nodes, safe_cleanup
        )

    return log_workflow_summary(results, title="TSM GRACE IO SUMMARY")
