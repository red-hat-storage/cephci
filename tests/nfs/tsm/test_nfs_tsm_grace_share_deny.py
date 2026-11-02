"""NFS TSM grace-period share-deny workflows.

C1: raw NFSv4.1 OPEN via open_share.py (explicit share_access / share_deny).
C2: kernel InteractiveSession (same as test_nfs_tsm_grace_io.py) for
    conflicting / non-conflicting opens + locks.
Grace hold: spare + iptables (unchanged).

Design (all TCs — do not regress):
  1. C1 OPEN is always *during* grace (never plant-before-grace).
  2. C2 expect values stay as in WORKFLOWS (solo-TC proven); do not
     retarget success/denied → blocked for pre-grace planting.

No-deleg (*0):
Rn0..DB0 — primary export; WANT_NO_DELEG (see WORKFLOWS).

With-deleg (*4 READ / *5 WRITE) on delegations=rw + --want-deleg:
  Locks under an active deleg are client-local (TSM lock unchanged) until
  another client's open recalls the deleg — then lock records appear (same
  as grace_io C13/C14). During C2 RO+nrlock: open+1 lock+0; after recall:
  after_open_deleg_delta=-1 and optional after_open_lock_delta.
Rn4: C1 r deny n READ; RO+nrlock close during grace; after grace RW success + recall.
Rb4: C1 r deny b READ; RO denied; after RO/WO denied (no recall).
Rw4: C1 r deny w READ; RO+nrlock close; WO denied (share-deny); after WO denied.
Wn5: C1 w deny n WRITE; RO blocked → after success + recall.
Ww5: C1 w deny w WRITE; RO blocked → after RO + recall; WO denied.
Dn5: C1 rw deny n WRITE; RO blocked → after success + recall.
DW5: C1 rw deny w WRITE; RO blocked → after RO + recall; WO denied.
DR5: C1 rw deny r WRITE; WO blocked → after WO + recall; RO denied.
DB5: C1 rw deny b WRITE; RO blocked→denied; WO denied (no recall).

Between TCs: wait NOT IN GRACE → clean close C2/C1 → TSM drain open=0.
"""

import re
import time

from tests.nfs.lib.common_lib import get_nfs_container_id, get_node_time
from tests.nfs.lib.tsm.grace_hold import (
    GraceCtx,
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
from tests.nfs.lib.tsm.share_open import ShareOpenSession, ensure_open_share_script
from tests.nfs.tsm.test_nfs_tsm_basic import deploy_step, safe_cleanup
from utility.log import Log

log = Log(__name__)

TSM_PORT = 36369
DEFAULT_NFS_COUNT = 4
MIN_CLIENTS = 3  # C1 + C2 + spare
SPARE_FILE_IDX = 0
WF_FILE_IDX = 1  # C1 RPC + C2 interactive share the same wf1.txt

WO, RO, RW = "WRITE_ONLY", "READ_ONLY", "READ_WRITE"
MODE_SA = {RO: 1, WO: 2, RW: 3}
ACCESS_SA = {"r": 1, "w": 2, "rw": 3}

CONFLICT_SA_RE = re.compile(r"Open conflict existing_sa=(\d+) new_sa=(\d+)", re.I)
CONFLICT_GRACE_RE = re.compile(r"Conflicting open\.\s*Return NFS4ERR_GRACE", re.I)

WORKFLOWS = {
    "TC-TSM-G-SD-Rn0": {
        "desc": (
            "r deny n; C2 RO+nrlock then close; RW blocked during grace; "
            "after grace RW open success (C1 lease alive)"
        ),
        "c1_access": "r",
        "c1_deny": "n",
        "c2_during": [
            # Close RO before RW so the kernel client does not merge/upgrade
            # the open (same NFS client owner) and leave a stale TSM record.
            {
                "mode": RO,
                "expect": "success",
                "lock": "nrlock",
                "close_after": True,
            },
            {"mode": RW, "expect": "blocked"},
        ],
    },
    "TC-TSM-G-SD-Rr0": {
        "desc": (
            "r deny r; C2 RO denied during grace; after grace RO denied, "
            "WO success (C1 lease alive)"
        ),
        "c1_access": "r",
        "c1_deny": "r",
        "c2_during": [
            {"mode": RO, "expect": "denied"},
        ],
        "c2_after": [
            {"mode": RO, "expect": "denied"},
            {"mode": WO, "expect": "success"},
        ],
    },
    "TC-TSM-G-SD-Rw0": {
        "desc": (
            "r deny w; C2 RO+nrlock then close; WO denied during grace; "
            "after grace WO denied (C1 lease alive)"
        ),
        "c1_access": "r",
        "c1_deny": "w",
        "c2_during": [
            # Close RO before WO — same client owner merges opens otherwise.
            {
                "mode": RO,
                "expect": "success",
                "lock": "nrlock",
                "close_after": True,
            },
            # deny w → immediate SHARE_DENIED (not grace-blocked).
            {"mode": WO, "expect": "denied"},
        ],
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-Rb0": {
        "desc": (
            "r deny b; C2 RO denied during grace; after grace RO denied, "
            "WO denied (C1 lease alive)"
        ),
        "c1_access": "r",
        "c1_deny": "b",
        "c2_during": [
            {"mode": RO, "expect": "denied"},
        ],
        "c2_after": [
            {"mode": RO, "expect": "denied"},
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-Wn0": {
        "desc": (
            "w deny n; C2 RO blocked during grace; after grace RO open success "
            "(C1 lease alive)"
        ),
        "c1_access": "w",
        "c1_deny": "n",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
    },
    "TC-TSM-G-SD-Ww0": {
        "desc": (
            "w deny w; C2 RO blocked during grace; after grace RO success, "
            "WO denied (C1 lease alive)"
        ),
        "c1_access": "w",
        "c1_deny": "w",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-Dn0": {
        "desc": (
            "rw deny n; C2 RO blocked during grace; after grace RO open success "
            "(C1 lease alive)"
        ),
        "c1_access": "rw",
        "c1_deny": "n",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
    },
    "TC-TSM-G-SD-DW0": {
        "desc": (
            "rw deny w; C2 RO blocked during grace; after grace RO success, "
            "WO denied (C1 lease alive)"
        ),
        "c1_access": "rw",
        "c1_deny": "w",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-DR0": {
        "desc": (
            "rw deny r; C2 WO blocked during grace; after grace WO success, "
            "RO denied (C1 lease alive)"
        ),
        "c1_access": "rw",
        "c1_deny": "r",
        "c2_during": [
            {"mode": WO, "expect": "blocked"},
        ],
        "c2_after": [
            {"mode": RO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-DB0": {
        "desc": (
            "rw deny b; C2 RO blocked during grace → denied after; "
            "WO denied (C1 lease alive)"
        ),
        "c1_access": "rw",
        "c1_deny": "b",
        "c2_during": [
            {"mode": RO, "expect": "blocked", "after_expect": "denied"},
        ],
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    # --- With-deleg (*4 READ / *5 WRITE): same trim as *0 + TSM deleg/recall ---
    "TC-TSM-G-SD-Rn4": {
        "desc": (
            "r deny n READ deleg; C2 RO+nrlock then close during grace; "
            "after grace RW open success + recall"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "r",
        "c1_deny": "n",
        "c2_during": [
            {
                "mode": RO,
                "expect": "success",
                "lock": "nrlock",
                "close_after": True,
            },
            # RW is after NOT IN GRACE (see c2_after) — not a during-grace hang.
        ],
        "c2_after": [
            {"mode": RW, "expect": "success", "deleg_delta": -1},
        ],
    },
    "TC-TSM-G-SD-Rb4": {
        "desc": (
            "r deny b READ deleg; C2 RO denied; after RO/WO denied "
            "(C1 READ stays, no recall)"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "r",
        "c1_deny": "b",
        "c2_during": [
            {"mode": RO, "expect": "denied"},
        ],
        "c2_after": [
            {"mode": RO, "expect": "denied"},
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-Rw4": {
        "desc": (
            "r deny w READ deleg; C2 RO+nrlock then close; WO denied "
            "(share-deny; after WO denied)"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "r",
        "c1_deny": "w",
        "c2_during": [
            {
                "mode": RO,
                "expect": "success",
                "lock": "nrlock",
                "close_after": True,
            },
            # deny w → immediate SHARE_DENIED (not grace-blocked).
            {"mode": WO, "expect": "denied"},
        ],
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-Wn5": {
        "desc": ("w deny n WRITE deleg; C2 RO blocked; after RO success + recall"),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "w",
        "c1_deny": "n",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
        "after_open_deleg_delta": -1,
    },
    "TC-TSM-G-SD-Ww5": {
        "desc": (
            "w deny w WRITE deleg; C2 RO blocked; after RO success + recall; "
            "WO denied"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "w",
        "c1_deny": "w",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
        "after_open_deleg_delta": -1,
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-Dn5": {
        "desc": ("rw deny n WRITE deleg; C2 RO blocked; after RO success + recall"),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "rw",
        "c1_deny": "n",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
        "after_open_deleg_delta": -1,
    },
    "TC-TSM-G-SD-DW5": {
        "desc": (
            "rw deny w WRITE deleg; C2 RO blocked; after RO success + recall; "
            "WO denied"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "rw",
        "c1_deny": "w",
        "c2_during": [
            {"mode": RO, "expect": "blocked"},
        ],
        "after_open_deleg_delta": -1,
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-DR5": {
        "desc": (
            "rw deny r WRITE deleg; C2 WO blocked; after WO success + recall; "
            "RO denied"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "rw",
        "c1_deny": "r",
        "c2_during": [
            {"mode": WO, "expect": "blocked"},
        ],
        "after_open_deleg_delta": -1,
        "c2_after": [
            {"mode": RO, "expect": "denied"},
        ],
    },
    "TC-TSM-G-SD-DB5": {
        "desc": (
            "rw deny b WRITE deleg; C2 RO blocked→denied; WO denied "
            "(C1 WRITE stays, no recall)"
        ),
        "use_deleg_export": True,
        "want_deleg": True,
        "c1_access": "rw",
        "c1_deny": "b",
        "c2_during": [
            {"mode": RO, "expect": "blocked", "after_expect": "denied"},
        ],
        "c2_after": [
            {"mode": WO, "expect": "denied"},
        ],
    },
}


def _file_base(mount, kind="wf"):
    return file_base(mount, kind)


def _share_filename(idx=WF_FILE_IDX):
    """Export-relative name matching InteractiveSession {base}{idx}.txt."""
    return f"wf{idx}.txt"


def _stamp(nodes):
    since, _ = get_node_time(nodes)
    return since


def _workflow_peers(ctx):
    return workflow_peers(ctx)


def _seed_wf_file(client, mount, indices=None):
    seed_wf_files(client, mount, indices if indices is not None else [WF_FILE_IDX])


def _close_and_stop(sess, indexes=None, unlock_indexes=None):
    close_interactive(sess, indexes=indexes, unlock_indexes=unlock_indexes)


def _c2_open_check(
    ctx,
    client,
    base,
    tag,
    mode,
    expect,
    peers,
    node_id,
    label,
    open_delta=1,
    lock_delta=0,
    deleg_delta=None,
    timeout=30,
):
    """One-shot C2 open + TSM check + stop. Return 0 ok, 1 fail."""
    open_since = _stamp(peers)
    baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)
    sess = InteractiveSession(client, base, tag=tag)
    if not sess.start():
        return 1
    got = sess.open_file(WF_FILE_IDX, mode, timeout=timeout)
    if got != expect:
        log.error("[%s] want=%s got=%s", label, expect, got)
        _close_and_stop(sess, indexes=[WF_FILE_IDX] if got == "success" else [])
        return 1
    if _tsm_expect(
        ctx,
        baseline,
        label,
        open_since,
        expect == "success",
        open_delta=open_delta,
        lock_delta=lock_delta,
        deleg_delta=deleg_delta if expect == "success" else None,
    ):
        _close_and_stop(sess, indexes=[WF_FILE_IDX] if got == "success" else [])
        return 1
    _close_and_stop(sess, indexes=[WF_FILE_IDX] if expect == "success" else [])
    log.info("[%s] → %s", label, expect)
    return 0


def _wait_tsm_drained(ctx, label, since=None, timeout=90, expect_deleg=None):
    """Poll peers until Node-id open=0 lock=0 (inter-workflow cleanup gate).

    Stamp ``since`` before tearing sessions down so the close-generated
    ``open=0`` summary is visible. When ``expect_deleg`` is set (deleg TCs),
    also wait for that deleg count (usually 0). Return 0 ok, 1 timeout.
    """
    peers = _workflow_peers(ctx)
    node_id = ctx.server_node_id
    if not peers or node_id is None:
        log.warning("[%s] skip TSM drain (no peers / node_id)", label)
        return 0
    if since is None:
        since = _stamp(peers)
    announce(
        f"[{label}] wait TSM Node-id {node_id} drain",
        fmt_tsm(0, 0, expect_deleg),
    )
    if (
        wait_peer_counts(
            peers,
            ctx.nfs_name,
            since,
            0,
            0,
            label,
            expect_deleg=expect_deleg,
            timeout=timeout,
            node_id=node_id,
        )
        is None
    ):
        snap = peer_summary(peers, ctx.nfs_name, since=since, node_id=node_id)
        log.error(
            "[%s] TSM drain timeout: want open=0 lock=0 deleg=%s last=%s",
            label,
            expect_deleg,
            snap,
        )
        return 1
    log.info("[%s] TSM drained (open=0 lock=0)", label)
    return 0


def _mount_spare(ctx, nfs_version):
    return mount_spare_export(ctx, nfs_version)


def _start_spare_hold(ctx):
    return start_spare_hold(ctx, file_idx=SPARE_FILE_IDX)


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

    deleg_delta=None → do not assert deleg (*0); int → assert base+delta (*4/*5).
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
        log.error("[%s] unexpected TSM open=%s", label, want_open)
        return 1
    if lock_delta and int(snap.get("lock", 0)) == want_lock:
        log.error("[%s] unexpected TSM lock=%s", label, want_lock)
        return 1
    if deleg_delta is not None and int(snap.get("deleg", 0)) == want_deleg:
        log.error("[%s] unexpected TSM deleg=%s", label, want_deleg)
        return 1
    log.info(
        "[%s] no TSM bump (open=%s lock=%s deleg=%s) — ok",
        label,
        snap.get("open"),
        snap.get("lock"),
        snap.get("deleg"),
    )
    return 0


def _validate_conflict_logs(ctx, c1_access, c2_mode, since, label):
    existing_sa = ACCESS_SA[c1_access]
    new_sa = MODE_SA[c2_mode]
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


def tc_share_deny(ctx, spec, nfs_version="4.2"):
    """C1 open_share (access/deny); C2 interactive probes during/after grace."""
    tag = spec["id"]
    c1_access = spec["c1_access"]
    c1_deny = spec["c1_deny"]
    use_deleg = bool(spec.get("use_deleg_export"))
    want_deleg = bool(spec.get("want_deleg"))
    after_open_deleg_delta = spec.get("after_open_deleg_delta")
    # Locks held under a live deleg stay client-local until recall.
    after_open_lock_delta = int(spec.get("after_open_lock_delta") or 0)
    c1 = ctx.client_a
    c2 = ctx.client_b

    if ctx.server_node_id is None:
        log.error("[%s] need server_node_id", tag)
        return 1
    if ensure_open_share_script(c1):
        return 1
    if ensure_interactive_binary(c2):
        return 1

    if use_deleg:
        if ensure_deleg_export(ctx, nfs_version):
            return 1
        mount = ctx.deleg_mount
        export = ctx.deleg_export
    else:
        mount = ctx.mount
        export = ctx.export

    base = _file_base(mount)
    share_file = _share_filename(WF_FILE_IDX)

    peers = _workflow_peers(ctx)
    if not peers:
        log.error("[%s] need ≥1 peer for TSM summary", tag)
        return 1
    node_id = ctx.server_node_id

    # Seed can grant a sticky write-deleg; wait until TSM is clean before grace.
    if use_deleg:
        seed_since = _stamp(peers)
        _seed_wf_file(c1, mount, indices=[WF_FILE_IDX])
        if _wait_tsm_drained(
            ctx, f"{tag} post-seed", since=seed_since, expect_deleg=0, timeout=120
        ):
            return 1
    else:
        _seed_wf_file(c1, mount, indices=[WF_FILE_IDX])
        time.sleep(1)
    if _mount_spare(ctx, nfs_version):
        return 1

    c1_sess = None
    c2_ok_sess = None
    c2_blocked = []
    c2_ok_unlock = []
    blocked_baseline = None
    after_since = None
    drain_since = None
    grace_since = None
    try:
        spare_sess = _start_spare_hold(ctx)
        if not spare_sess:
            return 1
        ctx._spare_session = spare_sess

        # Design #1: hold grace first; C1 OPEN during grace (not pre-planted).
        announce(f"[{tag}] hold cluster grace (spare + iptables -I)")
        rc, grace_since = hold_cluster_grace(ctx, spare_sess)
        if rc:
            return 1

        # --- C1 during grace ---
        label = f"{tag} C1 during {c1_access} deny {c1_deny}"
        if want_deleg:
            label += " want_deleg"
        announce(f"[{label}] open_share hold expect=success")
        baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)
        open_since = _stamp(peers)
        c1_sess = ShareOpenSession(
            c1,
            server=ctx.server.hostname,
            port=ctx.nfs_port,
            export=export,
            filename=share_file,
            tag="c1",
        )
        if c1_sess.open_hold(c1_access, c1_deny, want_deleg=want_deleg) != "success":
            log.error("[%s] C1 open_hold failed", label)
            return 1
        if _tsm_expect(
            ctx,
            baseline,
            label,
            open_since,
            True,
            open_delta=1,
            deleg_delta=1 if want_deleg else None,
        ):
            return 1
        log.info("[%s] → success (open_share hold)", label)

        # --- C2 during grace ---
        for i, probe in enumerate(spec.get("c2_during") or []):
            mode = probe["mode"]
            expect = probe["expect"]
            lock = probe.get("lock")
            label = f"{tag} C2 during {mode}" + (f"+{lock}" if lock else "")
            announce(f"[{label}] open expect={expect}")
            open_since = _stamp([ctx.server] + peers)
            baseline = peer_summary(peers, ctx.nfs_name, node_id=node_id)

            sess = InteractiveSession(c2, base, tag=f"c2d{i}")
            if not sess.start():
                return 1
            timeout = 8 if expect == "blocked" else 30
            got = sess.open_file(WF_FILE_IDX, mode, timeout=timeout)
            if got != expect:
                log.error("[%s] want=%s got=%s", label, expect, got)
                _close_and_stop(sess, indexes=[WF_FILE_IDX] if got == "success" else [])
                return 1

            if expect == "blocked":
                if _validate_conflict_logs(ctx, c1_access, mode, open_since, label):
                    _close_and_stop(sess)
                    return 1
                if _tsm_expect(ctx, baseline, label, open_since, False, open_delta=1):
                    _close_and_stop(sess)
                    return 1
                if blocked_baseline is None:
                    blocked_baseline = baseline
                    after_since = _stamp([ctx.server] + peers)
                    log.info(
                        "[%s] blocked TSM floor open=%s lock=%s deleg=%s "
                        "after_since=%s",
                        tag,
                        blocked_baseline.get("open"),
                        blocked_baseline.get("lock"),
                        blocked_baseline.get("deleg"),
                        after_since,
                    )
                c2_blocked.append(
                    {
                        "sess": sess,
                        "mode": mode,
                        "after_expect": probe.get("after_expect", "success"),
                    }
                )
                log.info("[%s] → blocked (session kept for after-grace)", label)
            elif expect == "denied":
                if _tsm_expect(ctx, baseline, label, open_since, False, open_delta=1):
                    _close_and_stop(sess)
                    return 1
                _close_and_stop(sess)
                log.info("[%s] → denied", label)
            else:
                if lock:
                    announce(f"[{label}] {lock} expect=success")
                    if sess.lock_file(WF_FILE_IDX, lock, timeout=15) != "success":
                        log.error("[%s] %s failed", label, lock)
                        _close_and_stop(sess, indexes=[WF_FILE_IDX])
                        return 1
                    c2_ok_unlock = [WF_FILE_IDX]
                # Under deleg: fcntl succeeds but TSM lock stays 0 until recall
                # (grace_io _hold_expect). Without deleg: lock bumps TSM.
                lock_bump = 0 if use_deleg else (1 if lock else 0)
                if _tsm_expect(
                    ctx,
                    baseline,
                    label,
                    open_since,
                    True,
                    open_delta=1,
                    lock_delta=lock_bump,
                ):
                    _close_and_stop(
                        sess, indexes=[WF_FILE_IDX], unlock_indexes=c2_ok_unlock
                    )
                    return 1
                if probe.get("close_after"):
                    announce(f"[{label}] close before next C2 probe")
                    # Stamp before unlock/close so post-close TSM lines are visible.
                    # Wait until counts return to the pre-probe floor (C1-only)
                    # before the next C2 open — under deleg, CLOSE/DELEGRETURN
                    # can lag and leave open/deleg high for the RW conflict.
                    close_since = _stamp(peers)
                    floor_open = int((baseline or {}).get("open", 0))
                    floor_lock = int((baseline or {}).get("lock", 0))
                    floor_deleg = (
                        int((baseline or {}).get("deleg", 0)) if use_deleg else None
                    )
                    _close_and_stop(
                        sess,
                        indexes=[WF_FILE_IDX],
                        unlock_indexes=c2_ok_unlock,
                    )
                    c2_ok_unlock = []
                    announce(
                        f"[{label}] wait TSM back to floor",
                        fmt_tsm(floor_open, floor_lock, floor_deleg),
                    )
                    if (
                        wait_peer_counts(
                            peers,
                            ctx.nfs_name,
                            close_since,
                            floor_open,
                            floor_lock,
                            f"{label} post-close",
                            expect_deleg=floor_deleg,
                            timeout=90,
                            node_id=node_id,
                        )
                        is None
                    ):
                        log.error(
                            "[%s] TSM did not return to floor open=%s lock=%s "
                            "deleg=%s after close",
                            label,
                            floor_open,
                            floor_lock,
                            floor_deleg,
                        )
                        return 1
                    log.info("[%s] → success (closed; TSM at floor)", label)
                else:
                    c2_ok_sess = sess
                    log.info("[%s] → success", label)

        # --- End grace ---
        announce(f"[{tag}] unblock spare TCP (end grace hold)")
        unblock_cluster_grace(ctx)
        announce(f"[{tag}] wait NOT IN GRACE")
        if wait_grace_exit(ctx, grace_since, timeout=150):
            log.error("[%s] grace did not end in time", tag)
            return 1

        # --- After grace: hanging opens complete ---
        n_blocked = len(c2_blocked)
        n_blocked_success = 0
        for item in c2_blocked:
            mode = item["mode"]
            after_expect = item.get("after_expect", "success")
            label = f"{tag} C2 after {mode} (same open)"
            announce(f"[{label}] wait hanging open expect={after_expect}")
            got = item["sess"].wait_opened(WF_FILE_IDX, timeout=90)
            if got != after_expect:
                log.error("[%s] want=%s got=%s", label, after_expect, got)
                return 1
            if after_expect == "success":
                n_blocked_success += 1
                log.info("[%s] → open success", label)
            else:
                _close_and_stop(item["sess"])
                item["sess"] = None
                log.info("[%s] → %s", label, after_expect)

        if n_blocked:
            if blocked_baseline is None or after_since is None:
                log.error("[%s] missing blocked TSM floor / after_since", tag)
                return 1
            if n_blocked_success:
                label = f"{tag} C2 after grace ({n_blocked_success} opens)"
                announce(f"[{label}] TSM open +{n_blocked_success}")
                if _tsm_expect(
                    ctx,
                    blocked_baseline,
                    label,
                    after_since,
                    True,
                    open_delta=n_blocked_success,
                    lock_delta=after_open_lock_delta,
                    deleg_delta=after_open_deleg_delta,
                ):
                    return 1
            else:
                label = f"{tag} C2 after grace (hangs denied)"
                if _tsm_expect(
                    ctx,
                    blocked_baseline,
                    label,
                    after_since,
                    False,
                    open_delta=1,
                ):
                    return 1

        c2_blocked = [x for x in c2_blocked if x.get("sess")]

        # --- After grace: fresh opens ---
        for i, probe in enumerate(spec.get("c2_after") or []):
            mode = probe["mode"]
            expect = probe["expect"]
            label = f"{tag} C2 after {mode}"
            announce(f"[{label}] open expect={expect}")
            # Recall path (Rn4): longer timeout — may wait on CB_RECALL.
            timeout = 90 if expect == "success" and probe.get("deleg_delta") else 30
            if _c2_open_check(
                ctx,
                c2,
                base,
                f"c2a{i}",
                mode,
                expect,
                peers,
                node_id,
                label,
                open_delta=1 if expect == "success" else 1,
                deleg_delta=probe.get("deleg_delta"),
                timeout=timeout,
            ):
                return 1

        log.info("[%s] PASSED", tag)
        return 0
    finally:
        # Always: end grace → close C2 → close C1 → drain (even on try failure).
        announce(f"[{tag}] wait grace end then clean close (C2 → C1 → drain)")
        try:
            unblock_cluster_grace(ctx)
            if grace_since is not None:
                if wait_grace_exit(ctx, grace_since, timeout=150):
                    log.warning("[%s] grace exit wait timed out before close", tag)
        except Exception as exc:
            log.warning("[%s] unblock/wait grace before close: %s", tag, exc)

        if peers and node_id is not None:
            drain_since = _stamp(peers)
        for item in c2_blocked:
            sess = item.get("sess")
            if not sess:
                continue
            _close_and_stop(
                sess,
                indexes=[WF_FILE_IDX],
                unlock_indexes=item.get("unlock") or [],
            )
            item["sess"] = None
        if c2_ok_sess:
            _close_and_stop(
                c2_ok_sess, indexes=[WF_FILE_IDX], unlock_indexes=c2_ok_unlock
            )
            c2_ok_sess = None
        if c1_sess:
            if not c1_sess.stop():
                log.warning("[%s] C1 open_share did not report CLEAN_CLOSE", tag)
            else:
                log.info("[%s] C1 open_share CLEAN_CLOSE ok", tag)
            c1_sess = None
        release_cluster_grace(ctx)
        drain_deleg = 0 if use_deleg else None
        if drain_since is not None and _wait_tsm_drained(
            ctx,
            f"{tag} post-cleanup",
            since=drain_since,
            expect_deleg=drain_deleg,
        ):
            ctx.tsm_drain_failed = True


def run(ceph_cluster, **kw):
    """Deploy TSM cluster and run share-deny grace WORKFLOWS."""
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
            {"precheck": "failed"}, title="TSM GRACE SHARE-DENY SUMMARY"
        )
    if len(clients) < MIN_CLIENTS:
        log.error("Need ≥%s clients (have %s)", MIN_CLIENTS, len(clients))
        return log_workflow_summary(
            {"precheck": "failed"}, title="TSM GRACE SHARE-DENY SUMMARY"
        )

    nodes = nfs_nodes[:nfs_count]
    use_clients = clients[:MIN_CLIENTS]
    name = "tsmg-sdeny"
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
            announce("Do workflow [%s] — %s" % (tc_id, spec["desc"]))
            if getattr(ctx, "tsm_drain_failed", False):
                log.error(
                    "[%s] skipped: prior workflow TSM did not drain to open=0",
                    tc_id,
                )
                results[tc_id] = "failed"
                continue
            try:
                rc = tc_share_deny(ctx, dict(spec, id=tc_id), nfs_version=nfs_version)
                results[tc_id] = "passed" if rc == 0 else "failed"
            except Exception as exc:
                log.error("[%s] FAILED: %s", tc_id, exc)
                results[tc_id] = "failed"
            if getattr(ctx, "tsm_drain_failed", False) and results[tc_id] == "passed":
                log.error("[%s] PASSED body but TSM drain failed", tc_id)
                results[tc_id] = "failed"
            if results[tc_id] == "passed" and check_coredumps(
                nodes, coredump_since, tc_id
            ):
                results[tc_id] = "failed"
            _, coredump_since = get_node_time(nodes)
    except Exception as exc:
        log.error("Share-deny grace suite FAILED: %s", exc)
        results["suite"] = "failed"
    finally:
        cleanup_grace_suite(
            ctx, use_clients, mount, nfs_name, export, nodes, safe_cleanup
        )

    return log_workflow_summary(results, title="TSM GRACE SHARE-DENY SUMMARY")
