"""NFS TSM record validation workflows.

Deploy a 2-daemon TSM cluster once, mount, then for each workflow:
  1. Read peer TSM record summary
  2. Open / hold / lock on the client (keep PID running)
  3. Wait until peer summary matches expected deltas

After all workflows: close every PID, wait for baseline, then cleanup.
"""

import time

from tests.nfs.lib.common_lib import get_node_time
from tests.nfs.lib.tsm.constants import NfsFcntlLock, NfsFdHold
from tests.nfs.lib.tsm.helpers import announce, check_coredumps, fmt_tsm
from tests.nfs.lib.tsm.helpers import kill_holds as _kill_holds
from tests.nfs.lib.tsm.helpers import log_workflow_summary
from tests.nfs.lib.tsm.helpers import mount_clients as _mount_clients
from tests.nfs.lib.tsm.helpers import peer_summary as _peer_summary
from tests.nfs.lib.tsm.helpers import start_bg as _bg
from tests.nfs.lib.tsm.helpers import wait_counts_ok as _wait_counts
from tests.nfs.tsm.test_nfs_tsm_basic import deploy_step, safe_cleanup
from utility.log import Log

log = Log(__name__)

NFS_PORT = 12050
TSM_PORT = 36371

# ---------------------------------------------------------------------------
# Each workflow: background hold cmd(s) + expected open/lock delta.
# PIDs are kept until all workflows finish; then closed together.
# ---------------------------------------------------------------------------
WORKFLOWS = {
    # Write-open hold (share_access=2) — open +1
    "write_open": {
        "delta_open": 1,
        "delta_lock": 0,
        "cmds": [
            NfsFdHold.write_create("{mount}/tsm_hold.txt"),
        ],
    },
    # Second write-open hold — open +1
    "open_hold_close": {
        "delta_open": 1,
        "delta_lock": 0,
        "cmds": [
            NfsFdHold.write_create("{mount}/tsm_hold_close.txt"),
        ],
    },
    # Read-only open (share_access=1) — open +1
    "read_open": {
        "delta_open": 1,
        "delta_lock": 0,
        "setup": [
            "bash -c 'echo data > {mount}/r_base.txt'",
        ],
        "cmds": [
            NfsFdHold.read_open("{mount}/r_base.txt"),
        ],
    },
    # Read+write open — open +1
    "read_write_open": {
        "delta_open": 1,
        "delta_lock": 0,
        "cmds": [
            NfsFdHold.write_rw("{mount}/rw_$$.txt"),
        ],
    },
    # Open + exclusive fcntl lock — open +1, lock +1
    "open_lock": {
        "delta_open": 1,
        "delta_lock": 1,
        "cmds": [
            NfsFcntlLock.exclusive_hold("{mount}/excl_lock.txt"),
        ],
    },
    # Two opens same file same owner — open +1
    "two_opens_same_file": {
        "delta_open": 1,
        "delta_lock": 0,
        "cmds": [
            "bash -c 'f={mount}/multi_$$.txt; exec 3>$f; exec 4>$f; sleep infinity'",
        ],
    },
    # Two opens same file different owners — open +2
    "two_opens_diff_owner": {
        "delta_open": 2,
        "delta_lock": 0,
        "min_clients": 1,
        "setup": [
            "bash -c 'id testuser >/dev/null 2>&1 || useradd -m testuser'",
            "bash -c 'touch {mount}/multi_owner.txt; chmod 666 {mount}/multi_owner.txt'",
        ],
        "cmds": [
            NfsFdHold.write_create("{mount}/multi_owner.txt"),
            "bash -c 'sudo -u testuser bash -c \"exec 3<{mount}/multi_owner.txt; sleep infinity\"'",
        ],
    },
    # Two opens same file from two clients — open +2
    "two_opens_two_clients": {
        "delta_open": 2,
        "delta_lock": 0,
        "min_clients": 2,
        "setup": [
            "bash -c 'touch {mount}/share.txt; chmod 666 {mount}/share.txt'",
        ],
        "cmds": [
            {"client": 0, "cmd": NfsFdHold.write_create("{mount}/share.txt")},
            {"client": 1, "cmd": NfsFdHold.write_append("{mount}/share.txt")},
        ],
    },
    # mmap hold — open +1
    "mmap_open": {
        "delta_open": 1,
        "delta_lock": 0,
        "cmds": [
            (
                'python3 -c "'
                "import mmap,os,time;"
                "p='{mount}/mmap_%d.txt'%time.time();"
                "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o644);"
                "os.write(fd,b'x'*4096);os.lseek(fd,0,0);"
                'm=mmap.mmap(fd,4096);time.sleep(10**9)"'
            ),
        ],
    },
    # Directory open — must NOT increase open counter
    "directory_open": {
        "delta_open": 0,
        "delta_lock": 0,
        "cmds": [
            "bash -c 'exec 3<{mount}; sleep infinity'",
        ],
    },
    # A holds exclusive lock; B opens then waits (blocked).
    # B's open is counted even while fcntl blocks → open +2, lock +1.
    # A holds lock; B open then CLOSE while blocked → net open +1, lock +1
    "two_locks_unlock": {
        "delta_open": 1,
        "delta_lock": 1,
        "min_clients": 2,
        "custom": "two_locks_unlock",
    },
    # Byte-range exclusive lock — open +1, lock +1
    "byte_range_lock": {
        "delta_open": 1,
        "delta_lock": 1,
        "setup": [
            (
                'python3 -c "'
                "import os;"
                "p='{mount}/br_lock.txt';"
                "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o644);"
                "os.write(fd,b'x'*4096);os.close(fd)\""
            ),
        ],
        "cmds": [
            NfsFcntlLock.byte_range_hold("{mount}/br_lock.txt", 100, 0),
        ],
    },
    # O_TRUNC write open — open +1
    "trunc_open": {
        "delta_open": 1,
        "delta_lock": 0,
        "setup": [
            "bash -c 'echo seed > {mount}/trunc.txt'",
        ],
        "cmds": [
            (
                'python3 -c "'
                "import os,time;"
                "fd=os.open('{mount}/trunc.txt',os.O_WRONLY|os.O_TRUNC);"
                'time.sleep(10**9)"'
            ),
        ],
    },
    # Append-only open — open +1
    "append_open": {
        "delta_open": 1,
        "delta_lock": 0,
        "setup": [
            "bash -c 'echo seed > {mount}/append.txt'",
        ],
        "cmds": [
            NfsFdHold.write_append("{mount}/append.txt"),
        ],
    },
    # Non-overlapping byte-range locks — expect open +2, lock +2
    "two_byte_range_locks": {
        "delta_open": 2,
        "delta_lock": 2,
        "min_clients": 2,
        "setup": [
            (
                'python3 -c "'
                "import os;"
                "p='{mount}/br_pair.txt';"
                "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o666);"
                "os.write(fd,b'x'*4096);os.close(fd)\""
            ),
        ],
        "cmds": [
            {
                "client": 0,
                "cmd": NfsFcntlLock.byte_range_hold("{mount}/br_pair.txt", 100, 0),
            },
            {
                "client": 1,
                "cmd": NfsFcntlLock.byte_range_hold("{mount}/br_pair.txt", 100, 200),
            },
        ],
    },
    # Shared fcntl lock — expect open +1, lock +1
    "shared_lock": {
        "delta_open": 1,
        "delta_lock": 1,
        "setup": [
            "bash -c 'touch {mount}/shared_lock.txt; chmod 666 {mount}/shared_lock.txt'",
        ],
        "cmds": [
            NfsFcntlLock.shared_hold("{mount}/shared_lock.txt"),
        ],
    },
    # Two shared locks, two clients — expect open +2, lock +2
    "two_shared_locks_two_clients": {
        "delta_open": 2,
        "delta_lock": 2,
        "min_clients": 2,
        "setup": [
            "bash -c 'touch {mount}/shared_pair.txt; chmod 666 {mount}/shared_pair.txt'",
        ],
        "cmds": [
            {
                "client": 0,
                "cmd": NfsFcntlLock.shared_hold("{mount}/shared_pair.txt"),
            },
            {
                "client": 1,
                "cmd": NfsFcntlLock.shared_hold("{mount}/shared_pair.txt"),
            },
        ],
    },
    # # Shared lock on write open — open +1, lock +1
    "shared_on_write_open": {
        "delta_open": 1,
        "delta_lock": 1,
        "cmds": [
            NfsFcntlLock.shared_hold("{mount}/shared_wo.txt", mode="w+"),
        ],
    },
    # # Exclusive lock on read-write open — open +1, lock +1
    "exclusive_on_rw_open": {
        "delta_open": 1,
        "delta_lock": 1,
        "setup": [
            "bash -c 'echo data > {mount}/excl_rw.txt; chmod 666 {mount}/excl_rw.txt'",
        ],
        "cmds": [
            NfsFcntlLock.exclusive_hold("{mount}/excl_rw.txt", mode="r+"),
        ],
    },
    # Shared lock on read-write open — open +1, lock +1
    "shared_on_rw_open": {
        "delta_open": 1,
        "delta_lock": 1,
        "setup": [
            "bash -c 'echo data > {mount}/shared_rw.txt; chmod 666 {mount}/shared_rw.txt'",
        ],
        "cmds": [
            NfsFcntlLock.shared_hold("{mount}/shared_rw.txt", mode="r+"),
        ],
    },
    # Shared byte-range lock — open +1, lock +1
    "byte_range_shared": {
        "delta_open": 1,
        "delta_lock": 1,
        "setup": [
            (
                'python3 -c "'
                "import os;"
                "p='{mount}/br_shared.txt';"
                "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o666);"
                "os.write(fd,b'x'*4096);os.close(fd)\""
            ),
        ],
        "cmds": [
            NfsFcntlLock.byte_range_hold("{mount}/br_shared.txt", 100, 0, shared=True),
        ],
    },
    # Two shared byte-range locks (same range, two clients) — open +2, lock +2
    "two_byte_range_shared": {
        "delta_open": 2,
        "delta_lock": 2,
        "min_clients": 2,
        "setup": [
            (
                'python3 -c "'
                "import os;"
                "p='{mount}/br_shared_pair.txt';"
                "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o666);"
                "os.write(fd,b'x'*4096);os.close(fd)\""
            ),
        ],
        "cmds": [
            {
                "client": 0,
                "cmd": NfsFcntlLock.byte_range_hold(
                    "{mount}/br_shared_pair.txt", 100, 0, shared=True
                ),
            },
            {
                "client": 1,
                "cmd": NfsFcntlLock.byte_range_hold(
                    "{mount}/br_shared_pair.txt", 100, 0, shared=True
                ),
            },
        ],
    },
    # A exclusive [0,100); B reads unlocked [200,300) — open +2, lock +1
    "byte_range_peer_read_unlocked": {
        "delta_open": 2,
        "delta_lock": 1,
        "min_clients": 2,
        "setup": [
            (
                'python3 -c "'
                "import os;"
                "p='{mount}/br_access.txt';"
                "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o666);"
                "os.write(fd,b'x'*4096);os.close(fd)\""
            ),
        ],
        "cmds": [
            {
                "client": 0,
                "cmd": NfsFcntlLock.byte_range_hold("{mount}/br_access.txt", 100, 0),
            },
            {
                "client": 1,
                "cmd": NfsFcntlLock.read_at_offset_hold(
                    "{mount}/br_access.txt", 200, 100
                ),
            },
        ],
    },
    # Overlapping exclusive byte-range: B open then CLOSE while blocked — open +1, lock +1
    "byte_range_overlap_block": {
        "delta_open": 1,
        "delta_lock": 1,
        "min_clients": 2,
        "custom": "byte_range_overlap_block",
    },
    # # Unlock without close: lock then unlock, FD stays — end open +1, lock +0
    "unlock_without_close": {
        "delta_open": 1,
        "delta_lock": 0,
        "custom": "unlock_without_close",
    },
}


def _fmt(cmd, mount):
    return cmd.format(mount=mount)


def _run_setup(clients, setup, mount):
    for raw in setup or []:
        clients[0].exec_command(sudo=True, cmd=_fmt(raw, mount), check_ec=False)


def _start_holds(clients, cmds, mount):
    started = []
    for entry in cmds:
        if isinstance(entry, dict):
            idx = entry.get("client", 0)
            cmd = _fmt(entry["cmd"], mount)
        else:
            idx = 0
            cmd = _fmt(entry, mount)
        if idx >= len(clients):
            log.error("Need client index %s, have %s", idx, len(clients))
            return None
        pid = _bg(clients[idx], cmd)
        if not pid:
            return None
        started.append((clients[idx], pid))
    return started


def _run_two_locks_unlock(clients, peers, nfs_name, mount, cur_open, cur_lock):
    """A holds exclusive lock; B OPEN then CLOSE while blocked → net open +1."""
    lockfile = f"{mount}/lock_pair_{int(time.time())}.txt"
    hold_since, _ = get_node_time(peers)

    a_pid = _bg(clients[0], NfsFcntlLock.exclusive_hold(lockfile))
    if not a_pid:
        return "failed", []
    announce(
        "Do lock hold [two_locks_unlock:A]",
        fmt_tsm(cur_open + 1, cur_lock + 1),
    )
    if _wait_counts(
        peers,
        nfs_name,
        hold_since,
        cur_open + 1,
        cur_lock + 1,
        "two_locks_unlock:A-hold",
    ):
        return "failed", [(clients[0], a_pid)]

    b_since, _ = get_node_time(peers)
    announce(
        "Do lock hold [two_locks_unlock:B]",
        fmt_tsm(cur_open + 1, cur_lock + 1),
    )
    b_pid = _bg(clients[1], NfsFcntlLock.exclusive_hold(lockfile))
    if not b_pid:
        return "failed", [(clients[0], a_pid)]
    # B briefly OPEN (+1) then CLOSE (-1) while lock-blocked; settle at A's counts.
    if _wait_counts(
        peers,
        nfs_name,
        b_since,
        cur_open + 1,
        cur_lock + 1,
        "two_locks_unlock:B-open-blocked",
    ):
        return "failed", [(clients[0], a_pid), (clients[1], b_pid)]

    log.info("two_locks_unlock: B settled open+1-1 (pids kept until final close)")
    return "passed", [(clients[0], a_pid), (clients[1], b_pid)]


def _run_byte_range_overlap_block(clients, peers, nfs_name, mount, cur_open, cur_lock):
    """A exclusive [0,100); B overlapping OPEN then CLOSE while blocked → net open +1."""
    path = f"{mount}/br_overlap.txt"
    clients[0].exec_command(
        sudo=True,
        cmd=(
            'python3 -c "'
            f"import os;p='{path}';"
            "fd=os.open(p,os.O_RDWR|os.O_CREAT,0o666);"
            "os.write(fd,b'x'*4096);os.close(fd)\""
        ),
        check_ec=False,
    )
    time.sleep(5)
    hold_since, _ = get_node_time(peers)
    a_pid = _bg(clients[0], NfsFcntlLock.byte_range_hold(path, 100, 0))
    if not a_pid:
        return "failed", []
    announce(
        "Do byte-range hold [overlap:A]",
        fmt_tsm(cur_open + 1, cur_lock + 1),
    )
    if _wait_counts(
        peers,
        nfs_name,
        hold_since,
        cur_open + 1,
        cur_lock + 1,
        "byte_range_overlap_block:A",
    ):
        return "failed", [(clients[0], a_pid)]

    b_since, _ = get_node_time(peers)
    b_pid = _bg(clients[1], NfsFcntlLock.byte_range_hold(path, 100, 50))
    if not b_pid:
        return "failed", [(clients[0], a_pid)]
    # B briefly OPEN (+1) then CLOSE (-1) while lock-blocked; settle at A's counts.
    if _wait_counts(
        peers,
        nfs_name,
        b_since,
        cur_open + 1,
        cur_lock + 1,
        "byte_range_overlap_block:B-blocked",
    ):
        return "failed", [(clients[0], a_pid), (clients[1], b_pid)]

    log.info(
        "byte_range_overlap_block: B settled open+1-1 (pids kept until final close)"
    )
    return "passed", [(clients[0], a_pid), (clients[1], b_pid)]


def _run_unlock_without_close(clients, peers, nfs_name, mount, cur_open, cur_lock):
    """LOCK_EX then LOCK_UN; FD stays open → open +1, lock back to cur."""
    path = f"{mount}/unlock_hold.txt"
    hold_since, _ = get_node_time(peers)
    announce(
        "Do lock-then-unlock hold [unlock_without_close]",
        fmt_tsm(cur_open + 1, cur_lock + 1),
    )
    pid = _bg(
        clients[0],
        NfsFcntlLock.exclusive_then_unlock_hold(path, hold_secs=8),
    )
    if not pid:
        return "failed", []
    holds = [(clients[0], pid)]

    if _wait_counts(
        peers,
        nfs_name,
        hold_since,
        cur_open + 1,
        cur_lock + 1,
        "unlock_without_close:while-locked",
    ):
        return "failed", holds
    if _wait_counts(
        peers,
        nfs_name,
        hold_since,
        cur_open + 1,
        cur_lock,
        "unlock_without_close:after-unlock",
    ):
        return "failed", holds

    log.info("unlock_without_close: lock dropped, open held (pid kept)")
    return "passed", holds


def run_one(
    name, spec, clients, peers, nfs_name, mount, cur_open, cur_lock, since, test_num=0
):
    """Start holds for one workflow; do not kill. Return (status, holds)."""
    log.info(
        "\n==============================\n"
        "Test %s: record workflow [%s]\n"
        "==============================",
        test_num,
        name,
    )
    min_clients = spec.get("min_clients", 1)
    if len(clients) < min_clients:
        log.warning("[%s] skip (need %s clients)", name, min_clients)
        return "skipped", []

    custom = spec.get("custom")
    if custom == "two_locks_unlock":
        return _run_two_locks_unlock(
            clients, peers, nfs_name, mount, cur_open, cur_lock
        )
    if custom == "byte_range_overlap_block":
        return _run_byte_range_overlap_block(
            clients, peers, nfs_name, mount, cur_open, cur_lock
        )
    if custom == "unlock_without_close":
        return _run_unlock_without_close(
            clients, peers, nfs_name, mount, cur_open, cur_lock
        )

    _run_setup(clients, spec.get("setup"), mount)
    time.sleep(5)

    want_open = cur_open + spec.get("delta_open", 0)
    want_lock = cur_lock + spec.get("delta_lock", 0)
    announce(
        "Do open/hold [%s]" % name,
        fmt_tsm(want_open, want_lock),
    )
    hold_since, _ = get_node_time(peers)
    started = _start_holds(clients, spec.get("cmds", []), mount)
    if started is None:
        return "failed", []

    time.sleep(2)
    d_open = spec.get("delta_open", 0)
    d_lock = spec.get("delta_lock", 0)
    if d_open == 0 and d_lock == 0:
        after = _peer_summary(peers, nfs_name, since=since)
        if after.get("seen") and (
            after["open"] != want_open or after["lock"] != want_lock
        ):
            log.error(
                "[%s] expected open=%s lock=%s unchanged", name, want_open, want_lock
            )
            return "failed", started
        log.info("[%s] no counter bump as expected (held pids kept)", name)
    elif _wait_counts(
        peers, nfs_name, hold_since, want_open, want_lock, f"{name}:while-held"
    ):
        return "failed", started

    log.info("[%s] PASSED (pids kept for final close)", name)
    return "passed", started


def _close_all_and_validate(holds, peers, nfs_name, baseline_open, baseline_lock):
    announce(
        "Do CLOSE / release hold(s)",
        fmt_tsm(baseline_open, baseline_lock),
    )
    log.info(
        "\n==============================\n"
        "Test close: final close/unlock of %s held pid(s)\n"
        "==============================",
        len(holds),
    )
    close_since, _ = get_node_time(peers)
    _kill_holds(holds)
    time.sleep(2)
    if _wait_counts(
        peers, nfs_name, close_since, baseline_open, baseline_lock, "final-close"
    ):
        return 1
    log.info("Final close/unlock: peer counts back to baseline")
    return 0


def _log_summary(results):
    return log_workflow_summary(results, title="TSM RECORD VALIDATION SUMMARY")


def run(ceph_cluster, **kw):
    """Deploy once, run workflows (holds kept), close all, always cleanup + summary."""
    config = kw.get("config") or {}
    nfs_version = str(config.get("nfs_version", "4.2"))
    nfs_nodes = sorted(ceph_cluster.get_nodes("nfs"), key=lambda n: n.hostname)
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]
    if len(nfs_nodes) < 2 or not clients:
        log.error("Need ≥2 NFS nodes and ≥1 client")
        return 1

    nodes = nfs_nodes[:2]
    use_clients = clients[:2] if len(clients) >= 2 else clients[:1]
    name = "tsmr-record"
    nfs_name = f"cephfs-nfs-{name}"
    export = f"/export_{name}"
    mount = f"/mnt/nfs_{name}"
    results = {}
    all_holds = []
    peers = nodes[1:]
    rc = 1

    try:
        result = deploy_step(
            ceph_cluster,
            installer,
            use_clients[0],
            nodes,
            {"nfs_count": 2, "nfs_port": NFS_PORT, "tsm_port": TSM_PORT},
            nfs_name,
            name,
        )
        if result is None:
            results["deploy"] = "failed"
            return _log_summary(results)

        _, active, nfs_port, _ = result
        server, peers = active[0], active[1:]

        if _mount_clients(
            use_clients,
            nfs_name,
            export,
            mount,
            nfs_port,
            server.hostname,
            nfs_version=nfs_version,
        ):
            results["mount"] = "failed"
            return _log_summary(results)

        since, _ = get_node_time(peers)
        baseline = _peer_summary(peers, nfs_name, since=since)
        cur_open, cur_lock = baseline["open"], baseline["lock"]
        log.info("Initial baseline: open=%s lock=%s", cur_open, cur_lock)

        _, coredump_since = get_node_time(nodes)
        for i, (wf_name, spec) in enumerate(WORKFLOWS.items(), 1):
            status, holds = run_one(
                wf_name,
                spec,
                use_clients,
                peers,
                nfs_name,
                mount,
                cur_open,
                cur_lock,
                since,
                test_num=i,
            )
            if status == "passed" and check_coredumps(nodes, coredump_since, wf_name):
                status = "failed"
            results[wf_name] = status
            all_holds.extend(holds)
            _, coredump_since = get_node_time(nodes)
            if holds:
                snap = _peer_summary(peers, nfs_name, since=since)
                if snap.get("seen"):
                    cur_open, cur_lock = snap["open"], snap["lock"]
                    log.info(
                        "[%s] synced cur from peer: open=%s lock=%s",
                        wf_name,
                        cur_open,
                        cur_lock,
                    )
                elif status == "passed":
                    cur_open += spec.get("delta_open", 0)
                    cur_lock += spec.get("delta_lock", 0)
            elif status == "passed":
                cur_open += spec.get("delta_open", 0)
                cur_lock += spec.get("delta_lock", 0)

        if _close_all_and_validate(
            all_holds, peers, nfs_name, baseline["open"], baseline["lock"]
        ):
            results["final_close"] = "failed"
        else:
            results["final_close"] = "passed"
            all_holds = []  # already killed in close step
        if results.get("final_close") == "passed" and check_coredumps(
            nodes, coredump_since, "final_close"
        ):
            results["final_close"] = "failed"

        rc = _log_summary(results)
    except Exception as exc:
        log.error("Record validation FAILED: %s", exc)
        results.setdefault("error", "failed")
        rc = _log_summary(results)
    finally:
        _kill_holds(all_holds)
        safe_cleanup(use_clients[0], mount, nfs_name, export, nodes)
        for client in use_clients[1:]:
            client.exec_command(
                sudo=True,
                cmd="timeout 30 umount -l %s 2>/dev/null || true" % mount,
                check_ec=False,
                timeout=45,
            )

    return rc
