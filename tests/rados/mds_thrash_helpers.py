"""
MDS thrash helpers for holistic daemon chaos.

Used by ``test_osd_thrashing`` via ``thrash_mds``. Default ops match the legacy
three-op loop (failover, rolling_restart, random_fail). Extra ops are opt-in
via suite ``mds_operations``.

CephFS FIO/snap threads in the parent test provide baseline IO. Ops ``dir_pin``
and ``cap_revoke_failover`` need ``client_node`` + ``cephfs_mount_path``.
``multi_fs_fail`` needs >= 2 filesystems in ``fs_names``. Power-cycle is not
implemented.
"""

from __future__ import annotations

import random
import shlex
import time
from typing import Any, Dict, List, Optional, Set

from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.thrash_helpers import _kill_daemon, _sleep_with_stop
from utility.log import Log

log = Log(__name__)

_DEFAULT_OPS = ["failover", "rolling_restart", "random_fail"]


def post_duration_slack(n_ops: int) -> int:
    """Seconds past suite ``duration`` for fair-bag drain / hard deadline."""
    return max(300, max(1, int(n_ops)) * 90)


# (config key, min, max) — integers; cache limit in bytes
_CONFIG_POOL = [
    ("mds_cache_memory_limit", 1 << 30, 8 << 30),
    ("mds_bal_interval", 5, 60),
    ("mds_log_max_segments", 32, 256),
    ("mds_beacon_grace", 10, 45),
    ("mds_recall_max_caps", 1000, 50000),
    ("mds_session_timeout", 30, 120),
]


def kwargs_from_config(config: dict, cluster_fsid: Optional[str] = None) -> dict:
    """Map suite flat knobs to ``thrash_mds`` kwargs."""
    out: Dict[str, Any] = {"cluster_fsid": cluster_fsid}
    if config.get("mds_operations") is not None:
        out["operations"] = config["mds_operations"]
    for src, dst, default in (
        ("mds_kill_count", "kill_count", 5),
        ("mds_kill_interval", "kill_interval", 5),
        ("mds_fail_count", "fail_count", 1),
        ("mds_scale_wait", "scale_wait", 20),
        ("mds_recovery_wait", "recovery_wait", 15),
        ("mds_pin_dirs", "pin_dirs", 20),
        ("mds_cap_workers", "cap_workers", 4),
    ):
        out[dst] = config.get(src, default)
    if config.get("mds_scale_path") is not None:
        out["scale_path"] = config["mds_scale_path"]
    out["scale_during_promote"] = bool(config.get("mds_scale_during_promote", False))
    return out


# ---------------------------------------------------------------------------
# Status / wait / mutate primitives
# ---------------------------------------------------------------------------


def _is_active_state(state: str) -> bool:
    s = str(state or "").lower()
    return s.startswith("active") or "up:active" in s


def _is_standby_replay_state(state: str) -> bool:
    s = str(state or "").lower().replace("_", "-")
    return "standby-replay" in s


def _is_standby_state(state: str) -> bool:
    s = str(state or "").lower()
    if _is_standby_replay_state(s):
        return False
    return s == "standby" or s.endswith(":standby") or s.endswith("standby")


def _mds_status(
    rados_obj: RadosOrchestrator, fs_name: str, *, full: bool = True
) -> Dict[str, Any]:
    """Return actives (all ranks), standbys, standby-replay, and optionally max_mds/flags.

    Args:
        full: If True, also fetch ``ceph fs get`` for max_mds / allow_standby_replay.
              Pass False on hot poll paths (wait/kill loops).
    """
    empty = {
        "actives": [],
        "standbys": [],
        "standby_replay": [],
        "max_mds": 1,
        "allow_standby_replay": False,
    }
    try:
        fs_status = rados_obj.run_ceph_command(cmd=f"ceph fs status {fs_name}") or {}
        actives: List[Dict] = []
        standbys: List[str] = []
        standby_replay: List[Dict] = []
        for mds in fs_status.get("mdsmap", []) or []:
            name = mds.get("name")
            state = str(mds.get("state", ""))
            entry = {"name": name, "rank": mds.get("rank", -1), "state": state}
            if _is_standby_replay_state(state):
                standby_replay.append(entry)
            elif _is_active_state(state):
                actives.append(entry)
            elif _is_standby_state(state) and name:
                standbys.append(name)

        result = {
            "actives": actives,
            "standbys": standbys,
            "standby_replay": standby_replay,
            "max_mds": max(1, len(actives)) if actives else 1,
            "allow_standby_replay": False,
        }
        if not full:
            return result

        try:
            info = rados_obj.run_ceph_command(cmd=f"ceph fs get {fs_name}") or {}
            mdsmap = info.get("mdsmap", info) or {}
            result["max_mds"] = int(mdsmap.get("max_mds", result["max_mds"]) or 1)
            flags = mdsmap.get("flags_state") or {}
            result["allow_standby_replay"] = bool(
                flags.get("allow_standby_replay", False)
            )
        except Exception as err:
            log.debug("fs get for %s failed: %s", fs_name, err)
        return result
    except Exception as e:
        log.error("Failed to get MDS status for %s: %s", fs_name, e)
        return empty


def _wait_active(
    rados_obj: RadosOrchestrator,
    fs_name: str,
    expected: int,
    timeout: int,
    stop_flag: Dict,
) -> bool:
    """Poll until active count >= expected or timeout / stop."""
    deadline = time.time() + max(1, timeout)
    while time.time() < deadline and not stop_flag.get("stop"):
        if len(_mds_status(rados_obj, fs_name, full=False)["actives"]) >= expected:
            return True
        _sleep_with_stop(stop_flag, total_seconds=5, step=2)
    have = len(_mds_status(rados_obj, fs_name, full=False)["actives"])
    log.warning("[%s] wait_active timeout: have %s want >= %s", fs_name, have, expected)
    return False


def _ceph_shell(rados_obj: RadosOrchestrator, cmd: str) -> None:
    """Run a ceph CLI that may not return useful JSON."""
    rados_obj.node.shell([cmd])


def _set_max_mds(rados_obj: RadosOrchestrator, fs_name: str, n: int) -> None:
    n = max(1, int(n))
    cmd = f"ceph fs set {fs_name} max_mds {n} --yes-i-really-mean-it"
    try:
        _ceph_shell(rados_obj, cmd)
    except Exception:
        _ceph_shell(rados_obj, f"ceph fs set {fs_name} max_mds {n}")


def _set_mds_config(rados_obj: RadosOrchestrator, key: str, value) -> None:
    _ceph_shell(rados_obj, f"ceph config set mds {key} {value}")


def _get_mds_config(rados_obj: RadosOrchestrator, key: str) -> Optional[str]:
    # Shell only: run_ceph_command appends ``-f json``, but scalar MDS
    # options (e.g. mds_session_timeout) return a bare int and blow up parsing.
    try:
        out, _ = rados_obj.node.shell([f"ceph config get mds {key}"])
        val = str(out).strip() if out is not None else ""
        return val or None
    except Exception as e:
        log.debug("config get %s failed: %s", key, e)
        return None


def _fail_mds(rados_obj: RadosOrchestrator, target) -> None:
    # Prefer shell: run_ceph_command always appends -f json
    try:
        _ceph_shell(rados_obj, f"ceph mds fail {target}")
    except Exception:
        rados_obj.run_ceph_command(cmd=f"ceph mds fail {target}")


def _host_from_mds_name(name: str) -> str:
    """Parse host from daemon name ``<fs>.<host>.<id>``."""
    parts = str(name or "").split(".")
    return parts[1] if len(parts) >= 2 else "unknown"


def _fail_ranks(
    rados_obj: RadosOrchestrator,
    fs_name: str,
    mode: str,
    n: int = 1,
    stop_flag: Optional[Dict] = None,
    recovery_wait: int = 15,
) -> bool:
    """Fail MDS ranks. mode: rank0 | random | colocated."""
    stop_flag = stop_flag or {}
    status = _mds_status(rados_obj, fs_name, full=False)
    actives = status["actives"]
    standbys = status["standbys"]

    if mode == "rank0":
        if not actives:
            log.warning("[%s] failover: no active MDS", fs_name)
            return False
        if not standbys:
            log.warning("[%s] failover: no standby available", fs_name)
            return False
        preferred = next((a for a in actives if a.get("rank") == 0), actives[0])
        before = preferred["name"]
        log.info("[%s] failing rank0/active %s", fs_name, before)
        _fail_mds(rados_obj, before)
        _sleep_with_stop(stop_flag, recovery_wait, step=3)
        after_names = {
            a["name"] for a in _mds_status(rados_obj, fs_name, full=False)["actives"]
        }
        if after_names and before not in after_names:
            return True
        # Multi-rank: same rank may return under a new daemon name
        return bool(after_names)

    if mode == "colocated":
        by_host: Dict[str, List] = {}
        for a in actives:
            by_host.setdefault(_host_from_mds_name(a.get("name")), []).append(a)
        candidates = [v for v in by_host.values() if len(v) >= 2]
        if not candidates:
            log.warning("[%s] colocated_fail: no host with 2+ actives", fs_name)
            return False
        pair = random.sample(random.choice(candidates), 2)
        for a in pair:
            log.info(
                "[%s] colocated fail rank=%s name=%s",
                fs_name,
                a.get("rank"),
                a.get("name"),
            )
            _fail_mds(rados_obj, a.get("rank", a.get("name")))
        _sleep_with_stop(stop_flag, recovery_wait, step=3)
        return bool(_mds_status(rados_obj, fs_name, full=False)["actives"])

    # random — keep legacy guard: need >=2 targets when failing one
    pool = [a["name"] for a in actives if a.get("name")] + list(standbys)
    n = max(1, min(int(n), len(pool))) if pool else 0
    if len(pool) < max(2, n):
        log.warning(
            "[%s] random_fail: need >=%s MDS targets, have %s",
            fs_name,
            max(2, n),
            len(pool),
        )
        return False
    targets = random.sample(pool, n)
    for t in targets:
        log.info("[%s] random fail %s", fs_name, t)
        _fail_mds(rados_obj, t)
    _sleep_with_stop(stop_flag, recovery_wait, step=3)
    return bool(_mds_status(rados_obj, fs_name, full=False)["actives"])


def _snapshot_state(
    rados_obj: RadosOrchestrator, fs_names: List[str]
) -> Dict[str, Any]:
    """Snapshot per-FS max_mds / standby_replay and current MDS config values."""
    fs_snap = {}
    for fs in fs_names:
        st = _mds_status(rados_obj, fs, full=True)
        fs_snap[fs] = {
            "max_mds": st["max_mds"],
            "allow_standby_replay": st["allow_standby_replay"],
        }
    cfg_snap = {}
    for key, _, _ in _CONFIG_POOL:
        val = _get_mds_config(rados_obj, key)
        if val is not None:
            cfg_snap[key] = val
    return {"filesystems": fs_snap, "mds_configs": cfg_snap}


def _restore_state(
    rados_obj: RadosOrchestrator,
    snap: Dict[str, Any],
    touched_configs: Optional[Set[str]] = None,
) -> None:
    """Restore FS flags and only MDS configs that were mutated this run."""
    for fs, state in (snap.get("filesystems") or {}).items():
        try:
            _set_max_mds(rados_obj, fs, state.get("max_mds", 1))
        except Exception as e:
            log.warning("restore max_mds on %s failed: %s", fs, e)
        try:
            flag = "true" if state.get("allow_standby_replay") else "false"
            _ceph_shell(rados_obj, f"ceph fs set {fs} allow_standby_replay {flag}")
        except Exception as e:
            log.warning("restore allow_standby_replay on %s failed: %s", fs, e)

    cfg_snap = snap.get("mds_configs") or {}
    keys = touched_configs if touched_configs is not None else set(cfg_snap)
    for key in keys:
        if key not in cfg_snap:
            continue
        try:
            _set_mds_config(rados_obj, key, cfg_snap[key])
        except Exception as e:
            log.debug("restore config %s failed: %s", key, e)


def _resolve_mds_daemon_id(
    rados_obj: RadosOrchestrator, mds_name: str
) -> Optional[str]:
    """Map MDS name to orch daemon_id for systemctl kill.

    systemd unit is ``ceph-<fsid>@mds.<daemon_id>.service`` where daemon_id
    matches the MDS name (e.g. ``cephfs.folio01.erbrpo``).
    """
    try:
        daemons = rados_obj.run_ceph_command(cmd="ceph orch ps --daemon_type mds") or []
        for d in daemons:
            did = str(d.get("daemon_id", ""))
            dname = str(d.get("daemon_name", ""))
            if mds_name in (did, dname) or dname.endswith(mds_name) or did == mds_name:
                return did
            if mds_name and (mds_name in did or mds_name in dname):
                return did
    except Exception as e:
        log.warning("orch ps mds failed: %s", e)
    return mds_name


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _op_rolling_restart(rados_obj, fs_name, stop_flag, **_):
    log.info("[%s] MDS rolling restart via orch", fs_name)
    ok = rados_obj.restart_daemon_services(daemon="mds")
    _sleep_with_stop(stop_flag, 30, step=5)
    return bool(ok)


def _op_failover(rados_obj, fs_name, stop_flag, recovery_wait=15, **_):
    return _fail_ranks(
        rados_obj, fs_name, "rank0", stop_flag=stop_flag, recovery_wait=recovery_wait
    )


def _op_random_fail(rados_obj, fs_name, stop_flag, fail_count=1, recovery_wait=15, **_):
    return _fail_ranks(
        rados_obj,
        fs_name,
        "random",
        n=fail_count,
        stop_flag=stop_flag,
        recovery_wait=recovery_wait,
    )


def _op_colocated_fail(rados_obj, fs_name, stop_flag, recovery_wait=15, **_):
    return _fail_ranks(
        rados_obj,
        fs_name,
        "colocated",
        stop_flag=stop_flag,
        recovery_wait=recovery_wait,
    )


def _op_kill_loop(
    rados_obj,
    fs_name,
    stop_flag,
    cluster_fsid=None,
    kill_count=5,
    kill_interval=5,
    recovery_wait=15,
    **_,
):
    if not cluster_fsid:
        try:
            cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        except Exception as e:
            log.warning("[%s] kill_loop: no fsid: %s", fs_name, e)
            return False
    status = _mds_status(rados_obj, fs_name, full=False)
    if not status["actives"]:
        log.warning("[%s] kill_loop: no actives", fs_name)
        return False
    killed = 0
    for i in range(max(1, int(kill_count))):
        if stop_flag.get("stop"):
            break
        status = _mds_status(rados_obj, fs_name, full=False)
        if not status["actives"]:
            break
        target = random.choice(status["actives"])
        name = target.get("name")
        daemon_id = _resolve_mds_daemon_id(rados_obj, name)
        log.info(
            "[%s] kill_loop %s/%s -> %s (id=%s)",
            fs_name,
            i + 1,
            kill_count,
            name,
            daemon_id,
        )
        if _kill_daemon(rados_obj, "mds", str(daemon_id), cluster_fsid):
            killed += 1
        if i + 1 < kill_count:
            _sleep_with_stop(stop_flag, kill_interval, step=2)
    if not _wait_active(rados_obj, fs_name, 1, recovery_wait * 2, stop_flag):
        log.warning("[%s] kill_loop: wait_active failed after kills", fs_name)
        return False
    return killed > 0


def _op_max_mds_scale(
    rados_obj, fs_name, stop_flag, scale_path=None, scale_wait=20, **_
):
    status = _mds_status(rados_obj, fs_name, full=True)
    hi = status["max_mds"] or 1
    path = list(scale_path) if scale_path else [hi, 1, hi]
    log.info("[%s] max_mds_scale path=%s", fs_name, path)
    for target in path:
        if stop_flag.get("stop"):
            break
        _set_max_mds(rados_obj, fs_name, target)
        if not _wait_active(
            rados_obj, fs_name, max(1, int(target)), max(10, int(scale_wait)), stop_flag
        ):
            log.warning(
                "[%s] max_mds_scale: wait_active failed for target=%s",
                fs_name,
                target,
            )
            return False
    return True


def _op_config_storm(
    rados_obj, fs_name, stop_flag, recovery_wait=15, touched_configs=None, **_
):
    picks = random.sample(_CONFIG_POOL, k=min(3, len(_CONFIG_POOL)))
    for key, lo, hi in picks:
        val = random.randint(lo, hi)
        log.info("[%s] config_storm set %s=%s", fs_name, key, val)
        try:
            _set_mds_config(rados_obj, key, val)
            if touched_configs is not None:
                touched_configs.add(key)
        except Exception as e:
            log.warning("config set %s failed: %s", key, e)
    return bool(
        _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    )


def _op_fs_fail_scale(
    rados_obj, fs_name, stop_flag, scale_wait=20, recovery_wait=15, **_
):
    """E5: fs fail → shrink max_mds → joinable → restore max_mds."""
    status = _mds_status(rados_obj, fs_name, full=True)
    orig = status["max_mds"] or 1
    lower = 1 if orig <= 2 else random.choice([1, max(1, orig // 2)])
    log.info(
        "[%s] fs_fail_scale: fail, max_mds %s -> %s -> %s", fs_name, orig, lower, orig
    )
    try:
        _ceph_shell(rados_obj, f"ceph fs fail {fs_name}")
    except Exception as e:
        log.warning("fs fail %s: %s", fs_name, e)
        return False
    try:
        _set_max_mds(rados_obj, fs_name, lower)
    except Exception as e:
        log.warning("max_mds shrink during fs fail: %s", e)
    try:
        _ceph_shell(rados_obj, f"ceph fs set {fs_name} joinable true")
    except Exception as e:
        log.warning("joinable true failed: %s", e)
    if not _wait_active(rados_obj, fs_name, lower, max(30, scale_wait * 2), stop_flag):
        log.warning(
            "[%s] fs_fail_scale: wait_active failed at max_mds=%s", fs_name, lower
        )
        return False
    _set_max_mds(rados_obj, fs_name, orig)
    if not _wait_active(rados_obj, fs_name, orig, max(30, scale_wait * 2), stop_flag):
        log.warning(
            "[%s] fs_fail_scale: wait_active failed restoring max_mds=%s", fs_name, orig
        )
        return False
    return True


def _op_standby_replay_fail(
    rados_obj,
    fs_name,
    stop_flag,
    scale_during_promote=False,
    scale_wait=20,
    recovery_wait=15,
    **_,
):
    """B3/D4: enable standby-replay, fail active, optional mid-promote scale."""
    status = _mds_status(rados_obj, fs_name, full=True)
    orig_max = status["max_mds"] or 1
    try:
        _ceph_shell(rados_obj, f"ceph fs set {fs_name} allow_standby_replay true")
    except Exception as e:
        log.warning("enable standby_replay failed: %s", e)
        return False
    replay = None
    for _ in range(12):
        if stop_flag.get("stop"):
            return False
        status = _mds_status(rados_obj, fs_name, full=False)
        if status["standby_replay"]:
            replay = status["standby_replay"][0]
            break
        _sleep_with_stop(stop_flag, 5, step=2)
    if not replay:
        log.warning("[%s] standby_replay_fail: no standby-replay appeared", fs_name)
        return False
    actives = status["actives"]
    if not actives:
        return False
    victim = next((a for a in actives if a.get("rank") == 0), actives[0])
    log.info(
        "[%s] standby_replay_fail: fail %s (replay=%s)",
        fs_name,
        victim.get("name"),
        replay.get("name"),
    )
    _fail_mds(rados_obj, victim.get("name"))
    if scale_during_promote:
        nudge = max(1, orig_max - 1) if orig_max > 1 else orig_max + 1
        try:
            _set_max_mds(rados_obj, fs_name, nudge)
            _sleep_with_stop(stop_flag, max(5, scale_wait // 2), step=2)
            _set_max_mds(rados_obj, fs_name, orig_max)
        except Exception as e:
            log.warning("scale_during_promote failed: %s", e)
    if not _wait_active(rados_obj, fs_name, 1, max(30, recovery_wait * 2), stop_flag):
        log.warning(
            "[%s] standby_replay_fail: wait_active failed after promote", fs_name
        )
        return False
    return True


def _ensure_pin_dirs(client_node, mount_path: str, count: int) -> List[str]:
    """Create pin_test dirs on the CephFS mount; return paths that exist."""
    base = f"{mount_path.rstrip('/')}/mds_thrash_pin"
    qb = shlex.quote(base)
    n = int(count)
    # base=<quoted>; always expand as "$base/..." (shlex.quote + concat breaks on spaces)
    client_node.exec_command(
        sudo=True,
        cmd=(
            f'base={qb}; mkdir -p "$base" && '
            f"for i in $(seq 1 {n}); do "
            f"mkdir -p \"$base\"/d_$(printf '%04d' $i); "
            f"done"
        ),
        timeout=60,
        check_ec=False,
    )
    out, _ = client_node.exec_command(
        sudo=True,
        cmd=f'base={qb}; find "$base" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -{n}',
        timeout=30,
        check_ec=False,
    )
    return [p for p in str(out).splitlines() if p.strip()]


def _op_dir_pin(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    pin_dirs=20,
    recovery_wait=15,
    **_,
):
    """A1-style: rapid ceph.dir.pin changes to force subtree export/import."""
    if not client_node or not cephfs_mount_path:
        log.warning("[%s] dir_pin: need client_node + cephfs_mount_path", fs_name)
        return False
    status = _mds_status(rados_obj, fs_name, full=False)
    n_active = len(status["actives"])
    if n_active < 2:
        log.warning("[%s] dir_pin: need >=2 active ranks (have %s)", fs_name, n_active)
        return False
    try:
        dirs = _ensure_pin_dirs(client_node, cephfs_mount_path, max(10, int(pin_dirs)))
    except Exception as e:
        log.warning("[%s] dir_pin: setup failed: %s", fs_name, e)
        return False
    if len(dirs) < 5:
        log.warning("[%s] dir_pin: too few dirs (%s)", fs_name, len(dirs))
        return False
    pins = 0
    batch = min(len(dirs), max(10, int(pin_dirs)))
    for d in random.sample(dirs, batch):
        if stop_flag.get("stop"):
            break
        rank = random.randrange(n_active)
        try:
            client_node.exec_command(
                sudo=True,
                cmd=f"setfattr -n ceph.dir.pin -v {int(rank)} -- {shlex.quote(d)}",
                timeout=15,
                check_ec=False,
            )
            pins += 1
        except Exception:
            pass
    log.info("[%s] dir_pin: applied %s pins across %s dirs", fs_name, pins, batch)
    # Race pins with a rank fail (export during failover)
    _fail_ranks(
        rados_obj,
        fs_name,
        "random",
        n=1,
        stop_flag=stop_flag,
        recovery_wait=recovery_wait,
    )
    return pins > 0


def _pkill_cap_workers(client_node) -> None:
    """Best-effort stop of cap_revoke background RW loops on the CephFS client."""
    if not client_node:
        return
    try:
        client_node.exec_command(
            sudo=True,
            cmd="pkill -f mds_thrash_cap_worker 2>/dev/null || true",
            timeout=15,
            check_ec=False,
        )
    except Exception:
        pass


def _op_cap_revoke_failover(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    cap_workers=4,
    recovery_wait=15,
    **_,
):
    """C4-lite: shared-file RW storm on mount, then MDS fail mid-contention."""
    if not client_node or not cephfs_mount_path:
        log.warning(
            "[%s] cap_revoke_failover: need client_node + cephfs_mount_path", fs_name
        )
        return False
    shared = f"{cephfs_mount_path.rstrip('/')}/mds_thrash_caps"
    qs = shlex.quote(shared)
    n_workers = max(2, int(cap_workers))
    workers_started = False
    try:
        client_node.exec_command(
            sudo=True,
            cmd=(
                f'base={qs}; mkdir -p "$base" && '
                f"for i in $(seq 1 20); do "
                f'dd if=/dev/urandom of="$base/f_$i" bs=4k count=4 status=none 2>/dev/null; '
                f"done"
            ),
            timeout=60,
            check_ec=False,
        )
        # Background readers/writers (cap Fr/Fw contention).
        worker_script = (
            f"base={qs}; "
            "while true; do "
            'f=$(ls "$base"/f_* 2>/dev/null | shuf | head -1); '
            '[ -n "$f" ] || continue; '
            'cat "$f" >/dev/null 2>&1; '
            'echo cap >> "$f" 2>/dev/null; '
            "sleep 0.1; done"
        )
        client_node.exec_command(
            sudo=True,
            cmd=(
                f"pkill -f mds_thrash_cap_worker 2>/dev/null; "
                f"for w in $(seq 1 {n_workers}); do "
                f"nohup bash -c {shlex.quote(worker_script)} "
                f">/tmp/mds_thrash_cap_worker_$w.log 2>&1 & "
                f"done; sleep 2"
            ),
            timeout=30,
            check_ec=False,
        )
        workers_started = True
        log.info(
            "[%s] cap_revoke_failover: %s workers, failing MDS", fs_name, n_workers
        )
        return _fail_ranks(
            rados_obj,
            fs_name,
            "rank0",
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    except Exception as e:
        log.warning("[%s] cap_revoke_failover failed: %s", fs_name, e)
        return False
    finally:
        if workers_started:
            _pkill_cap_workers(client_node)


def _op_multi_fs_fail(
    rados_obj,
    fs_name,
    stop_flag,
    fs_names=None,
    recovery_wait=15,
    scale_wait=20,
    **_,
):
    """E1: fail one filesystem; verify peer FS stays active; recover victim."""
    names = [f for f in (fs_names or []) if f]
    if len(names) < 2:
        log.warning(
            "multi_fs_fail: need >=2 filesystems (have %s) — skip", names or fs_name
        )
        return False
    # Prefer failing a non-primary if current pick is primary
    victim = fs_name if fs_name in names else names[-1]
    peers = [f for f in names if f != victim]
    if not peers:
        victim, peers = names[1], [names[0]]
    peer = peers[0]
    peer_before = len(_mds_status(rados_obj, peer, full=False)["actives"])
    log.info(
        "multi_fs_fail: fail %s; expect %s stay up (actives=%s)",
        victim,
        peer,
        peer_before,
    )
    try:
        _ceph_shell(rados_obj, f"ceph fs fail {victim}")
    except Exception as e:
        log.warning("fs fail %s: %s", victim, e)
        return False
    _sleep_with_stop(stop_flag, max(5, recovery_wait // 2), step=2)
    peer_after = len(_mds_status(rados_obj, peer, full=False)["actives"])
    if peer_after < 1:
        log.warning(
            "multi_fs_fail: peer %s lost actives (%s -> %s)",
            peer,
            peer_before,
            peer_after,
        )
    try:
        _ceph_shell(rados_obj, f"ceph fs set {victim} joinable true")
    except Exception as e:
        log.warning("joinable true on %s failed: %s", victim, e)
    if not _wait_active(rados_obj, victim, 1, max(30, scale_wait * 2), stop_flag):
        log.warning("[%s] multi_fs_fail: wait_active failed after joinable", victim)
        return False
    return peer_after >= 1


_OPS = {
    "failover": _op_failover,
    "rolling_restart": _op_rolling_restart,
    "random_fail": _op_random_fail,
    "colocated_fail": _op_colocated_fail,
    "kill_loop": _op_kill_loop,
    "max_mds_scale": _op_max_mds_scale,
    "config_storm": _op_config_storm,
    "fs_fail_scale": _op_fs_fail_scale,
    "standby_replay_fail": _op_standby_replay_fail,
    "dir_pin": _op_dir_pin,
    "cap_revoke_failover": _op_cap_revoke_failover,
    "multi_fs_fail": _op_multi_fs_fail,
}


def _refill_op_bag(ops: List[str]) -> List[str]:
    """Shuffled copy of ops — each op appears once per round (fair coverage)."""
    bag = list(ops)
    random.shuffle(bag)
    return bag


def thrash_mds(
    rados_obj: RadosOrchestrator,
    fs_names: List[str],
    duration: int,
    stop_flag: Dict,
    cluster_fsid: Optional[str] = None,
    operations: Optional[List[str]] = None,
    end_time: Optional[float] = None,
    **opts,
) -> int:
    """
    Holistic MDS thrashing — random FS + fair op selection until duration/stop.

    Ops are drawn from a shuffled bag (without replacement). When the bag is
    empty it is refilled and reshuffled. That way every enabled op runs once
    per round before any op can run again — no pure-random starvation.

    Default operations: failover, rolling_restart, random_fail (legacy).

    ``end_time``: optional parent wall deadline (epoch seconds). When set,
    thrash stops against that clock instead of ``now + duration``.

    Optional opts: kill_count, kill_interval, fail_count, scale_path,
    scale_wait, scale_during_promote, recovery_wait, client_node,
    cephfs_mount_path, pin_dirs, cap_workers.
    """
    if not fs_names:
        log.warning("thrash_mds: no filesystems")
        return 0

    ops = list(operations) if operations else list(_DEFAULT_OPS)
    unknown = [o for o in ops if o not in _OPS]
    if unknown:
        log.warning("Ignoring unknown mds operations: %s", unknown)
        ops = [o for o in ops if o in _OPS] or list(_DEFAULT_OPS)

    kill_count = int(opts.get("kill_count", 5))
    kill_interval = int(opts.get("kill_interval", 5))
    fail_count = int(opts.get("fail_count", 1))
    scale_path = opts.get("scale_path")
    scale_wait = int(opts.get("scale_wait", 20))
    scale_during_promote = bool(opts.get("scale_during_promote", False))
    recovery_wait = int(opts.get("recovery_wait", 15))
    client_node = opts.get("client_node")
    cephfs_mount_path = opts.get("cephfs_mount_path")
    pin_dirs = int(opts.get("pin_dirs", 20))
    cap_workers = int(opts.get("cap_workers", 4))

    # Soft-skip client/multi-FS ops when prerequisites missing (keep bag fair)
    if ("dir_pin" in ops or "cap_revoke_failover" in ops) and (
        not client_node or not cephfs_mount_path
    ):
        log.warning(
            "dir_pin/cap_revoke_failover need client mount — dropping from this run"
        )
        ops = [o for o in ops if o not in ("dir_pin", "cap_revoke_failover")]
    if "multi_fs_fail" in ops and len(fs_names) < 2:
        log.warning("multi_fs_fail needs >=2 FS — dropping from this run")
        ops = [o for o in ops if o != "multi_fs_fail"]
    if not ops:
        ops = list(_DEFAULT_OPS)

    if cluster_fsid is None and "kill_loop" in ops:
        try:
            cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        except Exception as e:
            log.warning("Could not fetch fsid for kill_loop: %s", e)

    # Parent passes end_time so a late executor start cannot extend past suite duration.
    if end_time is None:
        end_time = time.time() + duration
    snap = _snapshot_state(rados_obj, fs_names)
    touched_configs: Set[str] = set()
    op_counts = {o: 0 for o in ops}
    op_bag = _refill_op_bag(ops)
    log.info(
        "Starting MDS thrashing fs=%s ops=%s duration=%ss snapshot=%s "
        "(fair shuffle bag) mount=%s",
        fs_names,
        ops,
        duration,
        snap,
        cephfs_mount_path,
    )

    ops_completed = 0
    iteration = 0
    # ≥1 fair bag after parent stop/duration; ops always honor stop_flag.
    # hard_deadline caps wall-clock if an op ignores stop.
    fair_rounds_done = 0
    slack = post_duration_slack(len(ops))
    hard_deadline = end_time + slack
    try:
        while time.time() < hard_deadline:
            timed_out = time.time() >= end_time
            stopped = bool(stop_flag.get("stop"))
            if not op_bag:
                fair_rounds_done += 1
                if stopped or timed_out:
                    log.info(
                        "MDS stop after fair round %s (stop=%s timed_out=%s)",
                        fair_rounds_done,
                        stopped,
                        timed_out,
                    )
                    break
                op_bag = _refill_op_bag(ops)
                log.info("MDS op bag refilled (round complete): %s", op_bag)
            elif fair_rounds_done >= 1 and (stopped or timed_out):
                log.info(
                    "MDS stop after fair round %s (stop=%s timed_out=%s)",
                    fair_rounds_done,
                    stopped,
                    timed_out,
                )
                break
            iteration += 1
            fs_name = random.choice(fs_names)
            operation = op_bag.pop()
            op_counts[operation] += 1
            log.info(
                "MDS thrash iteration %s: fs=%s op=%s (counts=%s)",
                iteration,
                fs_name,
                operation,
                op_counts,
            )
            try:
                ok = _OPS[operation](
                    rados_obj,
                    fs_name,
                    stop_flag,
                    cluster_fsid=cluster_fsid,
                    kill_count=kill_count,
                    kill_interval=kill_interval,
                    fail_count=fail_count,
                    scale_path=scale_path,
                    scale_wait=scale_wait,
                    scale_during_promote=scale_during_promote,
                    recovery_wait=recovery_wait,
                    touched_configs=touched_configs,
                    client_node=client_node,
                    cephfs_mount_path=cephfs_mount_path,
                    fs_names=fs_names,
                    pin_dirs=pin_dirs,
                    cap_workers=cap_workers,
                )
                if ok:
                    ops_completed += 1
                    log.info("MDS op %s on %s ok", operation, fs_name)
                else:
                    log.warning("MDS op %s on %s did not complete", operation, fs_name)
            except Exception as e:
                log.warning("MDS thrash iteration %s failed: %s", iteration, e)
            if not (stop_flag.get("stop") or time.time() >= end_time):
                _sleep_with_stop(stop_flag, recovery_wait, step=3)
        else:
            log.warning(
                "MDS hard deadline reached after fair round %s "
                "(end_time+%ss) — abandoning remaining bag %s",
                fair_rounds_done,
                slack,
                op_bag,
            )
    finally:
        _pkill_cap_workers(client_node)
        log.info("Restoring MDS FS state: %s touched_configs=%s", snap, touched_configs)
        _restore_state(rados_obj, snap, touched_configs)

    log.info(
        "MDS thrashing done: %s ops in %s iterations across %s fs; op_counts=%s",
        ops_completed,
        iteration,
        len(fs_names),
        op_counts,
    )
    return ops_completed
