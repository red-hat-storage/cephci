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


# (config key, min, max) — integers for config_storm; cache limit in bytes
_CONFIG_POOL = [
    ("mds_cache_memory_limit", 1 << 30, 8 << 30),
    ("mds_bal_interval", 5, 60),
    ("mds_log_max_segments", 32, 256),
    ("mds_beacon_grace", 10, 45),
    ("mds_recall_max_caps", 1000, 50000),
    ("mds_session_timeout", 30, 120),
    ("mds_max_caps_per_client", 10000, 200000),
]
# Extra keys snapshotted/restored when touched (floats / non-storm)
_EXTRA_SNAP_KEYS = (
    "mds_cache_reservation",
    "mds_bal_mode",
    "mds_bal_split_size",
    "mds_bal_merge_size",
)

_CLIENT_OPS = frozenset(
    {
        "dir_pin",
        "cap_revoke_failover",
        "cap_explosion",
        "scale_to_one_pinned",
        "snap_failover",
        "clone_during_rejoin",
        "client_reconnect_storm",
        "journal_replay_load",
        "fs_fail_quiesce",
        "dir_split_merge",
        "export_during_snapshot",
    }
)
_MULTI_FS_OPS = frozenset({"multi_fs_fail", "bidirectional_maxmds"})
_KILL_OPS = frozenset({"kill_loop", "double_fault"})


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


def _ceph_shell(rados_obj: RadosOrchestrator, cmd: str, timeout: int = 120) -> None:
    """Run a ceph CLI that may not return useful JSON.

    Default 120s — thrash ops must not sit on the node.shell 600s default
    (Q5PH9C EI-aggressive: clone_during_rejoin finally hung 10+ min on
    ``fs subvolume rm``).
    """
    rados_obj.node.shell([cmd], timeout=timeout)


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
    for key in _EXTRA_SNAP_KEYS:
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


def _op_rolling_restart(rados_obj, fs_name, stop_flag, recovery_wait=15, **_):
    """Restart only ``mds.<fs_name>`` (not every MDS service on the cluster).

    ``restart_daemon_services(daemon="mds")`` shares one 600s clock across all
    MDS orch services; with multiple FS that starves later services and trips
    drain_timeout. Scope to the FS under test and wait for its actives.
    """
    service = f"mds.{fs_name}"
    log.info("[%s] MDS rolling restart via orch (%s)", fs_name, service)
    try:
        _ceph_shell(rados_obj, f"ceph orch restart {service}")
    except Exception as e:
        log.warning("[%s] orch restart %s failed: %s", fs_name, service, e)
        return False
    need = max(1, int(_mds_status(rados_obj, fs_name, full=True).get("max_mds") or 1))
    # Per-FS budget (honours stop_flag). Scoped restart should not need 600s.
    wait_s = max(180, int(recovery_wait) * 8)
    if not _wait_active(rados_obj, fs_name, need, wait_s, stop_flag):
        log.warning(
            "[%s] rolling_restart: fewer than %s actives after %s",
            fs_name,
            need,
            service,
        )
        return False
    _sleep_with_stop(stop_flag, 30, step=5)
    return True


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
        _ceph_shell(rados_obj, f"ceph fs fail {fs_name} --yes-i-really-mean-it")
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


def _apply_dir_pins(client_node, dirs: List[str], n_active: int, stop_flag=None) -> int:
    """setfattr ceph.dir.pin to a random rank for each dir; return pin count."""
    pins = 0
    n = max(1, int(n_active))
    for d in dirs:
        if stop_flag and stop_flag.get("stop"):
            break
        try:
            client_node.exec_command(
                sudo=True,
                cmd=(
                    f"setfattr -n ceph.dir.pin -v {random.randrange(n)} "
                    f"-- {shlex.quote(d)}"
                ),
                timeout=15,
                check_ec=False,
            )
            pins += 1
        except Exception:
            pass
    return pins


def _touch_mount(client_node, mount_path: str) -> None:
    """Best-effort ls on mount to provoke client reconnect."""
    if not client_node or not mount_path:
        return
    try:
        client_node.exec_command(
            sudo=True,
            cmd=f"ls {shlex.quote(mount_path)} >/dev/null 2>&1 || true",
            timeout=20,
            check_ec=False,
        )
    except Exception:
        pass


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
    batch = min(len(dirs), max(10, int(pin_dirs)))
    pins = _apply_dir_pins(client_node, random.sample(dirs, batch), n_active, stop_flag)
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
        _ceph_shell(rados_obj, f"ceph fs fail {victim} --yes-i-really-mean-it")
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


def _snap_dir_cycle(client_node, mount_path: str, n_snaps: int = 5) -> str:
    """Create a work dir, take/delete .snap snapshots; return work dir path."""
    base = f"{mount_path.rstrip('/')}/mds_thrash_snap"
    work = f"{base}/w_{int(time.time())}_{random.randint(0, 9999)}"
    qw, qb = shlex.quote(work), shlex.quote(base)
    n = max(1, int(n_snaps))
    client_node.exec_command(
        sudo=True,
        cmd=(
            f'base={qb}; work={qw}; mkdir -p "$work" "$work/.snap" 2>/dev/null; '
            f'echo thrash > "$work/f0" 2>/dev/null; '
            f"for i in $(seq 1 {n}); do "
            f'mkdir -p "$work/.snap/s_$i" 2>/dev/null || true; '
            f"done; "
            f"for i in $(seq 1 {n}); do "
            f'rmdir "$work/.snap/s_$i" 2>/dev/null || true; '
            f"done"
        ),
        timeout=90,
        check_ec=False,
    )
    return work


def _op_cap_explosion(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    recovery_wait=15,
    touched_configs=None,
    **_,
):
    """C1-lite: many files + lower max_caps_per_client, then fail MDS."""
    if not client_node or not cephfs_mount_path:
        return False
    base = f"{cephfs_mount_path.rstrip('/')}/mds_thrash_capx"
    qb = shlex.quote(base)
    try:
        client_node.exec_command(
            sudo=True,
            cmd=(
                f'base={qb}; mkdir -p "$base" && '
                f"for i in $(seq 1 200); do "
                f'touch "$base/f_$i" 2>/dev/null; '
                f'cat "$base/f_$i" >/dev/null 2>&1; '
                f"done"
            ),
            timeout=90,
            check_ec=False,
        )
        _set_mds_config(rados_obj, "mds_max_caps_per_client", 50000)
        _set_mds_config(rados_obj, "mds_recall_max_caps", 5000)
        if touched_configs is not None:
            touched_configs.update({"mds_max_caps_per_client", "mds_recall_max_caps"})
        log.info("[%s] cap_explosion: caps pressure, failing MDS", fs_name)
        return _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    except Exception as e:
        log.warning("[%s] cap_explosion failed: %s", fs_name, e)
        return False


def _op_cache_limit_storm(
    rados_obj,
    fs_name,
    stop_flag,
    recovery_wait=15,
    touched_configs=None,
    **_,
):
    """C3 / IBMCEPH-17454 knobs: reservation=0.95 + low memory limit + fail."""
    try:
        _set_mds_config(rados_obj, "mds_cache_reservation", 0.95)
        _set_mds_config(rados_obj, "mds_cache_memory_limit", 1 << 30)
        if touched_configs is not None:
            touched_configs.update({"mds_cache_reservation", "mds_cache_memory_limit"})
        log.info(
            "[%s] cache_limit_storm: reservation=0.95 mem=1G, failing MDS", fs_name
        )
        ok = _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
        _sleep_with_stop(stop_flag, max(5, recovery_wait // 2), step=2)
        return ok
    except Exception as e:
        log.warning("[%s] cache_limit_storm failed: %s", fs_name, e)
        return False


def _op_scale_to_one_pinned(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    pin_dirs=20,
    scale_wait=20,
    **_,
):
    """D2-lite: pin dirs then max_mds → 1 → restore."""
    if not client_node or not cephfs_mount_path:
        return False
    status = _mds_status(rados_obj, fs_name, full=True)
    orig = status["max_mds"] or 1
    n_active = len(status["actives"])
    if n_active < 2:
        log.warning("[%s] scale_to_one_pinned: need >=2 actives", fs_name)
        return False
    try:
        dirs = _ensure_pin_dirs(client_node, cephfs_mount_path, max(10, int(pin_dirs)))
        _apply_dir_pins(
            client_node, dirs[: min(len(dirs), int(pin_dirs))], n_active, stop_flag
        )
        log.info("[%s] scale_to_one_pinned: %s -> 1 -> %s", fs_name, orig, orig)
        _set_max_mds(rados_obj, fs_name, 1)
        if not _wait_active(rados_obj, fs_name, 1, max(20, scale_wait), stop_flag):
            return False
        _set_max_mds(rados_obj, fs_name, orig)
        return _wait_active(
            rados_obj, fs_name, max(1, orig), max(20, scale_wait), stop_flag
        )
    except Exception as e:
        log.warning("[%s] scale_to_one_pinned failed: %s", fs_name, e)
        return False


def _op_rapid_maxmds_oscillation(rados_obj, fs_name, stop_flag, scale_wait=20, **_):
    """D3-lite: burst max_mds flips with short waits."""
    status = _mds_status(rados_obj, fs_name, full=True)
    hi = max(2, status["max_mds"] or 2)
    lo = 1
    path = [hi, lo, hi, lo, hi]
    log.info("[%s] rapid_maxmds_oscillation path=%s", fs_name, path)
    for target in path:
        if stop_flag.get("stop"):
            break
        try:
            _set_max_mds(rados_obj, fs_name, target)
        except Exception as e:
            log.warning("max_mds %s failed: %s", target, e)
            return False
        _sleep_with_stop(stop_flag, max(2, min(5, scale_wait // 4)), step=1)
    return _wait_active(rados_obj, fs_name, 1, max(15, scale_wait), stop_flag)


def _op_fs_fail_during_scaledown(
    rados_obj, fs_name, stop_flag, scale_wait=20, recovery_wait=15, **_
):
    """E2: start scale-down, race fs fail, joinable, restore max_mds."""
    status = _mds_status(rados_obj, fs_name, full=True)
    orig = status["max_mds"] or 1
    lower = 1 if orig <= 2 else max(1, orig // 2)
    log.info("[%s] fs_fail_during_scaledown: %s -> %s + fs fail", fs_name, orig, lower)
    try:
        _set_max_mds(rados_obj, fs_name, lower)
        _sleep_with_stop(stop_flag, max(3, recovery_wait // 3), step=1)
        _ceph_shell(rados_obj, f"ceph fs fail {fs_name} --yes-i-really-mean-it")
        _sleep_with_stop(stop_flag, max(5, recovery_wait // 2), step=2)
        _ceph_shell(rados_obj, f"ceph fs set {fs_name} joinable true")
    except Exception as e:
        log.warning("[%s] fs_fail_during_scaledown: %s", fs_name, e)
        return False
    if not _wait_active(rados_obj, fs_name, 1, max(30, scale_wait * 2), stop_flag):
        return False
    _set_max_mds(rados_obj, fs_name, orig)
    return _wait_active(
        rados_obj, fs_name, max(1, orig), max(30, scale_wait * 2), stop_flag
    )


def _op_bidirectional_maxmds(
    rados_obj, fs_name, stop_flag, fs_names=None, scale_wait=20, **_
):
    """E3: scale one FS up and peer FS down (needs >=2 FS)."""
    names = [f for f in (fs_names or []) if f]
    if len(names) < 2:
        log.warning("bidirectional_maxmds: need >=2 FS — skip")
        return False
    a, b = names[0], names[1]
    sa = _mds_status(rados_obj, a, full=True)
    sb = _mds_status(rados_obj, b, full=True)
    oa, ob = sa["max_mds"] or 1, sb["max_mds"] or 1
    up_a = oa + 1
    down_b = max(1, ob - 1) if ob > 1 else ob
    log.info("bidirectional_maxmds: %s %s->%s ; %s %s->%s", a, oa, up_a, b, ob, down_b)
    try:
        _set_max_mds(rados_obj, a, up_a)
        _set_max_mds(rados_obj, b, down_b)
        _sleep_with_stop(stop_flag, max(5, scale_wait // 2), step=2)
        _set_max_mds(rados_obj, a, oa)
        _set_max_mds(rados_obj, b, ob)
    except Exception as e:
        log.warning("bidirectional_maxmds failed: %s", e)
        return False
    ok_a = _wait_active(rados_obj, a, 1, max(20, scale_wait), stop_flag)
    ok_b = _wait_active(rados_obj, b, 1, max(20, scale_wait), stop_flag)
    return ok_a and ok_b


def _op_fs_fail_joinable_storm(
    rados_obj, fs_name, stop_flag, scale_wait=20, recovery_wait=15, **_
):
    """E4-lite: 2–3× fs fail → joinable → wait active."""
    rounds = random.randint(2, 3)
    log.info("[%s] fs_fail_joinable_storm rounds=%s", fs_name, rounds)
    for i in range(rounds):
        if stop_flag.get("stop"):
            break
        try:
            _ceph_shell(rados_obj, f"ceph fs fail {fs_name} --yes-i-really-mean-it")
            _sleep_with_stop(stop_flag, max(3, recovery_wait // 3), step=1)
            _ceph_shell(rados_obj, f"ceph fs set {fs_name} joinable true")
        except Exception as e:
            log.warning("[%s] fs_fail_joinable_storm iter %s: %s", fs_name, i, e)
            return False
        if not _wait_active(rados_obj, fs_name, 1, max(25, scale_wait * 2), stop_flag):
            return False
    return True


def _op_snap_failover(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    recovery_wait=15,
    **_,
):
    """F1+F2+H2: snap create/rm storm and/or fail MDS around snaps."""
    if not client_node or not cephfs_mount_path:
        return False
    mode = random.choice(["storm", "fail_during", "trim_fail"])
    log.info("[%s] snap_failover mode=%s", fs_name, mode)
    try:
        if mode == "storm":
            _snap_dir_cycle(client_node, cephfs_mount_path, n_snaps=8)
            return True
        if mode == "fail_during":
            work = _snap_dir_cycle(client_node, cephfs_mount_path, n_snaps=3)
            # leave one snap, fail MDS
            client_node.exec_command(
                sudo=True,
                cmd=f"mkdir -p {shlex.quote(work)}/.snap/live_fail 2>/dev/null || true",
                timeout=20,
                check_ec=False,
            )
            return _fail_ranks(
                rados_obj,
                fs_name,
                "random",
                n=1,
                stop_flag=stop_flag,
                recovery_wait=recovery_wait,
            )
        # trim_fail: create+delete then fail
        _snap_dir_cycle(client_node, cephfs_mount_path, n_snaps=6)
        return _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    except Exception as e:
        log.warning("[%s] snap_failover failed: %s", fs_name, e)
        return False


def _op_clone_during_rejoin(
    rados_obj,
    fs_name,
    stop_flag,
    recovery_wait=15,
    scale_wait=20,
    **_,
):
    """F3-lite: subvolume snap+clone then fail MDS; best-effort cleanup."""
    svg = "mds_thrash_svg"
    sv = f"mds_thrash_sv_{random.randint(0, 9999)}"
    snap = "thrash_s"
    clone = f"{sv}_clone"
    # Short shells — under thrash, subvolume rm can hang past suite drain.
    t = 90
    try:
        _ceph_shell(
            rados_obj, f"ceph fs subvolumegroup create {fs_name} {svg}", timeout=t
        )
    except Exception:
        pass
    try:
        if stop_flag.get("stop"):
            return False
        _ceph_shell(
            rados_obj,
            f"ceph fs subvolume create {fs_name} {sv} --group_name {svg}",
            timeout=t,
        )
        _ceph_shell(
            rados_obj,
            f"ceph fs subvolume snapshot create {fs_name} {sv} {snap} --group_name {svg}",
            timeout=t,
        )
        try:
            _ceph_shell(
                rados_obj,
                f"ceph fs subvolume snapshot clone {fs_name} {sv} {snap} {clone} "
                f"--group_name {svg} --target_group_name {svg}",
                timeout=t,
            )
        except Exception as e:
            log.warning("[%s] clone_during_rejoin: clone API skip: %s", fs_name, e)
        log.info("[%s] clone_during_rejoin: failing MDS mid-clone", fs_name)
        ok = _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
        _wait_active(rados_obj, fs_name, 1, max(20, scale_wait), stop_flag)
        return ok
    except Exception as e:
        log.warning("[%s] clone_during_rejoin failed: %s", fs_name, e)
        return False
    finally:
        for cmd in (
            f"ceph fs subvolume rm {fs_name} {clone} --group_name {svg} --force",
            f"ceph fs subvolume snapshot rm {fs_name} {sv} {snap} --group_name {svg}",
            f"ceph fs subvolume rm {fs_name} {sv} --group_name {svg} --force",
        ):
            try:
                _ceph_shell(rados_obj, cmd, timeout=60)
            except Exception:
                pass


def _op_client_reconnect_storm(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    recovery_wait=15,
    **_,
):
    """H1-lite: evict sessions via mds tell; remount if needed; optional fail."""
    if not client_node or not cephfs_mount_path:
        return False
    try:
        # Evict sessions on rank 0; avoid leaving the thrash mount permanently dead.
        _ceph_shell(rados_obj, f"ceph tell mds.{fs_name}:0 client evict *")
    except Exception:
        try:
            _ceph_shell(rados_obj, f"ceph tell mds.{fs_name}:0 session evict *")
        except Exception as e:
            log.warning("[%s] client_reconnect_storm evict: %s", fs_name, e)
    _sleep_with_stop(stop_flag, max(3, recovery_wait // 3), step=1)
    # Touch mount to force reconnect
    try:
        client_node.exec_command(
            sudo=True,
            cmd=f"ls {shlex.quote(cephfs_mount_path)} >/dev/null 2>&1 || "
            f"mount -a 2>/dev/null || true",
            timeout=30,
            check_ec=False,
        )
    except Exception:
        pass
    if random.choice([True, False]):
        return _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    return True


def _op_standby_exhaustion(rados_obj, fs_name, stop_flag, recovery_wait=15, **_):
    """H3: fail more actives than available standbys."""
    status = _mds_status(rados_obj, fs_name, full=False)
    actives = status["actives"]
    standbys = status["standbys"]
    if len(actives) < 2:
        log.warning("[%s] standby_exhaustion: need >=2 actives", fs_name)
        return False
    n_fail = min(len(actives), max(2, len(standbys) + 1))
    targets = random.sample(actives, n_fail)
    log.info(
        "[%s] standby_exhaustion: failing %s (standbys=%s)",
        fs_name,
        n_fail,
        len(standbys),
    )
    for a in targets:
        if stop_flag.get("stop"):
            break
        _fail_mds(rados_obj, a.get("name") or a.get("rank"))
    _sleep_with_stop(stop_flag, max(8, recovery_wait), step=2)
    return bool(_mds_status(rados_obj, fs_name, full=False)["actives"])


def _op_journal_replay_load(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    recovery_wait=15,
    **_,
):
    """H4-lite: fail MDS then hammer metadata on mount."""
    if not client_node or not cephfs_mount_path:
        return False
    ok = _fail_ranks(
        rados_obj,
        fs_name,
        "rank0",
        stop_flag=stop_flag,
        recovery_wait=max(5, recovery_wait // 2),
    )
    base = shlex.quote(f"{cephfs_mount_path.rstrip('/')}/mds_thrash_jnl")
    try:
        client_node.exec_command(
            sudo=True,
            cmd=(
                f'base={base}; mkdir -p "$base" 2>/dev/null; '
                f"for i in $(seq 1 50); do "
                f'touch "$base/t_$i" 2>/dev/null; ls "$base" >/dev/null 2>&1; '
                f'stat "$base/t_$i" >/dev/null 2>&1; done'
            ),
            timeout=60,
            check_ec=False,
        )
    except Exception as e:
        log.warning("[%s] journal_replay_load hammer: %s", fs_name, e)
    return bool(ok)


def _op_double_fault(
    rados_obj,
    fs_name,
    stop_flag,
    cluster_fsid=None,
    recovery_wait=15,
    **_,
):
    """B4: kill two colocated active MDS daemons (not ceph mds fail)."""
    if not cluster_fsid:
        try:
            cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        except Exception as e:
            log.warning("[%s] double_fault: no fsid: %s", fs_name, e)
            return False
    status = _mds_status(rados_obj, fs_name, full=False)
    by_host: Dict[str, List] = {}
    for a in status["actives"]:
        by_host.setdefault(_host_from_mds_name(a.get("name")), []).append(a)
    candidates = [v for v in by_host.values() if len(v) >= 2]
    if not candidates:
        log.warning("[%s] double_fault: no host with 2+ actives", fs_name)
        return False
    pair = random.sample(random.choice(candidates), 2)
    killed = 0
    for a in pair:
        name = a.get("name")
        daemon_id = _resolve_mds_daemon_id(rados_obj, name)
        log.info("[%s] double_fault kill %s id=%s", fs_name, name, daemon_id)
        if _kill_daemon(rados_obj, "mds", str(daemon_id), cluster_fsid):
            killed += 1
    _sleep_with_stop(stop_flag, max(8, recovery_wait), step=2)
    _wait_active(rados_obj, fs_name, 1, max(30, recovery_wait * 2), stop_flag)
    return killed >= 2


def _op_fs_fail_quiesce(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    scale_wait=20,
    recovery_wait=15,
    **_,
):
    """B5: quiesce if available else snap hold; fs fail → joinable."""
    quiesced = False
    try:
        _ceph_shell(rados_obj, f"ceph fs quiesce {fs_name} --all --await")
        quiesced = True
    except Exception:
        try:
            _ceph_shell(rados_obj, f"ceph fs quiesce {fs_name} --await")
            quiesced = True
        except Exception:
            quiesced = False
    if not quiesced and client_node and cephfs_mount_path:
        try:
            _snap_dir_cycle(client_node, cephfs_mount_path, n_snaps=2)
        except Exception:
            pass
    log.info("[%s] fs_fail_quiesce: quiesced=%s, fs fail", fs_name, quiesced)
    try:
        _ceph_shell(rados_obj, f"ceph fs fail {fs_name} --yes-i-really-mean-it")
        _sleep_with_stop(stop_flag, max(5, recovery_wait // 2), step=2)
        _ceph_shell(rados_obj, f"ceph fs set {fs_name} joinable true")
    except Exception as e:
        log.warning("[%s] fs_fail_quiesce: %s", fs_name, e)
        return False
    return _wait_active(rados_obj, fs_name, 1, max(30, scale_wait * 2), stop_flag)


def _op_balancer_toggle(
    rados_obj,
    fs_name,
    stop_flag,
    recovery_wait=15,
    touched_configs=None,
    **_,
):
    """A2-lite: flip mds_bal_mode then fail an active rank."""
    mode = random.choice((0, 1, -1))
    try:
        _set_mds_config(rados_obj, "mds_bal_mode", mode)
        if touched_configs is not None:
            touched_configs.add("mds_bal_mode")
        log.info("[%s] balancer_toggle: mds_bal_mode=%s + fail", fs_name, mode)
        return _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    except Exception as e:
        log.warning("[%s] balancer_toggle failed: %s", fs_name, e)
        return False


def _op_dir_split_merge(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    recovery_wait=15,
    touched_configs=None,
    **_,
):
    """A3-lite: low split threshold, one dir populate/pin/fail, then delete/fail.

    # ponytail: ~120 files (not 50k soak); enough to cross thrash split_size=80.
    """
    if not client_node or not cephfs_mount_path:
        return False
    status = _mds_status(rados_obj, fs_name, full=False)
    n_active = max(1, len(status["actives"]))
    n_files = 120
    try:
        _set_mds_config(rados_obj, "mds_bal_split_size", 80)
        _set_mds_config(rados_obj, "mds_bal_merge_size", 40)
        if touched_configs is not None:
            touched_configs.update({"mds_bal_split_size", "mds_bal_merge_size"})
        dirs = _ensure_pin_dirs(client_node, cephfs_mount_path, 4)
        if not dirs:
            return False
        d = dirs[0]
        qd = shlex.quote(d)
        client_node.exec_command(
            sudo=True,
            cmd=(
                f"d={qd}; "
                f'for i in $(seq 1 {n_files}); do touch "$d/f_$i" 2>/dev/null; done'
            ),
            timeout=60,
            check_ec=False,
        )
        _apply_dir_pins(client_node, [d], n_active, stop_flag)
        log.info("[%s] dir_split_merge: split phase fail", fs_name)
        _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
        if stop_flag.get("stop"):
            return True
        client_node.exec_command(
            sudo=True,
            cmd=(
                f"d={qd}; "
                f'for i in $(seq 1 2 {n_files}); do rm -f "$d/f_$i" 2>/dev/null; done'
            ),
            timeout=60,
            check_ec=False,
        )
        log.info("[%s] dir_split_merge: merge phase fail", fs_name)
        return _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
    except Exception as e:
        log.warning("[%s] dir_split_merge failed: %s", fs_name, e)
        return False


def _op_export_during_snapshot(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    pin_dirs=20,
    recovery_wait=15,
    **_,
):
    """A4-lite: pin dirs (export pressure), create .snap, fail MDS."""
    if not client_node or not cephfs_mount_path:
        return False
    status = _mds_status(rados_obj, fs_name, full=False)
    n_active = len(status["actives"])
    if n_active < 2:
        log.warning("[%s] export_during_snapshot: need >=2 actives", fs_name)
        return False
    try:
        dirs = _ensure_pin_dirs(client_node, cephfs_mount_path, max(8, int(pin_dirs)))
        if len(dirs) < 4:
            return False
        _apply_dir_pins(
            client_node, dirs[: min(len(dirs), int(pin_dirs))], n_active, stop_flag
        )
        target = random.choice(dirs)
        snap = f"exp_snap_{random.randint(0, 9999)}"
        qs = shlex.quote(f"{target}/.snap/{snap}")
        client_node.exec_command(
            sudo=True,
            cmd=f"mkdir -p {qs} 2>/dev/null || true",
            timeout=30,
            check_ec=False,
        )
        log.info("[%s] export_during_snapshot: snap=%s + fail", fs_name, snap)
        ok = _fail_ranks(
            rados_obj,
            fs_name,
            "random",
            n=1,
            stop_flag=stop_flag,
            recovery_wait=recovery_wait,
        )
        client_node.exec_command(
            sudo=True,
            cmd=f"rmdir {qs} 2>/dev/null || true",
            timeout=20,
            check_ec=False,
        )
        return ok
    except Exception as e:
        log.warning("[%s] export_during_snapshot failed: %s", fs_name, e)
        return False


def _op_reconnect_scaledown(
    rados_obj,
    fs_name,
    stop_flag,
    client_node=None,
    cephfs_mount_path=None,
    scale_wait=20,
    **_,
):
    """C2-lite: step max_mds down then restore; touch mount for reconnect."""
    status = _mds_status(rados_obj, fs_name, full=True)
    orig = status["max_mds"] or 1
    if orig < 2:
        log.warning("[%s] reconnect_scaledown: need max_mds>=2", fs_name)
        return False
    floor = 1 if orig <= 2 else max(1, orig // 2)
    step = max(1, (orig - floor + 1) // 2)
    log.info("[%s] reconnect_scaledown: %s -> %s -> %s", fs_name, orig, floor, orig)
    try:
        cur = orig
        while cur > floor:
            if stop_flag.get("stop"):
                break
            nxt = max(floor, cur - step)
            _set_max_mds(rados_obj, fs_name, nxt)
            cur = nxt
            _sleep_with_stop(stop_flag, max(2, min(5, scale_wait // 4)), step=1)
            _touch_mount(client_node, cephfs_mount_path)
        if not _wait_active(rados_obj, fs_name, floor, max(20, scale_wait), stop_flag):
            return False
        _set_max_mds(rados_obj, fs_name, orig)
        ok = _wait_active(
            rados_obj, fs_name, max(1, orig), max(20, scale_wait * 2), stop_flag
        )
        _touch_mount(client_node, cephfs_mount_path)
        return ok
    except Exception as e:
        log.warning("[%s] reconnect_scaledown failed: %s", fs_name, e)
        return False


def _op_maxmds_during_promotion(
    rados_obj,
    fs_name,
    stop_flag,
    scale_wait=20,
    recovery_wait=15,
    **_,
):
    """D4: standby-replay promote raced with max_mds change (always on)."""
    # Do not forward suite scale_during_promote=False — this op always races.
    return _op_standby_replay_fail(
        rados_obj,
        fs_name,
        stop_flag,
        scale_during_promote=True,
        scale_wait=scale_wait,
        recovery_wait=recovery_wait,
    )


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
    "cap_explosion": _op_cap_explosion,
    "cache_limit_storm": _op_cache_limit_storm,
    "scale_to_one_pinned": _op_scale_to_one_pinned,
    "rapid_maxmds_oscillation": _op_rapid_maxmds_oscillation,
    "fs_fail_during_scaledown": _op_fs_fail_during_scaledown,
    "bidirectional_maxmds": _op_bidirectional_maxmds,
    "fs_fail_joinable_storm": _op_fs_fail_joinable_storm,
    "snap_failover": _op_snap_failover,
    "clone_during_rejoin": _op_clone_during_rejoin,
    "client_reconnect_storm": _op_client_reconnect_storm,
    "standby_exhaustion": _op_standby_exhaustion,
    "journal_replay_load": _op_journal_replay_load,
    "double_fault": _op_double_fault,
    "fs_fail_quiesce": _op_fs_fail_quiesce,
    "balancer_toggle": _op_balancer_toggle,
    "dir_split_merge": _op_dir_split_merge,
    "export_during_snapshot": _op_export_during_snapshot,
    "reconnect_scaledown": _op_reconnect_scaledown,
    "maxmds_during_promotion": _op_maxmds_during_promotion,
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
    if any(o in _CLIENT_OPS for o in ops) and (
        not client_node or not cephfs_mount_path
    ):
        drop = [o for o in ops if o in _CLIENT_OPS]
        log.warning("client-mount ops need mount — dropping %s", drop)
        ops = [o for o in ops if o not in _CLIENT_OPS]
    if any(o in _MULTI_FS_OPS for o in ops) and len(fs_names) < 2:
        drop = [o for o in ops if o in _MULTI_FS_OPS]
        log.warning("multi-FS ops need >=2 FS — dropping %s", drop)
        ops = [o for o in ops if o not in _MULTI_FS_OPS]
    if not ops:
        ops = list(_DEFAULT_OPS)

    if cluster_fsid is None and any(o in _KILL_OPS for o in ops):
        try:
            cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        except Exception as e:
            log.warning("Could not fetch fsid for kill ops: %s", e)

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
