"""mgr/cephadm/nfs_rados_command_timeout under OSD pause.

cephadm's NFS RADOS helper is configurable (default 30s, floor 5s).
In-lab we almost never see that latency, so this uses the QE proxy:
seed NFS so pool ``.nfs`` exists, ``ceph osd pause``, apply a NEW NFS
spec. That stalls the timeout wrapper (``rados get conf-nfs.*`` on
``.nfs``). It is not ``ganesha-rados-grace`` and not an HA / host-down
test.

Pass/Fail is product, not harness:
  * ``ceph config get mgr mgr/cephadm/nfs_rados_command_timeout``
  * ``ceph health detail``: ``CEPHADM_APPLY_SPEC_FAIL`` and
    ``timed out after N seconds`` (or ``N.0 seconds``)

Do not treat ``ceph orch apply`` wall-clock as the timeout.

``config rm`` is not used to restore 30: it only clears the mon override, so
``config get`` can show 30 while the live mgr still uses the last ``config set``.
Teardown uses ``config set … 30``.

Suite YAML keys (they are not interchangeable):
  * set_value — integer for ``config set`` (floats like 22.222 are EINVAL).
  * get_ok — allowed ``config get`` strings. Lab get is an integer
    (``30``, ``60``, ``5``, ``0``), never ``5.0``. A list because the
    below-floor row may store ``0`` or ``1``; leftover ``5`` must Fail.
    Also proves set stuck before pause (set 60 but get still 5 would
    otherwise Fail the health row for the wrong reason).
  * expect — health timeout only. Matcher is numeric, so ``5`` and
    ``5.0`` in health both Pass (cephadm TimeoutExpired prints int or
    float depending on build / whether the option was set). ``expect``
    is not compared to ``set_value``. Below-floor: set 0, get 0|1,
    expect 5.

Flow:
  1. Setup: one healthy NFS (creates .nfs). Leave it running.
  2. Default 30: first get must be 30. If not (leftover set / other
     test), warn, set 30, still measure health. If health is 30s the
     row still Fails (not compiled default). If get is still not 30
     after set, Fail immediately.
  3. set 60 → get 60 → pause → health 60s
  4. set 5 → get 5 → health 5s (floor)
  5. set 0 → get 0 or 1 → health 5s
  6. Teardown: unpause; config set 30; orch rm seed and stalls

Harness (not product):
  * Never apply, and never orch rm, while leftover pause is still set
    (lab: rados rm grace hung). Deliberate pause + apply is the test.
  * Matching health + failed unpause: this row still Passes; teardown
    must clear pause.
  * After unpause succeeds: 15s settle for mon/mgr. Do not poll MDS.
  * Paused apply SSH wait is max(120, 2N+60). SSH timeout is logged and
    health is still polled; other apply CommandFailed Fails immediately.
  * Unique service_id and ports per stall. ``cluster_qos_port`` is
    optional (tentacle); omit it on squid/8.1 NFS specs.
"""

import json
import re
from time import sleep, time

import yaml

from ceph.ceph import CommandFailed, SocketTimeoutException, TimeoutException
from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

TIMEOUT_OPTION = "mgr/cephadm/nfs_rados_command_timeout"
DEFAULT_SEED_ID = "nfs-14352-seed"
TIMEOUT_STR_RE = re.compile(r"timed out after (\d+(?:\.\d+)?) seconds")
# Floor SSH wait for orch apply while OSDs are paused. Per-case wait is
# max(this, 2*N+60) so a blocking apply is not cut off before the health
# window. Setup apply (not paused) keeps cephci exec_command default 600s.
PAUSED_APPLY_SSH_TIMEOUT = 120
PAUSE_FLAG_WAIT = 30
UNPAUSE_WAIT = 60
POST_UNPAUSE_SETTLE = 15
STALE_HEALTH_WAIT = 90
ORCH_RM_WAIT = 120
HEALTH_POLL = 2
DEFAULT_TIMEOUT = "30"
_APPLY_SSH_TIMEOUT_EXC = (
    CommandFailed,
    SocketTimeoutException,
    TimeoutException,
    TimeoutError,
)


def _ceph(installer, cmd, check_ec=True, mount=None, timeout=None):
    """Run a command inside ``cephadm shell``. Returns (stdout, stderr)."""
    prefix = "cephadm shell"
    if mount:
        prefix += f" --mount {mount}:{mount}"
    prefix += " --"
    kw = {
        "sudo": True,
        "cmd": f"{prefix} {cmd}",
        "check_ec": check_ec,
    }
    if timeout is not None:
        kw["timeout"] = timeout
    out, err = installer.exec_command(**kw)
    stdout = out.strip() if isinstance(out, str) else str(out or "").strip()
    stderr = err.strip() if isinstance(err, str) else str(err or "").strip()
    return stdout, stderr


def _first_json(text, opener="{"):
    """Decode the first JSON value in cephadm-shell output (banner-safe)."""
    start = text.find(opener)
    if start < 0:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(text[start:])
        return obj
    except json.JSONDecodeError:
        return None


def _health_text(installer):
    out, err = _ceph(installer, "ceph health detail", check_ec=False)
    return "\n".join(p for p in (out, err) if p)


def _osdmap_pause_flags(installer):
    """Return pause-related OSDMap flag names, or None if dump is unreadable."""
    out, err = _ceph(installer, "ceph osd dump --format json", check_ec=False)
    data = _first_json("\n".join(p for p in (out, err) if p), "{")
    if not isinstance(data, dict):
        return None
    flags = []
    flags_set = data.get("flags_set")
    if isinstance(flags_set, list):
        flags.extend(str(x) for x in flags_set)
    raw = data.get("flags")
    if isinstance(raw, str):
        flags.extend(part.strip() for part in raw.split(",") if part.strip())
    elif isinstance(raw, list):
        flags.extend(str(x) for x in raw)
    return flags


def _paused(installer):
    flags = _osdmap_pause_flags(installer)
    if flags is None:
        health = _health_text(installer)
        return "pauserd" in health or "pausewr" in health
    return "pauserd" in flags or "pausewr" in flags


def _config_get_timeout(installer):
    out, _ = _ceph(installer, f"ceph config get mgr {TIMEOUT_OPTION}")
    for line in reversed(out.splitlines()):
        token = line.strip()
        if re.fullmatch(r"-?\d+", token):
            return token
    return out.strip()


def _config_set_timeout(installer, value):
    log.info("ceph config set mgr %s %s (not config rm)", TIMEOUT_OPTION, value)
    _ceph(installer, f"ceph config set mgr {TIMEOUT_OPTION} {value}")


def _osd_pause(installer, timeout=PAUSE_FLAG_WAIT):
    out, _ = _ceph(installer, "ceph osd pause")
    log.info("ceph osd pause: %s", out)
    for _ in WaitUntil(timeout=timeout, interval=HEALTH_POLL):
        flags = _osdmap_pause_flags(installer)
        log.info("OSDMap flags after pause: %s", flags)
        if _paused(installer):
            return
    raise OperationFailedError(
        f"ceph osd pause did not set pauserd/pausewr within {timeout}s"
    )


def _osd_unpause(installer, timeout=UNPAUSE_WAIT):
    """Unpause OSDs. Return True if pauserd/pausewr are gone from OSDMap."""
    last = ""
    for _ in WaitUntil(timeout=timeout, interval=HEALTH_POLL):
        out, err = _ceph(installer, "ceph osd unpause", check_ec=False)
        last = "\n".join(p for p in (out, err) if p)
        log.info("ceph osd unpause: %s", last)
        flags = _osdmap_pause_flags(installer)
        log.info("OSDMap flags after unpause: %s", flags)
        if not _paused(installer):
            return True
    log.error("OSD unpause failed; pause flags still set:\n%s", last)
    return False


def _settle_after_unpause():
    """Short pause so mon/mgr catch up. Do not poll MDS; do not Fail on MDS lag."""
    log.info(
        "Waiting %ss after OSD unpause for mon/mgr to catch up",
        POST_UNPAUSE_SETTLE,
    )
    sleep(POST_UNPAUSE_SETTLE)


def _paused_apply_ssh_timeout(expected_seconds):
    """SSH cap for paused orch apply: at least 120s and at least the health window."""
    health_window = int(float(expected_seconds)) * 2 + 60
    return max(PAUSED_APPLY_SSH_TIMEOUT, health_window)


def _is_apply_ssh_timeout(exc):
    """True if cephci aborted the command because SSH/exec wait expired.

    ``SocketTimeoutException`` is raised directly. Channel wait uses
    ``TimeoutException`` wrapped as ``CommandFailed(tex)``. Other
    ``CommandFailed`` (SSH drop, paramiko, etc.) is not an apply timeout.
    """
    if isinstance(exc, (SocketTimeoutException, TimeoutException, TimeoutError)):
        return True
    if not isinstance(exc, CommandFailed):
        return False
    if exc.args and isinstance(
        exc.args[0], (TimeoutException, SocketTimeoutException, TimeoutError)
    ):
        return True
    msg = str(exc).lower()
    return (
        "failed to execute within" in msg
        or "allocated execution time" in msg
        or "exceed the allocated" in msg
    )


def _nfs_hosts(ceph_cluster):
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if not nfs_nodes:
        raise ConfigError("Need at least one node with the nfs role")
    return [n.hostname for n in nfs_nodes[:2]]


def _require_cephfs(installer):
    fs_ls, _ = _ceph(installer, "ceph fs ls")
    log.info("ceph fs ls:\n%s", fs_ls)
    if "cephfs" not in fs_ls:
        raise OperationFailedError(
            "CephFS volume 'cephfs' is required (suite bootstrap must create it)"
        )


def _require_dot_nfs_pool(installer):
    """Require pool .nfs from the seed NFS before pause+apply.

    Under OSD pause, a missing .nfs pool would stall on pool create. This
    suite must stall on rados get conf-nfs.* (the NFS RADOS timeout).
    """
    out, err = _ceph(installer, "ceph osd pool ls --format json", check_ec=False)
    data = _first_json("\n".join(p for p in (out, err) if p), "[")
    if isinstance(data, list):
        names = {str(item) for item in data}
    else:
        names = {tok for tok in re.split(r"\s+", out) if tok}
    log.info("osd pools: %s", sorted(names))
    if ".nfs" not in names:
        raise OperationFailedError(
            "Pool .nfs is missing. Setup must leave seed NFS running so the "
            "pool already exists. Pause+apply then times out on "
            "rados get conf-nfs.*, not on creating .nfs."
        )


def _nfs_spec(service_id, hosts, port, monitoring_port, qos_port=None):
    spec = {
        "port": int(port),
        "monitoring_port": int(monitoring_port),
    }
    if qos_port is not None:
        spec["cluster_qos_port"] = int(qos_port)
    return {
        "service_type": "nfs",
        "service_id": service_id,
        "placement": {
            "hosts": list(hosts),
            "count": len(hosts),
        },
        "spec": spec,
    }


def _ports_from_config(config):
    for key in ("port", "monitoring_port"):
        if config.get(key) is None:
            raise ConfigError(f"config.{key} is required (unique per NFS service)")
    qos = config.get("cluster_qos_port")
    return (
        int(config["port"]),
        int(config["monitoring_port"]),
        int(qos) if qos is not None else None,
    )


def _write_spec(installer, spec):
    path = f"/tmp/cephci-{spec['service_id']}.yaml"
    body = yaml.dump(spec, sort_keys=False, default_flow_style=False)
    spec_fp = None
    try:
        spec_fp = installer.remote_file(sudo=True, file_name=path, file_mode="wb")
        spec_fp.write(body.encode("utf-8"))
        spec_fp.flush()
    finally:
        if spec_fp is not None:
            try:
                spec_fp.close()
            except OSError:
                pass
    log.info("Wrote NFS spec %s:\n%s", path, body)
    return path


def _apply_spec(installer, spec_path, timeout=None):
    """Schedule orch apply. RADOS timeout is in health, not this CLI.

    ``timeout`` is the SSH/exec wait. None = cephci default 600s (setup).
    Paused stall applies pass max(120, 2*N+60). SSH timeout is logged and
    does not Fail the row; caller still polls health for the product string.
    """
    started = time()
    log.info(
        "ceph orch apply SSH wait=%s (None means default 600s)",
        timeout,
    )
    try:
        out, err = _ceph(
            installer,
            f"ceph orch apply -i {spec_path}",
            mount="/tmp",
            check_ec=False,
            timeout=timeout,
        )
    except _APPLY_SSH_TIMEOUT_EXC as exc:
        if timeout is None or not _is_apply_ssh_timeout(exc):
            raise
        elapsed = time() - started
        log.warning(
            "ceph orch apply SSH timed out after %.1fs (cap=%s): %s. "
            "Not failing yet; Pass/Fail is the health timeout string.",
            elapsed,
            timeout,
            exc,
        )
        return ""
    elapsed = time() - started
    text = "\n".join(p for p in (out, err) if p)
    log.info(
        "ceph orch apply returned in %.1fs (schedule only, not the RADOS timeout):\n%s",
        elapsed,
        text,
    )
    return text


def _nfs_orch_exists(installer, service_id):
    out, err = _ceph(
        installer,
        f"ceph orch ls --service-name nfs.{service_id} --format json",
        check_ec=False,
    )
    text = "\n".join(p for p in (out, err) if p)
    if re.search(r"No services reported", text, re.I):
        return False
    data = _first_json(text, "[")
    if data is None:
        return f"nfs.{service_id}" in text
    return bool(data)


def _orch_rm_nfs(installer, service_id):
    name = f"nfs.{service_id}"
    log.info("ceph orch rm %s --force", name)
    _ceph(installer, f"ceph orch rm {name} --force", check_ec=False)
    for _ in WaitUntil(timeout=ORCH_RM_WAIT, interval=5):
        if not _nfs_orch_exists(installer, service_id):
            log.info("Removed orch service nfs.%s", service_id)
            return
    log.warning("nfs.%s still listed after orch rm", service_id)


def _nfs_running_counts(installer, service_id):
    out, _ = _ceph(
        installer,
        f"ceph orch ls --service-name nfs.{service_id} --format json",
        check_ec=False,
    )
    rows = _first_json(out, "[")
    if not isinstance(rows, list) or not rows:
        return 0, 0
    status = rows[0].get("status") or {}
    return int(status.get("running") or 0), int(status.get("size") or 0)


def _wait_nfs_running(installer, service_id, timeout=180):
    for _ in WaitUntil(timeout=timeout, interval=5):
        running, size = _nfs_running_counts(installer, service_id)
        log.info("nfs.%s running %s/%s", service_id, running, size)
        if size > 0 and running == size:
            return
    raise OperationFailedError(
        f"nfs.{service_id} did not reach running==size within {timeout}s"
    )


def _timeout_line_for_service(health, service_id):
    for line in health.splitlines():
        if service_id in line and "timed out after" in line:
            return line
    return ""


def _apply_fail_lines(health, service_id):
    lines = []
    for line in health.splitlines():
        if service_id not in line:
            continue
        if (
            "timed out after" in line
            or "CEPHADM_APPLY_SPEC_FAIL" in line
            or "Failed to apply" in line
        ):
            lines.append(line)
    return lines


def _wait_stale_apply_health_gone(installer, service_id, timeout=STALE_HEALTH_WAIT):
    """Do not accept a leftover apply-fail line as this case's timeout."""
    last = ""
    for _ in WaitUntil(timeout=timeout, interval=HEALTH_POLL):
        last = _health_text(installer)
        if not _apply_fail_lines(last, service_id):
            log.info("No stale CEPHADM_APPLY_SPEC_FAIL for nfs.%s", service_id)
            return
    raise OperationFailedError(
        f"stale apply-fail health still mentions nfs.{service_id}; "
        f"refusing to false-Pass on a previous apply:\n{last}"
    )


def _timeout_seconds_match(got_text, expected_seconds):
    """True if health says N or N.0 seconds (older vs newer cephadm wording)."""
    want = float(expected_seconds)
    match = TIMEOUT_STR_RE.search(got_text)
    if not match:
        return False
    return abs(float(match.group(1)) - want) < 0.01


def _wait_apply_timeout_string(installer, service_id, expected_seconds):
    """Poll health detail for CEPHADM_APPLY_SPEC_FAIL and the mgr timeout.

    Accepts both ``timed out after 30 seconds`` and ``timed out after 30.0 seconds``.
    """
    want = float(expected_seconds)
    limit = int(want) * 2 + 60
    last_health = ""
    for _ in WaitUntil(timeout=limit, interval=HEALTH_POLL):
        last_health = _health_text(installer)
        if "CEPHADM_APPLY_SPEC_FAIL" not in last_health:
            continue
        line = _timeout_line_for_service(last_health, service_id)
        if not line:
            continue
        match = TIMEOUT_STR_RE.search(line)
        if not match:
            continue
        got = match.group(0)
        if not _timeout_seconds_match(got, want):
            raise OperationFailedError(
                f"nfs.{service_id}: expected timed out after {want:g} seconds "
                f"(N or N.0), got '{got}'\n{last_health}"
            )
        log.info("Measured mgr/health timeout string: %s", got)
        log.info("ceph health detail:\n%s", last_health)
        return last_health
    raise OperationFailedError(
        f"nfs.{service_id}: did not see CEPHADM_APPLY_SPEC_FAIL with "
        f"timed out after {want:g} seconds (N or N.0) within {limit}s."
        f"\nLast health:\n{last_health}"
    )


def _run_setup(installer, hosts, config):
    seed_id = config.get("service_id", DEFAULT_SEED_ID)
    spec = _nfs_spec(seed_id, hosts, *_ports_from_config(config))
    path = _write_spec(installer, spec)
    try:
        _require_cephfs(installer)
        version, _ = _ceph(installer, "ceph version")
        log.info("ceph version: %s", version)
        _apply_spec(installer, path)
        _wait_nfs_running(installer, seed_id)
        _require_dot_nfs_pool(installer)
        log.info(
            "TEST PASSED - NFS rados command timeout setup nfs.%s; pool .nfs present",
            seed_id,
        )
        return 0
    finally:
        installer.exec_command(sudo=True, cmd=f"rm -f {path}", check_ec=False)


def _run_teardown(installer, config):
    seed_id = config.get("service_id", DEFAULT_SEED_ID)
    stall_ids = [str(sid) for sid in (config.get("stall_service_ids") or [])]
    errors = []
    unpaused = False
    try:
        unpaused = _osd_unpause(installer)
    except Exception as exc:
        log.error("teardown: osd unpause failed: %s", exc)
        errors.append(f"osd unpause raised: {exc}")
    if not unpaused:
        errors.append("OSDs still paused after unpause")
    try:
        _config_set_timeout(installer, DEFAULT_TIMEOUT)
    except Exception as exc:
        log.error("teardown: config set 30 failed: %s", exc)
        errors.append(f"config set 30 failed: {exc}")
    rm_ids = list(stall_ids)
    if seed_id not in rm_ids:
        rm_ids.append(seed_id)
    if unpaused:
        for sid in rm_ids:
            try:
                _orch_rm_nfs(installer, sid)
            except Exception as exc:
                log.error("teardown: orch rm nfs.%s failed: %s", sid, exc)
                errors.append(f"orch rm nfs.{sid} failed: {exc}")
    else:
        log.error("teardown: skipping orch rm while OSDs are still paused: %s", rm_ids)
    if errors:
        raise OperationFailedError("teardown: " + "; ".join(errors))
    log.info(
        "TEST PASSED - NFS rados command timeout teardown; OSDs unpaused, timeout 30, orch rm done"
    )
    return 0


def _run_timeout_case(installer, hosts, config):
    seed_id = config.get("seed_service_id", DEFAULT_SEED_ID)
    service_id = config.get("service_id")
    if not service_id:
        raise ConfigError("config.service_id is required for a timeout case")
    if service_id == seed_id:
        raise ConfigError("timeout service_id must not be the default/seed NFS")

    expected = float(config.get("expect"))
    set_value = config.get("set_value")
    if set_value is not None:
        set_value = str(set_value)
    get_ok = {str(v) for v in config.get("get_ok", [])}
    strict_get = config.get("strict_get", True)

    # Unpause leftover pause (must not apply while paused). config set/get is
    # allowed even if unpause failed.
    unpaused = _osd_unpause(installer)
    if unpaused:
        _settle_after_unpause()
        _orch_rm_nfs(installer, service_id)
        _wait_stale_apply_health_gone(installer, service_id)

    path = None
    unpaused_ok = True
    case_exc = None
    default_wrong = False
    entry_got = None
    try:
        if set_value is None:
            got = _config_get_timeout(installer)
            log.info(
                "%s (no set_value; expect compiled default 30) = %s",
                TIMEOUT_OPTION,
                got,
            )
            if get_ok and got not in get_ok:
                default_wrong = True
                entry_got = got
                log.warning(
                    "%s is %r, not 30 (leftover config set or other test). "
                    "Setting 30 to measure health; this row will Fail even if "
                    "health matches (not proof of compiled default).",
                    TIMEOUT_OPTION,
                    got,
                )
                _config_set_timeout(installer, DEFAULT_TIMEOUT)
                got = _config_get_timeout(installer)
                log.info("config get %s after heal to 30 = %s", TIMEOUT_OPTION, got)
                if strict_get and got not in get_ok:
                    raise OperationFailedError(
                        f"After setting {TIMEOUT_OPTION}=30, get is {got!r}, "
                        f"expected one of {sorted(get_ok)}"
                    )
        else:
            _config_set_timeout(installer, set_value)
            got = _config_get_timeout(installer)
            log.info("config get %s = %s", TIMEOUT_OPTION, got)
            if strict_get and get_ok and got not in get_ok:
                raise OperationFailedError(
                    f"config get {TIMEOUT_OPTION}={got!r}, expected one of "
                    f"{sorted(get_ok)} (set {set_value} must take effect)"
                )

        if not unpaused:
            raise OperationFailedError(
                "ceph osd unpause failed; not applying NFS while OSDs are still paused"
            )

        _require_dot_nfs_pool(installer)
        running, size = _nfs_running_counts(installer, seed_id)
        log.info("seed nfs.%s running %s/%s before pause", seed_id, running, size)

        spec = _nfs_spec(service_id, hosts, *_ports_from_config(config))
        path = _write_spec(installer, spec)
        _osd_pause(installer)
        _apply_spec(
            installer,
            path,
            timeout=_paused_apply_ssh_timeout(expected),
        )
        _wait_apply_timeout_string(installer, service_id, expected)
    except Exception as exc:
        case_exc = exc
    finally:
        unpaused_ok = _osd_unpause(installer)
        if unpaused_ok:
            _orch_rm_nfs(installer, service_id)
        else:
            log.error(
                "Skipping orch rm nfs.%s; OSDs still paused after unpause", service_id
            )
        if path:
            installer.exec_command(sudo=True, cmd=f"rm -f {path}", check_ec=False)

    if case_exc:
        if not unpaused_ok:
            log.error("Also failed to unpause OSDs after this timeout case")
        raise case_exc
    if default_wrong:
        raise OperationFailedError(
            f"At entry {TIMEOUT_OPTION}={entry_got!r}, not compiled default 30 "
            f"(leftover config set / other test). After config set 30, health "
            f"timed out after {expected:g} seconds. Row Fails: default was not "
            f"30. Health match is not proof of compiled default."
        )
    if not unpaused_ok:
        log.error(
            "Timeout string matched for nfs.%s; OSDs still paused after unpause. "
            "Product row stays Pass; teardown must clear pause.",
            service_id,
        )
        return 0
    log.info(
        "TEST PASSED - NFS rados command timeout case nfs.%s expect=%s",
        service_id,
        expected,
    )
    return 0


def run(ceph_cluster, **kw):
    """Setup default NFS, one timeout case, or teardown."""
    config = kw.get("config") or {}
    mode = config.get("mode", "timeout")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    hosts = _nfs_hosts(ceph_cluster)

    try:
        if mode == "setup":
            return _run_setup(installer, hosts, config)
        if mode == "teardown":
            return _run_teardown(installer, config)
        if mode == "timeout":
            return _run_timeout_case(installer, hosts, config)
        raise ConfigError(f"Unknown mode {mode!r}; use setup, timeout, or teardown")
    except Exception as exc:
        log.exception("NFS rados command timeout %s failed: %s", mode, exc)
        return 1
