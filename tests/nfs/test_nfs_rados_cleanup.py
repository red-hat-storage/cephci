"""NFS orch rm warning + .nfs RADOS cleanup (IBMCEPH-14094 / PR#71098).

Scenarios (suite ``config.scenario``):
  * orch_rm_warning_leftovers — Acc: orch rm warns; durable leftovers remain
  * cluster_rm_after_orch_rm — Acc: nfs cluster rm empties namespace after orch rm
  * same_name_recreate — Acc: recreate same id without stale exports
  * cluster_rm_alone — Reg: nfs cluster rm alone cleans service + namespace
  * cross_cluster_isolation — Reg: remove A; A empty; B durable objects intact

Clusters are created via orch YAML apply on all NFS-role hosts with
hardcoded ports (never default 2049). Removed-cluster namespaces must be
fully empty (incl. grace/rec-*). Live peer asserts durable objects only.
"""

import json
import re
import shlex

from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import (
    check_nfs_daemons_removed,
    create_nfs_via_file_and_verify,
)
from utility.log import Log

log = Log(__name__)

ORCH_RM_WAIT = 180
NFS_READY_WAIT = 300
NAMESPACE_EMPTY_WAIT = 120
EXPORT_WAIT = 60
# Per-poll shell budget inside WaitUntil (keep below ORCH_RM_WAIT).
POLL_CMD_TIMEOUT = 60
FS_NAME = "cephfs"
EXPORT_PSEUDO = "/export_rados_cleanup"
DURABLE_PREFIXES = ("conf-", "export-", "qosconf-", "userconf-")
SERVICE_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")

# Orch rm warning (PR#71098) — match product wording, not Acc labels.
ORCH_RM_WARN_DAEMON = "only removes the NFS service daemon"
ORCH_RM_WARN_CLEANUP = "nfs cluster rm"
ORCH_RM_WARN_FULL = "full resource cleanup"


def _ceph(installer, cmd, check_ec=True, timeout=NFS_READY_WAIT):
    """Run a command inside ``cephadm shell``. Returns (stdout, stderr).

    Default timeout matches NFS helper waits (300s), tighter than CephNode's
    600s default so WaitUntil polls cannot hang one stuck shell that long.
    """
    out, err = installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- {cmd}",
        check_ec=check_ec,
        timeout=timeout,
    )
    stdout = out.strip() if isinstance(out, str) else str(out or "").strip()
    stderr = err.strip() if isinstance(err, str) else str(err or "").strip()
    return stdout, stderr


def _first_json(text, opener="{"):
    start = text.find(opener)
    if start < 0:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(text[start:])
        return obj
    except json.JSONDecodeError:
        return None


def _nfs_hosts(ceph_cluster):
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if len(nfs_nodes) < 2:
        raise ConfigError(
            f"Need at least 2 nodes with the nfs role, found {len(nfs_nodes)}"
        )
    return [n.hostname for n in nfs_nodes], nfs_nodes


def _require_cephfs(installer):
    fs_ls, _ = _ceph(installer, "ceph fs ls")
    log.info("ceph fs ls:\n%s", fs_ls)
    if FS_NAME not in fs_ls:
        raise OperationFailedError(
            f"CephFS volume '{FS_NAME}' is required (suite bootstrap must create it)"
        )


def _cluster_cfg(config, key):
    """Resolve cluster A/B from suite config; every field is required (no defaults)."""
    block = config.get(f"cluster_{key}") or config.get(key) or {}
    if not isinstance(block, dict):
        raise ConfigError(f"config.cluster_{key} must be a mapping")
    for field in ("service_id", "port", "monitoring_port", "cluster_qos_port"):
        if block.get(field) in (None, ""):
            raise ConfigError(f"config.cluster_{key}.{field} is required")
    service_id = str(block["service_id"])
    if not SERVICE_ID_RE.fullmatch(service_id):
        raise ConfigError(
            f"config.cluster_{key}.service_id is not a valid NFS id: {service_id!r}"
        )
    return {
        "service_id": service_id,
        "port": int(block["port"]),
        "monitoring_port": int(block["monitoring_port"]),
        "cluster_qos_port": int(block["cluster_qos_port"]),
    }


def _nfs_spec(service_id, hosts, port, monitoring_port, cluster_qos_port):
    return {
        "service_type": "nfs",
        "service_id": service_id,
        "placement": {
            "hosts": list(hosts),
            "count": len(hosts),
        },
        "spec": {
            "port": int(port),
            "monitoring_port": int(monitoring_port),
            "cluster_qos_port": int(cluster_qos_port),
        },
    }


def _apply_nfs_spec(installer, hosts, cluster, nfs_nodes=None):
    """Apply NFS orch YAML via shared helper (non-default ports + qos)."""
    sid = cluster["service_id"]
    spec = _nfs_spec(
        sid,
        hosts,
        cluster["port"],
        cluster["monitoring_port"],
        cluster["cluster_qos_port"],
    )
    if not create_nfs_via_file_and_verify(
        installer,
        [spec],
        timeout=NFS_READY_WAIT,
        nfs_name=sid,
        nfs_nodes=nfs_nodes,
    ):
        raise OperationFailedError(f"orch apply / verify failed for nfs.{sid}")
    return sid


def _assert_removed_deps(installer, service_id):
    """Stale daemon_config_deps check after nfs.<id> is gone (port reuse safety).

    Uses shared helper with prefix_cephadm=True so installer has no bare ``ceph``.
    That path skips the shared orch-ls retry (we already wait locally) and only
    runs the deps assert on builds >= 20.2.2-75.
    """
    check_nfs_daemons_removed(installer, service_id, prefix_cephadm=True)


def _nfs_orch_exists(installer, service_id):
    """True if orch still lists nfs.<id>.

    Missing service may return JSON ``[]`` or plain ``No services reported``
    (often with rc 0). Treat both as gone; other unparsable output raises.
    """
    svc = shlex.quote(f"nfs.{service_id}")
    out, err = _ceph(
        installer,
        f"ceph orch ls --service-name {svc} --format json",
        check_ec=False,
        timeout=POLL_CMD_TIMEOUT,
    )
    data = _first_json(out, "[")
    if data is not None:
        return bool(data)
    if re.search(r"No services reported", "\n".join((out, err)), re.I):
        return False
    raise OperationFailedError(f"orch ls nfs.{service_id}: unparsable:\n{out}\n{err}")


def _rados_ls(installer, namespace):
    """List objects in .nfs namespace. Fail hard on command error (no false-empty).

    Use stdout only: cephadm shell banners/errors go to stderr. Empty stdout
    after a successful exit means the namespace is empty.
    """
    out, _ = _ceph(
        installer,
        f"rados ls -N {shlex.quote(namespace)} -p .nfs",
        check_ec=True,
    )
    objs = {line.strip() for line in out.splitlines() if line.strip()}
    log.info("rados ls -N %s -p .nfs -> %s", namespace, sorted(objs))
    return objs


def _durable_objects(objs):
    return {o for o in objs if o.startswith(DURABLE_PREFIXES)}


def _wait_rados(installer, ns, pred, what, timeout=EXPORT_WAIT):
    """Poll ``rados ls`` until ``pred(objs)``; include last-seen objects on timeout."""
    objs = set()
    for _ in WaitUntil(timeout=timeout, interval=3):
        objs = _rados_ls(installer, ns)
        if pred(objs):
            return objs
    raise OperationFailedError(
        f"Timed out ({timeout}s) waiting for {what} in .nfs ns {ns}: {sorted(objs)}"
    )


def _wait_durable_present(installer, service_id, timeout=EXPORT_WAIT):
    def _pred(objs):
        durable = _durable_objects(objs)
        return any(o.startswith("conf-") for o in durable) and any(
            o.startswith("export-") for o in durable
        )

    return _durable_objects(
        _wait_rados(
            installer,
            service_id,
            _pred,
            "conf-/export- objects",
            timeout=timeout,
        )
    )


def _wait_conf_present(installer, service_id, timeout=EXPORT_WAIT):
    """Wait until recreated cluster has conf-* (no export expected)."""
    return _wait_rados(
        installer,
        service_id,
        lambda o: any(x.startswith("conf-") for x in o),
        "conf-*",
        timeout=timeout,
    )


def _wait_namespace_empty(installer, service_id, timeout=NAMESPACE_EMPTY_WAIT):
    _wait_rados(
        installer,
        service_id,
        lambda o: not o,
        "empty namespace",
        timeout=timeout,
    )


def _create_export(installer, service_id, pseudo=EXPORT_PSEUDO):
    out, err = _ceph(
        installer,
        f"ceph nfs export create cephfs {shlex.quote(service_id)} "
        f"{shlex.quote(pseudo)} {FS_NAME} --path=/",
        check_ec=True,
    )
    log.info(
        "export create %s:%s\n%s",
        service_id,
        pseudo,
        "\n".join(filter(None, (out, err))),
    )


def _export_ls(installer, service_id):
    """Return export list JSON. Fail hard if the command or JSON parse fails."""
    out, _ = _ceph(
        installer,
        f"ceph nfs export ls {shlex.quote(service_id)} --format json",
        check_ec=True,
    )
    data = _first_json(out, "[")
    if data is None:
        data = _first_json(out, "{")
    if data is None:
        raise OperationFailedError(
            f"nfs export ls {service_id}: could not parse JSON from stdout:\n{out}"
        )
    log.info("nfs export ls %s: %s", service_id, data)
    return data


def _deploy_cluster(installer, hosts, cluster, nfs_nodes=None):
    sid = _apply_nfs_spec(installer, hosts, cluster, nfs_nodes=nfs_nodes)
    _create_export(installer, sid)
    durable = _wait_durable_present(installer, sid)
    log.info("Deployed nfs.%s durable objects: %s", sid, sorted(durable))
    return durable


def _wait_daemons_gone(installer, service_id):
    """Wait until ``ceph orch ps`` reports no daemons for nfs.<id>.

    Spec removal can race ahead of daemon teardown; later rows reuse the same
    ports on the same hosts, so we must wait for daemons to release them.
    """
    svc = shlex.quote(f"nfs.{service_id}")
    last = ""
    for _ in WaitUntil(timeout=ORCH_RM_WAIT, interval=5):
        out, err = _ceph(
            installer,
            f"ceph orch ps --service-name {svc} --format json",
            check_ec=False,
            timeout=POLL_CMD_TIMEOUT,
        )
        last = "\n".join(p for p in (out, err) if p)
        log.info("orch ps nfs.%s -> %s", service_id, last[:500])
        if re.search(r"No.*(daemon|service).*reported", last, re.I):
            log.info("No daemons left for nfs.%s", service_id)
            return
        daemons = _first_json(out, "[")
        # Do not treat empty/unparsable stdout as "gone" (check_ec=False can
        # leave stdout empty on a failed cephadm shell). Retry until timeout.
        if daemons is None:
            log.warning(
                "orch ps nfs.%s unparsable, retrying:\n%s",
                service_id,
                last[:500],
            )
            continue
        if not daemons:
            log.info("No daemons left for nfs.%s", service_id)
            return
    raise OperationFailedError(
        f"nfs.{service_id} daemons still present after {ORCH_RM_WAIT}s:\n{last}"
    )


def _wait_orch_gone(installer, service_id, timeout=ORCH_RM_WAIT):
    """Wait until ``ceph orch ls`` no longer lists nfs.<id>."""
    for _ in WaitUntil(timeout=timeout, interval=5):
        if not _nfs_orch_exists(installer, service_id):
            log.info("Removed orch service nfs.%s", service_id)
            return
    raise OperationFailedError(
        f"nfs.{service_id} still listed in orch after {timeout}s"
    )


def _orch_rm_nfs(installer, service_id):
    name = f"nfs.{service_id}"
    log.info("ceph orch rm %s", name)
    # check_ec=False: always capture warning text; rc is logged via exit_status.
    out, err = _ceph(installer, f"ceph orch rm {shlex.quote(name)}", check_ec=False)
    rc = installer.exit_status
    text = "\n".join(p for p in (out, err) if p)
    log.info("orch rm rc=%s output:\n%s", rc, text)
    _wait_orch_gone(installer, service_id)
    _wait_daemons_gone(installer, service_id)
    _assert_removed_deps(installer, service_id)
    return text


def _assert_orch_rm_warning(orch_rm_output, service_id):
    text = orch_rm_output or ""
    lower = text.lower()
    missing = [
        w
        for w in (ORCH_RM_WARN_DAEMON, ORCH_RM_WARN_CLEANUP, ORCH_RM_WARN_FULL)
        if w.lower() not in lower
    ]
    if service_id not in text:
        missing.append(f"service_id {service_id} mentioned")
    if missing:
        raise OperationFailedError(
            f"orch rm warning missing expected PR#71098 wording {missing}:\n{text}"
        )


def _nfs_cluster_rm(installer, service_id):
    log.info("ceph nfs cluster rm %s", service_id)
    out, err = _ceph(
        installer,
        f"ceph nfs cluster rm {shlex.quote(service_id)}",
        check_ec=True,
    )
    text = "\n".join(p for p in (out, err) if p)
    log.info("nfs cluster rm output:\n%s", text)
    return text


def _cleanup_cluster(installer, service_id):
    """Idempotent teardown; leave nfs.<id> gone and namespace empty for next row.

    Always try ``nfs cluster rm`` so post-orch-rm RADOS orphans still take the
    product cleanup path (mgr may omit the id from ``nfs cluster ls``). On
    umbrella, already-gone prints ``Cluster does not exist`` with rc 0 (no
    raise); other failures are logged and cleanup continues.

    Returns True when cleanup finished cleanly, False on leak / incomplete wipe.
    """
    try:
        _nfs_cluster_rm(installer, service_id)
    except Exception as exc:
        log.warning("cleanup nfs cluster rm %s: %s", service_id, exc)
    try:
        if _nfs_orch_exists(installer, service_id):
            _orch_rm_nfs(installer, service_id)
        else:
            _wait_daemons_gone(installer, service_id)
            _assert_removed_deps(installer, service_id)
        # Last-resort harness wipe so the next suite row is not polluted.
        for obj in list(_rados_ls(installer, service_id)):
            _ceph(
                installer,
                f"rados -p .nfs -N {shlex.quote(service_id)} rm {shlex.quote(obj)}",
                check_ec=False,
                timeout=120,
            )
        _wait_namespace_empty(installer, service_id)
        return True
    except Exception as exc:
        log.error("cleanup of nfs.%s incomplete: %s", service_id, exc)
        return False


def _scenario_orch_rm_warning_leftovers(installer, hosts, config, nfs_nodes=None):
    cluster = _cluster_cfg(config, "a")
    sid = cluster["service_id"]
    _deploy_cluster(installer, hosts, cluster, nfs_nodes=nfs_nodes)
    orch_out = _orch_rm_nfs(installer, sid)
    _assert_orch_rm_warning(orch_out, sid)
    leftovers = _rados_ls(installer, sid)
    durable = _durable_objects(leftovers)
    if not any(o.startswith("conf-") for o in durable):
        raise OperationFailedError(
            f"Expected conf-* leftover after orch rm; got {sorted(leftovers)}"
        )
    if not any(o.startswith("export-") for o in durable):
        raise OperationFailedError(
            f"Expected export-* leftover after orch rm; got {sorted(leftovers)}"
        )


def _scenario_cluster_rm_after_orch_rm(installer, hosts, config, nfs_nodes=None):
    cluster = _cluster_cfg(config, "a")
    sid = cluster["service_id"]
    _deploy_cluster(installer, hosts, cluster, nfs_nodes=nfs_nodes)
    orch_out = _orch_rm_nfs(installer, sid)
    _assert_orch_rm_warning(orch_out, sid)
    if not _durable_objects(_rados_ls(installer, sid)):
        raise OperationFailedError("No durable leftovers before nfs cluster rm")
    _nfs_cluster_rm(installer, sid)
    _wait_daemons_gone(installer, sid)
    _assert_removed_deps(installer, sid)
    _wait_namespace_empty(installer, sid)


def _scenario_same_name_recreate(installer, hosts, config, nfs_nodes=None):
    """Recreate same id after full cleanup; must not inherit prior exports.

    Known product failure: IBMCEPH-21921 (ghost exports). Suite sets
    abort-on-fail: false so the suite continues on Fail; when 21921 is fixed
    this scenario should Pass without changing that flag (optional: set true
    later to gate the suite on this row).
    """
    cluster = _cluster_cfg(config, "a")
    sid = cluster["service_id"]
    _deploy_cluster(installer, hosts, cluster, nfs_nodes=nfs_nodes)
    orch_out = _orch_rm_nfs(installer, sid)
    _assert_orch_rm_warning(orch_out, sid)
    _nfs_cluster_rm(installer, sid)
    _wait_daemons_gone(installer, sid)
    _assert_removed_deps(installer, sid)
    _wait_namespace_empty(installer, sid)

    # Recreate cluster only (same id + hardcoded ports); do not create export.
    _apply_nfs_spec(installer, hosts, cluster, nfs_nodes=nfs_nodes)
    _wait_conf_present(installer, sid)
    # Bounded poll: fail on first non-empty export list (IBMCEPH-21921 ghost).
    for _ in WaitUntil(timeout=30, interval=5):
        exports = _export_ls(installer, sid)
        if exports:
            raise OperationFailedError(
                f"Same-name recreate inherited exports (IBMCEPH-21921?): {exports!r}"
            )
    objs = _rados_ls(installer, sid)
    export_objs = {o for o in objs if o.startswith("export-")}
    if export_objs:
        raise OperationFailedError(
            f"Same-name recreate has export-* in RADOS "
            f"(IBMCEPH-21921?): {sorted(export_objs)}"
        )


def _scenario_cluster_rm_alone(installer, hosts, config, nfs_nodes=None):
    cluster = _cluster_cfg(config, "a")
    sid = cluster["service_id"]
    _deploy_cluster(installer, hosts, cluster, nfs_nodes=nfs_nodes)
    _nfs_cluster_rm(installer, sid)
    _wait_orch_gone(installer, sid)
    _wait_daemons_gone(installer, sid)
    _assert_removed_deps(installer, sid)
    _wait_namespace_empty(installer, sid)


def _scenario_cross_cluster_isolation(installer, hosts, config, nfs_nodes=None):
    cluster_a = _cluster_cfg(config, "a")
    cluster_b = _cluster_cfg(config, "b")
    sid_a = cluster_a["service_id"]
    sid_b = cluster_b["service_id"]
    if sid_a == sid_b:
        raise ConfigError("cluster_a and cluster_b service_id must differ")
    if cluster_a["port"] == cluster_b["port"]:
        raise ConfigError("cluster_a and cluster_b port must differ")

    _deploy_cluster(installer, hosts, cluster_a, nfs_nodes=nfs_nodes)
    _deploy_cluster(installer, hosts, cluster_b, nfs_nodes=nfs_nodes)
    before_b_durable = _durable_objects(_rados_ls(installer, sid_b))
    if not before_b_durable:
        raise OperationFailedError(f"Cluster B {sid_b} has no durable objects")

    _nfs_cluster_rm(installer, sid_a)
    _wait_daemons_gone(installer, sid_a)
    _assert_removed_deps(installer, sid_a)
    _wait_namespace_empty(installer, sid_a)

    after_b = _rados_ls(installer, sid_b)
    after_b_durable = _durable_objects(after_b)
    if after_b_durable != before_b_durable:
        raise OperationFailedError(
            f"Cluster B durable .nfs objects changed after removing A: "
            f"before={sorted(before_b_durable)} after={sorted(after_b_durable)}"
        )


SCENARIOS = {
    "orch_rm_warning_leftovers": _scenario_orch_rm_warning_leftovers,
    "cluster_rm_after_orch_rm": _scenario_cluster_rm_after_orch_rm,
    "same_name_recreate": _scenario_same_name_recreate,
    "cluster_rm_alone": _scenario_cluster_rm_alone,
    "cross_cluster_isolation": _scenario_cross_cluster_isolation,
}


def run(ceph_cluster, **kw):
    """Entry point for CephCI."""
    config = kw.get("config") or {}
    scenario = config.get("scenario")
    if scenario not in SCENARIOS:
        raise ConfigError(
            f"config.scenario must be one of {sorted(SCENARIOS)}, got {scenario!r}"
        )

    installer = ceph_cluster.get_nodes(role="installer")[0]
    hosts, nfs_nodes = _nfs_hosts(ceph_cluster)
    log.info("NFS placement hosts (%s): %s", len(hosts), hosts)

    cluster_a = _cluster_cfg(config, "a")
    tracked = {cluster_a["service_id"]}
    if scenario == "cross_cluster_isolation":
        tracked.add(_cluster_cfg(config, "b")["service_id"])

    rc = 1
    try:
        _require_cephfs(installer)
        SCENARIOS[scenario](installer, hosts, config, nfs_nodes=nfs_nodes)
        rc = 0
    except Exception as exc:
        log.error("Scenario %s failed: %s", scenario, exc, exc_info=True)
    finally:
        leaked = [s for s in sorted(tracked) if not _cleanup_cluster(installer, s)]
    if leaked:
        log.error("Cleanup incomplete for %s", leaked)
        return 1
    if rc == 0:
        log.info("TEST PASSED - %s (clusters=%s)", scenario, sorted(tracked))
    return rc
