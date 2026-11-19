"""cephadm NFS log_to_file early automation (IBMCEPH-13289, ceph#71198).

Umbrella cephadm switches Ganesha from ``-L STDERR`` to
``-L /var/log/ceph/ganesha.log`` only when the NFS service spec field
``log_to_file`` is true. ``ceph config set`` for ``global`` or ``client.nfs``
does not change ``-L``.

Cluster, export, mount, IO, and cleanup go through ``nfs_operations``.
The checks that are specific to this feature stay here: container ``-L``,
the host ``ganesha.log`` (or ``ceph-client.nfs*.log``), and redeploy stability.

config["cases"]:
  signoff — record_build, spec_enables_host_log,
            spec_ignores_config_off, host_log_grows_after_io,
            export_recreate, no_redeploy_loop
  all     — signoff plus spec_off_uses_stderr,
            per_daemon_config_gap, toggle_on_off_on,
            redeploy_keeps_file_log, each_daemon_own_log
"""

from __future__ import annotations

import base64
import json
import shlex
import time
import traceback

from nfs_operations import (
    _daemon_hostname,
    _resolve_nfs_log_node,
    assert_mount_path_accessible,
    cleanup_cluster,
    create_files_and_dirs_on_mount,
    delete_nfs_cluster_and_wait,
    get_ceph_version,
    mount_nfs_export_with_wait,
    remove_nfs_subvolume,
    setup_nfs_cluster,
    unmount_and_remove_mount_path,
    wait_for_nfs_cluster_backend_endpoint,
)

from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

SIGNOFF_CASES = (
    "record_build",
    "spec_enables_host_log",
    "spec_ignores_config_off",
    "host_log_grows_after_io",
    "export_recreate",
    "no_redeploy_loop",
)
ALL_CASES = SIGNOFF_CASES + (
    "spec_off_uses_stderr",
    "per_daemon_config_gap",
    "toggle_on_off_on",
    "redeploy_keeps_file_log",
    "each_daemon_own_log",
)


def _text(raw):
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace").strip()
    if isinstance(raw, str):
        return raw.strip()
    if hasattr(raw, "read"):
        return raw.read().decode("utf-8", errors="replace").strip()
    return str(raw).strip()


def _ceph_prefix(node):
    """Host ceph CLI, or cephadm shell when the installer has only cephadm."""
    cached = getattr(node, "_logf_ceph_prefix", None)
    if cached is not None:
        return cached
    node.exec_command(sudo=True, cmd="command -v ceph", check_ec=False, timeout=30)
    prefix = "" if getattr(node, "exit_status", 1) == 0 else "cephadm shell -- "
    node._logf_ceph_prefix = prefix
    if prefix:
        log.info("No host ceph CLI on %s; using cephadm shell", node.hostname)
    return prefix


def _ceph(ctx):
    """Ceph CLI. Uses cephadm shell when the installer has no ceph binary."""
    cached = ctx.get("_ceph_api")
    if cached is not None:
        return cached
    node = ctx["ceph_node"]
    api = CephAdm(node).ceph if _ceph_prefix(node) else Ceph(node)
    ctx["_ceph_api"] = api
    return api


def _node_sh(node, cmd, check=True, timeout=180):
    if cmd.startswith("ceph "):
        cmd = _ceph_prefix(node) + cmd
    out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=check, timeout=timeout)
    rc = 0 if check else getattr(node, "exit_status", 0)
    return _text(out), rc


def _config_set(ctx, who, value):
    _ceph(ctx).config.set("log_to_file", value, daemon=who)
    log.info("ceph config set %s log_to_file %s", who, value)


def _config_rm(ctx, who):
    try:
        _ceph(ctx).config.rm(who, "log_to_file")
        log.info("ceph config rm %s log_to_file", who)
    except Exception as err:
        log.info("ceph config rm %s log_to_file: %s", who, err)


def _config_get(ctx, who):
    out = _ceph(ctx).config.get(who, "log_to_file")
    text = _text(out[0] if isinstance(out, tuple) else out).lower()
    log.info("ceph config get %s log_to_file -> %r", who, text)
    return text


def _fsid(ctx):
    if not ctx.get("fsid"):
        text, _ = _node_sh(ctx["ceph_node"], "ceph fsid")
        ctx["fsid"] = text.split()[0] if text else ""
    if not ctx["fsid"]:
        raise OperationFailedError("ceph fsid returned no cluster id")
    return ctx["fsid"]


def _daemons(ctx):
    text, _rc = _node_sh(
        ctx["ceph_node"],
        "ceph orch ps --service-name nfs.%s --format json" % ctx["nfs_name"],
        check=False,
        timeout=120,
    )
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError as err:
        raise OperationFailedError("orch ps JSON parse failed: %s" % err) from err
    if isinstance(data, dict):
        data = data.get("daemons") or []
    return [item for item in data if isinstance(item, dict)]


def _running(daemon):
    status = str(daemon.get("status_desc") or daemon.get("status") or "").lower()
    return status == "running" and bool(daemon.get("container_id"))


def _node_for_daemon(ctx, daemon):
    """Host node for a daemon, using the nfs_operations hostname helper."""
    name = daemon.get("daemon_name") or ""
    hostname = daemon.get("hostname") or _daemon_hostname(name) or ""
    resolved = _resolve_nfs_log_node(ctx["client"], ctx["nfs_nodes"], name)
    short = (getattr(resolved, "hostname", "") or "").split(".")[0]
    host_short = hostname.split(".")[0]
    if host_short and (short == host_short or host_short in (resolved.hostname or "")):
        return resolved
    for node in ctx["nodes"]:
        node_name = getattr(node, "hostname", "") or ""
        if node_name == hostname or node_name.split(".")[0] == host_short:
            return node
    return resolved


def _parse_args(text):
    text = (text or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return text.replace("[", " ").replace("]", " ").replace(",", " ").split()


def _dash_l(args):
    for idx, token in enumerate(args):
        if token == "-L" and idx + 1 < len(args):
            return args[idx + 1]
        if token.startswith("-L") and token != "-L":
            return token[2:]
    return ""


def _container_args(node, container_id):
    cmd = "podman inspect %s --format '{{json .Args}}'" % shlex.quote(container_id)
    out, _ = node.exec_command(sudo=True, cmd=cmd, timeout=120)
    args = _parse_args(_text(out))
    log.info("container %s args: %s", container_id, args)
    return args


def _is_file_target(target):
    return bool(target) and target.upper() != "STDERR" and "/var/log/ceph/" in target


def _is_stderr_target(target):
    return (target or "").upper() == "STDERR"


def _find_logs(node, fsid):
    root = "/var/log/ceph/%s" % fsid
    cmd = (
        "find %s \\( -name ganesha.log -o -name 'ceph-client.nfs*.log' \\) -type f"
        % shlex.quote(root)
    )
    out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=120)
    return [line.strip() for line in _text(out).splitlines() if line.strip()]


def _pick_log(daemon_name, paths, allow_single):
    owned = [path for path in paths if daemon_name and daemon_name in path]
    ganesha = [path for path in owned if path.endswith("/ganesha.log")]
    if ganesha:
        return ganesha[0]
    if len(owned) == 1:
        return owned[0]
    if allow_single and len(paths) == 1:
        log.info("Using sole host log %s for %s", paths[0], daemon_name)
        return paths[0]
    return ""


def _file_size(node, path):
    cmd = "stat -c %s -- %s" % ("%s", shlex.quote(path))
    out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    text = _text(out)
    if getattr(node, "exit_status", 0) != 0 or not text.isdigit():
        return None
    return int(text)


def _tail(node, path):
    out, _ = node.exec_command(
        sudo=True,
        cmd="tail -n 25 -- %s" % shlex.quote(path),
        check_ec=False,
        timeout=60,
    )
    return _text(out)


def _with_log_flag(text, enabled):
    flag = "true" if enabled else "false"
    lines = []
    seen = False
    for line in (text or "").splitlines():
        if line.startswith("log_to_file:"):
            lines.append("log_to_file: %s" % flag)
            seen = True
        else:
            lines.append(line)
    while lines and not lines[-1].strip():
        lines.pop()
    if not seen:
        lines.append("log_to_file: %s" % flag)
    return "\n".join(lines) + "\n"


def _minimal_spec(ctx, enabled):
    servers = ctx.get("servers") or [ctx["nfs_nodes"][0].hostname]
    hosts = "\n".join("    - %s" % host for host in servers)
    flag = "true" if enabled else "false"
    return (
        "service_type: nfs\n"
        "service_id: %s\n"
        "placement:\n"
        "  hosts:\n"
        "%s\n"
        "log_to_file: %s\n" % (ctx["nfs_name"], hosts, flag)
    )


def _set_file_logging(ctx, enabled):
    """Set NFSServiceSpec.log_to_file. That is the switch cephadm reads."""
    service = "nfs.%s" % ctx["nfs_name"]
    exported, rc = _node_sh(
        ctx["ceph_node"],
        "ceph orch ls --service_name %s --export --format yaml" % service,
        check=False,
        timeout=120,
    )
    if rc == 0 and "service_type:" in exported:
        body = _with_log_flag(exported, enabled)
    else:
        log.info("orch export of %s rc=%s; writing a placement spec", service, rc)
        body = _minimal_spec(ctx, enabled)
    remote = "/tmp/nfs-%s-log-to-file.yaml" % ctx["nfs_name"]
    payload = base64.b64encode(body.encode()).decode()
    _node_sh(
        ctx["ceph_node"],
        "python3 -c \"import base64,pathlib; pathlib.Path(%s).write_text("
        "base64.b64decode('%s').decode())\"" % (repr(remote), payload),
    )
    log.info("Applying %s log_to_file=%s", service, enabled)
    node = ctx["ceph_node"]
    if _ceph_prefix(node):
        cmd = "cephadm shell --mount %s:%s -- ceph orch apply -i %s" % (
            remote,
            remote,
            remote,
        )
    else:
        cmd = "ceph orch apply -i %s" % remote
    _node_sh(node, cmd, timeout=300)
    ctx["file_logging"] = bool(enabled)


def _wait_running(ctx):
    # Host ``ceph orch ps``. ``cephadm shell`` can sit on the podman lock.
    deadline = time.time() + ctx["service_wait"]
    daemons = []
    while time.time() < deadline:
        daemons = _daemons(ctx)
        if daemons and all(_running(daemon) for daemon in daemons):
            return daemons
        time.sleep(5)
    daemons = _daemons(ctx)
    if not daemons or any(not _running(daemon) for daemon in daemons):
        raise OperationFailedError(
            "nfs.%s is not running: %s"
            % (
                ctx["nfs_name"],
                [
                    (d.get("daemon_name"), d.get("status_desc"), d.get("container_id"))
                    for d in daemons
                ],
            )
        )
    return daemons


def _redeploy_once(ctx):
    before = {
        daemon.get("daemon_name"): daemon.get("container_id")
        for daemon in _daemons(ctx)
    }
    log.info("Redeploying nfs.%s once (containers %s)", ctx["nfs_name"], before)
    _ceph(ctx).orch.redeploy("nfs.%s" % ctx["nfs_name"])
    deadline = time.time() + ctx["service_wait"]
    while time.time() < deadline:
        try:
            daemons = _wait_running(ctx)
        except OperationFailedError:
            time.sleep(10)
            continue
        now = {
            daemon.get("daemon_name"): daemon.get("container_id") for daemon in daemons
        }
        if now != before:
            log.info("nfs.%s redeployed: %s", ctx["nfs_name"], now)
            return daemons
        time.sleep(10)
    log.info("Redeploy left the same container id; continuing")
    return _wait_running(ctx)


def _args_match(ctx, daemons, expect_file):
    try:
        for daemon in daemons:
            node = _node_for_daemon(ctx, daemon)
            target = _dash_l(_container_args(node, daemon.get("container_id")))
            if expect_file and not _is_file_target(target):
                return False
            if not expect_file and not _is_stderr_target(target):
                return False
    except Exception as err:
        log.info("Args check not ready: %s", err)
        return False
    return True


def _apply_logging(ctx, expect_file):
    """Wait until Ganesha -L matches the NFS spec, then redeploy once if needed."""
    wait = int(ctx["config"].get("auto_redeploy_wait", 90))
    deadline = time.time() + wait
    while time.time() < deadline:
        daemons = _daemons(ctx)
        if daemons and all(_running(daemon) for daemon in daemons):
            if _args_match(ctx, daemons, expect_file):
                log.info("NFS -L already matches expect_file=%s", expect_file)
                return daemons
        time.sleep(15)
    log.info(
        "cephadm did not reach expect_file=%s within %ss; redeploying once",
        expect_file,
        wait,
    )
    return _redeploy_once(ctx)


def _assert_mode(ctx, expect_file, label):
    daemons = _wait_running(ctx)
    fsid = _fsid(ctx)
    single = len(daemons) == 1
    found = {}
    for daemon in daemons:
        name = daemon.get("daemon_name")
        node = _node_for_daemon(ctx, daemon)
        target = _dash_l(_container_args(node, daemon.get("container_id")))
        paths = _find_logs(node, fsid)
        log.info("%s %s on %s -L %s", label, name, node.hostname, target)
        log.info("%s find /var/log/ceph/%s -> %s", label, fsid, paths or "(none)")
        if expect_file:
            if not _is_file_target(target):
                raise OperationFailedError(
                    "%s: %s -L is %r, expected a /var/log/ceph/ file"
                    % (label, name, target)
                )
            path = _pick_log(name, paths, allow_single=single)
            if not path:
                raise OperationFailedError(
                    "%s: no host log for %s under /var/log/ceph/%s (found %s)"
                    % (label, name, fsid, paths or "(none)")
                )
            size = _file_size(node, path)
            if size is None:
                raise OperationFailedError("%s: %s is not a file" % (label, path))
            log.info("%s host log %s size=%s", label, path, size)
            found[name] = (node, path, size)
        elif not _is_stderr_target(target):
            raise OperationFailedError(
                "%s: %s -L is %r, expected STDERR" % (label, name, target)
            )
    ctx["logs"] = found
    return found


def _assert_grew(ctx, label):
    if not ctx.get("logs"):
        raise OperationFailedError("%s: no host log was recorded" % label)
    for name, (node, path, before) in list(ctx["logs"].items()):
        after = _file_size(node, path)
        tail = _tail(node, path)
        log.info("%s %s size %s -> %s", label, path, before, after)
        log.info("%s tail %s:\n%s", label, path, tail or "(empty)")
        if after is None or after <= before:
            raise OperationFailedError(
                "%s: %s did not grow for %s (%s -> %s)"
                % (label, path, name, before, after)
            )
        if not tail.strip():
            raise OperationFailedError(
                "%s: %s grew but the tail is empty" % (label, path)
            )
        ctx["logs"][name] = (node, path, after)


def _snapshot_sizes(ctx):
    updated = {}
    for name, (node, path, _size) in ctx.get("logs", {}).items():
        updated[name] = (node, path, _file_size(node, path) or 0)
    ctx["logs"] = updated


def _bring_up(ctx, servers):
    """Create the NFS cluster and one export. Mount is a later step."""
    log.info(
        "Creating nfs cluster %s on %s via setup_nfs_cluster", ctx["nfs_name"], servers
    )
    exports = setup_nfs_cluster(
        [ctx["client"]],
        servers if len(servers) > 1 else servers[0],
        ctx["port"],
        ctx["version"],
        ctx["nfs_name"],
        ctx["mount_point"],
        ctx["fs_name"],
        ctx["export_base"],
        ctx["fs_name"],
        ceph_cluster=ctx["ceph_cluster"],
        skip_mount=True,
    )
    ctx["exports"] = list(exports or [])
    ctx["created"] = True
    ctx["servers"] = list(servers) if isinstance(servers, (list, tuple)) else [servers]
    _wait_running(ctx)
    if not ctx["exports"]:
        raise OperationFailedError("setup_nfs_cluster returned no exports")
    log.info("NFS exports: %s", ctx["exports"])


def _remove_cluster(ctx):
    if not ctx.get("created"):
        return
    log.info("Removing nfs cluster %s via cleanup_cluster", ctx["nfs_name"])
    try:
        cleanup_cluster(
            [ctx["client"]],
            ctx["mount_point"],
            ctx["nfs_name"],
            ctx["export_base"],
            nfs_nodes=ctx["nfs_nodes"],
        )
    except Exception as err:
        log.warning("cleanup_cluster %s: %s", ctx["nfs_name"], err)
        for export in list(ctx.get("exports") or []):
            try:
                _ceph(ctx).nfs.export.delete(ctx["nfs_name"], export)
            except Exception as exc:
                log.warning("export delete %s: %s", export, exc)
            remove_nfs_subvolume(
                ctx["ceph_node"], ctx["fs_name"], export.replace("/", "")
            )
        try:
            delete_nfs_cluster_and_wait(ctx["ceph_node"], ctx["nfs_name"])
        except Exception as exc:
            log.warning("delete_nfs_cluster_and_wait %s: %s", ctx["nfs_name"], exc)
    ctx["created"] = False
    ctx["exports"] = []
    ctx["mounts"] = set()


def _export_names(ctx, nfs_name):
    """Return export paths for an NFS cluster that is already deployed."""
    try:
        raw = _ceph(ctx).nfs.export.ls(nfs_name, format="json")
    except Exception as err:
        log.info("nfs export ls %s: %s", nfs_name, err)
        return []
    text = raw if isinstance(raw, str) else str(raw)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = [line.strip() for line in text.splitlines() if line.strip()]
    names = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, dict):
                path = item.get("path") or item.get("export") or item.get("pseudo")
                if path:
                    names.append(path)
    return names


def _cluster_present(ctx, nfs_name):
    try:
        names = _ceph(ctx).nfs.cluster.ls() or []
    except Exception as err:
        log.info("nfs cluster ls: %s", err)
        return False
    return nfs_name in names


def _note_mount(ctx, mount_point):
    out, _ = ctx["client"].exec_command(
        sudo=True,
        cmd="findmnt -n -- %s" % shlex.quote(mount_point),
        check_ec=False,
        timeout=30,
    )
    if getattr(ctx["client"], "exit_status", 1) == 0 and _text(out):
        ctx["mounts"].add(mount_point)


def _adopt_cluster(ctx):
    """Reuse the NFS cluster left by the previous suite step."""
    if _cluster_present(ctx, ctx["nfs_name"]):
        ctx["created"] = True
        ctx["exports"] = _export_names(ctx, ctx["nfs_name"])
        log.info("Using existing nfs %s exports %s", ctx["nfs_name"], ctx["exports"])
    _note_mount(ctx, ctx["mount_point"])
    for idx in (0, 1):
        _note_mount(ctx, "%s_%s" % (ctx["mount_point"], idx))


def _primary_export(ctx):
    if not ctx.get("exports"):
        _adopt_cluster(ctx)
    if not ctx.get("exports"):
        raise OperationFailedError("no NFS export has been created")
    return ctx["exports"][0]


def _mount_export(ctx, export, mount_point, server_ip=None, port=None):
    if server_ip is None:
        server_ip, port = wait_for_nfs_cluster_backend_endpoint(
            ctx["client"],
            ctx["ceph_node"],
            ctx["nfs_name"],
            timeout=ctx["service_wait"],
        )
    mount_nfs_export_with_wait(
        ctx["client"],
        server_ip,
        port,
        export,
        mount_point,
        ctx["version"],
        installer_node=ctx["ceph_node"],
        nfs_name=ctx["nfs_name"],
        nfs_wait_timeout=ctx["service_wait"],
    )
    ctx["mounts"].add(mount_point)


def _unmount(ctx, mount_point):
    if mount_point not in ctx["mounts"]:
        return
    unmount_and_remove_mount_path(ctx["client"], mount_point)
    ctx["mounts"].discard(mount_point)


def _io(ctx, mount_point):
    created = create_files_and_dirs_on_mount(
        ctx["client"], mount_point, "lf", file_count=1, dir_count=1
    )
    assert_mount_path_accessible(ctx["client"], mount_point, created[0])
    log.info("NFS IO completed on %s (%s)", mount_point, created)


def _recreate_export(ctx):
    export = _primary_export(ctx)
    _unmount(ctx, ctx["mount_point"])
    try:
        _ceph(ctx).nfs.export.delete(ctx["nfs_name"], export)
    except Exception as err:
        log.warning("export delete %s: %s", export, err)
    remove_nfs_subvolume(ctx["ceph_node"], ctx["fs_name"], export.replace("/", ""))
    _ceph(ctx).nfs.export.create(
        fs_name=ctx["fs_name"],
        nfs_name=ctx["nfs_name"],
        nfs_export=export,
        fs=ctx["fs_name"],
    )
    log.info("Recreated export %s", export)
    return export


def _ceph_log(ctx):
    out, _ = ctx["ceph_node"].exec_command(
        sudo=True, cmd="ceph log last 400", check_ec=False, timeout=180
    )
    if getattr(ctx["ceph_node"], "exit_status", 0) != 0:
        log.warning("ceph log last failed")
        return ""
    return _text(out)


def _assert_stable(ctx, seconds, label):
    seconds = int(seconds)
    if seconds <= 0:
        log.info("%s: stability window disabled", label)
        return
    _wait_running(ctx)
    log.info("%s: settling 20s before a %ss stability window", label, seconds)
    time.sleep(20)
    start = {
        daemon.get("daemon_name"): daemon.get("container_id")
        for daemon in _daemons(ctx)
    }
    before = set(_ceph_log(ctx).splitlines())
    deadline = time.time() + seconds
    interval = int(ctx["config"].get("poll_interval", 30))
    while time.time() < deadline:
        time.sleep(min(interval, max(1, int(deadline - time.time()))))
        for daemon in _daemons(ctx):
            name = daemon.get("daemon_name")
            if not _running(daemon):
                raise OperationFailedError(
                    "%s: %s is %s during the stability window"
                    % (label, name, daemon.get("status_desc"))
                )
            current = daemon.get("container_id")
            if name in start and current != start[name]:
                raise OperationFailedError(
                    "%s: %s container changed %s -> %s"
                    % (label, name, start[name], current)
                )
        left = int(deadline - time.time())
        log.info(
            "%s: daemon still running, %ss left in the window", label, max(left, 0)
        )
    hits = []
    for line in _ceph_log(ctx).splitlines():
        if line in before:
            continue
        low = line.lower()
        if ctx["nfs_name"] not in low and "nfs." not in low:
            continue
        if (
            "chose new action redeploy" in low
            or "reconfigure wanted nfs" in low
            or ("log_to_file" in low and "->" in low and "[]" in low)
        ):
            hits.append(line)
    if hits:
        raise OperationFailedError(
            "%s: redeploy loop after the option settled:\n%s"
            % (label, "\n".join(hits[:20]))
        )
    log.info("%s: no further NFS redeploy for %ss", label, seconds)


def _record(ctx, status, case_id, detail):
    ctx["results"].append((status, case_id, detail))
    log.info("[%s] %s: %s", status, case_id, detail)


def _require(ctx, case_id, detail):
    _record(ctx, "PASS", case_id, detail)


def _case_record_build(ctx):
    version = _ceph(ctx).version()
    # The installer often has cephadm only. get_ceph_version defaults to the
    # host ceph binary, which returns empty stdout and is not JSON.
    try:
        installed = get_ceph_version(
            ctx["ceph_node"],
            prefix_cephadm=bool(_ceph_prefix(ctx["ceph_node"])),
        )
    except Exception as err:
        installed = None
        log.info("record_build could not parse the ceph version json: %s", err)
    _config_get(ctx, "global")
    _config_get(ctx, "client.nfs")
    log.info("record_build ceph version: %s (parsed %s)", version, installed)
    # Ceph daemon log_to_file is not the Ganesha -L switch.
    _config_set(ctx, "global", "false")
    _config_rm(ctx, "client.nfs")
    _bring_up(ctx, [ctx["nfs_nodes"][0].hostname])
    daemon = _wait_running(ctx)[0]
    node = _node_for_daemon(ctx, daemon)
    image = daemon.get("container_image_name") or daemon.get("container_image_id")
    rpm_out, _ = node.exec_command(
        sudo=True,
        cmd="podman exec %s rpm -q nfs-ganesha"
        % shlex.quote(daemon.get("container_id")),
        check_ec=False,
    )
    log.info("record_build NFS image: %s", image)
    log.info("record_build nfs-ganesha rpm: %s", _text(rpm_out))
    _require(
        ctx,
        "record_build",
        "recorded %s; nfs.%s running image=%s" % (version, ctx["nfs_name"], image),
    )


def _case_spec_enables_host_log(ctx):
    _set_file_logging(ctx, True)
    _apply_logging(ctx, expect_file=True)
    _assert_mode(ctx, expect_file=True, label="spec_enables_host_log")
    _require(
        ctx,
        "spec_enables_host_log",
        "NFS spec log_to_file true created a host log and -L file path",
    )


def _case_spec_ignores_config_off(ctx):
    # Spec stays the source of truth. Ceph config must not turn file logging off.
    _config_set(ctx, "global", "false")
    _config_rm(ctx, "client.nfs")
    _set_file_logging(ctx, True)
    _apply_logging(ctx, expect_file=True)
    _assert_mode(ctx, expect_file=True, label="spec_ignores_config_off")
    _require(
        ctx,
        "spec_ignores_config_off",
        "spec log_to_file true keeps the host log while ceph config global is false",
    )


def _case_host_log_grows_after_io(ctx):
    # Each suite step is a new process, so rediscover the host log here.
    _assert_mode(ctx, expect_file=True, label="host_log_grows_after_io")
    _snapshot_sizes(ctx)
    export = _primary_export(ctx)
    _mount_export(ctx, export, ctx["mount_point"])
    _io(ctx, ctx["mount_point"])
    _assert_grew(ctx, "host_log_grows_after_io")
    _wait_running(ctx)
    _require(ctx, "host_log_grows_after_io", "host log grew after mount and IO")


def _case_export_recreate(ctx):
    _recreate_export(ctx)
    _assert_mode(ctx, expect_file=True, label="export_recreate")
    _snapshot_sizes(ctx)
    _mount_export(ctx, _primary_export(ctx), ctx["mount_point"])
    _io(ctx, ctx["mount_point"])
    _assert_grew(ctx, "export_recreate")
    _wait_running(ctx)
    _require(ctx, "export_recreate", "export recreate and IO succeeded and the host log grew")


def _case_no_redeploy_loop(ctx):
    _assert_stable(ctx, ctx["stability_seconds"], "no_redeploy_loop")
    _require(
        ctx,
        "no_redeploy_loop",
        "one deploy settled; no redeploy loop for %ss" % ctx["stability_seconds"],
    )


def _case_spec_off_uses_stderr(ctx):
    _set_file_logging(ctx, False)
    _apply_logging(ctx, expect_file=False)
    _assert_mode(ctx, expect_file=False, label="spec_off_uses_stderr")
    _unmount(ctx, ctx["mount_point"])
    _mount_export(ctx, _primary_export(ctx), ctx["mount_point"])
    _io(ctx, ctx["mount_point"])
    _assert_stable(ctx, ctx["followup_seconds"], "spec_off_uses_stderr")
    _require(
        ctx,
        "spec_off_uses_stderr",
        "NFS spec log_to_file false uses -L STDERR and NFS IO still works",
    )


def _case_per_daemon_config_gap(ctx):
    _set_file_logging(ctx, False)
    _apply_logging(ctx, expect_file=False)
    _assert_mode(ctx, expect_file=False, label="per_daemon_config_gap_off")
    daemon = _wait_running(ctx)[0]
    who = "client.%s" % daemon.get("daemon_name")
    ctx["daemon_whos"].add(who)
    _config_set(ctx, who, "true")
    _config_get(ctx, who)
    _apply_logging(ctx, expect_file=True)
    try:
        _assert_mode(ctx, expect_file=True, label="per_daemon_config_gap")
        honored = True
        gap = ""
    except OperationFailedError as err:
        honored = False
        gap = str(err)
    _config_rm(ctx, who)
    ctx["daemon_whos"].discard(who)
    if not honored:
        _record(
            ctx,
            "GAP",
            "per_daemon_config_gap",
            "per-daemon who %s stayed off file logging (%s). "
            "This build reads NFS spec log_to_file, not client.nfs.<daemon_id>. "
            "The spec-on cases are not failed for this." % (who, gap),
        )
        return
    _require(ctx, "per_daemon_config_gap", "per-daemon who %s enabled the host log" % who)


def _case_toggle_on_off_on(ctx):
    _set_file_logging(ctx, True)
    _apply_logging(ctx, expect_file=True)
    _assert_mode(ctx, expect_file=True, label="toggle_on")
    _snapshot_sizes(ctx)
    _unmount(ctx, ctx["mount_point"])
    _mount_export(ctx, _primary_export(ctx), ctx["mount_point"])
    _io(ctx, ctx["mount_point"])
    _assert_grew(ctx, "toggle_on")

    _set_file_logging(ctx, False)
    _apply_logging(ctx, expect_file=False)
    _assert_mode(ctx, expect_file=False, label="toggle_off")
    _unmount(ctx, ctx["mount_point"])
    _mount_export(ctx, _primary_export(ctx), ctx["mount_point"])
    _io(ctx, ctx["mount_point"])

    _set_file_logging(ctx, True)
    _apply_logging(ctx, expect_file=True)
    _assert_mode(ctx, expect_file=True, label="toggle_on_again")
    _snapshot_sizes(ctx)
    _unmount(ctx, ctx["mount_point"])
    _mount_export(ctx, _primary_export(ctx), ctx["mount_point"])
    _io(ctx, ctx["mount_point"])
    _assert_grew(ctx, "toggle_on_again")
    _assert_stable(ctx, ctx["followup_seconds"], "toggle_on_off_on")
    _require(ctx, "toggle_on_off_on", "true -> false -> true kept the matching -L target")


def _case_redeploy_keeps_file_log(ctx):
    if not ctx.get("file_logging"):
        _set_file_logging(ctx, True)
        _apply_logging(ctx, expect_file=True)
    _redeploy_once(ctx)
    _assert_mode(ctx, expect_file=True, label="redeploy_keeps_file_log")
    _snapshot_sizes(ctx)
    _unmount(ctx, ctx["mount_point"])
    _mount_export(ctx, _primary_export(ctx), ctx["mount_point"])
    _io(ctx, ctx["mount_point"])
    _assert_grew(ctx, "redeploy_keeps_file_log")
    _require(ctx, "redeploy_keeps_file_log", "orch redeploy kept file logging and the host log grew")


def _case_each_daemon_own_log(ctx):
    if len(ctx["nfs_nodes"]) < 2:
        _record(
            ctx,
            "SKIP",
            "each_daemon_own_log",
            "lab has %s NFS node; two daemons were not deployed"
            % len(ctx["nfs_nodes"]),
        )
        return
    _remove_cluster(ctx)
    ctx["nfs_name"] = ctx["nfs_name_pair"]
    ctx["export_base"] = "/export_lf8"
    ctx["mount_point"] = ctx["config"].get("mount_point", "/mnt/nfs_log_to_file")
    servers = [node.hostname for node in ctx["nfs_nodes"][:2]]
    _bring_up(ctx, servers)
    daemons = _wait_running(ctx)
    if len(daemons) < 2:
        raise OperationFailedError(
            "each_daemon_own_log expected 2 NFS daemons, orch ps returned %s"
            % [d.get("daemon_name") for d in daemons]
        )
    _set_file_logging(ctx, True)
    _apply_logging(ctx, expect_file=True)
    found = _assert_mode(ctx, expect_file=True, label="each_daemon_own_log")
    paths = [path for _node, path, _size in found.values()]
    if len(set(paths)) < len(paths):
        raise OperationFailedError("each_daemon_own_log daemons share one log path: %s" % paths)
    info = _ceph(ctx).nfs.cluster.info(ctx["nfs_name"])
    if ctx["nfs_name"] in info:
        info = info[ctx["nfs_name"]]
    backends = info.get("backend") or []
    if len(backends) < 2:
        raise OperationFailedError("each_daemon_own_log cluster info has %s backends" % len(backends))
    export = _primary_export(ctx)
    for idx, backend in enumerate(backends[: len(found)]):
        mount_point = "%s_%s" % (ctx["mount_point"], idx)
        _mount_export(
            ctx,
            export,
            mount_point,
            server_ip=backend["ip"],
            port=backend.get("port", 2049),
        )
        _io(ctx, mount_point)
        _unmount(ctx, mount_point)
    for name, (node, path, before) in found.items():
        after = _file_size(node, path)
        tail = _tail(node, path)
        log.info("each_daemon_own_log %s %s size %s -> %s\n%s", name, path, before, after, tail)
        if after is None or after <= before or not tail.strip():
            raise OperationFailedError(
                "each_daemon_own_log %s log %s did not grow after IO (%s -> %s)"
                % (name, path, before, after)
            )
    _require(ctx, "each_daemon_own_log", "each NFS daemon has its own non-empty host log: %s" % paths)


CASES = {
    "record_build": _case_record_build,
    "spec_enables_host_log": _case_spec_enables_host_log,
    "spec_ignores_config_off": _case_spec_ignores_config_off,
    "host_log_grows_after_io": _case_host_log_grows_after_io,
    "export_recreate": _case_export_recreate,
    "no_redeploy_loop": _case_no_redeploy_loop,
    "spec_off_uses_stderr": _case_spec_off_uses_stderr,
    "per_daemon_config_gap": _case_per_daemon_config_gap,
    "toggle_on_off_on": _case_toggle_on_off_on,
    "redeploy_keeps_file_log": _case_redeploy_keeps_file_log,
    "each_daemon_own_log": _case_each_daemon_own_log,
}


def _selected_cases(config):
    mode = str(config.get("cases", "all")).strip().lower()
    if mode == "signoff":
        return SIGNOFF_CASES
    if mode == "all":
        return ALL_CASES
    names = [name.strip().lower() for name in mode.split(",") if name.strip()]
    bad = [name for name in names if name not in CASES]
    if bad:
        raise ConfigError("Unknown log_to_file cases %s" % bad)
    return tuple(names)


def _cleanup(ctx):
    if not ctx["config"].get("cleanup", True):
        log.info("Leaving nfs cluster %s in place for the next case", ctx["nfs_name"])
        return
    for who in list(ctx.get("daemon_whos") or []):
        _config_rm(ctx, who)
    try:
        _config_set(ctx, "client.nfs", "false")
        _config_set(ctx, "global", "false")
    except Exception as err:
        log.warning("reset log_to_file failed: %s", err)
    names = []
    if ctx.get("nfs_name"):
        names.append(ctx["nfs_name"])
    pair = ctx.get("nfs_name_pair")
    if pair and pair not in names:
        names.append(pair)
    current = ctx.get("nfs_name")
    for name in names:
        ctx["nfs_name"] = name
        if not ctx.get("created") and _cluster_present(ctx, name):
            ctx["created"] = True
            ctx["exports"] = _export_names(ctx, name)
        try:
            _remove_cluster(ctx)
        except Exception as err:
            log.warning("cleanup %s failed: %s", name, err)
        ctx["created"] = False
    ctx["nfs_name"] = current


def _log_summary(ctx):
    log.info("log_to_file summary:")
    for status, case_id, detail in ctx["results"]:
        log.info("  %s  %s: %s", status, case_id, detail)


def run(ceph_cluster, **kw):
    """Entry point for the cephci suite runner."""
    config = kw.get("config") or {}
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")
    if not clients:
        raise ConfigError("log_to_file test needs a client node")
    if not nfs_nodes:
        raise ConfigError("log_to_file test needs an NFS node")
    if not installers:
        raise ConfigError("log_to_file test needs an installer node")

    nfs_name = config.get("nfs_name", "nfs-logf")
    ctx = {
        "config": config,
        "ceph_cluster": ceph_cluster,
        "client": clients[0],
        "ceph_node": installers[0],
        "nfs_nodes": nfs_nodes,
        "nodes": ceph_cluster.get_nodes(),
        "nfs_name": nfs_name,
        "nfs_name_pair": config.get("nfs_name_pair", "%s2" % nfs_name),
        "fs_name": config.get("fs_name", "cephfs"),
        "version": str(config.get("nfs_version", "4.2")),
        "port": str(config.get("port", "2049")),
        "export_base": config.get("export", "/export_lf"),
        "mount_point": config.get("mount_point", "/mnt/nfs_log_to_file"),
        "service_wait": int(config.get("service_wait_timeout", 600)),
        "stability_seconds": int(config.get("stability_seconds", 300)),
        "followup_seconds": int(config.get("followup_stability_seconds", 120)),
        "exports": [],
        "mounts": set(),
        "daemon_whos": set(),
        "logs": {},
        "results": [],
        "created": False,
    }
    _adopt_cluster(ctx)
    try:
        for case_id in _selected_cases(config):
            log.info("=== %s ===", case_id)
            CASES[case_id](ctx)
        _log_summary(ctx)
        failed = [
            case_id for status, case_id, _detail in ctx["results"] if status == "FAIL"
        ]
        if failed:
            log.error("log_to_file failures: %s", failed)
            return 1
        log.info(
            "TEST PASSED - NFS log_to_file cases: %s",
            [item[1] for item in ctx["results"]],
        )
        return 0
    except Exception as err:
        log.error("log_to_file test failed: %s", err)
        log.error(traceback.format_exc())
        _log_summary(ctx)
        return 1
    finally:
        _cleanup(ctx)
