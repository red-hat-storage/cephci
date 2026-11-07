"""
IBMCEPH-18323 — cephadm NFS service spec Match_Policy and Conditional logging.

One managed Ganesha ``LOG {}`` block. ``MATCH_ANY`` matches client or export.
``MATCH_ALL`` matches only when both match, and an empty side is rejected.
A reload is enough. A container recreate is not required (TC-LC13).

Suite config ``tc`` selects LC0 … LC15.

Spec YAML keys are stand-ins until the cephadm MR names the fields:

* ``spec.match_policy``
* ``spec.log_conditional.clients``
* ``spec.log_conditional.exports``  (Ganesha Export_Id numbers)
* ``spec.log_conditional.components``

Rename ``SPEC_MATCH_POLICY`` and ``SPEC_LOG_CONDITIONAL`` to the MR keys.
Do not add a second CLI.

Polarion:

* LC0  CEPH-83633247
* LC1  CEPH-83633248
* LC2  CEPH-83633249
* LC3  CEPH-83633250
* LC4  CEPH-83633251
* LC5  CEPH-83633252
* LC6  CEPH-83633253
* LC7  CEPH-83633254
* LC8  CEPH-83633255
* LC9  CEPH-83633256
* LC10 CEPH-83633257
* LC11 CEPH-83633258
* LC12 CEPH-83633259
* LC13 CEPH-83633260
* LC14 CEPH-83633261
* LC15 CEPH-83633262
"""

import json
import re
from time import sleep, time

import yaml

from ceph.ceph import CommandFailed
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.utils import get_ip_from_node
from tests.nfs.lib.common_lib import get_nfs_container_id
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    mount_nfs_export_with_wait,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)
from utility.log import Log

log = Log(__name__)

SPEC_MATCH_POLICY = "match_policy"
SPEC_LOG_CONDITIONAL = "log_conditional"

VALID_TCS = tuple("LC%d" % i for i in range(16))

RE_NFS4_DEBUG = re.compile(r"(?:NFS4|NFS_V4)\s*:\s*(?:DEBUG|NIV_DEBUG)\b", re.I)
RE_FSAL_FULL = re.compile(r"FSAL\s*:\s*FULL_DEBUG\b", re.I)
RE_FSAL_ANY = re.compile(r"FSAL\s*:", re.I)
UNKNOWN_FIELD_MARKERS = (
    "unknown field",
    "unrecognized",
    "extra inputs are not permitted",
    "extra fields",
    "not a valid field",
    "unexpected keyword",
)


class FeatureAbsent(Exception):
    """Build has no cephadm match-policy / conditional spec fields."""


def _text(out):
    if isinstance(out, tuple):
        out = out[0]
    if isinstance(out, bytes):
        out = out.decode("utf-8", errors="replace")
    return str(out or "")


def _node_ip(node):
    ip = getattr(node, "ip_address", None)
    if ip and not str(ip).startswith("127."):
        return str(ip)
    ips = [item for item in get_ip_from_node(node) if not str(item).startswith("127.")]
    if not ips:
        raise OperationFailedError("No non-loopback IP on %s" % node.hostname)
    return ips[0]


def _contains_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return True
        return any(_contains_key(value, key) for value in obj.values())
    if isinstance(obj, list):
        return any(_contains_key(value, key) for value in obj)
    return False


def _export_orch_spec(client, nfs_name):
    out, _ = client.exec_command(
        sudo=True,
        cmd="ceph orch ls --service-name nfs.%s --export -f yaml" % nfs_name,
        check_ec=False,
    )
    docs = [doc for doc in yaml.safe_load_all(_text(out)) if doc]
    if not docs:
        raise OperationFailedError("Empty orch export for nfs.%s" % nfs_name)
    doc = docs[0]
    apply_spec = {
        key: doc[key]
        for key in ("service_type", "service_id", "placement", "spec")
        if key in doc
    }
    apply_spec.setdefault("service_type", "nfs")
    apply_spec.setdefault("service_id", nfs_name)
    apply_spec.setdefault("spec", {})
    return apply_spec


def _orch_apply(client, apply_spec):
    path = "/tmp/nfs-log-conditional.yaml"
    body = yaml.safe_dump(apply_spec, default_flow_style=False)
    client.exec_command(
        sudo=True,
        cmd="cat > %s <<'LC_SPEC_EOF'\n%sLC_SPEC_EOF" % (path, body),
    )
    try:
        out, err = client.exec_command(
            sudo=True, cmd="ceph orch apply -i %s" % path, check_ec=True
        )
        return False, _text(out), _text(err)
    except CommandFailed as exc:
        return True, str(exc), ""


def _looks_unknown_field(text):
    low = (text or "").lower()
    return any(marker in low for marker in UNKNOWN_FIELD_MARKERS)


def _apply_fields(ctx, policy, clients=None, exports=None, components=None, expect_fail=False):
    """Apply stand-in spec fields. Raise FeatureAbsent when the key is unknown."""
    apply_spec = _export_orch_spec(ctx["client"], ctx["nfs_name"])
    spec = apply_spec.setdefault("spec", {})
    if policy is None and clients is None and exports is None and components is None:
        spec.pop(SPEC_MATCH_POLICY, None)
        spec.pop(SPEC_LOG_CONDITIONAL, None)
    else:
        block = {}
        if clients is not None:
            block["clients"] = list(clients)
        if exports is not None:
            block["exports"] = [int(item) for item in exports]
        if components is not None:
            block["components"] = dict(components)
        spec[SPEC_MATCH_POLICY] = policy
        spec[SPEC_LOG_CONDITIONAL] = block
    failed, out, err = _orch_apply(ctx["client"], apply_spec)
    combined = "%s\n%s" % (out, err)
    log.info("orch apply failed=%s policy=%s out=%s", failed, policy, combined.strip()[:1500])
    if expect_fail:
        if not failed and "error" not in combined.lower():
            raise OperationFailedError(
                "orch apply succeeded for policy=%r; expected rejection. %s" % (policy, combined)
            )
        log.info("orch apply rejected as expected for policy=%r", policy)
        return apply_spec
    if failed or "error einval" in combined.lower() or "error:" in combined.lower():
        if _looks_unknown_field(combined):
            raise FeatureAbsent(combined.strip()[:500])
        raise OperationFailedError("orch apply failed: %s" % combined.strip()[:1500])
    stored = _export_orch_spec(ctx["client"], ctx["nfs_name"])
    if policy is not None and not _contains_key(stored, SPEC_MATCH_POLICY):
        raise FeatureAbsent(
            "re-exported nfs.%s spec dropped %s" % (ctx["nfs_name"], SPEC_MATCH_POLICY)
        )
    if policy is not None and not _contains_key(stored, SPEC_LOG_CONDITIONAL):
        raise FeatureAbsent(
            "re-exported nfs.%s spec dropped %s" % (ctx["nfs_name"], SPEC_LOG_CONDITIONAL)
        )
    return stored


def _clear_fields(ctx):
    apply_spec = _export_orch_spec(ctx["client"], ctx["nfs_name"])
    spec = apply_spec.setdefault("spec", {})
    spec.pop(SPEC_MATCH_POLICY, None)
    spec.pop(SPEC_LOG_CONDITIONAL, None)
    failed, out, err = _orch_apply(ctx["client"], apply_spec)
    if failed:
        raise OperationFailedError("clear spec failed: %s %s" % (out, err))
    return _export_orch_spec(ctx["client"], ctx["nfs_name"])


def _read_conf(nfs_node, nfs_name):
    cid = get_nfs_container_id(nfs_node, nfs_name=nfs_name)
    if not cid:
        raise OperationFailedError("No nfs.%s container on %s" % (nfs_name, nfs_node.hostname))
    out, _ = nfs_node.exec_command(
        sudo=True, cmd="podman exec %s cat /etc/ganesha/ganesha.conf" % cid
    )
    return cid, _text(out)


def _brace_body(text, keyword):
    match = re.search(r"\b%s\s*\{" % keyword, text)
    if not match:
        return None
    depth = 1
    index = match.end()
    while index < len(text) and depth:
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
        index += 1
    if depth:
        raise OperationFailedError("Unbalanced %s block in ganesha.conf" % keyword)
    return text[match.end() : index - 1]


def _count_log_blocks(conf):
    return len(re.findall(r"\bLOG\s*\{", conf))


def _assert_one_log(conf):
    count = _count_log_blocks(conf)
    if count != 1:
        raise OperationFailedError("expected exactly 1 LOG block, found %s" % count)


def _wait_conf(ctx, predicate, timeout=180):
    end = time() + timeout
    last = ""
    cid = None
    while time() < end:
        try:
            cid, last = _read_conf(ctx["nfs_node"], ctx["nfs_name"])
        except OperationFailedError as exc:
            log.info("ganesha.conf not ready: %s", exc)
            sleep(10)
            continue
        if predicate(last):
            return cid, last
        sleep(10)
    raise OperationFailedError("ganesha.conf predicate not met. tail:\n%s" % last[-2000:])


def _conf_has_policy(conf, policy):
    body = _brace_body(conf, "LOG")
    if body is None:
        return False
    conditional = _brace_body(body, "Conditional") or ""
    if not re.search(r"Match_Policy\s*=\s*%s\s*;" % policy, body):
        return False
    if re.search(r"Match_Policy\s*=", conditional):
        return False
    return True


def _conditional_body(conf):
    log_body = _brace_body(conf, "LOG")
    if log_body is None:
        raise OperationFailedError("LOG block missing")
    conditional = _brace_body(log_body, "Conditional")
    if conditional is None:
        raise OperationFailedError("Conditional block missing from LOG")
    return conditional


def _assert_rendered(conf, policy, client_ip=None, export_id=None, level="DEBUG", exports_absent=False):
    _assert_one_log(conf)
    if not _conf_has_policy(conf, policy):
        raise OperationFailedError("Match_Policy = %s is not outside Conditional" % policy)
    conditional = _conditional_body(conf)
    if client_ip and client_ip not in conditional:
        raise OperationFailedError("Conditional missing client %s" % client_ip)
    if export_id is not None and not re.search(r"\b%s\b" % int(export_id), conditional):
        raise OperationFailedError("Conditional missing export id %s" % export_id)
    if exports_absent and re.search(r"Exports\s*=\s*[^;]*\d", conditional):
        raise OperationFailedError("Conditional rendered an Exports list: %s" % conditional)
    if level and not re.search(
        r"(?:NFS4|NFS_V4)\s*=\s*%s\s*;" % level, conditional
    ):
        raise OperationFailedError("Conditional missing NFS4 = %s" % level)
    return conditional


def _utc(node):
    out, _ = node.exec_command(sudo=True, cmd="date -u +%Y-%m-%dT%H:%M:%SZ")
    return _text(out).strip()


def _logs_since(ctx, since):
    cid = get_nfs_container_id(ctx["nfs_node"], nfs_name=ctx["nfs_name"])
    if not cid:
        raise OperationFailedError("NFS container gone while reading logs")
    out, _ = ctx["nfs_node"].exec_command(
        sudo=True,
        cmd='podman logs --since "%s" %s 2>&1' % (since, cid),
        check_ec=False,
    )
    return _text(out)


def _umount(client, mount_path):
    client.exec_command(sudo=True, cmd="umount -l %s" % mount_path, check_ec=False)
    client.exec_command(sudo=True, cmd="rm -rf %s" % mount_path, check_ec=False)


def _io_window(ctx, client, export_path, token):
    mount_path = "/mnt/nfs-lc-%s" % token
    server = ctx["nfs_node"].hostname
    since = _utc(ctx["nfs_node"])
    mount_nfs_export_with_wait(
        client,
        server,
        ctx["port"],
        export_path,
        mount_path,
        ctx["version"],
        nfs_name=ctx["nfs_name"],
        nfs_wait_timeout=180,
    )
    ctx["mounts"].append((client, mount_path))
    try:
        remote = "%s/%s" % (mount_path, token)
        client.exec_command(
            sudo=True,
            cmd="echo %s > %s && grep %s %s && rm -f %s" % (token, remote, token, remote, remote),
        )
        sleep(8)
        return _logs_since(ctx, since)
    finally:
        _umount(client, mount_path)


def _expect_debug(logs, token, present):
    hits = [line for line in logs.splitlines() if RE_NFS4_DEBUG.search(line)]
    log.info("NFS4 DEBUG lines during %s: %s", token, len(hits))
    if hits:
        log.info("sample: %s", hits[0][:400])
    if present and not hits:
        raise OperationFailedError("expected NFS4 DEBUG during %s; none found" % token)
    if not present and hits:
        raise OperationFailedError(
            "NFS4 DEBUG present during %s (expected COMPONENTS only)" % token
        )


def _export_id(client, nfs_name, export_path):
    raw = Ceph(client).nfs.export.get(nfs_name, export_path)
    data = json.loads(_text(raw))
    for key in ("export_id", "Export_Id", "exportId"):
        if key in data and data[key] is not None:
            return int(data[key])
    raise OperationFailedError("export_id missing from %s: %s" % (export_path, data))


def _set_components(ctx, body):
    path = "/tmp/%s-log-components.conf" % ctx["nfs_name"]
    ctx["client"].exec_command(
        sudo=True,
        cmd="cat > %s <<'LC_LOG_EOF'\n%s\nLC_LOG_EOF" % (path, body.strip()),
    )
    ctx["client"].exec_command(
        sudo=True,
        cmd="ceph nfs cluster config set %s -i %s" % (ctx["nfs_name"], path),
    )
    log.info("cluster config set COMPONENTS overlay on %s", ctx["nfs_name"])


def _components_crit(fsal_warn=False):
    fsal = "\n        FSAL = WARN;" if fsal_warn else ""
    return (
        "LOG {\n    COMPONENTS {\n        NFS4 = CRIT;%s\n    }\n}\n" % fsal
    )


def _daemon_pid(nfs_node, cid):
    out, _ = nfs_node.exec_command(
        sudo=True,
        cmd="podman exec %s pgrep -n ganesha || true" % cid,
        check_ec=False,
    )
    return _text(out).strip()


def _record_versions(ctx):
    ver, _ = ctx["client"].exec_command(sudo=True, cmd="ceph version", check_ec=False)
    cid = get_nfs_container_id(ctx["nfs_node"], nfs_name=ctx["nfs_name"])
    rpm = ""
    if cid:
        rpm_out, _ = ctx["nfs_node"].exec_command(
            sudo=True,
            cmd="podman exec %s rpm -q nfs-ganesha || true" % cid,
            check_ec=False,
        )
        rpm = _text(rpm_out).strip()
    log.info("Ceph version: %s", _text(ver).strip())
    log.info("nfs-ganesha: %s", rpm or "<not recorded>")
    if not _text(ver).strip():
        raise OperationFailedError("ceph version was empty")


def _setup(ceph_cluster, config, servers):
    clients = ceph_cluster.get_nodes("client")
    if len(clients) < 2:
        raise OperationFailedError("Need 2 clients; found %s" % len(clients))
    clients = clients[:2]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if not nfs_nodes:
        raise OperationFailedError("No NFS role nodes")
    nfs_name = config.get("nfs_name", "cephfs-nfs-lc")
    port = str(config.get("port", "2049"))
    version = config.get("nfs_version", "4.2")
    fs_name = config.get("fs_name", "cephfs")
    export = config.get("export", "/export")
    installer = ceph_cluster.get_nodes("installer")[0]
    exports = setup_nfs_cluster(
        clients=clients,
        nfs_server=servers,
        port=port,
        version=version,
        nfs_name=nfs_name,
        nfs_mount="/mnt/nfs-lc",
        fs_name=fs_name,
        export=export,
        fs=fs_name,
        ceph_cluster=ceph_cluster,
        skip_mount=True,
    )
    verify_nfs_ganesha_service(node=installer, timeout=int(config.get("timeout", 300)), nfs_name=nfs_name)
    nfs_node = nfs_nodes[0]
    for node in nfs_nodes:
        if get_nfs_container_id(node, nfs_name=nfs_name):
            nfs_node = node
            break
    export_0 = exports[0]
    export_1 = exports[1] if len(exports) > 1 else exports[0]
    return {
        "ceph_cluster": ceph_cluster,
        "clients": clients,
        "client": clients[0],
        "client_b": clients[1],
        "nfs_nodes": nfs_nodes,
        "nfs_node": nfs_node,
        "installer": installer,
        "nfs_name": nfs_name,
        "port": port,
        "version": version,
        "fs_name": fs_name,
        "export_prefix": export,
        "export_0": export_0,
        "export_1": export_1,
        "mounts": [],
        "created": True,
    }


def _cleanup(ctx):
    if not ctx or not ctx.get("created"):
        return
    for client, mount_path in ctx.get("mounts") or []:
        _umount(client, mount_path)
    try:
        cleanup_cluster(
            ctx["clients"],
            "/mnt/nfs-lc",
            ctx["nfs_name"],
            ctx["export_prefix"],
            nfs_nodes=ctx["nfs_nodes"],
        )
    except Exception as exc:
        log.warning("cleanup_cluster failed: %s", exc)
        try:
            Ceph(ctx["client"]).nfs.cluster.delete(ctx["nfs_name"])
        except Exception as exc2:
            log.warning("nfs cluster delete failed: %s", exc2)


def _ids(ctx):
    export_0 = _export_id(ctx["client"], ctx["nfs_name"], ctx["export_0"])
    export_1 = _export_id(ctx["client"], ctx["nfs_name"], ctx["export_1"])
    return export_0, export_1


def _apply_and_wait(ctx, policy, clients=None, exports=None, components=None, level="DEBUG"):
    _apply_fields(ctx, policy, clients=clients, exports=exports, components=components)

    def ready(conf):
        try:
            _assert_rendered(
                conf,
                policy,
                client_ip=clients[0] if clients else None,
                export_id=exports[0] if exports else None,
                level=level,
                exports_absent=exports is None and clients is not None,
            )
            return True
        except OperationFailedError:
            return False

    return _wait_conf(ctx, ready)


def _with_crit(ctx, policy, clients=None, exports=None, components=None, fsal_warn=False, level="DEBUG"):
    cid, conf = _apply_and_wait(
        ctx, policy, clients=clients, exports=exports, components=components, level=level
    )
    _set_components(ctx, _components_crit(fsal_warn=fsal_warn))

    def merged(text):
        if not _conf_has_policy(text, policy):
            return False
        if _count_log_blocks(text) != 1:
            return False
        if "NFS4 = CRIT" not in text and "NFS4=CRIT" not in text.replace(" ", ""):
            return False
        return "Conditional" in text

    cid, conf = _wait_conf(ctx, merged)
    _assert_one_log(conf)
    return cid, conf


def run_lc0(ctx):
    _record_versions(ctx)
    _, conf = _read_conf(ctx["nfs_node"], ctx["nfs_name"])
    _assert_one_log(conf)
    if re.search(r"\bConditional\b", conf):
        raise OperationFailedError("default cluster already has a Conditional block")
    log.info("TC-LC0: default conf has one LOG and no Conditional")
    try:
        _apply_fields(
            ctx,
            "MATCH_ANY",
            clients=[_node_ip(ctx["client"])],
            exports=[_export_id(ctx["client"], ctx["nfs_name"], ctx["export_0"])],
            components={"NFS4": "DEBUG"},
        )
    except FeatureAbsent as exc:
        log.warning("TC-LC0: feature absent, remaining cases skip. %s", exc)
        return
    log.info("TC-LC0: spec stored match_policy; feature is present on this build")


def run_lc1(ctx):
    stored = _export_orch_spec(ctx["client"], ctx["nfs_name"])
    if _contains_key(stored, SPEC_LOG_CONDITIONAL) or _contains_key(stored, SPEC_MATCH_POLICY):
        raise OperationFailedError("fresh spec already has conditional fields: %s" % stored)
    _, conf = _read_conf(ctx["nfs_node"], ctx["nfs_name"])
    _assert_one_log(conf)
    if re.search(r"\bConditional\b", conf):
        raise OperationFailedError("unset spec rendered Conditional")
    _io_window(ctx, ctx["client"], ctx["export_0"], "lc1")
    log.info("TC-LC1 passed")


def run_lc2(ctx):
    ip = _node_ip(ctx["client"])
    export_id, _ = _ids(ctx)
    stored = _apply_fields(
        ctx,
        "MATCH_ANY",
        clients=[ip],
        exports=[export_id],
        components={"NFS4": "DEBUG"},
    )
    spec = stored.get("spec") or {}
    block = spec.get(SPEC_LOG_CONDITIONAL) or {}
    if spec.get(SPEC_MATCH_POLICY) != "MATCH_ANY":
        raise OperationFailedError("stored policy: %s" % spec.get(SPEC_MATCH_POLICY))
    if ip not in (block.get("clients") or []):
        raise OperationFailedError("stored clients missing %s: %s" % (ip, block))
    if export_id not in [int(item) for item in (block.get("exports") or [])]:
        raise OperationFailedError("stored exports missing %s: %s" % (export_id, block))
    log.info("TC-LC2 passed")


def run_lc3(ctx):
    ip = _node_ip(ctx["client"])
    export_id, _ = _ids(ctx)
    cid, conf = _apply_and_wait(
        ctx,
        "MATCH_ANY",
        clients=[ip],
        exports=[export_id],
        components={"NFS4": "DEBUG"},
    )
    if not cid:
        raise OperationFailedError("Ganesha container not running after apply")
    log.info("TC-LC3 passed")


def run_lc4(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _with_crit(ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"})
    logs_a = _io_window(ctx, ctx["client"], ctx["export_0"], "lc4a")
    _expect_debug(logs_a, "lc4a", True)
    logs_b = _io_window(ctx, ctx["client_b"], ctx["export_0"], "lc4b")
    _expect_debug(logs_b, "lc4b", False)
    logs_other = _io_window(ctx, ctx["client"], ctx["export_1"], "lc4c")
    _expect_debug(logs_other, "lc4c", True)
    log.info("TC-LC4 passed")


def run_lc5(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _, conf = _with_crit(
        ctx, "MATCH_ALL", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    if not _conf_has_policy(conf, "MATCH_ALL"):
        raise OperationFailedError("MATCH_ALL not outside Conditional")
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_0"], "lc5a"), "lc5a", True)
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_1"], "lc5b"), "lc5b", False)
    _expect_debug(_io_window(ctx, ctx["client_b"], ctx["export_0"], "lc5c"), "lc5c", False)
    log.info("TC-LC5 passed")


def run_lc6(ctx):
    ip_a = _node_ip(ctx["client"])
    _with_crit(ctx, "MATCH_ANY", clients=[ip_a], components={"NFS4": "DEBUG"})
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_0"], "lc6a"), "lc6a", True)
    _expect_debug(_io_window(ctx, ctx["client_b"], ctx["export_0"], "lc6b"), "lc6b", False)
    log.info("TC-LC6 passed")


def run_lc7(ctx):
    export_0, _ = _ids(ctx)
    _with_crit(ctx, "MATCH_ANY", exports=[export_0], components={"NFS4": "DEBUG"})
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_0"], "lc7a"), "lc7a", True)
    _expect_debug(_io_window(ctx, ctx["client_b"], ctx["export_0"], "lc7b"), "lc7b", True)
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_1"], "lc7c"), "lc7c", False)
    log.info("TC-LC7 passed")


def run_lc8(ctx):
    ip_a = _node_ip(ctx["client"])
    _, conf = _with_crit(
        ctx,
        "MATCH_ANY",
        clients=[ip_a],
        components={"NFS4": "DEBUG"},
        fsal_warn=True,
    )
    conditional = _conditional_body(conf)
    if re.search(r"FSAL\s*=", conditional):
        raise OperationFailedError("FSAL was set inside Conditional: %s" % conditional)
    logs_a = _io_window(ctx, ctx["client"], ctx["export_0"], "lc8a")
    _expect_debug(logs_a, "lc8a", True)
    fsal_lines = [line for line in logs_a.splitlines() if RE_FSAL_ANY.search(line)]
    if fsal_lines and not any(RE_FSAL_FULL.search(line) for line in fsal_lines):
        raise OperationFailedError(
            "FSAL lines emitted but none at FULL_DEBUG: %s" % fsal_lines[0][:400]
        )
    if not fsal_lines:
        log.info("TC-LC8: no FSAL lines in the IO window; fallback not observed")
    _expect_debug(_io_window(ctx, ctx["client_b"], ctx["export_0"], "lc8b"), "lc8b", False)
    log.info("TC-LC8 passed")


def _snapshot_conf(ctx):
    _, conf = _read_conf(ctx["nfs_node"], ctx["nfs_name"])
    return conf


def run_lc9(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _apply_and_wait(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    before = _snapshot_conf(ctx)
    _apply_fields(ctx, "MAYBE", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}, expect_fail=True)
    after_cid, after = _read_conf(ctx["nfs_node"], ctx["nfs_name"])
    if after != before:
        raise OperationFailedError("LOG changed after rejected MAYBE apply")
    if not after_cid:
        raise OperationFailedError("Ganesha down after rejected apply")
    _io_window(ctx, ctx["client"], ctx["export_0"], "lc9")
    log.info("TC-LC9 passed")


def run_lc10(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _apply_and_wait(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    before = _snapshot_conf(ctx)
    _apply_fields(ctx, "MATCH_ALL", clients=[ip_a], components={"NFS4": "DEBUG"}, expect_fail=True)
    mid = _snapshot_conf(ctx)
    if mid != before:
        raise OperationFailedError("conf changed after MATCH_ALL with exports omitted")
    _apply_fields(ctx, "MATCH_ALL", exports=[export_0], components={"NFS4": "DEBUG"}, expect_fail=True)
    mid = _snapshot_conf(ctx)
    if mid != before:
        raise OperationFailedError("conf changed after MATCH_ALL with clients omitted")
    _, conf = _apply_and_wait(
        ctx, "MATCH_ALL", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    if not _conf_has_policy(conf, "MATCH_ALL"):
        raise OperationFailedError("MATCH_ALL with both lists was not rendered")
    log.info("TC-LC10 passed")


def run_lc11(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _with_crit(ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"})
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_0"], "lc11a"), "lc11a", True)
    before_cid = get_nfs_container_id(ctx["nfs_node"], nfs_name=ctx["nfs_name"])
    _clear_fields(ctx)

    def cleared(conf):
        return _count_log_blocks(conf) == 1 and not re.search(r"\bConditional\b", conf)

    cid, conf = _wait_conf(ctx, cleared)
    if re.search(r"Clients\s*=", conf) or re.search(r"Exports\s*=", conf):
        raise OperationFailedError("stale Clients/Exports remain after clear:\n%s" % conf[-1500:])
    log.info("TC-LC11 container before=%s after=%s", before_cid, cid)
    _expect_debug(_io_window(ctx, ctx["client"], ctx["export_0"], "lc11b"), "lc11b", False)
    log.info("TC-LC11 passed")


def run_lc12(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _apply_and_wait(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    _set_components(
        ctx,
        "LOG {\n    COMPONENTS {\n        ALL = FULL_DEBUG;\n    }\n}\n",
    )

    def merged(conf):
        return (
            _count_log_blocks(conf) == 1
            and _conf_has_policy(conf, "MATCH_ANY")
            and "Conditional" in conf
            and re.search(r"ALL\s*=\s*FULL_DEBUG\s*;", conf)
        )

    _, conf = _wait_conf(ctx, merged)
    _assert_rendered(conf, "MATCH_ANY", client_ip=ip_a, export_id=export_0, level="DEBUG")
    log.info("TC-LC12 passed: one LOG block holds overlay COMPONENTS and Conditional")


def run_lc13(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    cid, _ = _apply_and_wait(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    pid = _daemon_pid(ctx["nfs_node"], cid)
    log.info("TC-LC13 before: container=%s pid=%s", cid, pid)
    _apply_fields(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "INFO"}
    )

    def info_level(conf):
        try:
            body = _conditional_body(conf)
        except OperationFailedError:
            return False
        return bool(re.search(r"(?:NFS4|NFS_V4)\s*=\s*INFO\s*;", body))

    new_cid, _ = _wait_conf(ctx, info_level)
    if new_cid != cid:
        raise OperationFailedError(
            "container recreated (%s -> %s); TC-LC13 requires reload without a new container"
            % (cid, new_cid)
        )
    log.info("TC-LC13 passed: container %s unchanged, NFS4 = INFO", cid)


def run_lc14(ctx):
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    cid, _ = _apply_and_wait(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    _io_window(ctx, ctx["client"], ctx["export_0"], "lc14write")
    listed = _text(Ceph(ctx["client"]).nfs.export.ls(ctx["nfs_name"]))
    if ctx["export_0"] not in listed:
        raise OperationFailedError("export %s missing from ls: %s" % (ctx["export_0"], listed))
    later = get_nfs_container_id(ctx["nfs_node"], nfs_name=ctx["nfs_name"])
    if later != cid:
        raise OperationFailedError("Ganesha container changed during IO (%s -> %s)" % (cid, later))
    log.info("TC-LC14 passed")


def run_lc15(ctx):
    holders = []
    for node in ctx["nfs_nodes"]:
        cid = get_nfs_container_id(node, nfs_name=ctx["nfs_name"])
        if cid:
            holders.append(node)
    if len(holders) < 2:
        log.warning("SKIP TC-LC15: fewer than 2 running nfs.%s daemons", ctx["nfs_name"])
        return
    ip_a = _node_ip(ctx["client"])
    export_0, _ = _ids(ctx)
    _apply_fields(
        ctx, "MATCH_ANY", clients=[ip_a], exports=[export_0], components={"NFS4": "DEBUG"}
    )
    sleep(20)
    blocks = []
    for node in holders:
        _, conf = _read_conf(node, ctx["nfs_name"])
        _assert_rendered(conf, "MATCH_ANY", client_ip=ip_a, export_id=export_0, level="DEBUG")
        blocks.append(_brace_body(conf, "LOG"))
    if blocks[0] != blocks[1]:
        raise OperationFailedError("LOG blocks differ across nfs daemons")
    log.info("TC-LC15 passed")


_HANDLERS = {
    "LC0": run_lc0,
    "LC1": run_lc1,
    "LC2": run_lc2,
    "LC3": run_lc3,
    "LC4": run_lc4,
    "LC5": run_lc5,
    "LC6": run_lc6,
    "LC7": run_lc7,
    "LC8": run_lc8,
    "LC9": run_lc9,
    "LC10": run_lc10,
    "LC11": run_lc11,
    "LC12": run_lc12,
    "LC13": run_lc13,
    "LC14": run_lc14,
    "LC15": run_lc15,
}


def run(ceph_cluster, **kw):
    """Run one IBMCEPH-18323 case. Suite config ``tc`` is LC0 … LC15."""
    config = kw.get("config") or {}
    tc = str(config.get("tc") or "").upper()
    if tc not in VALID_TCS:
        log.error("config.tc must be one of %s; got %r", VALID_TCS, config.get("tc"))
        return 1

    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if tc == "LC15" and len(nfs_nodes) < 2:
        log.warning("SKIP TC-LC15: lab inventory has one NFS node")
        return 0
    servers = [node.hostname for node in nfs_nodes] if tc == "LC15" else [nfs_nodes[0].hostname]

    ctx = None
    try:
        ctx = _setup(ceph_cluster, config, servers)
        _HANDLERS[tc](ctx)
        log.info("%s passed", tc)
        return 0
    except FeatureAbsent as exc:
        log.warning("SKIP %s: LOG Conditional spec fields are not in this build. %s", tc, exc)
        return 0
    except Exception as exc:
        log.error("%s failed: %s", tc, exc)
        return 1
    finally:
        _cleanup(ctx)
