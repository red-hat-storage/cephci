"""NFS TSM + NFSv4 delegation lifecycle (TC-TSM-DELEG-01).

Deploy once and wait for first-boot TSM ready checks, then enable FULL_DEBUG
via ``ceph nfs cluster config set`` (Delegations are already true by default),
and run WORKFLOWS:

Lifecycle: grant → conflict/recall → close → create-hold replay →
delegations=none.

Export × type matrix (individual cells):
  rw write→type5, rw read→type4, ro read→type4, ro write→none,
  none write→none, none read→none.

Holds use ``sleep infinity`` (same pattern as test_nfs_tsm_record_validation.py).
TSM counters and grant/recall/CLOSE evidence come from ``podman logs --since``.
"""

import re
import time

from nfs_delegation_operations import (
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    log_delegation_open_types,
    skip_delegation_tests_unless_supported,
    update_export_delegation,
    validate_delegation_grant_capture,
    validate_delegation_recall_ganesha_capture,
)

from cli.exceptions import OperationFailedError
from tests.nfs.lib.common_lib import (
    enable_nfs_debug_logs,
    get_node_time,
    scrape_nfs_logs,
)
from tests.nfs.lib.multi_active.delegation import NfsMultiActiveDelegation
from tests.nfs.lib.tsm.constants import DELEG_LOG_RE, NfsFdHold
from tests.nfs.lib.tsm.helpers import (
    POLL_INTERVAL,
    POLL_TIMEOUT,
    announce,
    fmt_tsm,
    kill_holds,
    log_workflow_summary,
    mount_clients,
    peer_summary,
    run_step,
    start_bg,
    wait_peer_counts,
)
from tests.nfs.tsm.test_nfs_tsm_basic import deploy_step, safe_cleanup
from utility.log import Log

log = Log(__name__)

NFS_PORT = 12052
TSM_PORT = 36372
SUMMARY_TITLE = "TSM DELEGATION WORKFLOW SUMMARY"
# FSAL_UP emits ``Recalling``; NFS_CB emits ``successfully recalled``.
DELEG_DEBUG_COMPONENTS = ("TSM", "STATE", "NFS4", "FSAL", "FSAL_UP", "NFS_CB")

# ---------------------------------------------------------------------------
# Workflow overview — ``run`` callables bound after handlers below.
# ---------------------------------------------------------------------------
WORKFLOWS = {
    "grant_hold_open_deleg": {
        "desc": "Client A create+hold → open+1, deleg+1",
    },
    "peer_conflict_recall": {
        "desc": "Client B append+hold → open+2, recall",
    },
    "close_delegreturn_baseline": {
        "desc": "Close A+B holds → baseline",
    },
    "create_hold_grant": {
        "desc": "Replay grant→conflict→close on 2nd file",
    },
    "export_none_no_deleg": {
        "desc": "delegations=none → open+1, no type 4/5",
    },
    # Export mode × delegation type matrix (self-contained cells)
    "rw_write_deleg_type5": {
        "desc": "export=rw write-create → type 5, open+1 deleg+1",
    },
    "rw_read_deleg_type4": {
        "desc": "export=rw read-open → type 4, open+1 deleg+1",
    },
    "ro_read_deleg_type4": {
        "desc": "export=ro read-open → type 4, open+1 deleg+1",
    },
    "ro_write_no_deleg": {
        "desc": "export=ro write OPEN → no type 5, deleg unchanged",
    },
    "none_write_no_deleg": {
        "desc": "export=none write-create → no type 4/5, deleg unchanged",
    },
    "none_read_no_deleg": {
        "desc": "export=none read-open → no type 4/5, deleg unchanged",
    },
}


# ---------------------------------------------------------------------------
# Delegation-specific helpers (shared TSM bits live in lib/tsm/helpers.py)
# ---------------------------------------------------------------------------
def _deleg_hits(nodes, nfs_name, since=None):
    """Flatten ``scrape_nfs_logs`` hits for delegation validators."""
    collected = scrape_nfs_logs(
        nodes, DELEG_LOG_RE.pattern, nfs_name=nfs_name, since=since
    )
    if not collected:
        return []
    hits = []
    for text in collected.values():
        if text:
            hits.extend(text.splitlines())
    return hits


def _wait_deleg(nodes, nfs_name, since, validate_fn, label):
    """Poll scraped logs until validate_fn(hits) succeeds."""
    deadline = time.time() + POLL_TIMEOUT
    last_err, last_hits = None, []
    while time.time() < deadline:
        last_hits = _deleg_hits(nodes, nfs_name, since)
        try:
            validate_fn(last_hits)
            log.info("[%s] delegation log validation ok", label)
            return last_hits
        except OperationFailedError as exc:
            last_err = exc
            time.sleep(POLL_INTERVAL)
    raise OperationFailedError(
        "[%s] delegation log failed: %s (hits=%s)" % (label, last_err, len(last_hits))
    )


def _start_hold(client, cmd, holds, action, expecting):
    """Announce, start infinity hold, track PID."""
    announce(action, expecting)
    pid = start_bg(client, cmd)
    if not pid:
        raise OperationFailedError("failed to start hold: %s" % action)
    holds.append((client, pid))


def _close_holds(
    holds, peers, nodes, nfs_name, base_open, base_lock, base_deleg, label
):
    """Kill holds; assert CLOSE in logs; wait TSM back to baseline."""
    announce("Do CLOSE / release hold(s)", fmt_tsm(base_open, base_lock, base_deleg))
    close_since, _ = get_node_time(nodes)
    kill_holds(holds)
    time.sleep(2)

    def _need_close(hits):
        if not re.search(r"CLOSE handler|END OF.*CLOSE", "\n".join(hits or []), re.I):
            raise OperationFailedError("[%s] missing CLOSE in podman logs" % label)

    _wait_deleg(nodes, nfs_name, close_since, _need_close, label)
    return wait_peer_counts(
        peers,
        nfs_name,
        close_since,
        base_open,
        base_lock,
        label,
        expect_deleg=base_deleg,
        raise_on_timeout=True,
    )


class DelegCtx:
    """Handles and counters shared by every WORKFLOWS handler."""

    def __init__(
        self, clients, peers, nodes, nfs_cmd_host, nfs_name, export, mount, paths
    ):
        self.client_a = clients[0]
        self.client_b = clients[1]
        self.peers = peers
        self.nodes = nodes
        self.nfs_cmd_host = nfs_cmd_host
        self.nfs_name = nfs_name
        self.export = export
        self.mount = mount
        self.hold_path = paths["hold"]
        self.create_path = paths["create"]
        self.none_path = paths["none"]
        self.holds = []
        self.base_open = 0
        self.base_lock = 0
        self.base_deleg = 0


def _refresh_baseline(ctx):
    """Refresh open/lock/deleg baseline from latest peer summary."""
    snap = peer_summary(ctx.peers, ctx.nfs_name, since=None)
    if snap.get("seen"):
        ctx.base_open, ctx.base_lock, ctx.base_deleg = (
            snap["open"],
            snap["lock"],
            snap["deleg"],
        )


def _set_export_mode(ctx, mode):
    """Set export ``delegations`` and assert the value stuck."""
    update_export_delegation(ctx.nfs_cmd_host, ctx.nfs_name, ctx.export, mode)
    NfsMultiActiveDelegation.assert_export_delegation(
        ctx.nfs_cmd_host, ctx.nfs_name, ctx.export, mode
    )


def _seed_file(client, path):
    """Create/overwrite a small file for subsequent read-open holds."""
    client.exec_command(
        sudo=True,
        cmd="bash -c 'echo seed > %s'" % path,
        check_ec=False,
    )


def _wait_seed_deleg_cleared(ctx, label, since):
    """Block until peer open/deleg return to pre-seed baseline (DELEGRETURN done).

    Seed write often leaves sticky ``open=0 deleg=1`` until the client returns
    the write-deleg; a follow-on read-open under that deleg stays local and
    never hits Ganesha/TSM.
    """
    log.info(
        "[%s] waiting for seed write-deleg to clear (want %s)",
        label,
        fmt_tsm(ctx.base_open, ctx.base_lock, ctx.base_deleg),
    )
    wait_peer_counts(
        ctx.peers,
        ctx.nfs_name,
        since,
        ctx.base_open,
        ctx.base_lock,
        "%s_seed_clear" % label,
        expect_deleg=ctx.base_deleg,
        raise_on_timeout=True,
    )


def _assert_no_deleg_types(label, hits):
    """Fail if nfs4_op_open shows read (4) or write (5) delegation."""
    types = log_delegation_open_types(label, hits)
    if any(t in (4, 5) for t in types) or re.search(
        r"delegation_type\s+[45]\b", "\n".join(hits or []), re.I
    ):
        raise OperationFailedError("[%s] expected no type 4/5, got %r" % (label, types))
    return types


def _matrix_cell(ctx, *, mode, hold_kind, expect_type, path, label):
    """One export-mode × OPEN-kind cell: set mode → hold → assert → close → rw.

    ``expect_type`` is 4 (read), 5 (write), or 0 (unsupported / no grant).
    """
    # Seed while rw so read holds and ro mode have a real file path.
    # Wait for seed write-deleg to clear before the test hold, otherwise the
    # client may satisfy the next open locally (no OPEN / no TSM bump).
    if hold_kind == "read" or mode == "ro":
        if mode != "rw":
            _set_export_mode(ctx, "rw")
        _refresh_baseline(ctx)
        seed_since, _ = get_node_time(ctx.nodes)
        _seed_file(ctx.client_a, path)
        time.sleep(2)
        _wait_seed_deleg_cleared(ctx, label, seed_since)

    _set_export_mode(ctx, mode)
    _refresh_baseline(ctx)

    since_t, _ = get_node_time(ctx.nodes)
    expect_deleg = ctx.base_deleg + (1 if expect_type in (4, 5) else 0)
    cmd = (
        NfsFdHold.write_create(path)
        if hold_kind == "write"
        else NfsFdHold.read_open(path)
    )
    _start_hold(
        ctx.client_a,
        cmd,
        ctx.holds,
        "Do %s hold (export=%s, expect type %s)" % (hold_kind, mode, expect_type),
        fmt_tsm(ctx.base_open + 1, ctx.base_lock, expect_deleg),
    )
    held = wait_peer_counts(
        ctx.peers,
        ctx.nfs_name,
        since_t,
        ctx.base_open + 1,
        ctx.base_lock,
        label,
        expect_deleg=expect_deleg,
        raise_on_timeout=True,
    )

    if expect_type in (4, 5):
        hold_mode = "write" if expect_type == 5 else "read"
        hits = _wait_deleg(
            ctx.nodes,
            ctx.nfs_name,
            since_t,
            lambda h, hm=hold_mode, lb=label: validate_delegation_grant_capture(
                lb, h, hm
            ),
            "%s_logs" % label,
        )
        log_delegation_open_types(label, hits)
    else:
        time.sleep(5)
        hits = _deleg_hits(ctx.nodes, ctx.nfs_name, since_t)
        _assert_no_deleg_types(label, hits)
        if mode in ("ro", "none") and not re.search(
            r"Delegation type not supported", "\n".join(hits or []), re.I
        ):
            log.warning(
                "[%s] no 'Delegation type not supported' yet (types checked)", label
            )

    snap = _close_holds(
        ctx.holds,
        ctx.peers,
        ctx.nodes,
        ctx.nfs_name,
        ctx.base_open,
        ctx.base_lock,
        ctx.base_deleg,
        "%s_close" % label,
    )
    ctx.holds = []
    log.info(
        "%s TSM: held %s → closed %s",
        label,
        fmt_tsm(held["open"], held["lock"], held["deleg"]),
        fmt_tsm(snap["open"], snap["lock"], snap["deleg"]),
    )
    _set_export_mode(ctx, "rw")
    _refresh_baseline(ctx)


def _wf_grant(ctx):
    since_t, _ = get_node_time(ctx.nodes)
    _start_hold(
        ctx.client_a,
        NfsFdHold.write_create(ctx.hold_path),
        ctx.holds,
        "Do open + hold (client A create/write)",
        fmt_tsm(ctx.base_open + 1, ctx.base_lock, ctx.base_deleg + 1),
    )
    snap = wait_peer_counts(
        ctx.peers,
        ctx.nfs_name,
        since_t,
        ctx.base_open + 1,
        ctx.base_lock,
        "grant",
        expect_deleg=ctx.base_deleg + 1,
        raise_on_timeout=True,
    )
    hits = _wait_deleg(
        ctx.nodes,
        ctx.nfs_name,
        since_t,
        lambda h: validate_delegation_grant_capture("tsm_deleg_grant", h, "write"),
        "grant_logs",
    )
    log_delegation_open_types("tsm_deleg_grant", hits)
    log.info("grant TSM: %s", fmt_tsm(snap["open"], snap["lock"], snap["deleg"]))


def _wf_conflict(ctx):
    since_t, _ = get_node_time(ctx.nodes)
    _start_hold(
        ctx.client_b,
        NfsFdHold.write_append(ctx.hold_path, "client-b-conflict"),
        ctx.holds,
        "Do open + hold (client B append/conflict)",
        fmt_tsm(ctx.base_open + 2, ctx.base_lock, ctx.base_deleg),
    )
    snap = wait_peer_counts(
        ctx.peers,
        ctx.nfs_name,
        since_t,
        ctx.base_open + 2,
        ctx.base_lock,
        "conflict",
        expect_deleg=ctx.base_deleg,
        raise_on_timeout=True,
    )
    _wait_deleg(
        ctx.nodes,
        ctx.nfs_name,
        since_t,
        lambda h: validate_delegation_recall_ganesha_capture(
            "tsm_deleg_conflict", h, True
        ),
        "recall_logs",
    )
    log.info("conflict TSM: %s", fmt_tsm(snap["open"], snap["lock"], snap["deleg"]))


def _wf_close(ctx):
    snap = _close_holds(
        ctx.holds,
        ctx.peers,
        ctx.nodes,
        ctx.nfs_name,
        ctx.base_open,
        ctx.base_lock,
        ctx.base_deleg,
        "close",
    )
    ctx.holds = []
    log.info("close TSM: %s", fmt_tsm(snap["open"], snap["lock"], snap["deleg"]))


def _wf_replay(ctx):
    """Grant → conflict → close on a second file (same checks as tests 1–3)."""
    # Temporarily point hold path at the create file for grant/conflict helpers.
    orig = ctx.hold_path
    ctx.hold_path = ctx.create_path
    try:
        _wf_grant(ctx)
        _wf_conflict(ctx)
        _wf_close(ctx)
    finally:
        ctx.hold_path = orig


def _wf_none(ctx):
    update_export_delegation(ctx.nfs_cmd_host, ctx.nfs_name, ctx.export, "none")
    NfsMultiActiveDelegation.assert_export_delegation(
        ctx.nfs_cmd_host, ctx.nfs_name, ctx.export, "none"
    )
    snap = peer_summary(ctx.peers, ctx.nfs_name, since=None)
    if snap.get("seen"):
        ctx.base_open, ctx.base_lock, ctx.base_deleg = (
            snap["open"],
            snap["lock"],
            snap["deleg"],
        )

    since_t, _ = get_node_time(ctx.nodes)
    _start_hold(
        ctx.client_a,
        NfsFdHold.write_create(ctx.none_path),
        ctx.holds,
        "Do open + hold (client A, delegations=none)",
        fmt_tsm(ctx.base_open + 1, ctx.base_lock, ctx.base_deleg),
    )
    held = wait_peer_counts(
        ctx.peers,
        ctx.nfs_name,
        since_t,
        ctx.base_open + 1,
        ctx.base_lock,
        "none_hold",
        expect_deleg=ctx.base_deleg,
        raise_on_timeout=True,
    )
    time.sleep(5)
    hits = _deleg_hits(ctx.nodes, ctx.nfs_name, since_t)
    _assert_no_deleg_types("tsm_deleg_none", hits)

    snap = _close_holds(
        ctx.holds,
        ctx.peers,
        ctx.nodes,
        ctx.nfs_name,
        ctx.base_open,
        ctx.base_lock,
        ctx.base_deleg,
        "none_close",
    )
    ctx.holds = []
    log.info(
        "none TSM: held %s → closed %s",
        fmt_tsm(held["open"], held["lock"], held["deleg"]),
        fmt_tsm(snap["open"], snap["lock"], snap["deleg"]),
    )
    NfsMultiActiveDelegation.enable_rw_export(
        ctx.nfs_cmd_host, ctx.nfs_name, ctx.export
    )


def _wf_rw_write_type5(ctx):
    _matrix_cell(
        ctx,
        mode="rw",
        hold_kind="write",
        expect_type=5,
        path="%s/tsm_mx_rw_w.txt" % ctx.mount,
        label="rw_write_type5",
    )


def _wf_rw_read_type4(ctx):
    _matrix_cell(
        ctx,
        mode="rw",
        hold_kind="read",
        expect_type=4,
        path="%s/tsm_mx_rw_r.txt" % ctx.mount,
        label="rw_read_type4",
    )


def _wf_ro_read_type4(ctx):
    _matrix_cell(
        ctx,
        mode="ro",
        hold_kind="read",
        expect_type=4,
        path="%s/tsm_mx_ro_r.txt" % ctx.mount,
        label="ro_read_type4",
    )


def _wf_ro_write_no_deleg(ctx):
    _matrix_cell(
        ctx,
        mode="ro",
        hold_kind="write",
        expect_type=0,
        path="%s/tsm_mx_ro_w.txt" % ctx.mount,
        label="ro_write_no_deleg",
    )


def _wf_none_write_no_deleg(ctx):
    _matrix_cell(
        ctx,
        mode="none",
        hold_kind="write",
        expect_type=0,
        path="%s/tsm_mx_none_w.txt" % ctx.mount,
        label="none_write_no_deleg",
    )


def _wf_none_read_no_deleg(ctx):
    _matrix_cell(
        ctx,
        mode="none",
        hold_kind="read",
        expect_type=0,
        path="%s/tsm_mx_none_r.txt" % ctx.mount,
        label="none_read_no_deleg",
    )


# Bind handlers so static analysis sees them used (and call sites stay direct).
WORKFLOWS["grant_hold_open_deleg"]["run"] = _wf_grant
WORKFLOWS["peer_conflict_recall"]["run"] = _wf_conflict
WORKFLOWS["close_delegreturn_baseline"]["run"] = _wf_close
WORKFLOWS["create_hold_grant"]["run"] = _wf_replay
WORKFLOWS["export_none_no_deleg"]["run"] = _wf_none
WORKFLOWS["rw_write_deleg_type5"]["run"] = _wf_rw_write_type5
WORKFLOWS["rw_read_deleg_type4"]["run"] = _wf_rw_read_type4
WORKFLOWS["ro_read_deleg_type4"]["run"] = _wf_ro_read_type4
WORKFLOWS["ro_write_no_deleg"]["run"] = _wf_ro_write_no_deleg
WORKFLOWS["none_write_no_deleg"]["run"] = _wf_none_write_no_deleg
WORKFLOWS["none_read_no_deleg"]["run"] = _wf_none_read_no_deleg


def run(ceph_cluster, **kw):
    """TC-TSM-DELEG-01: held opens + TSM/delegation checks via podman logs."""
    config = kw.get("config") or {}
    if skip_delegation_tests_unless_supported(config):
        return 0

    nfs_version = str(config.get("nfs_version", "4.2"))
    nfs_nodes = sorted(ceph_cluster.get_nodes("nfs"), key=lambda n: n.hostname)
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]
    if len(nfs_nodes) < 2 or len(clients) < 2:
        log.error("Need ≥2 NFS nodes and ≥2 clients")
        return 1

    nodes = nfs_nodes[:2]
    use_clients = clients[:2]
    name = "tsmr-deleg"
    nfs_name = f"cephfs-nfs-{name}"
    export = f"/export_{name}"
    mount = f"/mnt/nfs_{name}"

    results = {}
    peers = nodes[1:]
    server = nodes[0]
    nfs_cmd_host = nodes[0]
    cluster_nodes = nodes
    debug_enabled = False
    ctx = None
    rc = 1

    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    try:
        result = deploy_step(
            ceph_cluster,
            installer,
            use_clients[0],
            nodes,
            {
                "nfs_count": 2,
                "nfs_port": NFS_PORT,
                "tsm_port": TSM_PORT,
                "enable_debug": False,
            },
            nfs_name,
            name,
        )
        if result is None:
            log.error("Bringup failed: deploy")
            return log_workflow_summary(results, title=SUMMARY_TITLE, name_width=28)

        _, active, nfs_port, _ = result
        server, peers = active[0], active[1:]
        nfs_cmd_host = server
        cluster_nodes = list(active)

        # First-boot verification already completed in deploy_step (assert_tsm_ready).
        # Enable FULL_DEBUG only after that, so boot markers are not drowned out.
        # Delegations are true by default — no template edit / redeploy needed.
        log.info(
            "Enabling FULL_DEBUG via ceph nfs cluster config set (%s)",
            ", ".join(DELEG_DEBUG_COMPONENTS),
        )
        enable_nfs_debug_logs(use_clients[0], nfs_name, DELEG_DEBUG_COMPONENTS)
        debug_enabled = True

        if mount_clients(
            use_clients,
            nfs_name,
            export,
            mount,
            nfs_port,
            server.hostname,
            nfs_version,
        ):
            log.error("Bringup failed: mount")
            return log_workflow_summary(results, title=SUMMARY_TITLE, name_width=28)

        NfsMultiActiveDelegation.enable_rw_export(nfs_cmd_host, nfs_name, export)

        ctx = DelegCtx(
            use_clients,
            peers,
            cluster_nodes,
            nfs_cmd_host,
            nfs_name,
            export,
            mount,
            {
                "hold": f"{mount}/tsm_deleg_hold.txt",
                "create": f"{mount}/tsm_deleg_create.txt",
                "none": f"{mount}/tsm_deleg_none.txt",
            },
        )
        since, _ = get_node_time(cluster_nodes)
        baseline = peer_summary(peers, nfs_name, since=since)
        if baseline.get("seen"):
            ctx.base_open, ctx.base_lock, ctx.base_deleg = (
                baseline["open"],
                baseline["lock"],
                baseline["deleg"],
            )
        log.info("Baseline: %s", fmt_tsm(ctx.base_open, ctx.base_lock, ctx.base_deleg))

        _, coredump_since = get_node_time(cluster_nodes)
        for i, (wf_name, spec) in enumerate(WORKFLOWS.items(), 1):
            log.info("\n>>> %s — %s", wf_name, spec["desc"])
            if not run_step(
                results,
                i,
                wf_name,
                lambda s=spec: s["run"](ctx),
                coredump_nodes=cluster_nodes,
                coredump_since=coredump_since,
            ):
                return log_workflow_summary(results, title=SUMMARY_TITLE, name_width=28)
            _, coredump_since = get_node_time(cluster_nodes)

        rc = log_workflow_summary(results, title=SUMMARY_TITLE, name_width=28)
    except Exception as exc:
        log.error("TSM delegation workflow FAILED: %s", exc)
        rc = log_workflow_summary(results, title=SUMMARY_TITLE, name_width=28) or 1
    finally:
        # Always release leftover holds and tear down, even on mid-test failure.
        if ctx is not None:
            kill_holds(ctx.holds)
        if debug_enabled:
            try:
                use_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs cluster config reset {nfs_name}",
                    check_ec=False,
                )
            except Exception as err:
                log.warning("NFS cluster config reset failed: %s", err)
        safe_cleanup(use_clients[0], mount, nfs_name, export, nodes)
        for client in use_clients[1:]:
            client.exec_command(
                sudo=True,
                cmd="timeout 30 umount -l %s 2>/dev/null || true" % mount,
                check_ec=False,
                timeout=45,
            )

    return rc
