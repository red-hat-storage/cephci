"""TSM grace-period helpers (additive — safe for other TSM tests).

- enter_grace: daemon restart + wait IN GRACE (integration TC-07)
- hold_cluster_grace: spare open + iptables on spare client + NFS restart
- unblock / release: cleanup (safe to call more than once)
"""

import time

from tests.nfs.lib.common_lib import (
    get_nfs_container_id,
    get_node_time,
    scrape_nfs_logs,
)
from tests.nfs.lib.tsm.helpers import (
    POLL_INTERVAL,
    get_nfs_daemon_name,
    wait_new_container,
)
from utility.log import Log

log = Log(__name__)

GRACE_IN_RE = r"NFS Server Now IN GRACE"
GRACE_OUT_RE = r"NFS Server Now NOT IN GRACE"
GRACE_TIMEOUT = 120


class GraceCtx:
    """Cluster handles for grace workflows (primary + optional spare)."""

    def __init__(
        self,
        clients,
        active,
        peers,
        server,
        nfs_name,
        export,
        mount,
        nfs_port,
        spare_node=None,
        spare_client=None,
        spare_export=None,
        spare_mount=None,
        tsm_port=36369,
        tsm_node_ids=None,
    ):
        self.clients = clients
        self.active = active
        self.peers = peers
        self.server = server
        self.nfs_name = nfs_name
        self.export = export
        self.mount = mount
        self.nfs_port = nfs_port
        self.spare_node = spare_node
        self.spare_client = spare_client
        self.spare_export = spare_export
        self.spare_mount = spare_mount
        self.tsm_port = tsm_port
        self.tsm_node_ids = tsm_node_ids or {}
        self.holds = []  # used by integration TC-07
        self._iptables_active = False
        self._spare_session = None

    @property
    def server_node_id(self):
        if not self.server:
            return None
        return self.tsm_node_ids.get(self.server.hostname)

    @property
    def client_a(self):
        return self.clients[0]

    @property
    def client_b(self):
        return self.clients[1] if len(self.clients) > 1 else None


def wait_log_pattern(nodes, nfs_name, pattern, since, label, timeout=GRACE_TIMEOUT):
    """Wait until scrape_nfs_logs finds pattern on any node. 0 ok, 1 timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        hits = scrape_nfs_logs(nodes, pattern, nfs_name=nfs_name, since=since)
        if hits and any(hits.get(n.hostname) for n in nodes):
            log.info("[%s] matched %r", label, pattern)
            return 0
        time.sleep(POLL_INTERVAL)
    log.error("[%s] timed out waiting for %r", label, pattern)
    return 1


def _restart_wait_in_grace(ctx, target, grace_node, label, wait_container=True):
    """Restart ``target``; stamp+wait IN GRACE on ``grace_node``.

    Time is taken on grace_node immediately before the restart command.
    Returns (0, grace_since) or (1, None).
    """
    old_cid = (
        get_nfs_container_id(target, nfs_name=ctx.nfs_name) if wait_container else None
    )
    daemon_name = get_nfs_daemon_name(ctx.client_a, ctx.nfs_name, target.hostname)
    if not daemon_name:
        return 1, None

    grace_since, _ = get_node_time([grace_node])
    log.info("Restarting daemon %s on %s", daemon_name, target.hostname)
    ctx.client_a.exec_command(
        sudo=True,
        cmd=f"ceph orch daemon restart {daemon_name} --force",
        timeout=300,
    )

    if old_cid and not wait_new_container(target, ctx.nfs_name, old_cid):
        return 1, None
    if wait_log_pattern([grace_node], ctx.nfs_name, GRACE_IN_RE, grace_since, label):
        return 1, None
    return 0, grace_since


def enter_grace(ctx, target_node=None):
    """Restart NFS on target (default: mount server); wait IN GRACE on that host.

    Returns (0, grace_since) on success, (1, None) on failure.
    """
    node = target_node or ctx.server
    return _restart_wait_in_grace(ctx, node, node, "enter_grace")


def wait_grace_exit(ctx, since, timeout=GRACE_TIMEOUT, node=None):
    """Wait until NOT IN GRACE on primary (or ``node``). 0 ok, 1 timeout."""
    primary = node or ctx.server
    return wait_log_pattern(
        [primary],
        ctx.nfs_name,
        GRACE_OUT_RE,
        since,
        "exit_grace",
        timeout=timeout,
    )


def grace_has_exited(ctx, since, node=None):
    """True if NOT IN GRACE already logged on primary since `since`."""
    primary = node or ctx.server
    hits = scrape_nfs_logs([primary], GRACE_OUT_RE, nfs_name=ctx.nfs_name, since=since)
    return bool(hits and hits.get(primary.hostname))


def iptables_nfs_block_spare(client, spare_nfs, nfs_port, add=True):
    """Block/unblock NFS TCP on the spare client only."""
    if add:
        out, _ = client.exec_command(
            sudo=True, cmd="command -v iptables", check_ec=False, timeout=30
        )
        if not (out or "").strip():
            client.exec_command(
                sudo=True,
                cmd=(
                    "timeout 120 dnf install -y iptables >/dev/null 2>&1 || "
                    "timeout 120 yum install -y iptables >/dev/null 2>&1"
                ),
                check_ec=False,
                timeout=180,
            )
            out, _ = client.exec_command(
                sudo=True,
                cmd="command -v iptables",
                check_ec=False,
                timeout=30,
            )
            if not (out or "").strip():
                log.error("iptables missing on %s", client.hostname)
                return False

    op = "-I" if add else "-D"
    spare_ip = spare_nfs.ip_address
    cmd = (
        f"iptables {op} OUTPUT -d {spare_ip} -p tcp --dport {nfs_port} -j DROP && "
        f"iptables {op} INPUT -s {spare_ip} -p tcp --sport {nfs_port} -j DROP"
    )
    try:
        client.exec_command(sudo=True, cmd=cmd, check_ec=add, timeout=60)
        log.info(
            "iptables %s on %s: NFS %s <-> %s (%s)",
            "add" if add else "del",
            client.hostname,
            nfs_port,
            spare_nfs.hostname,
            spare_ip,
        )
    except Exception as exc:
        log.error(
            "iptables %s failed on %s: %s",
            "add" if add else "del",
            client.hostname,
            exc,
        )
        if add:
            return False
    return True


def hold_cluster_grace(ctx, spare_session, restart_node=None):
    """Extend cluster grace (~90s): spare hold + iptables + NFS restart.

    restart_node: which NFS host to restart (default: spare). Pass ctx.server
    to restart the primary while spare+iptables still extends grace.

    On failure after iptables/session are set, calls release_cluster_grace.
    Returns (0, grace_since) or (1, None).
    """
    if not ctx.spare_node or not ctx.spare_client:
        log.error("hold_cluster_grace requires spare_node and spare_client")
        return 1, None

    target = restart_node or ctx.spare_node
    primary = ctx.server
    ctx._spare_session = spare_session
    if not iptables_nfs_block_spare(
        ctx.spare_client, ctx.spare_node, ctx.nfs_port, add=True
    ):
        return 1, None
    ctx._iptables_active = True

    # Wait for new container only when restarting primary (not spare).
    wait_cid = target.hostname != ctx.spare_node.hostname
    rc, grace_since = _restart_wait_in_grace(
        ctx, target, primary, "hold_cluster_grace", wait_container=wait_cid
    )
    if rc:
        release_cluster_grace(ctx)
        return 1, None

    log.info(
        "Cluster in grace (restart=%s spare_hold=%s primary=%s)",
        target.hostname,
        ctx.spare_node.hostname,
        primary.hostname,
    )
    return 0, grace_since


def unblock_cluster_grace(ctx):
    """Remove spare-client NFS iptables only (idempotent)."""
    if not (ctx.spare_node and ctx.spare_client and ctx._iptables_active):
        return
    iptables_nfs_block_spare(ctx.spare_client, ctx.spare_node, ctx.nfs_port, add=False)
    ctx._iptables_active = False
    log.info("Unblocked NFS iptables on spare client %s", ctx.spare_client.hostname)


def release_cluster_grace(ctx):
    """Unblock iptables (if set), close spare open, stop session. Idempotent."""
    unblock_cluster_grace(ctx)
    if ctx._spare_session is None:
        return
    try:
        ctx._spare_session.send("close 0")
        time.sleep(1)
        ctx._spare_session.stop()
    except Exception as exc:
        log.warning("spare session close/stop: %s", exc)
    ctx._spare_session = None
