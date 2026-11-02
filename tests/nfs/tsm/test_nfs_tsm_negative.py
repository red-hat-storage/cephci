"""NFS TSM negative workflows."""

import time

from cli.utilities.packages import Package
from tests.nfs.lib.common_lib import (
    get_nfs_container_id,
    get_node_time,
    scrape_nfs_logs,
)
from tests.nfs.lib.tsm import NfsTsmValidation
from tests.nfs.lib.tsm.constants import (
    TSM_DISABLE_PRESENT,
    TSM_PRIMARY_SELECTION_FAIL_PRESENT,
)
from tests.nfs.lib.tsm.helpers import (
    check_coredumps,
    get_nfs_daemon_name,
    wait_new_container,
)
from tests.nfs.lib.tsm.integration import wait_daemon_stopped
from tests.nfs.tsm.test_nfs_tsm_basic import (
    deploy_step,
    execute_workflows,
    run_io,
    safe_cleanup,
)
from utility.log import Log

log = Log(__name__)

TSM_PORT = 36369

WORKFLOWS = {
    "single_daemon": [{"nfs_count": 1, "expect_deploy_fail": True}],
    "state_retrieval_failure": [{"nfs_count": 2, "min_nfs_nodes": 2}],
    # "primary_selection_failure": [{"nfs_count": 3, "min_nfs_nodes": 3}],
}


def _ensure_pkg_binary(node, package, binary):
    """Install package if binary is missing. Returns False on failure."""
    out, _ = node.exec_command(sudo=True, cmd=f"command -v {binary}", check_ec=False)
    if out and str(out).strip():
        return True
    log.info("Installing %s on %s (missing %s)", package, node.hostname, binary)
    try:
        Package(node).install(package)
    except Exception as exc:
        log.error("Failed to install %s on %s: %s", package, node.hostname, exc)
        return False
    out, _ = node.exec_command(sudo=True, cmd=f"command -v {binary}", check_ec=False)
    if not (out and str(out).strip()):
        log.error("%s still missing on %s after install", binary, node.hostname)
        return False
    return True


def _ensure_fw_tools(nodes):
    for node in nodes:
        if not _ensure_pkg_binary(node, "iptables", "iptables"):
            return False
        if not _ensure_pkg_binary(node, "nmap-ncat", "nc"):
            return False
    return True


def _tsm_port_reachable(src, dst_ip, port, timeout=3):
    out, _ = src.exec_command(
        sudo=True,
        cmd=f"nc -z -w{timeout} {dst_ip} {port} >/dev/null 2>&1; echo $?",
        check_ec=False,
    )
    rc = (out or "").strip().splitlines()[-1].strip() if out else "1"
    return rc == "0"


def _iptables_tsm(peers, recovering, port, add=True):
    """Block/unblock TSM between peers and recovering."""
    ip = recovering.ip_address
    if add:
        if not _ensure_fw_tools(list(peers) + [recovering]):
            return False
        for peer in peers:
            if not _tsm_port_reachable(recovering, peer.ip_address, port):
                log.error(
                    "TSM %s:%s not reachable from %s before iptables",
                    peer.ip_address,
                    port,
                    recovering.hostname,
                )
                return False

    op = "-I" if add else "-D"
    for peer in peers:
        try:
            peer.exec_command(
                sudo=True,
                cmd=(
                    f"iptables {op} INPUT -p tcp -s {ip} --dport {port} -j DROP && "
                    f"iptables {op} OUTPUT -p tcp -d {ip} --dport {port} -j DROP"
                ),
                check_ec=add,
            )
        except Exception as exc:
            log.error(
                "iptables %s failed on %s: %s",
                "add" if add else "del",
                peer.hostname,
                exc,
            )
            if add:
                return False

    if not add:
        return True

    for peer in peers:
        if _tsm_port_reachable(recovering, peer.ip_address, port, timeout=5):
            log.error(
                "TSM partition incomplete: %s can still reach %s:%s",
                recovering.hostname,
                peer.ip_address,
                port,
            )
            return False
        log.info(
            "TSM blocked: %s -/-> %s:%s", recovering.hostname, peer.ip_address, port
        )
    return True


def _daemon_cmd(client, nfs_name, hostname, action):
    """restart/stop NFS daemon on host. Returns daemon_name or None."""
    name = get_nfs_daemon_name(client, nfs_name, hostname)
    if not name:
        return None
    log.info("%s daemon %s on %s", action, name, hostname)
    client.exec_command(sudo=True, cmd=f"ceph orch daemon {action} {name} --force")
    return name


def _start_nfs_daemon(client, daemon_name):
    if not daemon_name:
        return
    log.info("Starting daemon %s", daemon_name)
    client.exec_command(
        sudo=True, cmd=f"ceph orch daemon start {daemon_name} --force", check_ec=False
    )


def _wait_log_patterns(nodes, patterns, nfs_name, since, tries, sleep_s, label):
    """Poll until every pattern matches on every node."""
    log.info("waiting for %s", label)
    for _ in range(tries):
        ok = True
        for pat in patterns:
            hits = scrape_nfs_logs(nodes, pat, nfs_name=nfs_name, since=since)
            if not hits or any(not hits.get(n.hostname) for n in nodes):
                ok = False
                break
        if ok:
            return True
        time.sleep(sleep_s)
    log.error("%s not seen", label)
    return False


def _post_checks(
    client,
    nfs_name,
    export,
    mount,
    nfs_port,
    server,
    tag,
    coredump_nodes,
    coredump_since,
    nfs_version,
    assert_fn,
):
    """Run TSM assert + IO + coredump. Returns 'passed'/'failed'."""
    if assert_fn():
        return "failed"
    if run_io(
        client, nfs_name, export, mount, nfs_port, server, tag, nfs_version=nfs_version
    ):
        return "failed"
    if check_coredumps(coredump_nodes, coredump_since, tag):
        return "failed"
    log.info("[%s] PASSED", tag)
    return "passed"


def run_state_retrieval_failure(
    cluster, installer, client, all_nodes, name, steps, nfs_version="4.2"
):
    """Isolate+restart one daemon; expect TSM disable logs. Cleanup always."""
    min_nodes = steps[0].get("min_nfs_nodes", 3)
    if len(all_nodes) < min_nodes:
        log.warning("[%s] skip (need %s NFS nodes)", name, min_nodes)
        return "skipped"

    nodes = sorted(all_nodes, key=lambda n: n.hostname)[:3]
    nfs_name = f"cephfs-nfs-tsmn-{name}"
    export = f"/export_tsmn_{name}"
    mount = f"/mnt/nfs_tsmn_{name}"
    peers = recovering = None

    try:
        result = deploy_step(
            cluster, installer, client, nodes, steps[0], nfs_name, name
        )
        if result is None:
            return "failed"
        coredump_since, active, nfs_port, _ = result
        recovering, peers = active[-1], active[:-1]

        log_since, _ = get_node_time([recovering])
        if not _iptables_tsm(peers, recovering, TSM_PORT, add=True):
            log.error("[%s] failed to partition TSM to %s", name, recovering.hostname)
            return "failed"

        old_cid = get_nfs_container_id(recovering, nfs_name=nfs_name)
        if not _daemon_cmd(client, nfs_name, recovering.hostname, "restart"):
            return "failed"
        if not wait_new_container(recovering, nfs_name, old_cid):
            log.error("[%s] container not back on %s", name, recovering.hostname)
            return "failed"

        if not _wait_log_patterns(
            [recovering],
            TSM_DISABLE_PRESENT,
            nfs_name,
            log_since,
            tries=120,
            sleep_s=10,
            label=f"disable markers on recovering ({TSM_DISABLE_PRESENT})",
        ):
            return "failed"

        return _post_checks(
            client,
            nfs_name,
            export,
            mount,
            nfs_port,
            peers[0].hostname,
            name,
            peers,
            coredump_since,
            nfs_version,
            assert_fn=lambda: NfsTsmValidation(
                cluster, client, "enable_TSM", "Tsm_Port", TSM_PORT
            ).assert_tsm_disable_logs(nfs_name, recovering, since=log_since),
        )
    except Exception as exc:
        log.error("[%s] FAILED: %s", name, exc)
        return "failed"
    finally:
        if peers and recovering:
            _iptables_tsm(peers, recovering, TSM_PORT, add=False)
        safe_cleanup(client, mount, nfs_name, export, nodes)


def run_primary_selection_failure(
    cluster, installer, client, all_nodes, name, steps, nfs_version="4.2"
):
    """Stop peers, restart joining alone; expect disable after max retries."""
    min_nodes = steps[0].get("min_nfs_nodes", 3)
    if len(all_nodes) < min_nodes:
        log.warning("[%s] skip (need %s NFS nodes)", name, min_nodes)
        return "skipped"

    nodes = sorted(all_nodes, key=lambda n: n.hostname)[:3]
    nfs_name = f"cephfs-nfs-tsmn-{name}"
    export = f"/export_tsmn_{name}"
    mount = f"/mnt/nfs_tsmn_{name}"
    stopped = []
    joining = None

    try:
        result = deploy_step(
            cluster, installer, client, nodes, steps[0], nfs_name, name
        )
        if result is None:
            return "failed"
        coredump_since, active, nfs_port, _ = result
        joining, peers = active[-1], active[:-1]

        log_since, _ = get_node_time([joining])
        for peer in peers:
            dname = _daemon_cmd(client, nfs_name, peer.hostname, "stop")
            if not dname:
                return "failed"
            stopped.append(dname)
            if not wait_daemon_stopped(client, nfs_name, peer.hostname):
                return "failed"

        old_cid = get_nfs_container_id(joining, nfs_name=nfs_name)
        if not _daemon_cmd(client, nfs_name, joining.hostname, "restart"):
            return "failed"
        if not wait_new_container(joining, nfs_name, old_cid):
            log.error("[%s] container not back on %s", name, joining.hostname)
            return "failed"

        if not _wait_log_patterns(
            [joining],
            TSM_PRIMARY_SELECTION_FAIL_PRESENT,
            nfs_name,
            log_since,
            tries=60,
            sleep_s=5,
            label=f"primary-selection fail on {joining.hostname}",
        ):
            return "failed"

        return _post_checks(
            client,
            nfs_name,
            export,
            mount,
            nfs_port,
            joining.hostname,
            name,
            [joining],
            coredump_since,
            nfs_version,
            assert_fn=lambda: NfsTsmValidation(
                cluster, client, "enable_TSM", "Tsm_Port", TSM_PORT
            ).assert_tsm_primary_selection_fail_logs(
                nfs_name, joining, since=log_since
            ),
        )
    except Exception as exc:
        log.error("[%s] FAILED: %s", name, exc)
        return "failed"
    finally:
        for dname in stopped:
            _start_nfs_daemon(client, dname)
        safe_cleanup(client, mount, nfs_name, export, nodes)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    return execute_workflows(
        ceph_cluster,
        WORKFLOWS,
        "TSM NEGATIVE WORKFLOW SUMMARY",
        handlers={
            "state_retrieval_failure": run_state_retrieval_failure,
            "primary_selection_failure": run_primary_selection_failure,
        },
        nfs_version=str(config.get("nfs_version", "4.2")),
    )
