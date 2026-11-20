"""Common NFS helpers shared across feature libs/tests."""

import json
import re
import time
from datetime import datetime

from utility.log import Log

log = Log(__name__)


def read_ganesha_conf(nodes, nfs_name=None):
    """
    Cat and parse ganesha.conf on one or more NFS nodes.

    Returns nested dict keyed by hostname -> block -> key (string values),
    or None on failure.
    """
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]

    result = {}
    for node in nodes:
        pattern = f"/var/lib/ceph/*/nfs.{nfs_name}*/etc/ganesha/ganesha.conf"
        if not nfs_name:
            pattern = "/var/lib/ceph/*/nfs.*/etc/ganesha/ganesha.conf"
        out, _ = node.exec_command(
            sudo=True, cmd=f"cat {pattern} 2>/dev/null", check_ec=False
        )
        if not out:
            log.error("Unable to read ganesha.conf on %s", node.hostname)
            return None

        conf = {}
        for block, body in re.findall(
            r"([A-Za-z_][\w]*)\s*\{([^}]*)\}", out, flags=re.DOTALL
        ):
            conf[block] = {
                key: raw.strip().strip("\"'")
                for key, raw in re.findall(r"(\w+)\s*=\s*([^;]+);", body)
            }

        log.info("ganesha.conf on %s: %s", node.hostname, conf)
        result[node.hostname] = conf
    return result


def get_nfs_container_id_from_orch(client, nfs_name, hostname):
    """Return running container_id from ``ceph orch ps`` for nfs on hostname.

    Same source of truth used by TSM / multi-active / conditional-logging.
    """
    if not client or not nfs_name or not hostname:
        return None
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"ceph orch ps --service_name nfs.{nfs_name} --format json",
        check_ec=False,
    )
    try:
        daemons = json.loads(out or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        log.warning("Could not parse orch ps JSON for nfs.%s", nfs_name)
        return None
    if not isinstance(daemons, list):
        return None
    for daemon in daemons:
        if daemon.get("hostname") != hostname:
            continue
        cid = daemon.get("container_id")
        status = str(daemon.get("status_desc", "")).lower()
        if cid and status == "running":
            log.info(
                "NFS container on %s via orch: %s (daemon=%s)",
                hostname,
                cid,
                daemon.get("daemon_name"),
            )
            return cid
    return None


def get_nfs_container_id(node, nfs_name=None, client=None):
    """
    Return the NFS Ganesha container id on a node, or None if not found.

    Args:
        node: NFS host node.
        nfs_name: Optional cluster/service id filter (e.g. cephfs-nfs).
        client: Optional admin client for ``ceph orch ps`` fallback (TSM-style).

    Cephadm orch daemon names use dots (``nfs.cephfs-nfs...``) while the
    podman container name uses dashes
    (``...-nfs-cephfs-nfs-0-0-<host>-...``). Match either separator, then
    fall back to orch ``container_id`` when ``client`` is provided.
    """
    # awk: use explicit [.-] so nfs.NAME and nfs-NAME both hit.
    match = f"nfs[.-]{nfs_name}" if nfs_name else "nfs[.-]"
    out, _ = node.exec_command(
        sudo=True,
        cmd=(
            "podman ps --format '{{.ID}} {{.Names}}' "
            f"| awk '/{match}/ {{print $1; exit}}'"
        ),
        check_ec=False,
    )
    cid = (out or "").strip()
    if cid:
        log.info("NFS container on %s: %s", node.hostname, cid)
        return cid
    if client and nfs_name:
        return get_nfs_container_id_from_orch(client, nfs_name, node.hostname)
    return None


def nodes_running_nfs(nodes, nfs_name, client=None):
    """Return nodes that currently host the NFS Ganesha container (TSM-style)."""
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]
    active = [
        n for n in nodes if get_nfs_container_id(n, nfs_name=nfs_name, client=client)
    ]
    log.info(
        "NFS containers for nfs.%s on: %s",
        nfs_name,
        [n.hostname for n in active] or "<none>",
    )
    return active


def wait_nfs_containers(nodes, nfs_name, client=None, timeout=120, interval=5):
    """Poll until at least one node hosts ``nfs.<nfs_name>`` (post config-set)."""
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]
    deadline = time.time() + timeout
    active = []
    while time.time() < deadline:
        active = nodes_running_nfs(nodes, nfs_name, client=client)
        if active:
            return active
        log.warning(
            "NFS containers not up yet for nfs.%s; retrying in %ss",
            nfs_name,
            interval,
        )
        time.sleep(interval)
    log.error(
        "Timed out (%ss) waiting for nfs.%s containers on %s",
        timeout,
        nfs_name,
        [n.hostname for n in nodes],
    )
    return active


def wait_nfs_service_running(client, nfs_name, timeout=120, interval=5):
    """Poll ``ceph orch ps`` until nfs.<name> has a running container_id."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ps --service_name nfs.{nfs_name} --format json",
            check_ec=False,
        )
        try:
            daemons = json.loads(out or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            daemons = []
        running = [
            d
            for d in (daemons if isinstance(daemons, list) else [])
            if str(d.get("status_desc", "")).lower() == "running"
            and d.get("container_id")
        ]
        if running:
            log.info(
                "nfs.%s running: %s",
                nfs_name,
                [(d.get("hostname"), d.get("container_id")) for d in running],
            )
            return running
        log.warning("nfs.%s not running yet; retrying in %ss", nfs_name, interval)
        time.sleep(interval)
    log.error("Timed out (%ss) waiting for nfs.%s via orch ps", timeout, nfs_name)
    return []


def apply_nfs_cluster_config(client, nfs_name, body, conf_file="nfs_cluster.conf"):
    """Write ``body`` and ``ceph nfs cluster config set`` (replaces whole conf)."""
    conf = client.remote_file(sudo=True, file_name=conf_file, file_mode="w")
    conf.write(body)
    conf.flush()
    out, _ = client.exec_command(sudo=True, cmd=f"cat {conf_file}")
    log.info("NFS cluster conf %s:\n%s", conf_file, (out or "").strip())
    client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster config set {nfs_name} -i {conf_file}"
    )


def podman_nfs_logs(node, nfs_name, since=None, client=None, grep=None):
    """Return ``podman logs`` text for the NFS container on ``node``.

    Optional ``grep`` is an ``grep -Ei`` pattern piped on the remote host
    (useful for NFS4 FULL_DEBUG volume).
    """
    cid = get_nfs_container_id(node, nfs_name=nfs_name, client=client)
    if not cid:
        return ""
    node_since = since.get(node.hostname) if isinstance(since, dict) else since
    since_arg = f' --since "{node_since}"' if node_since else ""
    cmd = f"podman logs{since_arg} {cid} 2>&1"
    if grep:
        cmd = f"{cmd} | grep -Ei '{grep}' || true"
    out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return out or ""


def assert_port_listening(nodes, port, label="port"):
    """Return 0 if TCP/UDP port is listening on all nodes, else 1."""
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]
    port = int(port)
    for node in nodes:
        out, _ = node.exec_command(
            sudo=True,
            cmd=f"ss -H -tulpn sport = :{port}",
            check_ec=False,
        )
        if not (out or "").strip():
            log.error("%s %s is not listening on %s", label, port, node.hostname)
            return 1
        log.info("%s %s listening on %s", label, port, node.hostname)
    return 0


def get_node_time(nodes):
    """Return (log_since, coredump_since) from node(s).

    log_since: {hostname: utc RFC3339 str} for ``podman logs --since``
    coredump_since: {hostname: local datetime} for ``check_coredump_generated``
    """
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]
    log_since, coredump_since = {}, {}
    for node in nodes:
        out, _ = node.exec_command(
            sudo=True,
            cmd="date -u +%Y-%m-%dT%H:%M:%SZ; date +'%Y-%m-%d %H:%M:%S'",
        )
        lines = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
        utc, local = lines[0], lines[1]
        log_since[node.hostname] = utc
        coredump_since[node.hostname] = datetime.strptime(local, "%Y-%m-%d %H:%M:%S")
        log.info("Node time on %s: utc=%s local=%s", node.hostname, utc, local)
    return log_since, coredump_since


def enable_nfs_debug_logs(client, nfs_name, components, conf_file="nfs_debug.conf"):
    """Enable FULL_DEBUG for LOG COMPONENTS key(s) via cluster config set.

    ``components`` may be a string or a list/tuple of component names
    (e.g. ``\"TSM\"`` or ``[\"TSM\", \"STATE\"]``).
    """
    if isinstance(components, str):
        components = [components]
    else:
        components = list(components)
    lines = "\n".join(f"\t{c} = FULL_DEBUG;" for c in components)
    body = f"LOG {{\n    COMPONENTS {{\n{lines}\n    }}\n}}\n"
    apply_nfs_cluster_config(client, nfs_name, body, conf_file=conf_file)
    log.info("Enabled FULL_DEBUG for %s on nfs.%s", components, nfs_name)


def scrape_nfs_logs(
    nodes,
    pattern,
    nfs_name=None,
    since=None,
    require_match=False,
    require_absent=False,
    client=None,
):
    """Fetch podman logs and match ``pattern`` (regex).

    Only scrapes nodes that currently host the NFS container (TSM-style).
    Returns {hostname: matched_lines}. Returns None if no container is found, or
    when ``require_match`` / ``require_absent`` is not satisfied.
    ``since``: str or {hostname: ts} for ``podman logs --since``.
    ``client``: optional admin client for orch ``container_id`` fallback.
    """
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]
    if not isinstance(pattern, str):
        pattern = "|".join(pattern)

    active = nodes_running_nfs(nodes, nfs_name, client=client)
    if not active:
        log.error(
            "No NFS container on any of %s (nfs_name=%s)",
            [n.hostname for n in nodes],
            nfs_name,
        )
        return None

    collected = {}
    for node in active:
        out = podman_nfs_logs(node, nfs_name, since=since, client=client)
        matched = "\n".join(
            ln for ln in out.splitlines() if re.search(pattern, ln, re.I)
        )
        collected[node.hostname] = matched
        log.info(
            "NFS log match on %s (pattern=%r):\n%s",
            node.hostname,
            pattern,
            matched or "<none>",
        )
        if require_match and not matched:
            log.error("%s: pattern %r not found", node.hostname, pattern)
            return None
        if require_absent and matched:
            log.error("%s: forbidden pattern %r found", node.hostname, pattern)
            return None
    return collected
