"""Shared helpers for NFS-Ganesha Prometheus / scalable statistics CephCI tests."""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.nfs_delegation_operations import (
    backup_ganesha_template,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    redeploy_nfs_clusters,
    restore_ganesha_template,
    run_cephadm_shell,
)
from utility.log import Log

log = Log(__name__)

DEFAULT_MONITORING_PORT = 9587
DEFAULT_PROMETHEUS_PORT = 9095
CONF_KEY = "mgr/cephadm/services/nfs/ganesha.conf"
DEFAULT_TEMPLATE_PATH = (
    "/usr/share/ceph/mgr/cephadm/templates/services/nfs/ganesha.conf.j2"
)
MOUNTED_TEMPLATE_PATH = "/var/lib/ceph/ganesha.conf"
_PROM_RUN_ID = "%s_%s" % (os.getpid(), int(time.time() * 1000))
WORK_TEMPLATE_PATH = "/tmp/ganesha_prometheus_%s_work.conf.j2" % _PROM_RUN_ID
BACKUP_TEMPLATE_PATH = "/tmp/ganesha_prometheus_%s_backup.conf.j2" % _PROM_RUN_ID

PROMETHEUS_GANESHA_PARAM_NAMES = {
    "enable_metrics": "Enable_Metrics",
    "enable_dynamic_metrics": "Enable_Dynamic_Metrics",
    "monitoring_port": "Monitoring_Port",
    "enable_FULLV4STATS": "Enable_FULLV4_Stats",
}


def _prometheus_param_aliases(key, canonical):
    aliases = [key, canonical]
    if key == "enable_FULLV4STATS":
        aliases.extend(
            [
                "Enable_FULLV4STATS",
                "enable_FULLV4STATS",
                "Enable_FULLV4_Stats",
            ]
        )
    return tuple(dict.fromkeys(aliases))


PROMETHEUS_CORE_PARAM_ALIASES = {
    key: _prometheus_param_aliases(key, canonical)
    for key, canonical in PROMETHEUS_GANESHA_PARAM_NAMES.items()
}

# Ganesha 9.7 golden exposition (reference cluster 10.0.66.98:9587).
# Core counters: rpcs_*, nfs_requests_total, client_requests_total, mdcache_cache_*.
# Gauges: rpcs_in_flight, clients__confirmed_count, ganesha_build_info.
# Histograms: nfs_latency_ms, nfsv4__op_latency.
# Optional / dynamic: nfs_errors_total, nfs_requests_by_export_total, client_bytes_*,
# nfsv4__op_count, last_client_update, compound__latency, connection_manager__clients.
MANIFEST_V1_COUNTERS = (
    "rpcs_received_total",
    "rpcs_completed_total",
    "nfs_requests_total",
    "client_requests_total",
    "mdcache_cache_hits_total",
    "mdcache_cache_misses_total",
)

MANIFEST_V1_GAUGES = (
    "rpcs_in_flight",
    "clients__confirmed_count",
    "ganesha_build_info",
)

MANIFEST_V1_HISTOGRAMS = (
    "nfs_latency_ms",
    "nfsv4__op_latency",
)

# Present on some builds or after specific workload; warn only.
MANIFEST_V1_OPTIONAL = (
    "nfs_errors_total",
    "errors_total",
    "nfs_requests_by_export_total",
    "nfsv4__op_count",
    "last_client_update",
    "client_bytes_received_total",
    "client_bytes_sent_total",
    "nfs_bytes_received_by_export_total",
    "nfs_bytes_sent_by_export_total",
    "nfs_latency_ms_by_export",
    "compound__latency",
    "connection_manager__clients",
    "clients_confirmed_count",
    "requests_total",
    "latency_ms",
    "v4_op_latency",
    "request_size_bytes",
    "response_size_bytes",
    "bytes_received_total",
    "bytes_sent_total",
)

MANIFEST_METRIC_ALIASES = {
    "nfs_latency_ms": (
        "nfs_latency_ms",
        "latency_ms",
    ),
    "latency_ms": (
        "nfs_latency_ms",
        "latency_ms",
    ),
    "v4_op_latency": (
        "nfsv4__op_latency",
        "nfs_v4_op_latency",
        "v4_op_latency",
    ),
    "nfsv4__op_latency": (
        "nfsv4__op_latency",
        "nfs_v4_op_latency",
        "v4_op_latency",
    ),
    "nfs_requests_total": (
        "nfs_requests_total",
        "requests_total",
    ),
    "requests_total": (
        "nfs_requests_total",
        "requests_total",
    ),
    "client_requests_total": (
        "client_requests_total",
        "nfs_client_requests_total",
    ),
    "nfs_errors_total": (
        "nfs_errors_total",
        "errors_total",
    ),
    "errors_total": (
        "nfs_errors_total",
        "errors_total",
    ),
    "clients__confirmed_count": (
        "clients__confirmed_count",
        "clients_confirmed_count",
    ),
    "clients_confirmed_count": (
        "clients__confirmed_count",
        "clients_confirmed_count",
    ),
    "bytes_received_total": (
        "client_bytes_received_total",
        "nfs_client_bytes_received_total",
        "nfs_bytes_received_by_export_total",
        "bytes_received_by_export_total",
        "nfs_bytes_received_total",
        "bytes_received_total",
    ),
    "bytes_sent_total": (
        "client_bytes_sent_total",
        "nfs_client_bytes_sent_total",
        "nfs_bytes_sent_by_export_total",
        "bytes_sent_by_export_total",
        "nfs_bytes_sent_total",
        "bytes_sent_total",
    ),
    "nfs_bytes_sent_total": (
        "client_bytes_sent_total",
        "nfs_client_bytes_sent_total",
        "nfs_bytes_sent_by_export_total",
        "bytes_sent_by_export_total",
        "nfs_bytes_sent_total",
        "bytes_sent_total",
    ),
    "nfs_bytes_received_total": (
        "client_bytes_received_total",
        "nfs_client_bytes_received_total",
        "nfs_bytes_received_by_export_total",
        "bytes_received_by_export_total",
        "nfs_bytes_received_total",
        "bytes_received_total",
    ),
    "mdcache_cache_hits_total": (
        "mdcache_cache_hits_total",
        "mdcache_cache_hits_by_export_total",
        "nfs_mdcache_hits_total",
    ),
    "mdcache_cache_misses_total": (
        "mdcache_cache_misses_total",
        "mdcache_cache_misses_by_export_total",
        "nfs_mdcache_misses_total",
    ),
    "nfs_requests_by_export_total": (
        "nfs_requests_by_export_total",
        "requests_by_export_total",
    ),
    "nfsv4__op_count": (
        "nfsv4__op_count",
        "nfs_v4_op_count",
        "v4_op_count",
        "nfs4_op_count",
    ),
    "nfs_v4_op_count": (
        "nfsv4__op_count",
        "nfs_v4_op_count",
        "v4_op_count",
        "nfs4_op_count",
    ),
    "rpcs_received_total": (
        "rpcs_received_total",
        "nfs_rpcs_received_total",
    ),
    "nfs_rpcs_received_total": (
        "rpcs_received_total",
        "nfs_rpcs_received_total",
    ),
    "rpcs_completed_total": (
        "rpcs_completed_total",
        "nfs_rpcs_completed_total",
    ),
    "nfs_rpcs_completed_total": (
        "rpcs_completed_total",
        "nfs_rpcs_completed_total",
    ),
    "rpcs_in_flight": (
        "rpcs_in_flight",
        "nfs_rpcs_in_flight",
    ),
    "nfs_rpcs_in_flight": (
        "rpcs_in_flight",
        "nfs_rpcs_in_flight",
    ),
    "clients__lease_expire_count": (
        "clients__lease_expire_count",
        "nfs_clients_lease_expire_count",
        "clients_lease_expire_count",
    ),
    "nfs_clients_lease_expire_count": (
        "clients__lease_expire_count",
        "nfs_clients_lease_expire_count",
        "clients_lease_expire_count",
    ),
}

# Match any exported series in the family (e.g. *_by_export_total).
MANIFEST_METRIC_PREFIX_FAMILIES = {
    "mdcache_cache_hits_total": "mdcache_cache_hits",
    "mdcache_cache_misses_total": "mdcache_cache_misses",
}

LATENCY_HISTOGRAM_PREFIXES = (
    "nfs_latency_ms",
    "latency_ms",
    "nfsv4__op_latency",
    "v4_op_latency",
    "nfs_v4_op_latency",
    "nfs_latency_ms_by_export",
    "compound__latency",
)

V4_OP_COUNT_METRICS = (
    "nfsv4__op_count",
    "v4_op_count",
    "nfs_v4_op_count",
    "nfs4_op_count",
)

CLIENT_REQUEST_METRICS = (
    "client_requests_total",
    "nfs_client_requests_total",
)

CLIENT_BYTES_METRICS = (
    "client_bytes_received_total",
    "client_bytes_sent_total",
    "nfs_client_bytes_received_total",
    "nfs_client_bytes_sent_total",
)

BYTES_SENT_COUNTER_CANDIDATES = (
    "client_bytes_sent_total",
    "nfs_client_bytes_sent_total",
    "nfs_bytes_sent_by_export_total",
    "bytes_sent_by_export_total",
    "nfs_bytes_sent_total",
    "bytes_sent_total",
)

BYTES_RECEIVED_COUNTER_CANDIDATES = (
    "client_bytes_received_total",
    "nfs_client_bytes_received_total",
    "nfs_bytes_received_by_export_total",
    "bytes_received_by_export_total",
    "nfs_bytes_received_total",
    "bytes_received_total",
)

# Ganesha 9.7 byte counters are labeled by NFS operation (read, write, ...).
# Read payload is on *_received_*; write payload is on *_sent_* (server-side naming).
NFS_BYTE_READ_OPERATIONS = ("read",)
NFS_BYTE_WRITE_OPERATIONS = ("write",)
NFS_BYTE_READ_COUNTER = "bytes_received_total"
NFS_BYTE_WRITE_COUNTER = "bytes_sent_total"

MDCACHE_HIT_METRICS = (
    "mdcache_cache_hits_total",
    "nfs_mdcache_hits_total",
)

MDCACHE_MISS_METRICS = (
    "mdcache_cache_misses_total",
    "nfs_mdcache_misses_total",
)

PROMETHEUS_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def get_installer(ceph_cluster):
    return ceph_cluster.get_nodes(role="installer")[0]


def _install_ceph_common_if_needed(node):
    out, _ = node.exec_command(
        sudo=True,
        cmd="command -v ceph && echo present || echo absent",
        check_ec=False,
    )
    if "present" in (out or ""):
        return
    for install_cmd in (
        "dnf install -y ceph-common --nogpgcheck",
        "yum install -y ceph-common --nogpgcheck",
    ):
        _, rc = node.exec_command(
            sudo=True, cmd=install_cmd, check_ec=False, timeout=600
        )
        if rc == 0:
            log.info("Installed ceph-common on %s", node.hostname)
            return
    raise OperationFailedError("Failed to install ceph-common on %s" % node.hostname)


def prepare_cluster_ceph_access(ceph_cluster, installer, clients=None, nfs_nodes=None):
    """Install ceph-common and deploy ceph.conf + admin keyring on NFS and client nodes."""
    nfs_nodes = nfs_nodes or ceph_cluster.get_nodes(role="nfs")
    clients = clients or ceph_cluster.get_nodes(role="client")
    nodes = []
    seen = set()
    for node in list(nfs_nodes) + list(clients):
        if node.hostname in seen:
            continue
        seen.add(node.hostname)
        nodes.append(node)
    for node in nodes:
        _install_ceph_common_if_needed(node)
    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nodes)
    log.info(
        "ceph-common with admin keyring configured on: %s",
        ", ".join(sorted(seen)),
    )


def _format_ganesha_param(key, value):
    if isinstance(value, bool):
        value = "true" if value else "false"
    name = PROMETHEUS_GANESHA_PARAM_NAMES.get(key, key)
    return "%s = %s;" % (name, value)


def _prometheus_core_params_from_config(config):
    monitoring_port = int(config.get("monitoring_port", DEFAULT_MONITORING_PORT))
    return {
        "enable_metrics": bool(config.get("enable_metrics", True)),
        "enable_dynamic_metrics": bool(config.get("enable_dynamic_metrics", True)),
        "monitoring_port": monitoring_port,
        "enable_FULLV4STATS": bool(config.get("enable_FULLV4STATS", True)),
    }


def _patch_ganesha_work_template(cmd_host, config):
    """Insert Ganesha NFS_CORE_PARAM prometheus settings into the cephadm J2 template."""
    params = _prometheus_core_params_from_config(config)
    port = params["monitoring_port"]
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "sed -i -E "
            "'s/^[[:space:]]*Monitoring_Port[[:space:]]*=.*/"
            "        Monitoring_Port = %s;/' %s" % (port, WORK_TEMPLATE_PATH)
        ),
        check_ec=False,
    )
    for key in ("enable_metrics", "enable_dynamic_metrics", "enable_FULLV4STATS"):
        line = _format_ganesha_param(key, params[key])
        alias_pattern = "|".join(
            re.escape(a) for a in PROMETHEUS_CORE_PARAM_ALIASES[key]
        )
        escaped_line = line.replace("/", "\\/")
        cmd_host.exec_command(
            sudo=True,
            cmd=(
                "sed -i -E "
                "'s/^[[:space:]]*(%s)[[:space:]]*=[[:space:]]*[^;]+;/%s/' %s"
                % (alias_pattern, escaped_line, WORK_TEMPLATE_PATH)
            ),
            check_ec=False,
        )
        out, _ = cmd_host.exec_command(
            sudo=True,
            cmd="grep -Ei '(%s)[[:space:]]*=' %s" % (alias_pattern, WORK_TEMPLATE_PATH),
            check_ec=False,
        )
        if not (out or "").strip():
            cmd_host.exec_command(
                sudo=True,
                cmd=(
                    "sed -i '/allow_set_io_flusher_fail[[:space:]]*=/a\\"
                    "        %s' %s" % (escaped_line, WORK_TEMPLATE_PATH)
                ),
            )
        out, _ = cmd_host.exec_command(
            sudo=True,
            cmd="grep -F %r %s" % (line, WORK_TEMPLATE_PATH),
            check_ec=False,
        )
        if line not in (out or ""):
            raise OperationFailedError("Failed to set %s in ganesha template" % line)
    snippet, _ = cmd_host.exec_command(
        sudo=True,
        cmd="sed -n '/NFS_CORE_PARAM/,/^}/p' %s | head -20" % WORK_TEMPLATE_PATH,
        check_ec=False,
    )
    log.info(
        "Patched ganesha template NFS_CORE_PARAM block:\n%s",
        (snippet or "").strip(),
    )


def set_prometheus_ganesha_core_params(cmd_host, config):
    """Patch NFS_CORE_PARAM prometheus settings in the cephadm ganesha template."""
    run_cephadm_shell(
        cmd_host,
        "ceph config-key get %s > %s" % (CONF_KEY, WORK_TEMPLATE_PATH),
        check_ec=False,
    )
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "test -s %s || cephadm shell -- cat %s > %s"
            % (WORK_TEMPLATE_PATH, DEFAULT_TEMPLATE_PATH, WORK_TEMPLATE_PATH)
        ),
    )
    _patch_ganesha_work_template(cmd_host, config)
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "cephadm shell --mount %s:%s -- ceph config-key set %s -i %s"
            % (
                WORK_TEMPLATE_PATH,
                MOUNTED_TEMPLATE_PATH,
                CONF_KEY,
                MOUNTED_TEMPLATE_PATH,
            )
        ),
    )


def _expect_prometheus_param(conf, label, key, value):
    name = PROMETHEUS_GANESHA_PARAM_NAMES[key]
    if key == "monitoring_port":
        expected = "%s = %s" % (name, value)
    else:
        expected = _format_ganesha_param(key, value).rstrip(";")
    if expected not in (conf or "") and not re.search(
        r"(?i)%s\s*=\s*%s"
        % (
            re.escape(name),
            value if key == "monitoring_port" else ("true" if value else "false"),
        ),
        conf or "",
    ):
        raise OperationFailedError("%s missing %s in %s" % (label, expected, name))


def verify_prometheus_params_on_nfs_nodes(
    cephadm, nfs_nodes, nfs_name, config, timeout_seconds=120, poll_interval=5
):
    """Poll orch ps and verify prometheus NFS_CORE_PARAM in container ganesha.conf."""
    params = _prometheus_core_params_from_config(config)
    deadline = time.time() + int(timeout_seconds)
    last_error = None
    while time.time() < deadline:
        try:
            raw = cephadm.orch.ps(service_name="nfs.%s" % nfs_name, format="json")
            daemons = json.loads(raw) if raw else []
            if not daemons:
                raise OperationFailedError(
                    "No nfs daemons found in orch ps for nfs.%s" % nfs_name
                )
            for daemon in daemons:
                hostname = (daemon.get("hostname") or "").split(".")[0]
                nfs_node = next(
                    (
                        node
                        for node in nfs_nodes
                        if node.hostname.split(".")[0] == hostname
                        or hostname in node.hostname
                    ),
                    None,
                )
                if not nfs_node:
                    raise OperationFailedError(
                        "No nfs node matched orch daemon %s" % daemon.get("hostname")
                    )
                container_id = daemon.get("container_id")
                if not container_id:
                    raise OperationFailedError(
                        "NFS daemon on %s has no container_id (status=%s)"
                        % (nfs_node.hostname, daemon.get("status_desc"))
                    )
                conf, _ = nfs_node.exec_command(
                    sudo=True,
                    cmd="podman exec %s cat /etc/ganesha/ganesha.conf" % container_id,
                )
                label = "%s container %s" % (nfs_node.hostname, container_id)
                for key, value in params.items():
                    _expect_prometheus_param(conf, label, key, value)
                log.info(
                    "Verified prometheus NFS_CORE_PARAM on %s (container %s)",
                    nfs_node.hostname,
                    container_id,
                )
            return
        except OperationFailedError as err:
            last_error = err
            log.info(
                "Prometheus ganesha.conf verify not ready, retrying in %ss: %s",
                poll_interval,
                err,
            )
            time.sleep(int(poll_interval))
    raise last_error or OperationFailedError(
        "Timed out after %ss verifying prometheus params on nfs nodes" % timeout_seconds
    )


def apply_prometheus_ganesha_template(installer, nfs_node, nfs_name, config):
    """Backup, patch ganesha template on NFS node, redeploy via installer."""
    redeploy_wait = int(config.get("redeploy_wait", 10))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    verify_timeout = int(config.get("prometheus_verify_timeout", service_wait_timeout))
    verify_poll = int(config.get("prometheus_verify_poll_seconds", 5))
    log.info(
        "Running cephadm shell / config-key / template steps on NFS node %s",
        nfs_node.hostname,
    )
    cephadm = CephAdm(installer).ceph
    backup_exists = backup_ganesha_template(nfs_node, BACKUP_TEMPLATE_PATH)
    set_prometheus_ganesha_core_params(nfs_node, config)
    log.info(
        "Updated ganesha NFS_CORE_PARAM prometheus settings: %s",
        _prometheus_core_params_from_config(config),
    )
    redeploy_nfs_clusters(
        cephadm, [nfs_name], installer, redeploy_wait, service_wait_timeout
    )
    verify_prometheus_params_on_nfs_nodes(
        cephadm,
        nfs_nodes=[nfs_node],
        nfs_name=nfs_name,
        config=config,
        timeout_seconds=verify_timeout,
        poll_interval=verify_poll,
    )
    return backup_exists


def update_prometheus_ganesha_params(installer, nfs_node, nfs_name, config):
    """Patch ganesha template and redeploy without taking a new backup."""
    redeploy_wait = int(config.get("redeploy_wait", 10))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    verify_timeout = int(config.get("prometheus_verify_timeout", service_wait_timeout))
    verify_poll = int(config.get("prometheus_verify_poll_seconds", 5))
    cephadm = CephAdm(installer).ceph
    set_prometheus_ganesha_core_params(nfs_node, config)
    log.info(
        "Updated ganesha NFS_CORE_PARAM prometheus settings: %s",
        _prometheus_core_params_from_config(config),
    )
    redeploy_nfs_clusters(
        cephadm, [nfs_name], installer, redeploy_wait, service_wait_timeout
    )
    verify_prometheus_params_on_nfs_nodes(
        cephadm,
        nfs_nodes=[nfs_node],
        nfs_name=nfs_name,
        config=config,
        timeout_seconds=verify_timeout,
        poll_interval=verify_poll,
    )


def restore_prometheus_ganesha_template(
    installer, nfs_node, nfs_name, backup_exists, config
):
    """Restore ganesha template and redeploy NFS (delegation-style cleanup)."""
    if not config.get("restore_ganesha_template_on_exit", True):
        return

    redeploy_wait = int(config.get("redeploy_wait", 10))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    if config.get("reset_ganesha_template_on_exit", False):
        run_cephadm_shell(nfs_node, "ceph config-key rm %s" % CONF_KEY, check_ec=False)
    else:
        restore_ganesha_template(nfs_node, backup_exists, BACKUP_TEMPLATE_PATH)
    cephadm = CephAdm(installer).ceph
    redeploy_nfs_clusters(
        cephadm, [nfs_name], installer, redeploy_wait, service_wait_timeout
    )


def _load_ganesha_work_template(cmd_host):
    run_cephadm_shell(
        cmd_host,
        "ceph config-key get %s > %s" % (CONF_KEY, WORK_TEMPLATE_PATH),
        check_ec=False,
    )
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "test -s %s || cephadm shell -- cat %s > %s"
            % (WORK_TEMPLATE_PATH, DEFAULT_TEMPLATE_PATH, WORK_TEMPLATE_PATH)
        ),
    )


def _commit_ganesha_work_template(cmd_host):
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "cephadm shell --mount %s:%s -- ceph config-key set %s -i %s"
            % (
                WORK_TEMPLATE_PATH,
                MOUNTED_TEMPLATE_PATH,
                CONF_KEY,
                MOUNTED_TEMPLATE_PATH,
            )
        ),
    )


def inject_ganesha_template_line(cmd_host, line, after_pattern="Enable_Metrics"):
    escaped = line.replace("/", "\\/")
    cmd_host.exec_command(
        sudo=True,
        cmd="sed -i '/%s/a\\        %s' %s"
        % (after_pattern, escaped, WORK_TEMPLATE_PATH),
    )


def remove_ganesha_template_line(cmd_host, line):
    escaped = line.replace("/", "\\/").replace("&", "\\&")
    cmd_host.exec_command(
        sudo=True,
        cmd="sed -i '/%s/d' %s" % (escaped, WORK_TEMPLATE_PATH),
        check_ec=False,
    )


def restore_f01_good_ganesha_config(
    installer, nfs_node, nfs_name, config, typo_line=None
):
    """Remove injected typo (if any), re-apply valid prometheus params, redeploy."""
    _load_ganesha_work_template(nfs_node)
    if typo_line:
        remove_ganesha_template_line(nfs_node, typo_line)
    _patch_ganesha_work_template(nfs_node, config)
    _commit_ganesha_work_template(nfs_node)
    trigger_nfs_redeploy(installer, nfs_name, config, wait_for_running=True)
    log.info("F-01: restored valid ganesha template after typo injection")


def trigger_nfs_redeploy(installer, nfs_name, config, wait_for_running=True):
    from tests.nfs.nfs_delegation_operations import (
        verify_nfs_ganesha_service,
        wait_for_nfs_cluster_daemons_running,
    )

    cephadm = CephAdm(installer).ceph
    redeploy_wait = int(config.get("redeploy_wait", 10))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    cephadm.orch.redeploy("nfs.%s" % nfs_name)
    time.sleep(redeploy_wait)
    if wait_for_running:
        verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)
        wait_for_nfs_cluster_daemons_running(
            cephadm, [nfs_name], timeout_seconds=service_wait_timeout
        )


def get_nfs_daemon_info(cephadm, nfs_name):
    raw = cephadm.orch.ps(service_name="nfs.%s" % nfs_name, format="json")
    return json.loads(raw) if raw else []


def read_nfs_container_logs(nfs_node, container_id=None, tail_lines=300):
    if container_id:
        cmd = "podman logs --tail %s %s 2>&1" % (tail_lines, container_id)
    else:
        cmd = (
            "cid=$(podman ps -a --format '{{.ID}} {{.Names}}' "
            "| awk '/nfs|ganesha/ {print $1; exit}'); "
            '[ -n "$cid" ] && podman logs --tail %s "$cid" 2>&1'
        ) % tail_lines
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return out or ""


def apply_f01_invalid_monitoring_port(
    installer, nfs_node, nfs_name, base_config, invalid_port
):
    bad_config = dict(base_config)
    bad_config["monitoring_port"] = int(invalid_port)
    set_prometheus_ganesha_core_params(nfs_node, bad_config)
    trigger_nfs_redeploy(installer, nfs_name, base_config, wait_for_running=False)
    settle = int(base_config.get("f01_redeploy_settle_seconds", 30))
    time.sleep(settle)
    cephadm = CephAdm(installer).ceph
    daemons = get_nfs_daemon_info(cephadm, nfs_name)
    logs = read_nfs_container_logs(nfs_node, tail_lines=400)
    log.info(
        "F-01A redeploy settle=%ss daemon statuses: %s",
        settle,
        [d.get("status_desc") for d in daemons],
    )
    return logs, daemons


def apply_f01_config_typo_line(installer, nfs_node, nfs_name, base_config, typo_line):
    _load_ganesha_work_template(nfs_node)
    _patch_ganesha_work_template(nfs_node, base_config)
    inject_ganesha_template_line(nfs_node, typo_line)
    _commit_ganesha_work_template(nfs_node)
    trigger_nfs_redeploy(installer, nfs_name, base_config, wait_for_running=False)
    settle = int(base_config.get("f01_redeploy_settle_seconds", 30))
    time.sleep(settle)
    cephadm = CephAdm(installer).ceph
    daemons = get_nfs_daemon_info(cephadm, nfs_name)
    logs = read_nfs_container_logs(nfs_node, tail_lines=400)
    log.info(
        "F-01B redeploy settle=%ss daemon statuses: %s",
        settle,
        [d.get("status_desc") for d in daemons],
    )
    return logs, daemons


def verify_f01_invalid_port_outcome(ctx, logs, daemons, invalid_port):
    """Accept fatal config rejection, or running NFS without metrics on bad port."""
    log_text = (logs or "").lower()
    refuses = any(
        marker in log_text
        for marker in (
            "out of range",
            "fatal errors",
            "fatal:",
            "server exiting",
            "error setting parameters",
            "errors processing block",
        )
    )
    running = any((d.get("status_desc") or "").lower() == "running" for d in daemons)

    if refuses:
        log.info(
            "F-01A: Ganesha rejected invalid monitoring_port=%s (log error present)",
            invalid_port,
        )
        return

    if running:
        code = scrape_prometheus_metrics_http_code(
            ctx.client, ctx.nfs_ip, invalid_port, path="/metrics"
        )
        if code == "200":
            raise OperationFailedError(
                "F-01A: invalid monitoring_port=%s unexpectedly returned HTTP 200"
                % invalid_port
            )
        perform_s06_minimal_workload(ctx.client, ctx.nfs_mount)
        log.info(
            "F-01A: Ganesha running with invalid port=%s; NFS OK, metrics not on bad port",
            invalid_port,
        )
        return

    log.info(
        "F-01A: NFS daemon not running after invalid monitoring_port=%s (acceptable)",
        invalid_port,
    )


def verify_f01_typo_outcome(ctx, logs, daemons=None):
    """Typo line prevents clean start: expect parse error in log and/or NFS down."""
    log_text = (logs or "").lower()
    error_markers = (
        "syntax error",
        "fatal errors",
        "fatal:",
        "server exiting",
        "error setting parameters",
        "errors processing block",
    )
    has_error = any(marker in log_text for marker in error_markers)
    running = any(
        (d.get("status_desc") or "").lower() == "running" for d in (daemons or [])
    )

    if has_error:
        log.info("F-01B: ganesha log reports config error after typo line")
        if running:
            log.warning("F-01B: NFS daemon running despite config error in log")
            perform_s06_minimal_workload(ctx.client, ctx.nfs_mount)
        else:
            log.info("F-01B: NFS not running after typo (expected)")
        return

    if running:
        perform_s06_minimal_workload(ctx.client, ctx.nfs_mount)
        log.info("F-01B: typo line tolerated on this build; NFS operational")
        return

    log.info(
        "F-01B: NFS not running after typo line (acceptable; no error text in log tail)"
    )


def get_monitoring_port(nfs_node, nfs_name, config):
    configured = config.get("monitoring_port")
    if configured is not None:
        return int(configured)

    out, _ = nfs_node.exec_command(
        sudo=True,
        cmd="ceph orch ls nfs --export -f json",
        check_ec=False,
    )
    try:
        services = json.loads(str(out).strip()) if out else []
    except json.JSONDecodeError:
        services = []

    for svc in services:
        if svc.get("service_id") == nfs_name:
            port = svc.get("spec", {}).get("monitoring_port")
            if port is not None:
                return int(port)

    return DEFAULT_MONITORING_PORT


def scrape_prometheus_metrics(node, host, port, path="/metrics"):
    cmd = f"curl -sf http://{host}:{port}{path}"
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    body = (out or "").strip()
    if not body:
        raise OperationFailedError(
            f"Empty response from http://{host}:{port}{path}: {err}"
        )
    return body


def scrape_prometheus_metrics_http_code(node, host, port, path="/metrics"):
    cmd = f"curl -s -o /dev/null -w '%{{http_code}}' " f"http://{host}:{port}{path}"
    out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return (out or "").strip()


def _node_ip_for_hostname(ceph_cluster, hostname):
    short = (hostname or "").split(".")[0]
    for node in ceph_cluster.get_nodes():
        node_short = node.hostname.split(".")[0]
        if node_short == short or short in node.hostname:
            return node.ip_address
    if hostname and re.match(r"^\d+\.\d+\.\d+\.\d+$", hostname):
        return hostname
    raise OperationFailedError("No cluster node IP for hostname %s" % hostname)


def _pick_prometheus_daemon_port(daemon, default=DEFAULT_PROMETHEUS_PORT):
    ports = daemon.get("ports") or []
    for port in ports:
        if int(port) == default:
            return int(port)
    if ports:
        return int(ports[0])
    return default


def _prometheus_daemon_running(daemon):
    status_desc = (daemon.get("status_desc") or "").lower()
    if status_desc == "running":
        return True
    return daemon.get("status") == 1


def get_prometheus_api_url(ceph_cluster, installer, config=None):
    """Resolve cluster Prometheus base URL from ``ceph orch ps`` (e.g. http://10.0.66.24:9095)."""
    config = config or {}
    override = config.get("prometheus_api_url")
    if override:
        return str(override).rstrip("/")

    default_port = int(config.get("prometheus_port", DEFAULT_PROMETHEUS_PORT))
    cephadm = CephAdm(installer).ceph
    raw = cephadm.orch.ps(format="json")
    daemons = json.loads(raw) if raw else []
    prom_daemons = [
        daemon
        for daemon in daemons
        if "prometheus" in (daemon.get("service_name") or "").lower()
        or (daemon.get("daemon_type") or "").lower() == "prometheus"
    ]
    if not prom_daemons:
        raise OperationFailedError("No prometheus daemon found in ceph orch ps output")

    for daemon in prom_daemons:
        if not _prometheus_daemon_running(daemon):
            continue
        hostname = daemon.get("hostname")
        port = _pick_prometheus_daemon_port(daemon, default_port)
        host_ip = _node_ip_for_hostname(ceph_cluster, hostname)
        api_url = "http://%s:%s" % (host_ip, port)
        log.info(
            "Discovered Prometheus API at %s (orch ps daemon on %s, port %s)",
            api_url,
            hostname,
            port,
        )
        return api_url

    raise OperationFailedError(
        "Prometheus daemon found in orch ps but none are running"
    )


def fetch_prometheus_targets(scrape_node, api_base_url):
    url = "%s/api/v1/targets" % api_base_url.rstrip("/")
    out, err = scrape_node.exec_command(
        sudo=True, cmd="curl -sf '%s'" % url, check_ec=False
    )
    body = (out or "").strip()
    if not body:
        raise OperationFailedError(
            "Empty response from Prometheus targets API %s: %s" % (url, err)
        )
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OperationFailedError(
            "Invalid JSON from Prometheus targets API %s: %s" % (url, exc)
        )
    if payload.get("status") != "success":
        raise OperationFailedError(
            "Prometheus targets API status=%s url=%s" % (payload.get("status"), url)
        )
    return payload.get("data", {}).get("activeTargets", [])


def _nfs_ganesha_target_hosts(nfs_node, nfs_ip):
    hosts = []
    for value in (nfs_ip, nfs_node.hostname, nfs_node.hostname.split(".")[0]):
        if value and value not in hosts:
            hosts.append(value)
    return hosts


def _nfs_ganesha_target_matches(target, nfs_hosts, monitoring_port, nfs_name=None):
    scrape_url = str(target.get("scrapeUrl") or "").lower()
    port_token = ":%s" % monitoring_port
    if port_token not in scrape_url:
        return False

    labels_blob = json.dumps(target.get("labels") or {})
    discovered_blob = json.dumps(target.get("discoveredLabels") or {})
    combined = ("%s%s%s" % (scrape_url, labels_blob, discovered_blob)).lower()

    host_match = any(str(host).lower() in combined for host in nfs_hosts)
    if not host_match:
        return False

    if nfs_name and nfs_name.lower() not in combined:
        if "nfs" not in combined and "ganesha" not in combined:
            log.info(
                "NFS target matched on host/port but without nfs/ganesha labels: %s",
                target.get("scrapeUrl"),
            )
    return True


def verify_prometheus_nfs_scrape_target(
    scrape_node,
    ceph_cluster,
    installer,
    nfs_node,
    nfs_ip,
    monitoring_port,
    nfs_name=None,
    config=None,
):
    """Verify cluster Prometheus scrapes the NFS-Ganesha metrics endpoint."""
    config = config or {}
    api_url = get_prometheus_api_url(ceph_cluster, installer, config)
    targets_url = "%s/api/v1/targets" % api_url
    nfs_hosts = _nfs_ganesha_target_hosts(nfs_node, nfs_ip)
    timeout = int(config.get("prometheus_targets_timeout", 120))
    poll_interval = int(config.get("prometheus_targets_poll_interval", 10))
    deadline = time.time() + timeout
    last_targets = []

    log.info(
        "S-03: checking Prometheus targets at %s for NFS metrics on %s:%s",
        targets_url,
        nfs_ip,
        monitoring_port,
    )

    while time.time() < deadline:
        try:
            last_targets = fetch_prometheus_targets(scrape_node, api_url)
            for target in last_targets:
                if not _nfs_ganesha_target_matches(
                    target, nfs_hosts, monitoring_port, nfs_name=nfs_name
                ):
                    continue
                health = (target.get("health") or "").lower()
                scrape_url = target.get("scrapeUrl")
                if health == "up":
                    log.info(
                        "Prometheus NFS-Ganesha scrape target is UP: %s",
                        scrape_url,
                    )
                    return target
                log.info(
                    "Prometheus NFS target found but health=%s: %s",
                    health,
                    scrape_url,
                )
        except OperationFailedError as err:
            log.info(
                "Prometheus targets API not ready, retrying in %ss: %s",
                poll_interval,
                err,
            )
        time.sleep(poll_interval)

    sample = [
        "%s health=%s" % (t.get("scrapeUrl"), t.get("health"))
        for t in last_targets[:10]
    ]
    raise OperationFailedError(
        "No healthy Prometheus scrape target for NFS-Ganesha at %s:%s "
        "(api=%s, active_targets_sample=%s)"
        % (nfs_ip, monitoring_port, targets_url, sample)
    )


def get_prometheus_scrape_source_ip(ceph_cluster, installer, config, fallback_node):
    api_url = get_prometheus_api_url(ceph_cluster, installer, config)
    match = re.match(r"http://([^:/]+)", api_url)
    if match:
        return match.group(1)
    return fallback_node.ip_address


def get_prometheus_nfs_target_health(
    scrape_node,
    ceph_cluster,
    installer,
    nfs_node,
    nfs_ip,
    monitoring_port,
    nfs_name=None,
    config=None,
):
    config = config or {}
    api_url = get_prometheus_api_url(ceph_cluster, installer, config)
    nfs_hosts = _nfs_ganesha_target_hosts(nfs_node, nfs_ip)
    for target in fetch_prometheus_targets(scrape_node, api_url):
        if _nfs_ganesha_target_matches(
            target, nfs_hosts, monitoring_port, nfs_name=nfs_name
        ):
            return (target.get("health") or "").lower(), target.get("scrapeUrl")
    return None, None


def wait_prometheus_nfs_target_health(
    scrape_node,
    ceph_cluster,
    installer,
    nfs_node,
    nfs_ip,
    monitoring_port,
    expected_health,
    nfs_name=None,
    config=None,
):
    config = config or {}
    timeout = int(config.get("prometheus_targets_timeout", 120))
    poll_interval = int(config.get("prometheus_targets_poll_interval", 10))
    deadline = time.time() + timeout
    expected_health = expected_health.lower()

    while time.time() < deadline:
        health, scrape_url = get_prometheus_nfs_target_health(
            scrape_node,
            ceph_cluster,
            installer,
            nfs_node,
            nfs_ip,
            monitoring_port,
            nfs_name=nfs_name,
            config=config,
        )
        if health == expected_health:
            log.info(
                "Prometheus NFS target health=%s as expected: %s",
                health,
                scrape_url,
            )
            return
        if expected_health == "down" and health == "down":
            return
        log.info(
            "Waiting for Prometheus NFS target health=%s (current=%s url=%s)",
            expected_health,
            health,
            scrape_url,
        )
        time.sleep(poll_interval)

    raise OperationFailedError(
        "Prometheus NFS target did not reach health=%s within %ss"
        % (expected_health, timeout)
    )


def _metric_name_variants(metric_name):
    variants = [metric_name]
    if metric_name.startswith("nfs_"):
        variants.append(metric_name[4:])
    else:
        variants.append("nfs_%s" % metric_name)

    if "mdcache" in metric_name:
        for prefix in ("nfs_mdcache_", "mdcache_", "mdcache_cache_"):
            if metric_name.startswith(prefix):
                suffix = metric_name[len(prefix) :]
                variants.extend(
                    [
                        "mdcache_cache_%s" % suffix,
                        "nfs_mdcache_%s" % suffix,
                        "mdcache_%s" % suffix,
                    ]
                )
                break
    return list(dict.fromkeys(variants))


def _metric_exact_name_present(metrics_text, metric_name):
    pattern = re.compile(r"^%s(?:\{|\s)" % re.escape(metric_name))
    for line in metrics_text.splitlines():
        if line and not line.startswith("#") and pattern.match(line.strip()):
            return True
    return False


def _sum_counter_for_exact_name(metrics_text, metric_name):
    total = 0
    pattern = re.compile(rf"^{re.escape(metric_name)}(?:{{.*?}})?\s+([0-9.eE+-]+)")
    for line in metrics_text.splitlines():
        match = pattern.match(line.strip())
        if match:
            total += int(float(match.group(1)))
    return total


def metric_available(names, metric_name):
    candidates = list(_metric_name_variants(metric_name))
    candidates.extend(MANIFEST_METRIC_ALIASES.get(metric_name, ()))
    for variant in dict.fromkeys(candidates):
        if variant in names:
            return True
        if any(n.startswith("%s_" % variant) for n in names):
            return True
    prefix = MANIFEST_METRIC_PREFIX_FAMILIES.get(metric_name)
    if prefix and any(n.startswith(prefix) for n in names):
        return True
    return False


def verify_manifest_v1(metrics_text):
    names = parse_prometheus_metrics(metrics_text)
    missing = []
    optional_missing = []
    for metric in MANIFEST_V1_COUNTERS + MANIFEST_V1_GAUGES + MANIFEST_V1_HISTOGRAMS:
        if metric_available(names, metric):
            continue
        if metric in MANIFEST_V1_OPTIONAL:
            optional_missing.append(metric)
        else:
            missing.append(metric)
    if optional_missing:
        log.warning("Manifest v1 optional metrics not present: %s", optional_missing)
    if "mdcache_cache_hits_total" in missing and metric_available(
        names, "mdcache_cache_misses_total"
    ):
        log.warning("mdcache_cache_hits_total not exported yet; misses family present")
        missing.remove("mdcache_cache_hits_total")
    if missing:
        mdcache_names = sorted(n for n in names if "mdcache" in n)
        log.error(
            "Manifest v1 scrape has mdcache families: %s",
            mdcache_names[:30] or "(none)",
        )
        raise OperationFailedError("Manifest v1 metrics missing: %s" % missing)
    log.info(
        "Manifest v1 core metrics present (%s optional absent)",
        len(optional_missing),
    )


def parse_prometheus_metrics(metrics_text):
    names = set()
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        names.add(line.split("{", 1)[0].split(" ", 1)[0].strip())
    return names


def parse_metric_samples(metrics_text):
    samples = []
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            name, rest = line.split("{", 1)
            label_part, value = rest.rsplit("}", 1)
            labels = {}
            for item in label_part.split(","):
                if "=" in item:
                    key, val = item.split("=", 1)
                    labels[key.strip()] = val.strip().strip('"')
            samples.append((name.strip(), labels, float(value.strip())))
        else:
            parts = line.split()
            if len(parts) >= 2:
                samples.append((parts[0], {}, float(parts[1])))
    return samples


def get_counter_total(metrics_text, metric_name):
    candidates = list(_metric_name_variants(metric_name))
    candidates.extend(MANIFEST_METRIC_ALIASES.get(metric_name, ()))
    totals = []
    for variant in dict.fromkeys(candidates):
        if _metric_exact_name_present(metrics_text, variant):
            totals.append(_sum_counter_for_exact_name(metrics_text, variant))
    return max(totals) if totals else 0


def _bytes_counter_candidates(metric_name):
    if "sent" in metric_name:
        return BYTES_SENT_COUNTER_CANDIDATES
    if "received" in metric_name:
        return BYTES_RECEIVED_COUNTER_CANDIDATES
    candidates = list(_metric_name_variants(metric_name))
    candidates.extend(MANIFEST_METRIC_ALIASES.get(metric_name, ()))
    return tuple(dict.fromkeys(candidates))


def _metric_family_present(metrics_text, metric_name, operation_labels=None):
    for candidate in _bytes_counter_candidates(metric_name):
        if _labeled_counter_series_count(metrics_text, candidate, operation_labels) > 0:
            return True
    return False


def _labeled_counter_series_count(metrics_text, metric_name, operation_labels=None):
    operations = {op.lower() for op in operation_labels} if operation_labels else None
    count = 0
    for name, labels, _ in parse_metric_samples(metrics_text):
        if name != metric_name:
            continue
        if operations and labels.get("operation", "").lower() not in operations:
            continue
        count += 1
    return count


def get_labeled_counter_total(metrics_text, metric_name, operation_labels=None):
    """Sum a counter family, optionally filtering on the operation label."""
    operations = {op.lower() for op in operation_labels} if operation_labels else None
    for candidate in _bytes_counter_candidates(metric_name):
        total = 0
        series_count = 0
        for name, labels, value in parse_metric_samples(metrics_text):
            if name != candidate:
                continue
            if operations and labels.get("operation", "").lower() not in operations:
                continue
            total += value
            series_count += 1
        if series_count > 0:
            return total, candidate
    return 0, None


def bytes_counter_delta(
    metrics_before, metrics_after, metric_name, operation_labels=None
):
    """Return delta using operation-specific series first, then all labels."""
    strategies = []
    if operation_labels:
        strategies.append(operation_labels)
    strategies.append(None)

    last_resolved = metric_name
    for ops in strategies:
        before, resolved = get_labeled_counter_total(metrics_before, metric_name, ops)
        after, resolved_after = get_labeled_counter_total(
            metrics_after, metric_name, ops
        )
        if not resolved and not resolved_after:
            continue
        resolved = resolved or resolved_after
        last_resolved = resolved
        delta = after - before
        label_suffix = ""
        if ops:
            label_suffix = "{operation=%s}" % "|".join(ops)
        full_name = "%s%s" % (resolved, label_suffix)
        if delta > 0:
            return delta, full_name
        if ops is None:
            return delta, full_name
    return 0, last_resolved


def get_histogram_prefix_variants(metric_prefix):
    variants = [metric_prefix]
    if metric_prefix.startswith("nfs_"):
        variants.append(metric_prefix[4:])
    else:
        variants.append("nfs_%s" % metric_prefix)
    return list(dict.fromkeys(variants))


def get_histogram_count_sum(metrics_text, metric_prefix):
    for prefix in get_histogram_prefix_variants(metric_prefix):
        count = 0
        total_sum = 0.0
        for name, _, value in parse_metric_samples(metrics_text):
            if name == "%s_count" % prefix:
                count += int(value)
            elif name == "%s_sum" % prefix:
                total_sum += value
        if count > 0 or any(
            n.startswith("%s_" % prefix) for n in parse_prometheus_metrics(metrics_text)
        ):
            return count, total_sum
    return 0, 0.0


def assert_counter_increased(metrics_before, metrics_after, *metric_names):
    for metric_name in metric_names:
        before = get_counter_total(metrics_before, metric_name)
        after = get_counter_total(metrics_after, metric_name)
        if after > before:
            log.info(
                "Counter %s increased: %s -> %s",
                metric_name,
                before,
                after,
            )
            return
    raise OperationFailedError(
        "Counters did not increase (tried: %s)" % ", ".join(metric_names)
    )


def prometheus_query_instant(scrape_node, api_base_url, promql):
    url = "%s/api/v1/query?query=%s" % (api_base_url.rstrip("/"), quote(promql))
    out, err = scrape_node.exec_command(
        sudo=True, cmd="curl -sf '%s'" % url, check_ec=False
    )
    body = (out or "").strip()
    if not body:
        raise OperationFailedError(
            "Empty response from Prometheus query API %s: %s" % (url, err)
        )
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OperationFailedError(
            "Invalid JSON from Prometheus query API %s: %s" % (url, exc)
        )
    if payload.get("status") != "success":
        raise OperationFailedError(
            "Prometheus query API status=%s query=%r url=%s"
            % (payload.get("status"), promql, url)
        )
    return payload


def _sum_prometheus_instant_vector(payload):
    total = 0
    series = payload.get("data", {}).get("result", [])
    for entry in series:
        value = entry.get("value")
        if value and len(value) >= 2:
            total += int(float(value[1]))
    return total, len(series)


def get_prometheus_counter_value(scrape_node, api_base_url, metric_name):
    last_error = None
    for variant in _metric_name_variants(metric_name):
        try:
            payload = prometheus_query_instant(scrape_node, api_base_url, variant)
            total, series_count = _sum_prometheus_instant_vector(payload)
            if series_count > 0:
                return total, variant
        except OperationFailedError as err:
            last_error = err
    raise last_error or OperationFailedError(
        "Prometheus query returned no series for %s" % metric_name
    )


def assert_prometheus_counter_increased_after_workload(
    scrape_node,
    ceph_cluster,
    installer,
    workload_fn,
    *metric_names,
    config=None,
):
    """Query cluster Prometheus before/after workload (same as UI graph check)."""
    config = config or {}
    wait_seconds = int(config.get("prometheus_query_wait_seconds", 30))
    api_url = get_prometheus_api_url(ceph_cluster, installer, config)

    before_value = None
    used_metric = None
    for metric_name in metric_names:
        try:
            before_value, used_metric = get_prometheus_counter_value(
                scrape_node, api_url, metric_name
            )
            break
        except OperationFailedError:
            continue
    if used_metric is None:
        raise OperationFailedError(
            "No Prometheus series found for metrics: %s (api=%s)"
            % (", ".join(metric_names), api_url)
        )

    log.info(
        "S-06: Prometheus query %s before workload: %s (api=%s)",
        used_metric,
        before_value,
        api_url,
    )
    workload_fn()
    time.sleep(wait_seconds)

    after_value, _ = get_prometheus_counter_value(scrape_node, api_url, used_metric)
    if after_value <= before_value:
        raise OperationFailedError(
            "Prometheus counter %s did not increase after workload: "
            "%s -> %s (api=%s/api/v1/query)"
            % (used_metric, before_value, after_value, api_url)
        )
    log.info(
        "S-06: Prometheus counter %s increased %s -> %s via %s",
        used_metric,
        before_value,
        after_value,
        api_url,
    )


def _prometheus_vector_series(payload):
    series = {}
    for entry in payload.get("data", {}).get("result", []):
        metric = entry.get("metric", {})
        name = metric.get("__name__")
        value = entry.get("value")
        if not name or not value or len(value) < 2:
            continue
        labels = tuple(
            sorted((key, val) for key, val in metric.items() if key != "__name__")
        )
        series[(name, labels)] = float(value[1])
    return series


def _operation_from_label_tuple(labels):
    for key, val in labels:
        if key == "operation":
            return val
    return None


def get_prometheus_requests_by_operation(scrape_node, api_base_url, metric_name):
    last_error = None
    for variant in _metric_name_variants(metric_name):
        try:
            payload = prometheus_query_instant(scrape_node, api_base_url, variant)
            series = _prometheus_vector_series(payload)
            by_labels = {
                labels: value
                for (name, labels), value in series.items()
                if name == variant
            }
            if by_labels:
                return by_labels, variant
        except OperationFailedError as err:
            last_error = err
    raise last_error or OperationFailedError(
        "Prometheus query returned no %s series (api=%s)" % (metric_name, api_base_url)
    )


def _prometheus_increase_total(scrape_node, api_base_url, metric_name, window):
    last_error = None
    for variant in _metric_name_variants(metric_name):
        for promql in (
            "sum(increase(%s[%s]))" % (variant, window),
            "increase(%s[%s])" % (variant, window),
        ):
            try:
                payload = prometheus_query_instant(scrape_node, api_base_url, promql)
                total, series_count = _sum_prometheus_instant_vector(payload)
                if series_count > 0 and total > 0:
                    return total, promql
            except OperationFailedError as err:
                last_error = err
    if last_error:
        log.info(
            "Prometheus increase for %s returned no data: %s",
            metric_name,
            last_error,
        )
    return 0.0, None


def perform_s06_minimal_workload(client, nfs_mount):
    """Client mount check plus metadata ops (ls/stat) per S-06 manual."""
    out, _ = client.exec_command(sudo=True, cmd="mount | grep nfs", check_ec=False)
    mount_out = (out or "").strip()
    if not mount_out or nfs_mount not in mount_out:
        raise OperationFailedError(
            "NFS mount %s not found in mount | grep nfs:\n%s" % (nfs_mount, mount_out)
        )
    client.exec_command(sudo=True, cmd="ls -la %s" % nfs_mount)
    client.exec_command(sudo=True, cmd="stat %s" % nfs_mount)
    log.info(
        "S-06 workload on %s: mount verified, ls -la and stat completed",
        nfs_mount,
    )


def verify_s06_minimal_workload_prometheus(
    scrape_node,
    ceph_cluster,
    installer,
    workload_fn,
    config=None,
    nfs_ip=None,
    monitoring_port=None,
):
    """Verify requests_total per-operation increase via Prometheus (UI-equivalent)."""
    config = config or {}
    wait_seconds = int(config.get("prometheus_query_wait_seconds", 30))
    increase_window = config.get("prometheus_increase_window", "2m")
    api_url = get_prometheus_api_url(ceph_cluster, installer, config)

    before_direct = None
    if nfs_ip and monitoring_port:
        before_direct = scrape_prometheus_metrics(scrape_node, nfs_ip, monitoring_port)

    before_ops = {}
    metric_name = None
    try:
        before_ops, metric_name = get_prometheus_requests_by_operation(
            scrape_node, api_url, "requests_total"
        )
        log.info(
            "S-06: Prometheus %s series before workload: %s operations",
            metric_name,
            len(before_ops),
        )
    except OperationFailedError:
        log.info(
            "S-06: requests_total not in Prometheus yet; "
            "will check increase() after workload"
        )

    workload_fn()
    time.sleep(wait_seconds)

    if metric_name:
        try:
            after_ops, _ = get_prometheus_requests_by_operation(
                scrape_node, api_url, metric_name
            )
            increased_operations = []
            for labels, after_value in after_ops.items():
                before_value = before_ops.get(labels, 0)
                if after_value > before_value:
                    op = _operation_from_label_tuple(labels) or str(labels)
                    increased_operations.append(
                        "%s (%s -> %s)" % (op, int(before_value), int(after_value))
                    )
            if increased_operations:
                log.info(
                    "S-06: Prometheus %s operations increased: %s",
                    metric_name,
                    ", ".join(increased_operations),
                )
                return
        except OperationFailedError as err:
            log.info("S-06: per-operation compare unavailable: %s", err)

    for candidate in ("requests_total", "rpcs_received_total"):
        increase_total, promql = _prometheus_increase_total(
            scrape_node,
            api_url,
            candidate,
            increase_window,
        )
        if increase_total > 0:
            log.info(
                "S-06: Prometheus increase query %r total=%s",
                promql,
                increase_total,
            )
            return

    if before_direct is not None:
        after_direct = scrape_prometheus_metrics(scrape_node, nfs_ip, monitoring_port)
        assert_counter_increased(
            before_direct,
            after_direct,
            "requests_total",
            "rpcs_received_total",
        )
        log.info(
            "S-06: direct scrape counters increased on %s:%s",
            nfs_ip,
            monitoring_port,
        )
        return

    raise OperationFailedError(
        "S-06: no counter increase in Prometheus or direct scrape after workload "
        "(api=%s, tried increase(requests_total[%s]), increase(rpcs_received_total[%s]))"
        % (api_url, increase_window, increase_window)
    )


def prometheus_count_metric_series(scrape_node, api_base_url, metric_name):
    """Return series count via Prometheus ``count(<metric>)`` (UI-equivalent)."""
    last_error = None
    for variant in _metric_name_variants(metric_name):
        for promql in ("count(%s)" % variant, variant):
            try:
                payload = prometheus_query_instant(scrape_node, api_base_url, promql)
                series = payload.get("data", {}).get("result", [])
                if not series:
                    continue
                if promql.startswith("count("):
                    value = float(series[0].get("value", [0, 0])[1])
                    return int(value), variant, promql
                return len(series), variant, promql
            except OperationFailedError as err:
                last_error = err
    if last_error:
        log.info(
            "Prometheus count for %s returned no series: %s",
            metric_name,
            last_error,
        )
    return 0, None, None


def _find_v4_op_count_series(metrics_text):
    series = {}
    for name, labels, value in parse_metric_samples(metrics_text):
        if name not in V4_OP_COUNT_METRICS:
            continue
        op = labels.get("op")
        status = labels.get("status")
        if not op or not status:
            continue
        key = (name, op, status)
        series[key] = series.get(key, 0) + int(value)
    return series


def verify_nfsv4_op_count_after_workload(metrics_before, metrics_after):
    """S-08: nfsv4__op_count present with op/status labels and increases after v4 IO."""
    before = _find_v4_op_count_series(metrics_before)
    after = _find_v4_op_count_series(metrics_after)
    if not after:
        raise OperationFailedError(
            "nfsv4__op_count not found with op/status labels after workload"
        )

    increased = []
    for key, after_value in after.items():
        before_value = before.get(key, 0)
        if after_value > before_value:
            _, op, status = key
            increased.append(
                "%s/%s (%s -> %s)" % (op, status, before_value, after_value)
            )

    if not increased:
        raise OperationFailedError(
            "nfsv4__op_count series present but none increased after workload"
        )

    log.info(
        "S-08: nfsv4__op_count increased for %s operations (total series after=%s)",
        len(increased),
        len(after),
    )
    log.info("S-08 sample increases: %s", ", ".join(increased[:8]))


def verify_s07_dynamic_metrics_disabled(ctx, config=None):
    """S-07: dynamic metrics off — no per-client series; global counters still move."""
    config = config or {}
    wait_seconds = int(config.get("prometheus_query_wait_seconds", 30))
    max_client_series = int(config.get("s07_max_client_series_when_disabled", 0))
    api_url = get_prometheus_api_url(ctx.ceph_cluster, ctx.installer, config)

    before_global, global_metric = get_prometheus_counter_value(
        ctx.client, api_url, "rpcs_received_total"
    )
    client_series_before, client_metric, client_promql = prometheus_count_metric_series(
        ctx.client, api_url, "client_requests_total"
    )
    log.info(
        "S-07 before traffic: %s=%s, count(%s)=%s via %r",
        global_metric,
        before_global,
        client_metric or "client_requests_total",
        client_series_before,
        client_promql,
    )

    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1, file_name="s07_client0")
    for idx, (client, mount) in enumerate(ctx.extra_mounts, start=1):
        perform_nfs_io(client, mount, mb=1, file_name="s07_client%s" % idx)
    time.sleep(wait_seconds)

    client_series_after, _, after_promql = prometheus_count_metric_series(
        ctx.client, api_url, client_metric or "client_requests_total"
    )
    after_global, _ = get_prometheus_counter_value(ctx.client, api_url, global_metric)

    log.info(
        "S-07 after traffic: %s=%s -> %s, count(%s)=%s via %r",
        global_metric,
        before_global,
        after_global,
        client_metric or "client_requests_total",
        client_series_after,
        after_promql,
    )

    if client_series_after > max_client_series:
        raise OperationFailedError(
            "Per-client series count %s exceeds max %s with enable_dynamic_metrics=false "
            "(query=%r)" % (client_series_after, max_client_series, after_promql)
        )

    if after_global <= before_global:
        raise OperationFailedError(
            "Global counter %s did not increase with dynamic metrics disabled: "
            "%s -> %s" % (global_metric, before_global, after_global)
        )

    log.info(
        "S-07 PASS: per-client series=%s (max=%s), global %s increased",
        client_series_after,
        max_client_series,
        global_metric,
    )


def verify_help_and_type(metrics_text):
    if "# HELP" not in metrics_text or "# TYPE" not in metrics_text:
        raise OperationFailedError("Missing # HELP or # TYPE in exposition")


def verify_exposition_has_metric(metrics_text, metric_name):
    names = parse_prometheus_metrics(metrics_text)
    if not metric_available(names, metric_name):
        raise OperationFailedError("%s not in metrics exposition" % metric_name)


def verify_monitoring_port_listening(nfs_node, port):
    out, _ = nfs_node.exec_command(
        sudo=True, cmd=f"ss -H -tln sport = :{port}", check_ec=False
    )
    if str(port) not in (out or ""):
        raise OperationFailedError(
            f"Monitoring port {port} not listening on {nfs_node.hostname}"
        )


def verify_monitoring_port_not_listening(nfs_node, port):
    out, _ = nfs_node.exec_command(
        sudo=True, cmd=f"ss -H -tln sport = :{port}", check_ec=False
    )
    if str(port) in (out or ""):
        raise OperationFailedError(
            "Monitoring port %s still listening on %s with metrics disabled"
            % (port, nfs_node.hostname)
        )


def verify_metrics_endpoint_not_accessible(client, nfs_node, nfs_ip, port):
    """Expect connection refused / non-200 when Enable_Metrics=false."""
    verify_monitoring_port_not_listening(nfs_node, port)
    code = scrape_prometheus_metrics_http_code(client, nfs_ip, port)
    if code == "200":
        raise OperationFailedError(
            "Metrics endpoint returned HTTP 200 with Enable_Metrics=false"
        )
    log.info(
        "Metrics endpoint not accessible on %s:%s (http_code=%s)",
        nfs_ip,
        port,
        code or "unreachable",
    )


def verify_latency_histogram_after_workload(metrics_text):
    for prefix in LATENCY_HISTOGRAM_PREFIXES:
        count, _ = get_histogram_count_sum(metrics_text, prefix)
        if count > 0:
            verify_histogram_cumulative(metrics_text, prefix)
            log.info("Latency histogram %s count=%s after workload", prefix, count)
            return
    raise OperationFailedError(
        "No latency histogram count > 0 after workload (tried: %s)"
        % ", ".join(LATENCY_HISTOGRAM_PREFIXES)
    )


def verify_histogram_cumulative(metrics_text, metric_prefix):
    prefixes = get_histogram_prefix_variants(metric_prefix)
    if metric_prefix not in prefixes:
        prefixes = [metric_prefix] + prefixes
    buckets = {}
    for name, labels, value in parse_metric_samples(metrics_text):
        matched = None
        for prefix in prefixes:
            if name == "%s_bucket" % prefix:
                matched = prefix
                break
        if not matched:
            continue
        label_key = tuple(sorted((k, v) for k, v in labels.items() if k != "le"))
        le = labels.get("le", "")
        buckets[(matched, label_key, le)] = int(value)

    for (prefix, label_key, le), count in buckets.items():
        if le == "+Inf":
            continue
        try:
            le_val = float(le)
        except ValueError:
            continue
        for (_, label_key2, le2), count2 in buckets.items():
            if label_key2 != label_key or le2 == "+Inf":
                continue
            try:
                if float(le2) > le_val and count2 < count:
                    raise OperationFailedError(
                        "Non-cumulative bucket for %s labels=%s" % (prefix, label_key)
                    )
            except ValueError:
                continue
        inf_count = buckets.get((prefix, label_key, "+Inf"))
        count_name = "%s_count" % prefix
        series_count = 0
        for name, labels, val in parse_metric_samples(metrics_text):
            if name != count_name:
                continue
            sample_key = tuple(sorted(labels.items()))
            if sample_key == label_key:
                series_count = int(val)
                break
        if inf_count is not None and series_count and inf_count != series_count:
            log.warning(
                "%s +Inf bucket=%s != _count=%s for labels=%s",
                prefix,
                inf_count,
                series_count,
                label_key,
            )

    for prefix in prefixes:
        count, total_sum = get_histogram_count_sum(metrics_text, prefix)
        if count > 0:
            if total_sum < 0:
                raise OperationFailedError("%s_sum is negative" % prefix)
            log.info(
                "%s histogram OK: count=%s avg=%s",
                prefix,
                count,
                (total_sum / count) if count else 0,
            )
            return
    raise OperationFailedError(
        "%s_count is zero after workload (tried %s)" % (metric_prefix, prefixes)
    )


def verify_label_charset(metrics_text):
    for line in metrics_text.splitlines():
        if "{" not in line or line.startswith("#"):
            continue
        label_part = line.split("{", 1)[1].split("}", 1)[0]
        for item in label_part.split(","):
            if "=" not in item:
                continue
            key, val = item.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"')
            if not PROMETHEUS_LABEL_RE.match(key):
                raise OperationFailedError(f"Invalid label name: {key}")
            if not val or '"' in val or "\n" in val:
                raise OperationFailedError(f"Invalid label value for {key}: {val}")


def _parse_ganesha_release(version_text):
    for line in version_text.splitlines():
        if "NFS-Ganesha Release" in line and "=" in line:
            return line.split("=", 1)[1].strip()
    return None


def _parse_ganesha_git_describe(version_text):
    for line in version_text.splitlines():
        if "Git Describe" in line and "=" in line:
            return line.split("=", 1)[1].strip()
    return None


def _is_placeholder_ganesha_version(version):
    return (version or "").strip().upper() in ("", "ARGS", "UNKNOWN", "N/A")


def _ganesha_versions_compatible(metric_version, version_text):
    if metric_version and metric_version in version_text:
        return True

    git_describe = _parse_ganesha_git_describe(version_text)
    if git_describe and metric_version == git_describe:
        return True

    release = _parse_ganesha_release(version_text)
    if not release:
        return False

    if _is_placeholder_ganesha_version(metric_version):
        return True

    if release in metric_version or metric_version in release:
        return True

    release_core = release.lstrip("Vv").split("-", 1)[0]
    metric_core = metric_version.lstrip("Vv").split("-", 1)[0]
    return (
        release_core == metric_core
        or release_core in metric_core
        or metric_core in release_core
    )


def verify_ganesha_build_info(metrics_text, nfs_node, container_id=None):
    metric_version = None
    metric_built_time = None
    server_scope = None
    found = False
    for name, labels, _ in parse_metric_samples(metrics_text):
        if name in ("ganesha_build_info", "nfs_ganesha_build_info"):
            found = True
            metric_version = labels.get("GANESHA_VERSION")
            metric_built_time = labels.get("BUILT_TIME")
            server_scope = labels.get("SERVER_SCOPE")
            if not metric_version:
                raise OperationFailedError(
                    "ganesha_build_info missing GANESHA_VERSION label"
                )
    if not found:
        raise OperationFailedError("ganesha_build_info not found in metrics scrape")

    version_cmd = "ganesha.nfsd -v"
    if container_id:
        version_cmd = "podman exec %s ganesha.nfsd -v" % container_id
    out, _ = nfs_node.exec_command(sudo=True, cmd=version_cmd, check_ec=False)
    version_text = (out or "").strip()
    if not version_text and container_id:
        out, _ = nfs_node.exec_command(
            sudo=True,
            cmd=(
                "cid=$(podman ps --format '{{.ID}} {{.Names}}' "
                "| awk '/nfs|ganesha/ {print $1; exit}'); "
                '[ -n "$cid" ] && podman exec "$cid" ganesha.nfsd -v'
            ),
            check_ec=False,
        )
        version_text = (out or "").strip()
    if not version_text:
        raise OperationFailedError("ganesha.nfsd -v returned empty output")

    release = _parse_ganesha_release(version_text)
    git_describe = _parse_ganesha_git_describe(version_text)

    if not _ganesha_versions_compatible(metric_version, version_text):
        raise OperationFailedError(
            "ganesha_build_info GANESHA_VERSION=%r not compatible with "
            "ganesha.nfsd -v (release=%r, git_describe=%r)"
            % (metric_version, release, git_describe)
        )

    if _is_placeholder_ganesha_version(metric_version):
        log.info(
            "ganesha_build_info GANESHA_VERSION=%r is a build placeholder; "
            "matched ganesha.nfsd -v release=%r",
            metric_version,
            release,
        )
    elif git_describe and metric_version == git_describe:
        log.info(
            "ganesha_build_info GANESHA_VERSION matches Git Describe=%s",
            git_describe,
        )
    else:
        log.info(
            "ganesha_build_info GANESHA_VERSION=%s compatible with release=%s",
            metric_version,
            release,
        )

    if metric_built_time:
        built_token = metric_built_time.split()[0:3]
        built_hint = " ".join(built_token)
        if built_hint and built_hint not in version_text:
            log.warning(
                "ganesha_build_info BUILT_TIME=%r not matched literally in "
                "ganesha.nfsd -v (non-fatal)",
                metric_built_time,
            )

    if server_scope:
        log.info("ganesha_build_info SERVER_SCOPE=%s", server_scope)
    log.info("ganesha.nfsd -v: %s", version_text[:200])


def verify_idle_baseline(metrics_first, metrics_second, config=None):
    """Idle cluster: low in-flight RPCs; counters stable or slow background drift."""
    config = config or {}
    in_flight_max = int(config.get("idle_in_flight_max", 5))
    max_drift = int(config.get("idle_max_counter_drift", 100))

    for label, metrics in (("first", metrics_first), ("second", metrics_second)):
        in_flight = get_counter_total(metrics, "rpcs_in_flight")
        if in_flight > in_flight_max:
            raise OperationFailedError(
                "rpcs_in_flight=%s on %s scrape exceeds idle max %s"
                % (in_flight, label, in_flight_max)
            )

    for metric in ("rpcs_received_total", "rpcs_completed_total"):
        first = get_counter_total(metrics_first, metric)
        second = get_counter_total(metrics_second, metric)
        if second < first:
            raise OperationFailedError(
                "%s decreased while idle: %s -> %s" % (metric, first, second)
            )
        drift = second - first
        if drift > max_drift:
            raise OperationFailedError(
                "%s idle drift %s exceeds max %s (%s -> %s)"
                % (metric, drift, max_drift, first, second)
            )
        log.info(
            "Idle %s stable: %s -> %s (drift=%s)",
            metric,
            first,
            second,
            drift,
        )

    log.info(
        "Idle baseline OK: rpcs_in_flight first=%s second=%s",
        get_counter_total(metrics_first, "rpcs_in_flight"),
        get_counter_total(metrics_second, "rpcs_in_flight"),
    )


def concurrent_scrape(node, host, port, count=10):
    results = []

    def _scrape():
        return scrape_prometheus_metrics_http_code(node, host, port)

    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(_scrape) for _ in range(count)]
        for fut in as_completed(futures):
            results.append(fut.result())
    if any(code != "200" for code in results):
        raise OperationFailedError(f"Concurrent scrape failures: {results}")
    return results


def concurrent_scrape_metrics(node, host, port, count=10):
    """Run parallel full metric scrapes; return list of exposition bodies."""
    bodies = []

    def _scrape_body():
        return scrape_prometheus_metrics(node, host, port)

    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(_scrape_body) for _ in range(count)]
        for fut in as_completed(futures):
            bodies.append(fut.result())
    return bodies


def verify_concurrent_scrape_metrics(bodies):
    if not bodies:
        raise OperationFailedError("No concurrent scrape responses")
    rpc_totals = []
    for idx, metrics in enumerate(bodies, start=1):
        if not metrics or not metrics.strip():
            raise OperationFailedError("Concurrent scrape %s returned empty body" % idx)
        verify_help_and_type(metrics)
        verify_exposition_has_metric(metrics, "rpcs_received_total")
        rpc_totals.append(get_counter_total(metrics, "rpcs_received_total"))
    if max(rpc_totals) - min(rpc_totals) > 1000:
        log.warning(
            "Concurrent scrape rpcs_received_total spread %s..%s (race window)",
            min(rpc_totals),
            max(rpc_totals),
        )
    log.info(
        "Concurrent scrape OK: %s responses, rpcs_received_total range %s..%s",
        len(bodies),
        min(rpc_totals),
        max(rpc_totals),
    )


def verify_promtool_metrics(client, host, port, remote_path="/tmp/ganesha_live.prom"):
    client.exec_command(
        sudo=True,
        cmd="curl -sf http://%s:%s/metrics -o %s" % (host, port, remote_path),
    )
    out, _ = client.exec_command(
        sudo=True,
        cmd="command -v promtool >/dev/null && promtool check metrics %s || echo SKIP"
        % remote_path,
        check_ec=False,
    )
    result = (out or "").strip()
    if result == "SKIP":
        log.info("promtool not installed on client; skipping promtool check metrics")
        verify_help_and_type(scrape_prometheus_metrics(client, host, port))
        return
    if "SUCCESS" not in result.upper() and result:
        raise OperationFailedError("promtool check metrics failed: %s" % result[-500:])
    log.info("promtool check metrics passed")


def firewall_block_metrics_from_source(nfs_node, port, source_ip):
    rule = (
        'rule family="ipv4" source address=%s port port="%s" protocol="tcp" reject'
        % (source_ip, port)
    )
    nfs_node.exec_command(
        sudo=True,
        cmd="firewall-cmd --permanent --add-rich-rule='%s'" % rule,
        check_ec=False,
    )
    nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)
    log.info(
        "Blocked metrics port %s from source %s on %s",
        port,
        source_ip,
        nfs_node.hostname,
    )
    return rule


def firewall_unblock_metrics_from_source(nfs_node, rule):
    nfs_node.exec_command(
        sudo=True,
        cmd="firewall-cmd --permanent --remove-rich-rule='%s'" % rule,
        check_ec=False,
    )
    nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)
    log.info("Removed firewall rich-rule on %s", nfs_node.hostname)


def firewall_allow_port(node, port):
    node.exec_command(
        sudo=True,
        cmd=f"firewall-cmd --permanent --add-port={port}/tcp",
        check_ec=False,
    )
    node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)


def run_ganesha_stats(nfs_node, container_id, *args):
    cmd = "podman exec %s ganesha_stats %s" % (container_id, " ".join(args))
    out, err = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return (out or "").strip(), (err or "").strip()


def run_ganesha_stats_subcommands(nfs_node, container_id, subcommands):
    for subcmd in subcommands:
        out, err = run_ganesha_stats(nfs_node, container_id, *subcmd.split())
        combined = f"{out}\n{err}".strip()
        if not combined:
            raise OperationFailedError(f"ganesha_stats {subcmd} returned empty output")
        if "unknown command" in combined.lower():
            raise OperationFailedError(f"ganesha_stats {subcmd} failed: {combined}")
        log.info("ganesha_stats %s ok (len=%s)", subcmd, len(combined))


def get_nfs_container_id(installer, nfs_name, nfs_node):
    from tests.nfs.nfs_operations import get_ganesha_info_from_container

    _, info = get_ganesha_info_from_container(installer, nfs_name, nfs_node)
    return (info or {}).get("container_id")


def _ganesha_conf_param_aliases(param_key):
    canonical = PROMETHEUS_GANESHA_PARAM_NAMES.get(param_key, param_key)
    aliases = PROMETHEUS_CORE_PARAM_ALIASES.get(param_key, (param_key, canonical))
    return "|".join(re.escape(a) for a in aliases)


def patch_ganesha_conf_param_in_container(nfs_node, container_id, param_key, value):
    """Edit live container ganesha.conf (does not persist across redeploy)."""
    if isinstance(value, bool):
        value_str = "true" if value else "false"
    else:
        value_str = str(value)
    alias_pattern = _ganesha_conf_param_aliases(param_key)
    nfs_node.exec_command(
        sudo=True,
        cmd=(
            "podman exec %s sed -i -E "
            "'s/^([[:space:]]*(%s)[[:space:]]*=[[:space:]]*)[^;]+;/\\1%s;/' "
            "/etc/ganesha/ganesha.conf" % (container_id, alias_pattern, value_str)
        ),
        check_ec=False,
    )
    log.info(
        "Patched container ganesha.conf %s=%s (in-memory until redeploy)",
        param_key,
        value_str,
    )


def sighup_ganesha_in_container(nfs_node, container_id):
    pid_out, _ = nfs_node.exec_command(
        sudo=True,
        cmd="podman exec %s pgrep -o ganesha.nfsd" % container_id,
        check_ec=False,
    )
    pid = (pid_out or "").strip().split("\n")[0]
    if not pid:
        raise OperationFailedError(
            "No ganesha.nfsd pid in container %s for SIGHUP" % container_id
        )
    nfs_node.exec_command(
        sudo=True,
        cmd="podman exec %s kill -HUP %s" % (container_id, pid),
    )
    log.info("Sent SIGHUP to ganesha.nfsd pid %s in container %s", pid, container_id)


def metrics_scrape_ok(client, nfs_ip, port):
    if scrape_prometheus_metrics_http_code(client, nfs_ip, port) != "200":
        return False
    metrics = scrape_prometheus_metrics(client, nfs_ip, port)
    return metric_available(parse_prometheus_metrics(metrics), "rpcs_received_total")


def verify_f02_port_reload_vs_restart(ctx, alt_port):
    """Port change in live conf + SIGHUP does not bind new port; redeploy does."""
    base_port = ctx.monitoring_port
    log.info(
        "F-02A: change monitoring_port %s -> %s in container conf + SIGHUP",
        base_port,
        alt_port,
    )
    patch_ganesha_conf_param_in_container(
        ctx.nfs_node, ctx.container_id, "monitoring_port", alt_port
    )
    sighup_ganesha_in_container(ctx.nfs_node, ctx.container_id)
    time.sleep(int(ctx.config.get("f02_reload_wait_seconds", 5)))
    verify_monitoring_port_not_listening(ctx.nfs_node, alt_port)
    if metrics_scrape_ok(ctx.client, ctx.nfs_ip, alt_port):
        raise OperationFailedError(
            "F-02A: monitoring_port=%s responded before restart" % alt_port
        )
    if not metrics_scrape_ok(ctx.client, ctx.nfs_ip, base_port):
        log.warning(
            "F-02A: baseline port %s not scraping after SIGHUP (metrics may need restart)",
            base_port,
        )
    log.info("F-02A: monitoring_port change requires Ganesha restart (not SIGHUP)")


def verify_f02_enable_metrics_reload_vs_restart(ctx):
    """enable_metrics=false in live conf + SIGHUP leaves exporter up; redeploy disables it."""
    port = ctx.monitoring_port
    log.info("F-02B: enable_metrics=false in container conf + SIGHUP")
    patch_ganesha_conf_param_in_container(
        ctx.nfs_node, ctx.container_id, "enable_metrics", False
    )
    sighup_ganesha_in_container(ctx.nfs_node, ctx.container_id)
    time.sleep(int(ctx.config.get("f02_reload_wait_seconds", 5)))
    if metrics_scrape_ok(ctx.client, ctx.nfs_ip, port):
        log.info(
            "F-02B: metrics still UP after enable_metrics=false + SIGHUP (restart required)"
        )
    else:
        log.info("F-02B: metrics down after SIGHUP without redeploy")

    metrics_off = dict(ctx.config)
    metrics_off["enable_metrics"] = False
    metrics_off["monitoring_port"] = port
    update_prometheus_ganesha_params(
        ctx.installer, ctx.nfs_node, ctx.nfs_name, metrics_off
    )
    ctx.refresh_container_id()
    verify_metrics_endpoint_not_accessible(ctx.client, ctx.nfs_node, ctx.nfs_ip, port)
    log.info("F-02B: enable_metrics=false takes effect after redeploy")

    metrics_on = dict(ctx.config)
    metrics_on["enable_metrics"] = True
    metrics_on["monitoring_port"] = port
    update_prometheus_ganesha_params(
        ctx.installer, ctx.nfs_node, ctx.nfs_name, metrics_on
    )
    ctx.refresh_container_id()
    verify_monitoring_port_listening(ctx.nfs_node, port)
    if not metrics_scrape_ok(ctx.client, ctx.nfs_ip, port):
        raise OperationFailedError(
            "F-02B: metrics not restored after redeploy with enable_metrics=true"
        )
    log.info("F-02B: enable_metrics=true restored after redeploy")


def verify_f02_dynamic_metrics_reload_vs_restart(ctx):
    """enable_dynamic_metrics toggle requires redeploy; SIGHUP alone is insufficient."""
    port = ctx.monitoring_port
    log.info("F-02C: enable_dynamic_metrics=false in container conf + SIGHUP")
    patch_ganesha_conf_param_in_container(
        ctx.nfs_node, ctx.container_id, "enable_dynamic_metrics", False
    )
    sighup_ganesha_in_container(ctx.nfs_node, ctx.container_id)
    time.sleep(int(ctx.config.get("f02_reload_wait_seconds", 5)))
    if not metrics_scrape_ok(ctx.client, ctx.nfs_ip, port):
        raise OperationFailedError(
            "F-02C: global metrics unavailable after dynamic_metrics SIGHUP test"
        )
    log.info(
        "F-02C: SIGHUP did not disable exporter; per-client series need redeploy to change"
    )

    dyn_off = dict(ctx.config)
    dyn_off["enable_dynamic_metrics"] = False
    dyn_off["monitoring_port"] = port
    update_prometheus_ganesha_params(ctx.installer, ctx.nfs_node, ctx.nfs_name, dyn_off)
    ctx.refresh_container_id()
    if not metrics_scrape_ok(ctx.client, ctx.nfs_ip, port):
        raise OperationFailedError(
            "F-02C: metrics unavailable after enable_dynamic_metrics=false redeploy"
        )

    dyn_on = dict(ctx.config)
    dyn_on["enable_dynamic_metrics"] = True
    dyn_on["monitoring_port"] = port
    update_prometheus_ganesha_params(ctx.installer, ctx.nfs_node, ctx.nfs_name, dyn_on)
    ctx.refresh_container_id()
    if not metrics_scrape_ok(ctx.client, ctx.nfs_ip, port):
        raise OperationFailedError(
            "F-02C: metrics not restored after enable_dynamic_metrics=true redeploy"
        )
    log.info("F-02C: enable_dynamic_metrics toggle requires redeploy (documented)")


def perform_nfs_io(client, mount_path, file_name="prometheus_stats_testfile", mb=2):
    test_file = f"{mount_path}/{file_name}"
    client.exec_command(
        sudo=True,
        cmd=(
            f"dd if=/dev/urandom of={test_file} bs=1M count={mb} conv=fsync "
            f"status=none && sync && cat {test_file} > /dev/null"
        ),
    )
    client.exec_command(sudo=True, cmd=f"ls -la {mount_path} && stat {test_file}")


def perform_metadata_workload(client, mount_path):
    client.exec_command(
        sudo=True,
        cmd=(
            f"mkdir -p {mount_path}/mdtest && "
            f"for i in $(seq 1 50); do touch {mount_path}/mdtest/f$i; done && "
            f"ls -la {mount_path}/mdtest | head"
        ),
    )


def perform_f05_manifest_warmup(client, mount_path, file_count=100):
    """Cold then warm metadata so mdcache hit counters are exported."""
    testdir = "%s/f05_manifest_%s" % (mount_path, int(time.time()))
    client.exec_command(sudo=True, cmd="mkdir -p %s" % testdir)
    client.exec_command(
        sudo=True,
        cmd=(
            "for i in $(seq 1 %s); do "
            "touch %s/file_$i; stat %s/file_$i >/dev/null; "
            "done" % (file_count, testdir, testdir)
        ),
        timeout=180,
    )
    for pass_num in range(1, 4):
        client.exec_command(
            sudo=True,
            cmd=(
                "for i in $(seq 1 %s); do "
                "stat %s/file_$i >/dev/null; ls %s/file_$i >/dev/null; "
                "done; ls -la %s >/dev/null; "
                "find %s -maxdepth 1 -type f -exec stat {} \\; >/dev/null"
                % (file_count, testdir, testdir, testdir, testdir)
            ),
            timeout=180,
        )
    log.info("F-05 manifest warmup completed under %s", testdir)


def perform_f10_mdcache_workload(client, mount_path, file_count=500):
    """Cold then warm metadata paths for mdcache hit/miss counters."""
    testdir = "%s/f10_mdcache_%s" % (mount_path, int(time.time()))
    client.exec_command(sudo=True, cmd="mkdir -p %s" % testdir)
    client.exec_command(
        sudo=True,
        cmd=(
            "for i in $(seq 1 %s); do "
            "touch %s/file_$i; stat %s/file_$i >/dev/null; "
            "done" % (file_count, testdir, testdir)
        ),
        timeout=max(600, file_count // 2),
    )
    client.exec_command(
        sudo=True,
        cmd=(
            "find %s -type f | wc -l; ls -laR %s >/dev/null; "
            "for i in $(seq 1 %s); do "
            "stat %s/file_$i >/dev/null; ls %s/file_$i >/dev/null; "
            "done" % (testdir, testdir, min(file_count, 500), testdir, testdir)
        ),
        timeout=max(600, file_count // 2),
    )
    for _ in range(5):
        client.exec_command(
            sudo=True,
            cmd="ls -la %s >/dev/null" % testdir,
            check_ec=False,
        )
    log.info("F-10 metadata workload completed under %s", testdir)


def verify_mdcache_counters_increased(metrics_before, metrics_after):
    before_hits = max(get_counter_total(metrics_before, m) for m in MDCACHE_HIT_METRICS)
    before_misses = max(
        get_counter_total(metrics_before, m) for m in MDCACHE_MISS_METRICS
    )
    after_hits = max(get_counter_total(metrics_after, m) for m in MDCACHE_HIT_METRICS)
    after_misses = max(
        get_counter_total(metrics_after, m) for m in MDCACHE_MISS_METRICS
    )
    if after_hits <= before_hits and after_misses <= before_misses:
        raise OperationFailedError(
            "mdcache hits/misses did not increase (hits %s->%s misses %s->%s)"
            % (before_hits, after_hits, before_misses, after_misses)
        )
    log.info(
        "mdcache counters increased: hits %s->%s misses %s->%s",
        before_hits,
        after_hits,
        before_misses,
        after_misses,
    )


def get_export_bytes_by_export(metrics_text):
    totals = {}
    for name, labels, value in parse_metric_samples(metrics_text):
        if "bytes_received_by_export_total" not in name and (
            "bytes_sent_by_export_total" not in name
        ):
            continue
        export = labels.get("export") or labels.get("export_id") or ""
        path = labels.get("path", "")
        key = export or path
        if key:
            totals[key] = totals.get(key, 0) + value
    return totals


def verify_export_traffic_attribution(metrics_before, metrics_after, min_delta=1024):
    before = get_export_bytes_by_export(metrics_before)
    after = get_export_bytes_by_export(metrics_after)
    deltas = {
        key: after.get(key, 0) - before.get(key, 0) for key in set(before) | set(after)
    }
    if not deltas:
        raise OperationFailedError("No per-export byte counters in exposition")
    positive = {k: v for k, v in deltas.items() if v > 0}
    if not positive:
        raise OperationFailedError(
            "No per-export byte counter movement (deltas=%s)" % deltas
        )
    active = max(positive, key=lambda k: positive[k])
    if positive[active] < min_delta:
        raise OperationFailedError(
            "Export %s byte delta %s below min_delta %s (deltas=%s)"
            % (active, positive[active], min_delta, deltas)
        )
    log.info("Per-export bytes increased for %s: %s", active, deltas)
    return active, deltas


def count_client_request_samples(metrics_text):
    names = set(CLIENT_REQUEST_METRICS)
    return sum(1 for name, _, _ in parse_metric_samples(metrics_text) if name in names)


def count_client_label_series(metrics_text):
    clients = set()
    client_metrics = set(CLIENT_REQUEST_METRICS)
    for name, labels, _ in parse_metric_samples(metrics_text):
        if name in CLIENT_BYTES_METRICS or name in client_metrics:
            client = labels.get("client") or labels.get("client_id")
            if client:
                clients.add(client)
    return clients


def verify_client_label_present(metrics_text, min_clients=1):
    clients = count_client_label_series(metrics_text)
    if len(clients) < min_clients:
        raise OperationFailedError(
            "Expected at least %s client label series, found %s"
            % (min_clients, sorted(clients))
        )
    verify_label_charset(metrics_text)
    log.info("Client label series present: %s", sorted(clients))
    return clients


def drop_client_page_cache(client):
    """Drop local page cache so a subsequent read must hit the NFS server."""
    client.exec_command(
        sudo=True,
        cmd="sync; echo 3 > /proc/sys/vm/drop_caches",
        check_ec=False,
        timeout=60,
    )
    log.info("Requested page cache drop on %s", client.hostname)


def run_fio_rw(client, mount_path, size="256M", runtime=120, rw="randrw"):
    job = f"""
[global]
directory={mount_path}
ioengine=psync
direct=1
runtime={runtime}
time_based=1
group_reporting=1

[meta]
rw=randread
bs=4k
size=64M
numjobs=1

[data]
rw={rw}
bs=1M
size={size}
numjobs=1
"""
    remote = "/tmp/nfs_prom_fio.job"
    with client.remote_file(sudo=True, file_name=remote, file_mode="w") as fp:
        fp.write(job)
        fp.flush()
    client.exec_command(
        sudo=True,
        cmd=f"fio {remote} --output-format=json",
        timeout=max(runtime * 3, 120) + 180,
    )


def create_exports(nfs_node, nfs_name, fs_name, count, prefix="/export_prom"):
    exports = []
    for i in range(1, count + 1):
        export = f"{prefix}_{i}"
        Ceph(nfs_node).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=export,
            fs=fs_name,
        )
        exports.append(export)
    return exports


def mount_export(client, server, export, mount_path, version, port):
    client.create_dirs(dir_path=mount_path, sudo=True)
    if Mount(client).nfs(
        mount=mount_path,
        version=version,
        port=port,
        server=server,
        export=export,
    ):
        raise OperationFailedError(
            f"Failed to mount {server}:{export} on {client.hostname}"
        )


def unmount_export(client, mount_path):
    if Unmount(client).unmount(mount_path):
        raise OperationFailedError(f"Failed to unmount {mount_path}")


def delete_exports(nfs_node, nfs_name, exports):
    for export in exports:
        try:
            Ceph(nfs_node).nfs.export.delete(nfs_name, export)
        except Exception as exc:
            log.warning("Export delete %s: %s", export, exc)


def get_prometheus_api_host(installer):
    out = CephAdm(installer).ceph.dashboard(**{"get-prometheus-api-host": ""})
    if isinstance(out, tuple):
        out = out[0]
    return str(out).strip()


def inject_permission_error(client, mount_path):
    client.exec_command(
        sudo=True,
        cmd=f"stat {mount_path}/no_such_file_for_getattr_test",
        check_ec=False,
    )


def restart_nfs_container(nfs_node, container_id):
    nfs_node.exec_command(sudo=True, cmd=f"podman restart {container_id}")
    time.sleep(30)


def kill_nfs_container(nfs_node, container_id):
    nfs_node.exec_command(sudo=True, cmd=f"podman kill -s KILL {container_id}")
    time.sleep(45)


def wait_metrics_recovery(scrape_node, host, port, timeout=120):
    for _ in range(timeout // 10):
        try:
            if scrape_prometheus_metrics_http_code(scrape_node, host, port) == "200":
                return
        except OperationFailedError:
            pass
        time.sleep(10)
    raise OperationFailedError(f"Metrics endpoint did not recover on {host}:{port}")


def rpc_gap_within_tolerance(metrics_before, metrics_after, tolerance_pct=5):
    received_delta = get_counter_total(
        metrics_after, "rpcs_received_total"
    ) - get_counter_total(metrics_before, "rpcs_received_total")
    completed_delta = get_counter_total(
        metrics_after, "rpcs_completed_total"
    ) - get_counter_total(metrics_before, "rpcs_completed_total")
    if received_delta <= 0 or completed_delta <= 0:
        raise OperationFailedError(
            f"RPC counters did not increase: received={received_delta}, "
            f"completed={completed_delta}"
        )
    gap_pct = abs(received_delta - completed_delta) * 100.0 / max(received_delta, 1)
    if gap_pct > tolerance_pct:
        raise OperationFailedError(
            f"RPC gap {gap_pct:.1f}% exceeds tolerance {tolerance_pct}%"
        )
    log.info(
        "rpcs_received_total/completed delta gap %.1f%% within tolerance %s%% "
        "(received +%s, completed +%s)",
        gap_pct,
        tolerance_pct,
        received_delta,
        completed_delta,
    )


def bytes_delta_within_tolerance(
    metrics_before,
    metrics_after,
    metric_name,
    expected_bytes,
    tolerance_pct=10,
    operation_labels=None,
):
    delta, resolved = bytes_counter_delta(
        metrics_before,
        metrics_after,
        metric_name,
        operation_labels=operation_labels,
    )
    if (
        not _metric_family_present(metrics_after, metric_name, operation_labels)
        and not _metric_family_present(metrics_before, metric_name, operation_labels)
        and not _metric_family_present(metrics_after, metric_name)
        and not _metric_family_present(metrics_before, metric_name)
    ):
        raise OperationFailedError(
            "no byte counter family found for %s (tried: %s)"
            % (metric_name, ", ".join(_bytes_counter_candidates(metric_name)))
        )
    if expected_bytes <= 0:
        raise OperationFailedError("expected_bytes must be positive")
    diff_pct = abs(delta - expected_bytes) * 100.0 / expected_bytes
    if diff_pct > tolerance_pct:
        raise OperationFailedError(
            "%s delta %s vs expected %s "
            "(%.1f%% > %s%%)"
            % (resolved, delta, expected_bytes, diff_pct, tolerance_pct)
        )
    log.info(
        "%s delta %s within %.1f%% of expected %s",
        resolved,
        delta,
        diff_pct,
        expected_bytes,
    )
