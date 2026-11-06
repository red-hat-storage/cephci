"""
NFS-Ganesha Prometheus — new metrics & nomenclature (TC-PROM-NEW / NOM / REG).

Automates the plan executed against Umbrella / LEGACY_METRICS=OFF builds:
uptime, export capacity/inode gauges, nomenclature mode, and regression gates.

Polarion requirement: CEPH-83633129
Per-TC Polarion IDs: CEPH-83633131 … CEPH-83633148 (NEW-0…7, NOM-0…5, REG-1…4).

Suite YAML sets ``config.polarion-id`` to one of those IDs (one suite entry per TC).
Metric names resolve via aliases for both legacy and ``nfs_``-prefixed forms.
"""

from __future__ import annotations

import time

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    create_nfs_via_file_and_verify,
    setup_nfs_cluster,
)
from tests.nfs.nfs_prometheus_stats_utils import (
    MANIFEST_METRIC_ALIASES,
    apply_prometheus_ganesha_template,
    delete_exports,
    firewall_allow_port,
    firewall_remove_port,
    get_counter_total,
    get_installer,
    get_monitoring_port,
    get_nfs_container_id,
    metric_available,
    mount_export,
    parse_metric_samples,
    parse_prometheus_metrics,
    perform_f10_mdcache_workload,
    perform_nfs_io,
    prepare_cluster_ceph_access,
    restart_nfs_container,
    restore_prometheus_ganesha_template,
    run_fio_rw,
    run_ganesha_stats,
    scrape_prometheus_metrics,
    scrape_prometheus_metrics_http_code,
    unmount_export,
    update_prometheus_ganesha_params,
    verify_mdcache_counters_increased,
    verify_metrics_endpoint_not_accessible,
    verify_monitoring_port_listening,
    verify_nfsv4_op_count_after_workload,
    wait_metrics_recovery,
)
from utility.log import Log

log = Log(__name__)

POLARION_REQUIREMENT = "CEPH-83633129"

# One Polarion testcase ID per TC-PROM-* under CEPH-83633129
POLARION_TEST_IDS = {
    "CEPH-83633131": ("NEW-0",),
    "CEPH-83633132": ("NEW-1",),
    "CEPH-83633133": ("NEW-2",),
    "CEPH-83633134": ("NEW-3",),
    "CEPH-83633135": ("NEW-4",),
    "CEPH-83633136": ("NEW-5",),
    "CEPH-83633137": ("NEW-6",),
    "CEPH-83633138": ("NEW-7",),
    "CEPH-83633139": ("NOM-0",),
    "CEPH-83633140": ("NOM-1",),
    "CEPH-83633141": ("NOM-2",),
    "CEPH-83633142": ("NOM-3",),
    "CEPH-83633143": ("NOM-4",),
    "CEPH-83633144": ("NOM-5",),
    "CEPH-83633145": ("REG-1",),
    "CEPH-83633146": ("REG-2",),
    "CEPH-83633147": ("REG-3",),
    "CEPH-83633148": ("REG-4",),
}

# Core NEW families (uptime + export capacity). Present when export metrics
# land in the Ganesha image — independent of inode gauges.
NEW_METRIC_CORE = (
    "ganesha_uptime_seconds",
    "export_total_size",
    "export_available_size",
    "export_free_bytes_percent",
)
# Inode gauges: often absent or CephFS-bogus (f_favail=-1). Gate NEW-5 only.
NEW_METRIC_INODE = (
    "export_available_file_count",
    "export_inode_utilization",
)
NEW_METRIC_CANONICAL = NEW_METRIC_CORE + NEW_METRIC_INODE


class TestNotSupported(Exception):
    """Subtest not applicable on this build / topology."""


class TestContext:
    def __init__(self, ceph_cluster, config):
        self.ceph_cluster = ceph_cluster
        self.config = config
        self.installer = get_installer(ceph_cluster)
        self.clients = ceph_cluster.get_nodes(role="client")
        self.nfs_nodes = ceph_cluster.get_nodes(role="nfs")
        if not self.clients or not self.nfs_nodes:
            raise ConfigError("Need client and nfs nodes for PROM new-metrics tests")
        self.client = self.clients[0]
        self.nfs_node = self.nfs_nodes[0]
        self.nfs_name = config.get("nfs_name", "cephfs-nfs")
        self.nfs_export = config.get("nfs_export", "/export")
        self.nfs_mount = config.get("nfs_mount", "/mnt/nfs")
        self.fs_name = config.get("fs_name", "cephfs")
        self.nfs_version = config.get("nfs_version", 4.1)
        self.nfs_port = config.get("port", 2049)
        self.nfs_ip = self.nfs_node.ip_address
        self.nfs_server = self.nfs_node.hostname
        self.monitoring_port = None
        self.container_id = None
        self.extra_exports = []
        self.extra_mounts = []
        self.template_backup_exists = None
        self.metrics_via_product = False
        self.runtime_firewall_ports = set()
        self.nomenclature_mode = {}
        self.feature_present = False
        self.inode_metrics_present = False
        self.missing_new_metrics = []
        self.export_path_filter = config.get("export_path_filter")
        self.service_wait_timeout = int(config.get("service_wait_timeout", 300))

    def nfs_placement_hosts(self, count=None):
        """Host list for NFS orch placement (supports multi-daemon REG-4)."""
        if count is None:
            count = int(self.config.get("nfs_daemon_count", 1))
        count = max(1, int(count))
        hosts = [n.hostname for n in self.nfs_nodes[:count]]
        if not hosts:
            hosts = [self.nfs_server]
        return hosts

    def nfs_metrics_spec(self, enable_metrics=True, hosts=None):
        """Orch NFS spec with product ``enable_nfs_metrics`` (IBMCEPH-14515)."""
        mon_port = int(
            self.config.get("monitoring_port") or self.monitoring_port or 9587
        )
        if hosts is None:
            hosts = self.nfs_placement_hosts()
        return {
            "service_type": "nfs",
            "service_id": self.nfs_name,
            "placement": {"hosts": list(hosts)},
            "spec": {
                "port": int(self.nfs_port),
                "monitoring_port": mon_port,
                "enable_nfs_metrics": bool(enable_metrics),
            },
        }

    def apply_nfs_metrics(self, enable_metrics=True, hosts=None):
        """Toggle Enable_Metrics via cephadm orch (not ganesha template inject)."""
        ok = create_nfs_via_file_and_verify(
            self.installer,
            [self.nfs_metrics_spec(enable_metrics=enable_metrics, hosts=hosts)],
            timeout=self.service_wait_timeout,
            nfs_nodes=self.nfs_nodes,
            service_id=self.nfs_name,
        )
        if not ok:
            raise OperationFailedError(
                "Failed to apply NFS orch spec enable_nfs_metrics=%s for %s"
                % (enable_metrics, self.nfs_name)
            )
        self.metrics_via_product = True
        self.monitoring_port = get_monitoring_port(
            self.nfs_node, self.nfs_name, self.config
        )
        self.refresh_container_id()
        return self.monitoring_port

    def remount_primary(self):
        """Remount primary export after orch redeploy."""
        try:
            unmount_export(self.client, self.nfs_mount)
        except Exception as exc:
            log.warning("Pre-remount unmount %s: %s", self.nfs_mount, exc)
        mount_export(
            self.client,
            self.nfs_ip,
            self.primary_export,
            self.nfs_mount,
            self.nfs_version,
            self.nfs_port,
        )

    def _set_primary_export(self):
        # Primary export path used by setup_nfs_cluster is ``{export}_0``
        self.primary_export = "%s_0" % self.nfs_export
        if not self.export_path_filter:
            self.export_path_filter = self.primary_export

    def _refresh_endpoints(self, wait_metrics=True):
        self.monitoring_port = get_monitoring_port(
            self.nfs_node, self.nfs_name, self.config
        )
        self.refresh_container_id()
        firewall_allow_port(self.nfs_node, self.monitoring_port)
        self.runtime_firewall_ports.add(int(self.monitoring_port))
        if wait_metrics:
            wait_metrics_recovery(
                self.client, self.nfs_ip, self.monitoring_port, timeout=180
            )

    def ensure_primary_mounted(self):
        """Mount primary export if the client mountpoint is missing."""
        out, _ = self.client.exec_command(
            sudo=True,
            cmd="mountpoint -q %s && echo mounted || echo absent" % self.nfs_mount,
            check_ec=False,
        )
        if "mounted" in str(out):
            return
        log.info("Primary mount %s absent — remounting", self.nfs_mount)
        self.remount_primary()

    def attach_existing(self, require_metrics=True):
        """Reuse suite-level NFS cluster/export (no create/delete)."""
        prepare_cluster_ceph_access(
            self.ceph_cluster,
            self.installer,
            clients=[self.client],
            nfs_nodes=self.nfs_nodes,
        )
        self._set_primary_export()
        self.monitoring_port = get_monitoring_port(
            self.nfs_node, self.nfs_name, self.config
        )
        self.refresh_container_id()
        if require_metrics:
            firewall_allow_port(self.nfs_node, self.monitoring_port)
            self.runtime_firewall_ports.add(int(self.monitoring_port))
            self.ensure_primary_mounted()
            wait_metrics_recovery(
                self.client, self.nfs_ip, self.monitoring_port, timeout=180
            )
        log.info(
            "Attached existing NFS %s export=%s mount=%s metrics_port=%s",
            self.nfs_name,
            self.primary_export,
            self.nfs_mount,
            self.monitoring_port,
        )

    def setup_cluster(self):
        setup_nfs_cluster(
            clients=[self.client],
            nfs_server=self.nfs_server,
            port=self.nfs_port,
            version=self.nfs_version,
            nfs_name=self.nfs_name,
            nfs_mount=self.nfs_mount,
            fs_name=self.fs_name,
            export=self.nfs_export,
            fs=self.fs_name,
            ceph_cluster=self.ceph_cluster,
        )
        prepare_cluster_ceph_access(
            self.ceph_cluster,
            self.installer,
            clients=[self.client],
            nfs_nodes=self.nfs_nodes,
        )
        # Prefer product path (ceph nfs create --enable-nfs-metrics /
        # orch enable_nfs_metrics). Template inject is legacy opt-in only.
        use_template = bool(self.config.get("configure_ganesha_metrics", False))
        enable_product = self.config.get("enable_nfs_metrics")
        if enable_product is None:
            enable_product = not use_template
        self._set_primary_export()

        if use_template:
            self.template_backup_exists = apply_prometheus_ganesha_template(
                self.installer, self.nfs_node, self.nfs_name, self.config
            )
        elif enable_product:
            self.apply_nfs_metrics(enable_metrics=True)
            # Orch redeploy drops client mounts; remount for IO-backed TCs.
            self.remount_primary()
        self._refresh_endpoints(wait_metrics=bool(enable_product or use_template))

    def refresh_container_id(self):
        self.container_id = get_nfs_container_id(
            self.installer, self.nfs_name, self.nfs_node
        )

    def cleanup_extras(self):
        """Drop ephemeral exports/mounts created by a single TC (e.g. NEW-7)."""
        for client, mount in list(self.extra_mounts):
            try:
                unmount_export(client, mount)
            except Exception as exc:
                log.warning("Unmount %s: %s", mount, exc)
        self.extra_mounts.clear()
        if self.extra_exports:
            try:
                delete_exports(self.nfs_node, self.nfs_name, self.extra_exports)
            except Exception as exc:
                log.warning("Delete extra exports %s: %s", self.extra_exports, exc)
            self.extra_exports.clear()

    def scrape(self, double=True):
        """Scrape /metrics; double scrape refreshes dynamic export gauges."""
        body = scrape_prometheus_metrics(self.client, self.nfs_ip, self.monitoring_port)
        if double:
            time.sleep(1)
            body = scrape_prometheus_metrics(
                self.client, self.nfs_ip, self.monitoring_port
            )
        return body

    def cleanup(self):
        self.cleanup_extras()
        for port in list(self.runtime_firewall_ports):
            try:
                firewall_remove_port(self.nfs_node, port)
            except Exception as exc:
                log.warning("Firewall cleanup %s: %s", port, exc)
        self.runtime_firewall_ports.clear()
        if self.template_backup_exists is not None:
            try:
                restore_prometheus_ganesha_template(
                    self.installer,
                    self.nfs_node,
                    self.nfs_name,
                    self.template_backup_exists,
                    self.config,
                )
            except Exception as exc:
                log.warning("Failed to restore ganesha template: %s", exc)
        cleanup_cluster(
            self.client,
            self.nfs_mount,
            self.nfs_name,
            self.nfs_export,
            nfs_nodes=self.nfs_node,
        )


def _alias_names(metric_name):
    names = list(MANIFEST_METRIC_ALIASES.get(metric_name, (metric_name,)))
    if metric_name not in names:
        names.insert(0, metric_name)
    return tuple(dict.fromkeys(names))


def _gauge_samples(metrics_text, metric_name, path_substr=None):
    """Return list of (labels, value) for gauge family aliases."""
    wanted = set(_alias_names(metric_name))
    out = []
    for name, labels, value in parse_metric_samples(metrics_text):
        if name not in wanted:
            continue
        if path_substr:
            path = labels.get("path") or labels.get("export_path") or ""
            if path_substr not in path and path_substr not in str(labels):
                continue
        out.append((labels, float(value)))
    return out


def get_gauge(metrics_text, metric_name, path_substr=None):
    samples = _gauge_samples(metrics_text, metric_name, path_substr=path_substr)
    if not samples:
        raise OperationFailedError(
            "Gauge %s not found (path_filter=%s)" % (metric_name, path_substr)
        )
    return samples[0][1]


def _probe_feature(ctx):
    """Probe NEW metric families; core vs inode tracked separately.

    ``feature_present`` means uptime + export *capacity* gauges are scrapeable.
    Inode gauges (NEW-5) are optional — often missing or CephFS-bogus downstream.
    """
    body = ctx.scrape()
    names = parse_prometheus_metrics(body)
    absent_core = [m for m in NEW_METRIC_CORE if not metric_available(names, m)]
    absent_inode = [m for m in NEW_METRIC_INODE if not metric_available(names, m)]
    ctx.missing_new_metrics = absent_core + absent_inode
    ctx.feature_present = len(absent_core) == 0
    ctx.inode_metrics_present = len(absent_inode) == 0
    if absent_core or absent_inode:
        log.warning(
            "Feature probe missing core=%s inode=%s",
            absent_core or "[]",
            absent_inode or "[]",
        )
    return ctx.feature_present


def _require_feature(ctx, need_inode=False):
    if not ctx.feature_present:
        _probe_feature(ctx)
    if not ctx.feature_present:
        raise TestNotSupported(
            "NEW capacity/uptime metrics absent on this build "
            "(missing=%s)" % (ctx.missing_new_metrics or "core")
        )
    if need_inode and not ctx.inode_metrics_present:
        raise TestNotSupported(
            "NEW inode metrics absent on this scrape "
            "(export_available_file_count / export_inode_utilization); "
            "capacity/uptime metrics are present"
        )


def test_new_0(ctx):
    log.info("NEW-0: feature gate — probe NEW metric families (core + inode)")
    verify_monitoring_port_listening(ctx.nfs_node, ctx.monitoring_port)
    _probe_feature(ctx)
    body = ctx.scrape()
    names = parse_prometheus_metrics(body)
    present = [m for m in NEW_METRIC_CANONICAL if metric_available(names, m)]
    absent = list(ctx.missing_new_metrics)
    log.info("NEW-0 present=%s absent=%s", present, absent)
    absent_core = [m for m in absent if m in NEW_METRIC_CORE]
    absent_inode = [m for m in absent if m in NEW_METRIC_INODE]
    if absent_core:
        if ctx.config.get("require_new_metrics", False):
            raise OperationFailedError(
                "NEW-0: required core NEW metrics missing: %s" % absent_core
            )
        log.warning(
            "NEW-0: core feature incomplete (absent=%s); NEW-1…4/6/7 will SKIP",
            absent_core,
        )
        return
    if absent_inode:
        log.warning(
            "NEW-0: inode gauges absent (%s); NEW-5 will SKIP, "
            "capacity/uptime cases still run",
            absent_inode,
        )
    log.info(
        "NEW-0: PASS — core families present (inode_present=%s)",
        ctx.inode_metrics_present,
    )


def test_new_1(ctx):
    _require_feature(ctx)
    log.info("NEW-1: uptime increases")
    u0 = get_gauge(ctx.scrape(), "ganesha_uptime_seconds")
    time.sleep(int(ctx.config.get("uptime_wait_seconds", 20)))
    u1 = get_gauge(ctx.scrape(), "ganesha_uptime_seconds")
    log.info("uptime %s -> %s", u0, u1)
    if not (u1 > u0):
        raise OperationFailedError("uptime did not increase (%s -> %s)" % (u0, u1))
    log.info("NEW-1: PASS")


def test_new_2(ctx):
    _require_feature(ctx)
    log.info("NEW-2: uptime resets after NFS restart")
    u0 = get_gauge(ctx.scrape(), "ganesha_uptime_seconds")
    if u0 < 5:
        time.sleep(10)
        u0 = get_gauge(ctx.scrape(), "ganesha_uptime_seconds")
    ctx.refresh_container_id()
    if not ctx.container_id:
        raise OperationFailedError("No NFS container_id for restart")
    restart_nfs_container(ctx.nfs_node, ctx.container_id)
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=180)
    ctx.refresh_container_id()
    ctx.ensure_primary_mounted()
    u1 = get_gauge(ctx.scrape(), "ganesha_uptime_seconds")
    max_after = float(ctx.config.get("uptime_reset_max_seconds", 120))
    log.info("uptime before=%s after_restart=%s (max=%s)", u0, u1, max_after)
    if not (u1 < u0 and u1 < max_after):
        raise OperationFailedError(
            "uptime did not reset after restart (%s -> %s)" % (u0, u1)
        )
    perform_nfs_io(ctx.client, ctx.nfs_mount, file_name="new2_post_restart", mb=1)
    log.info("NEW-2: PASS")


def test_new_3(ctx):
    _require_feature(ctx)
    log.info("NEW-3: export size metrics present")
    body = ctx.scrape()
    path = ctx.export_path_filter
    total = get_gauge(body, "export_total_size", path_substr=path)
    avail = get_gauge(body, "export_available_size", path_substr=path)
    pct = get_gauge(body, "export_free_bytes_percent", path_substr=path)
    log.info("total=%s avail=%s free_pct=%s path~%s", total, avail, pct, path)
    if total <= 0:
        raise OperationFailedError("export_total_size <= 0")
    if not (0 <= avail <= total):
        raise OperationFailedError("available_size out of range")
    if not (0 <= pct <= 100):
        raise OperationFailedError("free_bytes_percent out of [0,100]")
    log.info("NEW-3: PASS")


def test_new_4(ctx):
    _require_feature(ctx)
    log.info("NEW-4: capacity reacts to fill/delete")
    path = ctx.export_path_filter
    body = ctx.scrape()
    a0 = get_gauge(body, "export_available_size", path_substr=path)
    p0 = get_gauge(body, "export_free_bytes_percent", path_substr=path)
    t0 = get_gauge(body, "export_total_size", path_substr=path)
    fill_mb = int(ctx.config.get("new4_fill_mb", 1024))
    fill_path = "%s/fill_new4.bin" % ctx.nfs_mount
    ctx.client.exec_command(
        sudo=True,
        cmd=(
            "dd if=/dev/zero of=%s bs=1M count=%s conv=fsync status=none; sync"
            % (fill_path, fill_mb)
        ),
        timeout=max(600, fill_mb),
    )
    time.sleep(3)
    body1 = ctx.scrape()
    a1 = get_gauge(body1, "export_available_size", path_substr=path)
    p1 = get_gauge(body1, "export_free_bytes_percent", path_substr=path)
    t1 = get_gauge(body1, "export_total_size", path_substr=path)
    log.info("after_fill avail %s->%s pct %s->%s total %s->%s", a0, a1, p0, p1, t0, t1)
    if not (a1 < a0):
        raise OperationFailedError("available_size did not decrease after fill")
    if not (p1 <= p0):
        raise OperationFailedError("free_bytes_percent increased after fill")
    # CephFS pool total can drift — warn only unless strict mode
    if t0 != t1:
        log.warning(
            "NEW-4: total_size drifted %s -> %s (CephFS shared pool; non-strict)",
            t0,
            t1,
        )
        if ctx.config.get("new4_require_stable_total", False):
            raise OperationFailedError("total_size not stable across fill")

    ctx.client.exec_command(sudo=True, cmd="rm -f %s; sync" % fill_path)
    recovered = False
    for i in range(int(ctx.config.get("new4_reclaim_tries", 6))):
        time.sleep(5)
        a2 = get_gauge(ctx.scrape(), "export_available_size", path_substr=path)
        log.info("after_delete try=%s avail=%s", i + 1, a2)
        if a2 > a1:
            recovered = True
            break
    if not recovered:
        log.warning("NEW-4: reclaim did not raise available within retry window")
        if ctx.config.get("new4_require_reclaim", True):
            raise OperationFailedError("available_size did not recover after delete")
    log.info("NEW-4: PASS (recovered=%s)", recovered)


def test_new_5(ctx):
    _require_feature(ctx, need_inode=True)
    log.info("NEW-5: inode metrics react to mass create")
    path = ctx.export_path_filter
    body = ctx.scrape()
    f0 = get_gauge(body, "export_available_file_count", path_substr=path)
    i0 = get_gauge(body, "export_inode_utilization", path_substr=path)
    count = int(ctx.config.get("new5_file_count", 3000))
    ctx.client.exec_command(
        sudo=True,
        cmd=(
            "for i in $(seq 1 %s); do touch %s/inode_$i; done; sync"
            % (count, ctx.nfs_mount)
        ),
        timeout=max(600, count // 2),
    )
    time.sleep(3)
    body1 = ctx.scrape()
    f1 = get_gauge(body1, "export_available_file_count", path_substr=path)
    i1 = get_gauge(body1, "export_inode_utilization", path_substr=path)
    log.info("files %s->%s inode_util %s->%s", f0, f1, i0, i1)
    if f0 < 0 or f1 < 0:
        # Known CephFS f_favail=-1 behavior
        if ctx.config.get("waive_cephfs_inode_unsupported", True):
            raise TestNotSupported(
                "CephFS reports available_file_count=%s (unsupported inode free); "
                "inode_util %s->%s" % (f0 if f0 < 0 else f1, i0, i1)
            )
        raise OperationFailedError(
            "available_file_count negative (%s -> %s)" % (f0, f1)
        )
    if not (f1 < f0):
        raise OperationFailedError("available_file_count did not decrease")
    if not (i1 >= i0 and 0 <= i1 <= 100):
        raise OperationFailedError("inode_utilization did not increase sanely")
    log.info("NEW-5: PASS")


def test_new_6(ctx):
    _require_feature(ctx)
    log.info("NEW-6: percent consistency")
    path = ctx.export_path_filter
    body = ctx.scrape()
    total = get_gauge(body, "export_total_size", path_substr=path)
    avail = get_gauge(body, "export_available_size", path_substr=path)
    pct = get_gauge(body, "export_free_bytes_percent", path_substr=path)
    if total <= 0:
        raise OperationFailedError("total_size <= 0")
    expected = round(100.0 * avail / total)
    tol = float(ctx.config.get("new6_percent_tolerance", 2))
    log.info("reported_pct=%s expected~%s (tol=%s)", pct, expected, tol)
    if abs(pct - expected) > tol:
        raise OperationFailedError(
            "free_bytes_percent %s vs expected %s (tol %s)" % (pct, expected, tol)
        )
    if not (0 <= pct <= 100):
        raise OperationFailedError("percent out of range")
    log.info("NEW-6: PASS")


def test_new_7(ctx):
    _require_feature(ctx)
    log.info("NEW-7: multi-export attribution")
    export_b = "%s_prom_b" % ctx.nfs_export
    mount_b = "%s_b" % ctx.nfs_mount
    try:
        Ceph(ctx.nfs_node).nfs.export.create(
            fs_name=ctx.fs_name,
            nfs_name=ctx.nfs_name,
            nfs_export=export_b,
            fs=ctx.fs_name,
        )
        ctx.extra_exports.append(export_b)
        mount_export(
            ctx.client,
            ctx.nfs_server,
            export_b,
            mount_b,
            ctx.nfs_version,
            ctx.nfs_port,
        )
        ctx.extra_mounts.append((ctx.client, mount_b))

        body0 = ctx.scrape()
        a1_0 = get_gauge(body0, "export_available_size", path_substr=ctx.primary_export)
        # second export path label is the export pseudo
        a2_0 = get_gauge(body0, "export_available_size", path_substr=export_b)
        fill_mb = int(ctx.config.get("new7_fill_mb", 512))
        ctx.client.exec_command(
            sudo=True,
            cmd=(
                "dd if=/dev/zero of=%s/only_a.bin bs=1M count=%s conv=fsync "
                "status=none; sync" % (ctx.nfs_mount, fill_mb)
            ),
            timeout=max(300, fill_mb),
        )
        time.sleep(3)
        body1 = ctx.scrape()
        a1_1 = get_gauge(body1, "export_available_size", path_substr=ctx.primary_export)
        a2_1 = get_gauge(body1, "export_available_size", path_substr=export_b)
        d1 = a1_0 - a1_1
        d2 = a2_0 - a2_1
        log.info(
            "expA %s->%s (d=%s); expB %s->%s (d=%s)", a1_0, a1_1, d1, a2_0, a2_1, d2
        )
        # Shared CephFS pool: both may drop; require distinct series + A delta >= 0
        if d1 < 0:
            raise OperationFailedError("export A available increased after write on A")
        log.info(
            "NEW-7: PASS (distinct series; shared-pool deltas may correlate "
            "d1=%s d2=%s)",
            d1,
            d2,
        )
    finally:
        # Shared-suite mode: drop ephemeral export so later TCs stay clean.
        ctx.cleanup_extras()


def _detect_mode(names, legacy_keys, new_keys):
    has_legacy = any(metric_available(names, k) for k in legacy_keys)
    has_new = any(metric_available(names, k) for k in new_keys)
    if has_legacy and has_new:
        return "dual"
    if has_new:
        return "new-only"
    if has_legacy:
        return "legacy-only"
    return "absent"


def test_nom_0(ctx):
    log.info("NOM-0: nomenclature mode detection")
    names = parse_prometheus_metrics(ctx.scrape())
    modes = {
        "mdcache": _detect_mode(
            names,
            ("mdcache_cache_misses_total", "mdcache_cache_hits_total"),
            (
                "nfs_mdcache_misses_total",
                "nfs_mdcache_hits_total",
                "mdcache_misses_total",
            ),
        ),
        "nfs_v4": _detect_mode(
            names,
            ("nfsv4__op_count",),
            ("nfs_v4_op_count",),
        ),
        "nfs_prefix": (
            "new-only" if any(n.startswith("nfs_") for n in names) else "legacy-only"
        ),
    }
    ctx.nomenclature_mode = modes
    log.info("NOM-0 modes: %s", modes)
    if all(v == "absent" for v in (modes["mdcache"], modes["nfs_v4"])):
        raise OperationFailedError("NOM-0: no mdcache/nfs_v4 families found")
    log.info("NOM-0: PASS")


def test_nom_1(ctx):
    log.info("NOM-1: mdcache counters move under workload")
    before = ctx.scrape()
    perform_f10_mdcache_workload(
        ctx.client,
        ctx.nfs_mount,
        file_count=int(ctx.config.get("nom1_file_count", 200)),
    )
    time.sleep(2)
    after = ctx.scrape()
    verify_mdcache_counters_increased(before, after)
    log.info("NOM-1: PASS")


def test_nom_2(ctx):
    log.info("NOM-2: nfs_ prefix / critical families present")
    body = ctx.scrape()
    names = parse_prometheus_metrics(body)
    # Standalone suite entries do not share NOM-0 state — detect mode live.
    if not ctx.nomenclature_mode:
        test_nom_0(ctx)
    mode = (ctx.nomenclature_mode or {}).get("nfs_prefix", "unknown")
    # requests family: product may ship nfs_requests_total OR rpcs_received_total
    critical = (
        "nfs_requests_total",
        "mdcache_cache_misses_total",
        "nfsv4__op_count",
    )
    missing = [m for m in critical if not metric_available(names, m)]
    if missing:
        sample = sorted(n for n in names if "rpc" in n or "request" in n)[:20]
        raise OperationFailedError(
            "NOM-2 critical families missing via aliases: %s (mode=%s); "
            "request/rpc sample=%s" % (missing, mode, sample)
        )
    # On new-only builds, legacy mdcache_cache_ / nfsv4__ should be gone
    if mode == "new-only" or (ctx.nomenclature_mode or {}).get("mdcache") == "new-only":
        legacy_left = [
            n
            for n in names
            if n.startswith("mdcache_cache_") or n.startswith("nfsv4__")
        ]
        if legacy_left and ctx.config.get("nom2_forbid_legacy_on_new_only", False):
            raise OperationFailedError(
                "NOM-2 new-only still exports legacy names: %s" % legacy_left[:10]
            )
        log.info(
            "NOM-2 new-only: legacy leftover count=%s",
            len(legacy_left),
        )
    log.info("NOM-2: PASS (mode=%s)", mode)


def test_nom_3(ctx):
    log.info("NOM-3: nfs_v4 / nfsv4 op_count increases after R/W")
    before = ctx.scrape()
    run_fio_rw(
        ctx.client,
        ctx.nfs_mount,
        size=ctx.config.get("nom3_fio_size", "64M"),
        runtime=int(ctx.config.get("nom3_fio_runtime", 30)),
        rw="randrw",
    )
    time.sleep(2)
    after = ctx.scrape()
    verify_nfsv4_op_count_after_workload(before, after)
    log.info("NOM-3: PASS")


def test_nom_4(ctx):
    log.info("NOM-4: dual-mode consistency")
    if not ctx.nomenclature_mode:
        test_nom_0(ctx)
    modes = ctx.nomenclature_mode or {}
    if modes.get("mdcache") != "dual" and modes.get("nfs_v4") != "dual":
        raise TestNotSupported("NOM-4 N/A — nomenclature mode is not dual (%s)" % modes)
    body = ctx.scrape()
    names = parse_prometheus_metrics(body)
    if "nfs_mdcache_misses_total" in names and "mdcache_cache_misses_total" in names:
        leg = sum(
            v
            for n, _, v in parse_metric_samples(body)
            if n == "mdcache_cache_misses_total"
        )
        neu = sum(
            v
            for n, _, v in parse_metric_samples(body)
            if n == "nfs_mdcache_misses_total"
        )
        tol = float(ctx.config.get("nom4_tolerance_pct", 5))
        if leg <= 0:
            raise TestNotSupported("NOM-4 dual pair has zero legacy miss total")
        drift = abs(leg - neu) * 100.0 / leg
        log.info("dual mdcache misses legacy=%s new=%s drift=%.2f%%", leg, neu, drift)
        if drift > tol:
            raise OperationFailedError(
                "dual mdcache miss totals diverge %.2f%% > %s%%" % (drift, tol)
            )
    else:
        legacy = get_counter_total(body, "mdcache_cache_misses_total")
        log.info("NOM-4 dual theme present; alias sample legacy misses=%s", legacy)
    log.info("NOM-4: PASS")


def test_nom_5(ctx):
    raise TestNotSupported(
        "NOM-5 upgrade/compat window requires dedicated N-1→N upgrade suite"
    )


def test_reg_1(ctx):
    log.info("REG-1: Enable_Metrics gates /metrics (orch enable_nfs_metrics)")
    verify_monitoring_port_listening(ctx.nfs_node, ctx.monitoring_port)
    if (
        scrape_prometheus_metrics_http_code(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
        != "200"
    ):
        raise OperationFailedError("metrics not HTTP 200 before disable")

    use_template = bool(ctx.config.get("configure_ganesha_metrics", False))

    def _remount():
        try:
            unmount_export(ctx.client, ctx.nfs_mount)
        except Exception as exc:
            log.warning("REG-1 unmount %s: %s", ctx.nfs_mount, exc)
        mount_export(
            ctx.client,
            ctx.nfs_ip,
            ctx.primary_export,
            ctx.nfs_mount,
            ctx.nfs_version,
            ctx.nfs_port,
        )

    try:
        if use_template:
            metrics_off = dict(ctx.config)
            metrics_off["enable_metrics"] = False
            update_prometheus_ganesha_params(
                ctx.installer, ctx.nfs_node, ctx.nfs_name, metrics_off
            )
            ctx.refresh_container_id()
        else:
            ctx.apply_nfs_metrics(enable_metrics=False)
            _remount()
        verify_metrics_endpoint_not_accessible(
            ctx.client, ctx.nfs_node, ctx.nfs_ip, ctx.monitoring_port
        )
        perform_nfs_io(ctx.client, ctx.nfs_mount, file_name="reg1_metrics_off", mb=1)
    finally:
        if use_template:
            metrics_on = dict(ctx.config)
            metrics_on["enable_metrics"] = True
            update_prometheus_ganesha_params(
                ctx.installer, ctx.nfs_node, ctx.nfs_name, metrics_on
            )
            ctx.refresh_container_id()
        else:
            ctx.apply_nfs_metrics(enable_metrics=True)
            _remount()
        wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=180)
    if (
        scrape_prometheus_metrics_http_code(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
        != "200"
    ):
        raise OperationFailedError("metrics did not recover after re-enable")
    log.info("REG-1: PASS")


def test_reg_2(ctx):
    raise TestNotSupported(
        "REG-2: run suites/tentacle/nfs/tier2-nfs-prometheus-stats.yaml "
        "(aliases already extended for new names)"
    )


def test_reg_3(ctx):
    log.info("REG-3: ganesha_stats coexistence")
    ctx.refresh_container_id()
    if not ctx.container_id:
        raise OperationFailedError("No container_id for ganesha_stats")
    out, err = run_ganesha_stats(ctx.nfs_node, ctx.container_id, "display")
    combined = "%s\n%s" % (out, err)
    if not combined.strip() or "not found" in combined.lower():
        raise TestNotSupported(
            "ganesha_stats not available in NFS container: %s" % combined.strip()[:200]
        )
    if (
        scrape_prometheus_metrics_http_code(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
        != "200"
    ):
        raise OperationFailedError("/metrics not HTTP 200 alongside ganesha_stats")
    log.info("REG-3: PASS")


def test_reg_4(ctx):
    log.info("REG-4: multi-daemon scrape")
    import json

    from cli.cephadm.cephadm import CephAdm

    if not ctx.feature_present:
        _probe_feature(ctx)

    want = int(ctx.config.get("nfs_daemon_count", 2))
    if len(ctx.nfs_nodes) < want:
        raise TestNotSupported(
            "REG-4 needs >=%s NFS-role nodes (found %s)" % (want, len(ctx.nfs_nodes))
        )

    hosts = ctx.nfs_placement_hosts(count=want)
    log.info("REG-4: scaling NFS placement to hosts=%s", hosts)
    ctx.apply_nfs_metrics(enable_metrics=True, hosts=hosts)
    ctx.remount_primary()
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=180)

    deadline = time.time() + int(ctx.config.get("reg4_daemon_wait", 180))
    daemons = []
    while time.time() < deadline:
        raw = CephAdm(ctx.installer).ceph.orch.ps(
            service_name="nfs.%s" % ctx.nfs_name, format="json"
        )
        daemons = json.loads(raw) if raw else []
        if len(daemons) >= want:
            break
        time.sleep(5)
    if len(daemons) < 2:
        raise OperationFailedError(
            "REG-4 expected >=2 NFS daemons after placement %s (found %s)"
            % (hosts, len(daemons))
        )
    uptimes = []
    for daemon in daemons:
        host = (daemon.get("hostname") or "").split(".")[0]
        node = next(
            (
                n
                for n in ctx.nfs_nodes
                if n.hostname.split(".")[0] == host or host in n.hostname
            ),
            None,
        )
        if not node:
            raise OperationFailedError("No node for daemon host %s" % host)
        firewall_allow_port(node, ctx.monitoring_port)
        ctx.runtime_firewall_ports.add(int(ctx.monitoring_port))
        code = scrape_prometheus_metrics_http_code(
            ctx.client, node.ip_address, ctx.monitoring_port
        )
        if code != "200":
            raise OperationFailedError("daemon %s metrics HTTP %s" % (host, code))
        body = scrape_prometheus_metrics(
            ctx.client, node.ip_address, ctx.monitoring_port
        )
        # double scrape for export gauges
        time.sleep(1)
        body = scrape_prometheus_metrics(
            ctx.client, node.ip_address, ctx.monitoring_port
        )
        u = get_gauge(body, "ganesha_uptime_seconds")
        uptimes.append(u)
        names = parse_prometheus_metrics(body)
        if ctx.feature_present and not metric_available(names, "export_total_size"):
            raise OperationFailedError("export_total_size missing on daemon %s" % host)
    log.info("REG-4 uptimes per daemon: %s", uptimes)
    # Restore single-host placement for suite cleanup / later reuse.
    try:
        ctx.apply_nfs_metrics(
            enable_metrics=True, hosts=ctx.nfs_placement_hosts(count=1)
        )
        ctx.remount_primary()
    except Exception as exc:
        log.warning("REG-4 restore single-host placement: %s", exc)
    log.info("REG-4: PASS")


TEST_HANDLERS = {
    "NEW-0": test_new_0,
    "NEW-1": test_new_1,
    "NEW-2": test_new_2,
    "NEW-3": test_new_3,
    "NEW-4": test_new_4,
    "NEW-5": test_new_5,
    "NEW-6": test_new_6,
    "NEW-7": test_new_7,
    "NOM-0": test_nom_0,
    "NOM-1": test_nom_1,
    "NOM-2": test_nom_2,
    "NOM-3": test_nom_3,
    "NOM-4": test_nom_4,
    "NOM-5": test_nom_5,
    "REG-1": test_reg_1,
    "REG-2": test_reg_2,
    "REG-3": test_reg_3,
    "REG-4": test_reg_4,
}


def _resolve_test_ids(config):
    polarion_id = (
        str(config.get("polarion-id") or config.get("polarion_id") or "")
        .split(",")[0]
        .strip()
    )
    if not polarion_id:
        raise ConfigError(
            "polarion-id is required in test config "
            "(e.g. polarion-id: CEPH-83633131)"
        )
    test_ids = POLARION_TEST_IDS.get(polarion_id)
    if not test_ids:
        raise ConfigError(
            "Unknown polarion-id for PROM new-metrics: %s "
            "(requirement %s; expected one of CEPH-83633131…83633148)"
            % (polarion_id, POLARION_REQUIREMENT)
        )
    return polarion_id, list(test_ids)


def _config_flag(config, key, default=True):
    """Parse YAML bool-ish flags (true/false/yes/no/1/0)."""
    if key not in config or config.get(key) is None:
        return default
    value = config.get(key)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def run(ceph_cluster, **kw):
    config = kw.get("config", {}) or {}
    action = str(config.get("action") or "").strip().lower()
    ctx = TestContext(ceph_cluster, config)

    # Suite bookends: one shared NFS deploy / one shared teardown.
    if action == "setup":
        try:
            log.info("PROM new-metrics shared setup — creating NFS + export")
            ctx.setup_cluster()
            log.info("PROM new-metrics shared setup: PASS")
            return 0
        except (ConfigError, OperationFailedError) as exc:
            log.error("PROM new-metrics shared setup FAIL: %s", exc)
            try:
                ctx.cleanup()
            except Exception as cleanup_error:
                log.warning("Setup-failure cleanup: %s", cleanup_error)
            return 1

    if action == "cleanup":
        try:
            log.info("PROM new-metrics shared cleanup — deleting NFS + export")
            ctx.attach_existing(require_metrics=False)
            ctx.cleanup()
            log.info("PROM new-metrics shared cleanup: PASS")
            return 0
        except Exception as exc:
            log.warning("PROM new-metrics shared cleanup error: %s", exc)
            try:
                ctx.cleanup()
            except Exception as cleanup_error:
                log.warning("Forced cleanup: %s", cleanup_error)
            return 0

    polarion_id, test_ids = _resolve_test_ids(config)
    do_setup = _config_flag(config, "setup_nfs", True)
    do_cleanup = _config_flag(config, "cleanup_nfs", True)
    try:
        if do_setup:
            ctx.setup_cluster()
        else:
            ctx.attach_existing(require_metrics=True)
        log.info(
            "Running PROM new-metrics Polarion %s (%s) setup_nfs=%s cleanup_nfs=%s",
            polarion_id,
            ", ".join(test_ids),
            do_setup,
            do_cleanup,
        )
        passed, skipped = [], []
        for test_id in test_ids:
            handler = TEST_HANDLERS.get(test_id)
            if not handler:
                raise ConfigError("Unknown test_id: %s" % test_id)
            log.info("=== %s ===", test_id)
            try:
                handler(ctx)
                passed.append(test_id)
                log.info("%s: PASS", test_id)
            except TestNotSupported as exc:
                skipped.append(test_id)
                log.info("%s: SKIPPED — %s", test_id, exc)
            except (ConfigError, OperationFailedError) as exc:
                log.error("%s: FAIL — %s", test_id, exc)
                log.error(
                    "PROM new-metrics EXIT 1: passed=%s skipped=%s failed=%s",
                    passed,
                    skipped,
                    test_id,
                )
                return 1
        if not passed and skipped:
            log.info(
                "PROM new-metrics EXIT skipped: all %s subtests SKIPPED: %s",
                len(skipped),
                skipped,
            )
            return -1
        log.info(
            "PROM new-metrics EXIT 0: %s passed, %s skipped: passed=%s skipped=%s",
            len(passed),
            len(skipped),
            passed,
            skipped,
        )
        return 0
    finally:
        if do_cleanup:
            try:
                ctx.cleanup()
            except Exception as cleanup_error:
                log.warning("Cleanup error: %s", cleanup_error)
        else:
            # Soft per-TC hygiene only — leave shared NFS for the next entry.
            try:
                ctx.cleanup_extras()
            except Exception as cleanup_error:
                log.warning("Extra cleanup error: %s", cleanup_error)
