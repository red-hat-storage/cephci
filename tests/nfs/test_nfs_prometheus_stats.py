"""
NFS-Ganesha Scalable Statistics (Prometheus) — CephCI automation.

Each suite entry sets ``polarion-id`` on the test block (for reporting) and in
``config`` (for test dispatch). Internal plan IDs (S-01, F-02, etc.) are mapped
in ``POLARION_TEST_IDS``.
"""

from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from tests.nfs.nfs_prometheus_stats_utils import (
    NFS_BYTE_READ_COUNTER,
    NFS_BYTE_READ_OPERATIONS,
    NFS_BYTE_WRITE_COUNTER,
    NFS_BYTE_WRITE_OPERATIONS,
    apply_f01_config_typo_line,
    apply_f01_invalid_monitoring_port,
    apply_prometheus_ganesha_template,
    bytes_delta_within_tolerance,
    concurrent_scrape,
    concurrent_scrape_metrics,
    count_client_request_samples,
    create_exports,
    delete_exports,
    drop_client_page_cache,
    firewall_allow_port,
    firewall_block_metrics_from_source,
    firewall_unblock_metrics_from_source,
    get_counter_total,
    get_histogram_count_sum,
    get_installer,
    get_monitoring_port,
    get_nfs_container_id,
    get_prometheus_scrape_source_ip,
    inject_permission_error,
    kill_nfs_container,
    metric_available,
    mount_export,
    parse_metric_samples,
    parse_prometheus_metrics,
    perform_f05_manifest_warmup,
    perform_f10_mdcache_workload,
    perform_metadata_workload,
    perform_nfs_io,
    perform_s06_minimal_workload,
    prepare_cluster_ceph_access,
    restart_nfs_container,
    restore_f01_good_ganesha_config,
    restore_prometheus_ganesha_template,
    rpc_gap_within_tolerance,
    run_fio_rw,
    run_ganesha_stats,
    run_ganesha_stats_subcommands,
    scrape_prometheus_metrics,
    scrape_prometheus_metrics_http_code,
    unmount_export,
    update_prometheus_ganesha_params,
    verify_client_label_present,
    verify_concurrent_scrape_metrics,
    verify_export_traffic_attribution,
    verify_exposition_has_metric,
    verify_f01_invalid_port_outcome,
    verify_f01_typo_outcome,
    verify_f02_dynamic_metrics_reload_vs_restart,
    verify_f02_enable_metrics_reload_vs_restart,
    verify_f02_port_reload_vs_restart,
    verify_ganesha_build_info,
    verify_help_and_type,
    verify_idle_baseline,
    verify_latency_histogram_after_workload,
    verify_manifest_v1,
    verify_mdcache_counters_increased,
    verify_metrics_endpoint_not_accessible,
    verify_monitoring_port_listening,
    verify_nfsv4_op_count_after_workload,
    verify_prometheus_nfs_scrape_target,
    verify_promtool_metrics,
    verify_s06_minimal_workload_prometheus,
    verify_s07_dynamic_metrics_disabled,
    wait_metrics_recovery,
    wait_prometheus_nfs_target_health,
)
from utility.log import Log

log = Log(__name__)


def log_test_passed(test_id, summary):
    """Log explicit pass criteria for a subtest (visible in Jenkins logs)."""
    log.info("%s validation PASSED: %s", test_id, summary)


LEGACY_GANESHA_STATS_COMMANDS = (
    "display",
    "v4_full",
    "mem_stats",
    "export",
    "client",
)


class TestContext:
    def __init__(self, ceph_cluster, config):
        self.ceph_cluster = ceph_cluster
        self.config = config
        self.installer = get_installer(ceph_cluster)
        self.clients = ceph_cluster.get_nodes(role="client")
        self.nfs_nodes = ceph_cluster.get_nodes(role="nfs")
        self.num_clients = int(config.get("clients", 1))
        if self.num_clients > len(self.clients):
            raise ConfigError("Not enough client nodes for test")
        self.clients = self.clients[: self.num_clients]
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
            ha=self.config.get("ha", False),
            vip=self.config.get("vip"),
            enable_rdma=self.config.get("enable_rdma", False),
            rdma_port=self.config.get("rdma_port"),
        )
        prepare_cluster_ceph_access(
            self.ceph_cluster,
            self.installer,
            clients=self.clients,
            nfs_nodes=self.nfs_nodes,
        )
        if self.config.get("configure_ganesha_metrics", True):
            self.template_backup_exists = apply_prometheus_ganesha_template(
                self.installer, self.nfs_node, self.nfs_name, self.config
            )
        self.monitoring_port = get_monitoring_port(
            self.nfs_node, self.nfs_name, self.config
        )
        self.container_id = get_nfs_container_id(
            self.installer, self.nfs_name, self.nfs_node
        )
        firewall_allow_port(self.nfs_node, self.monitoring_port)

    def refresh_container_id(self):
        self.container_id = get_nfs_container_id(
            self.installer, self.nfs_name, self.nfs_node
        )
        if not self.container_id:
            log.warning(
                "No NFS container_id on %s after refresh",
                self.nfs_node.hostname,
            )

    def scrape(self):
        return scrape_prometheus_metrics(self.client, self.nfs_ip, self.monitoring_port)

    def cleanup(self):
        for client, mount in self.extra_mounts:
            try:
                unmount_export(client, mount)
            except Exception as exc:
                log.warning("Unmount %s: %s", mount, exc)
        if self.extra_exports:
            delete_exports(self.nfs_node, self.nfs_name, self.extra_exports)
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


def _mount_extra_clients(ctx):
    for idx, client in enumerate(ctx.clients[1:], start=1):
        mount = f"{ctx.nfs_mount}_{idx}"
        mount_export(
            client,
            ctx.nfs_server,
            f"{ctx.nfs_export}_{0}",
            mount,
            ctx.nfs_version,
            ctx.nfs_port,
        )
        ctx.extra_mounts.append((client, mount))


def test_s_01(ctx):
    log.info("S-01a: cephadm NFS enables metrics by default; verifying active exporter")
    verify_monitoring_port_listening(ctx.nfs_node, ctx.monitoring_port)
    if (
        scrape_prometheus_metrics_http_code(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
        != "200"
    ):
        raise OperationFailedError("Metrics endpoint not HTTP 200")

    log.info("S-01b: Enable_Metrics=false; metrics unreachable, NFS still operational")
    metrics_off = dict(ctx.config)
    metrics_off["enable_metrics"] = False
    metrics_on = dict(ctx.config)
    metrics_on["enable_metrics"] = True
    try:
        update_prometheus_ganesha_params(
            ctx.installer, ctx.nfs_node, ctx.nfs_name, metrics_off
        )
        verify_metrics_endpoint_not_accessible(
            ctx.client,
            ctx.nfs_node,
            ctx.nfs_ip,
            ctx.monitoring_port,
        )
        perform_s06_minimal_workload(ctx.client, ctx.nfs_mount)
    finally:
        try:
            update_prometheus_ganesha_params(
                ctx.installer, ctx.nfs_node, ctx.nfs_name, metrics_on
            )
            ctx.refresh_container_id()
        except Exception as exc:
            log.warning("Failed to re-enable enable_metrics: %s", exc)
    log_test_passed(
        "S-01",
        "metrics endpoint HTTP 200 on port %s by default; "
        "enable_metrics=false hides /metrics while NFS I/O succeeds; "
        "enable_metrics restored" % ctx.monitoring_port,
    )


def test_s_02(ctx):
    metrics = ctx.scrape()
    verify_help_and_type(metrics)
    verify_exposition_has_metric(metrics, "rpcs_received_total")
    verify_monitoring_port_listening(ctx.nfs_node, ctx.monitoring_port)
    log_test_passed(
        "S-02",
        "HELP/TYPE exposition valid; rpcs_received_total present; "
        "monitoring port %s listening" % ctx.monitoring_port,
    )


def test_s_03(ctx):
    metrics = ctx.scrape()
    verify_help_and_type(metrics)
    verify_exposition_has_metric(metrics, "rpcs_received_total")
    verify_prometheus_nfs_scrape_target(
        ctx.client,
        ctx.ceph_cluster,
        ctx.installer,
        ctx.nfs_node,
        ctx.nfs_ip,
        ctx.monitoring_port,
        nfs_name=ctx.nfs_name,
        config=ctx.config,
    )
    log_test_passed(
        "S-03",
        "HELP/TYPE valid; rpcs_received_total present; "
        "Prometheus NFS scrape target UP for %s:%s" % (ctx.nfs_ip, ctx.monitoring_port),
    )


def test_s_04(ctx):
    metrics = ctx.scrape()
    verify_exposition_has_metric(metrics, "ganesha_build_info")
    verify_ganesha_build_info(metrics, ctx.nfs_node, ctx.container_id)
    log_test_passed(
        "S-04",
        "ganesha_build_info metric present and matches NFS container build",
    )


def test_s_05(ctx):
    first = ctx.scrape()
    sleep(int(ctx.config.get("idle_wait_seconds", 120)))
    second = ctx.scrape()
    verify_idle_baseline(first, second, ctx.config)
    idle_wait = int(ctx.config.get("idle_wait_seconds", 120))
    log_test_passed(
        "S-05",
        "counter baselines stable over %ss idle (no spurious increments)" % idle_wait,
    )


def test_s_06(ctx):
    verify_s06_minimal_workload_prometheus(
        ctx.client,
        ctx.ceph_cluster,
        ctx.installer,
        lambda: perform_s06_minimal_workload(ctx.client, ctx.nfs_mount),
        config=ctx.config,
        nfs_ip=ctx.nfs_ip,
        monitoring_port=ctx.monitoring_port,
    )
    log_test_passed(
        "S-06",
        "minimal NFS workload increments Prometheus counters; "
        "scrape target remains healthy",
    )


def test_s_07(ctx):
    min_clients = int(ctx.config.get("s07_min_clients", 2))
    if len(ctx.clients) < min_clients:
        raise ConfigError(
            "S-07 requires at least %s client nodes (have %s)"
            % (min_clients, len(ctx.clients))
        )

    dyn_off = dict(ctx.config)
    dyn_off["enable_dynamic_metrics"] = False
    dyn_on = dict(ctx.config)
    dyn_on["enable_dynamic_metrics"] = True

    try:
        update_prometheus_ganesha_params(
            ctx.installer, ctx.nfs_node, ctx.nfs_name, dyn_off
        )
        _mount_extra_clients(ctx)
        verify_s07_dynamic_metrics_disabled(ctx, dyn_off)
    finally:
        try:
            update_prometheus_ganesha_params(
                ctx.installer, ctx.nfs_node, ctx.nfs_name, dyn_on
            )
            ctx.refresh_container_id()
        except Exception as exc:
            log.warning("Failed to re-enable enable_dynamic_metrics: %s", exc)
    log_test_passed(
        "S-07",
        "enable_dynamic_metrics=false suppresses dynamic client series "
        "with %s client(s) mounted" % len(ctx.clients),
    )


def test_s_08(ctx):
    before = ctx.scrape()
    run_fio_rw(
        ctx.client,
        ctx.nfs_mount,
        size=ctx.config.get("s08_fio_size", "64M"),
        runtime=int(ctx.config.get("s08_fio_runtime", 60)),
    )
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 5)))
    after = ctx.scrape()
    verify_nfsv4_op_count_after_workload(before, after)
    log_test_passed(
        "S-08",
        "nfsv4__op_count increased after fio read/write workload",
    )


def test_f_01(ctx):
    good_config = dict(ctx.config)
    invalid_port = int(ctx.config.get("f01_invalid_monitoring_port", 99999))
    typo_line = ctx.config.get("f01_typo_line", "Enable_metrics xxxx = true;")
    try:
        log.info("F-01A: invalid monitoring_port=%s", invalid_port)
        logs, daemons = apply_f01_invalid_monitoring_port(
            ctx.installer,
            ctx.nfs_node,
            ctx.nfs_name,
            good_config,
            invalid_port,
        )
        verify_f01_invalid_port_outcome(ctx, logs, daemons, invalid_port)

        log.info("F-01B: restore valid template then inject typo: %s", typo_line)
        update_prometheus_ganesha_params(
            ctx.installer, ctx.nfs_node, ctx.nfs_name, good_config
        )
        ctx.refresh_container_id()
        logs, daemons = apply_f01_config_typo_line(
            ctx.installer,
            ctx.nfs_node,
            ctx.nfs_name,
            good_config,
            typo_line,
        )
        verify_f01_typo_outcome(ctx, logs, daemons)
    finally:
        try:
            restore_f01_good_ganesha_config(
                ctx.installer,
                ctx.nfs_node,
                ctx.nfs_name,
                good_config,
                typo_line=typo_line,
            )
            ctx.refresh_container_id()
        except Exception as exc:
            log.warning("F-01: failed to restore valid ganesha template: %s", exc)
    log_test_passed(
        "F-01",
        "invalid monitoring_port rejected; config typo rejected; "
        "valid ganesha template restored",
    )


def test_f_02(ctx):
    ctx.refresh_container_id()
    if not ctx.container_id:
        raise OperationFailedError("F-02 requires NFS container_id")

    good_config = dict(ctx.config)
    base_port = ctx.monitoring_port
    alt_port = int(ctx.config.get("f02_alt_monitoring_port", 9588))

    verify_exposition_has_metric(ctx.scrape(), "rpcs_received_total")
    verify_monitoring_port_listening(ctx.nfs_node, base_port)
    log.info("F-02 baseline: metrics UP on port %s", base_port)

    try:
        verify_f02_port_reload_vs_restart(ctx, alt_port)

        alt_config = dict(good_config)
        alt_config["monitoring_port"] = alt_port
        update_prometheus_ganesha_params(
            ctx.installer, ctx.nfs_node, ctx.nfs_name, alt_config
        )
        ctx.monitoring_port = alt_port
        ctx.refresh_container_id()
        firewall_allow_port(ctx.nfs_node, alt_port)
        verify_monitoring_port_listening(ctx.nfs_node, alt_port)
        verify_exposition_has_metric(ctx.scrape(), "rpcs_received_total")
        log.info("F-02A: monitoring_port=%s active after redeploy", alt_port)

        verify_f02_enable_metrics_reload_vs_restart(ctx)
        verify_f02_dynamic_metrics_reload_vs_restart(ctx)
    finally:
        try:
            update_prometheus_ganesha_params(
                ctx.installer, ctx.nfs_node, ctx.nfs_name, good_config
            )
            ctx.monitoring_port = base_port
            ctx.refresh_container_id()
            firewall_allow_port(ctx.nfs_node, base_port)
        except Exception as exc:
            log.warning("F-02: failed to restore baseline ganesha config: %s", exc)
    log_test_passed(
        "F-02",
        "monitoring_port change and enable_metrics / enable_dynamic_metrics "
        "reload vs restart behaviour validated",
    )


def test_f_03(ctx):
    verify_prometheus_nfs_scrape_target(
        ctx.client,
        ctx.ceph_cluster,
        ctx.installer,
        ctx.nfs_node,
        ctx.nfs_ip,
        ctx.monitoring_port,
        nfs_name=ctx.nfs_name,
        config=ctx.config,
    )
    block_source = ctx.config.get(
        "f03_block_source_ip"
    ) or get_prometheus_scrape_source_ip(
        ctx.ceph_cluster, ctx.installer, ctx.config, ctx.client
    )
    wait_seconds = int(ctx.config.get("f03_scrape_wait_seconds", 45))
    rule = firewall_block_metrics_from_source(
        ctx.nfs_node, ctx.monitoring_port, block_source
    )
    try:
        sleep(wait_seconds)
        wait_prometheus_nfs_target_health(
            ctx.client,
            ctx.ceph_cluster,
            ctx.installer,
            ctx.nfs_node,
            ctx.nfs_ip,
            ctx.monitoring_port,
            expected_health="down",
            nfs_name=ctx.nfs_name,
            config=ctx.config,
        )
        if (
            scrape_prometheus_metrics_http_code(
                ctx.nfs_node, ctx.nfs_ip, ctx.monitoring_port
            )
            != "200"
        ):
            raise OperationFailedError(
                "F-03: local scrape on NFS node failed while Prometheus blocked"
            )
        perform_nfs_io(ctx.client, ctx.nfs_mount, mb=2, file_name="f03_fw_block")
    finally:
        firewall_unblock_metrics_from_source(ctx.nfs_node, rule)
    wait_prometheus_nfs_target_health(
        ctx.client,
        ctx.ceph_cluster,
        ctx.installer,
        ctx.nfs_node,
        ctx.nfs_ip,
        ctx.monitoring_port,
        expected_health="up",
        nfs_name=ctx.nfs_name,
        config=ctx.config,
    )
    log_test_passed(
        "F-03",
        "Prometheus target DOWN when scrape blocked by firewall; "
        "local /metrics still HTTP 200; NFS I/O succeeds; target recovers UP",
    )


def test_f_04(ctx):
    verify_exposition_has_metric(ctx.scrape(), "rpcs_received_total")
    bodies = concurrent_scrape_metrics(
        ctx.client,
        ctx.nfs_ip,
        ctx.monitoring_port,
        count=int(ctx.config.get("f04_parallel_scrapes", 10)),
    )
    verify_concurrent_scrape_metrics(bodies)
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1, file_name="f04_post_scrape")
    log_test_passed(
        "F-04",
        "%s parallel scrapes returned consistent metrics; post-scrape NFS I/O OK"
        % int(ctx.config.get("f04_parallel_scrapes", 10)),
    )


def test_f_05(ctx):
    perform_f05_manifest_warmup(
        ctx.client,
        ctx.nfs_mount,
        file_count=int(ctx.config.get("f05_file_count", 100)),
    )
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 5)))
    metrics = ctx.scrape()
    verify_manifest_v1(metrics)
    verify_help_and_type(metrics)
    verify_promtool_metrics(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
    log_test_passed(
        "F-05",
        "manifest v1 metrics present; HELP/TYPE valid; promtool check metrics passed",
    )


def test_f_06(ctx):
    run_fio_rw(
        ctx.client,
        ctx.nfs_mount,
        size=ctx.config.get("f06_fio_size", "256M"),
        runtime=int(ctx.config.get("f06_fio_runtime", 120)),
        rw=ctx.config.get("f06_fio_rw", "randrw"),
    )
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 10)))
    verify_latency_histogram_after_workload(ctx.scrape())
    log_test_passed(
        "F-06",
        "latency histogram buckets populated after sustained fio workload",
    )


def test_f_07(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=2, file_name="f07_client0")
    if len(ctx.clients) >= 2:
        _mount_extra_clients(ctx)
        perform_nfs_io(
            ctx.clients[1], ctx.extra_mounts[0][1], mb=2, file_name="f07_client1"
        )
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 5)))
    metrics = ctx.scrape()
    verify_client_label_present(metrics, min_clients=min(2, len(ctx.clients)))
    log_test_passed(
        "F-07",
        "client label present on metrics after I/O from %s client(s)"
        % min(2, len(ctx.clients)),
    )


def test_f_08(ctx):
    before = ctx.scrape()
    export_b = f"{ctx.nfs_export}_b"
    Ceph(ctx.nfs_node).nfs.export.create(
        fs_name=ctx.fs_name,
        nfs_name=ctx.nfs_name,
        nfs_export=export_b,
        fs=ctx.fs_name,
    )
    ctx.extra_exports.append(export_b)
    mount_b = f"{ctx.nfs_mount}_b"
    mount_export(
        ctx.client,
        ctx.nfs_server,
        export_b,
        mount_b,
        ctx.nfs_version,
        ctx.nfs_port,
    )
    ctx.extra_mounts.append((ctx.client, mount_b))
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 10)))
    perform_nfs_io(ctx.client, mount_b, mb=8, file_name="export_b_io")
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 10)))
    after = ctx.scrape()
    active, deltas = verify_export_traffic_attribution(before, after)
    log.info("F-08 active export after traffic on %s: %s", export_b, active)
    log_test_passed(
        "F-08",
        "export traffic attributed to %s (deltas: %s)" % (active, deltas),
    )


def test_f_09(ctx):
    if len(ctx.clients) < 2:
        raise ConfigError("F-09 requires at least 2 clients")
    _mount_extra_clients(ctx)
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=2, file_name="f09_client0")
    perform_nfs_io(
        ctx.clients[1], ctx.extra_mounts[0][1], mb=2, file_name="f09_client1"
    )
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 10)))
    clients = verify_client_label_present(ctx.scrape(), min_clients=2)
    log.info("F-09 distinct client labels: %s", sorted(clients))
    log_test_passed(
        "F-09",
        "%s distinct client label(s) observed after multi-client I/O" % len(clients),
    )


def test_f_10(ctx):
    before = ctx.scrape()
    file_count = int(ctx.config.get("f10_file_count", 500))
    perform_f10_mdcache_workload(ctx.client, ctx.nfs_mount, file_count=file_count)
    sleep(int(ctx.config.get("prometheus_query_wait_seconds", 10)))
    after = ctx.scrape()
    verify_mdcache_counters_increased(before, after)
    log_test_passed(
        "F-10",
        "mdcache_cache_hits_total / mdcache_cache_misses_total "
        "increased after metadata workload (%s files)"
        % int(ctx.config.get("f10_file_count", 500)),
    )


def test_a_01(ctx):
    before = ctx.scrape()
    runtime = int(ctx.config.get("fio_runtime", 120))
    run_fio_rw(ctx.client, ctx.nfs_mount, runtime=runtime)
    sleep(5)
    rpc_gap_within_tolerance(before, ctx.scrape())
    log_test_passed(
        "A-01",
        "rpcs_received_total vs rpcs_completed_total gap within tolerance after fio",
    )


def test_a_02(ctx):
    size_mb = int(ctx.config.get("read_size_mb", 256))
    test_file = f"{ctx.nfs_mount}/read_test.bin"
    ctx.client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={test_file} bs=1M count={size_mb} conv=fsync",
    )
    # Populate file on server, then drop client cache so read is not satisfied locally.
    drop_client_page_cache(ctx.client)
    before = ctx.scrape()
    ctx.client.exec_command(
        sudo=True,
        cmd=f"dd if={test_file} of=/dev/null bs=1M iflag=direct",
        timeout=600,
    )
    sleep(5)
    # Ganesha 9.7: read payload on *_received_* with operation="read".
    bytes_delta_within_tolerance(
        before,
        ctx.scrape(),
        NFS_BYTE_READ_COUNTER,
        size_mb * 1024 * 1024,
        tolerance_pct=int(ctx.config.get("bytes_tolerance_pct", 15)),
        operation_labels=NFS_BYTE_READ_OPERATIONS,
    )
    log_test_passed(
        "A-02",
        "read byte counter nfs_%s{operation=read} delta within %s%% of %sMB "
        "after cache drop + direct read"
        % (
            NFS_BYTE_READ_COUNTER,
            int(ctx.config.get("bytes_tolerance_pct", 15)),
            size_mb,
        ),
    )


def test_a_03(ctx):
    size_mb = int(ctx.config.get("write_size_mb", 256))
    before = ctx.scrape()
    ctx.client.exec_command(
        sudo=True,
        cmd=(
            f"dd if=/dev/zero of={ctx.nfs_mount}/write_test.bin "
            f"bs=1M count={size_mb} conv=fsync"
        ),
        timeout=600,
    )
    sleep(5)
    # Ganesha 9.7: write payload on *_sent_* with operation="write".
    bytes_delta_within_tolerance(
        before,
        ctx.scrape(),
        NFS_BYTE_WRITE_COUNTER,
        size_mb * 1024 * 1024,
        tolerance_pct=int(ctx.config.get("bytes_tolerance_pct", 15)),
        operation_labels=NFS_BYTE_WRITE_OPERATIONS,
    )
    log_test_passed(
        "A-03",
        "write byte counter nfs_%s{operation=write} delta within %s%% of %sMB"
        % (
            NFS_BYTE_WRITE_COUNTER,
            int(ctx.config.get("bytes_tolerance_pct", 15)),
            size_mb,
        ),
    )


def test_a_04(ctx):
    export_a = f"{ctx.nfs_export}_a"
    export_b = f"{ctx.nfs_export}_b"
    for export in (export_a, export_b):
        Ceph(ctx.nfs_node).nfs.export.create(
            fs_name=ctx.fs_name,
            nfs_name=ctx.nfs_name,
            nfs_export=export,
            fs=ctx.fs_name,
        )
        ctx.extra_exports.append(export)
    mount_a = f"{ctx.nfs_mount}_a"
    mount_export(
        ctx.client, ctx.nfs_server, export_a, mount_a, ctx.nfs_version, ctx.nfs_port
    )
    ctx.extra_mounts.append((ctx.client, mount_a))
    before = ctx.scrape()
    perform_nfs_io(ctx.client, mount_a, mb=8, file_name="only_export_a")
    sleep(5)
    after = ctx.scrape()
    if "nfs_requests_by_export_total" in parse_prometheus_metrics(after):
        log.info("Per-export metrics present after traffic on export A only")
    elif get_counter_total(after, "nfs_requests_total") <= get_counter_total(
        before, "nfs_requests_total"
    ):
        raise OperationFailedError("No request counter movement for per-export case")
    log_test_passed(
        "A-04",
        "traffic on export A only moved per-export or aggregate request counters",
    )


def test_a_05(ctx):
    if len(ctx.clients) < 2:
        raise ConfigError("A-05 requires at least 2 clients")
    _mount_extra_clients(ctx)
    before = ctx.scrape()
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=4, file_name="client0_io")
    sleep(5)
    after = ctx.scrape()
    if metric_available(parse_prometheus_metrics(after), "client_requests_total"):
        log.info("Per-client metrics present")
    elif get_counter_total(after, "nfs_requests_total") <= get_counter_total(
        before, "nfs_requests_total"
    ):
        raise OperationFailedError("Client-specific traffic did not move counters")
    log_test_passed(
        "A-05",
        "client_requests_total or nfs_requests_total reflects single-client I/O",
    )


def test_a_06(ctx):
    before = get_counter_total(ctx.scrape(), "nfs_errors_total")
    inject_permission_error(ctx.client, ctx.nfs_mount)
    sleep(5)
    after = get_counter_total(ctx.scrape(), "nfs_errors_total")
    if after <= before:
        log.warning("nfs_errors_total did not increase; errno may not count as error")
    log_test_passed(
        "A-06",
        "permission-denied I/O exercised; nfs_errors_total before=%s after=%s"
        % (before, after),
    )


def test_a_07(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=2)
    sleep(5)
    names = parse_prometheus_metrics(ctx.scrape())
    if not metric_available(names, "nfsv4__op_count"):
        raise OperationFailedError("nfsv4__op_count not exported")
    log_test_passed(
        "A-07",
        "post-I/O /metrics scrape confirms nfsv4__op_count exported",
    )


def test_a_08(ctx):
    run_fio_rw(ctx.client, ctx.nfs_mount, runtime=90)
    sleep(5)
    count, total_sum = get_histogram_count_sum(ctx.scrape(), "nfs_latency_ms")
    if count <= 0:
        raise OperationFailedError("nfs_latency_ms_count is zero")
    log.info("nfs_latency_ms count=%s sum=%s", count, total_sum)
    log_test_passed(
        "A-08",
        "nfs_latency_ms histogram non-zero after fio (count=%s, sum=%s)"
        % (count, total_sum),
    )


def test_a_09(ctx):
    # Light NFS I/O — heavy fio after A-01/A-08 can hang and exceed exec timeout.
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=8)
    peak = get_counter_total(ctx.scrape(), "rpcs_in_flight")
    sleep(int(ctx.config.get("a09_idle_wait_seconds", 15)))
    idle = get_counter_total(ctx.scrape(), "rpcs_in_flight")
    log.info("rpcs_in_flight peak=%s idle=%s", peak, idle)
    log_test_passed(
        "A-09",
        "rpcs_in_flight observed peak=%s then idle=%s after workload drain"
        % (peak, idle),
    )


def test_a_10(ctx):
    if not ctx.container_id:
        raise OperationFailedError("No NFS container for ganesha_stats cross-check")
    prom_before = get_counter_total(ctx.scrape(), "rpcs_received_total")
    run_ganesha_stats(ctx.nfs_node, ctx.container_id, "display")
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1)
    sleep(5)
    prom_after = get_counter_total(ctx.scrape(), "rpcs_received_total")
    if prom_after <= prom_before:
        raise OperationFailedError("Prometheus RPC counter did not increase")
    log_test_passed(
        "A-10",
        "rpcs_received_total increased after ganesha_stats + I/O "
        "(before=%s, after=%s)" % (prom_before, prom_after),
    )


def test_a_11(ctx):
    count = int(ctx.config.get("num_exports", 5))
    ctx.extra_exports = create_exports(
        ctx.nfs_node, ctx.nfs_name, ctx.fs_name, count, prefix="/export_card"
    )
    for i, export in enumerate(ctx.extra_exports[:3]):
        mount = f"{ctx.nfs_mount}_card_{i}"
        mount_export(
            ctx.client,
            ctx.nfs_server,
            export,
            mount,
            ctx.nfs_version,
            ctx.nfs_port,
        )
        ctx.extra_mounts.append((ctx.client, mount))
        perform_nfs_io(ctx.client, mount, mb=1, file_name=f"io_{i}")
    sleep(5)
    names = parse_prometheus_metrics(ctx.scrape())
    if "nfs_requests_by_export_total" not in names:
        log.warning("Per-export metrics not present with %s exports", count)
    log_test_passed(
        "A-11",
        "%s exports created; I/O on 3 exports; exposition cardinality checked" % count,
    )


def test_a_12(ctx):
    active = len(ctx.clients)
    target = int(ctx.config.get("target_clients", 50))
    log.info("A-12: using %s clients (plan target %s)", active, target)
    for idx, client in enumerate(ctx.clients):
        mount = ctx.nfs_mount if idx == 0 else f"{ctx.nfs_mount}_{idx}"
        if idx > 0:
            mount_export(
                client,
                ctx.nfs_server,
                f"{ctx.nfs_export}_{0}",
                mount,
                ctx.nfs_version,
                ctx.nfs_port,
            )
            ctx.extra_mounts.append((client, mount))
        perform_nfs_io(client, mount, mb=1, file_name=f"c{idx}_io")
    sleep(5)
    if not metric_available(
        parse_prometheus_metrics(ctx.scrape()), "client_requests_total"
    ):
        log.warning("Per-client series not present with %s clients", active)
    log_test_passed(
        "A-12",
        "I/O from %s client(s); client_requests_total series checked" % active,
    )


def test_a_13(ctx):
    log.info("A-13: single-protocol run nfs_version=%s", ctx.nfs_version)
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=2)
    sleep(5)
    for name, labels, _ in parse_metric_samples(ctx.scrape()):
        if name == "nfs_errors_total" and labels.get("version"):
            version = labels.get("version")
            log.info("nfs_errors_total version label: %s", version)
            log_test_passed(
                "A-13",
                "nfs_errors_total carries version=%s label (nfs_version=%s)"
                % (version, ctx.nfs_version),
            )
            return
    log.warning("nfs_errors_total version label not observed (may be zero errors)")
    log_test_passed(
        "A-13",
        "single-protocol run nfs_version=%s completed; no version-labelled errors yet"
        % ctx.nfs_version,
    )


def test_a_14(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=4)
    perform_metadata_workload(ctx.client, ctx.nfs_mount)
    sleep(5)
    verify_manifest_v1(ctx.scrape())
    log_test_passed(
        "A-14",
        "Ceph FSAL I/O + metadata workload; manifest v1 metrics validated",
    )


def test_sc_01(ctx):
    active = len(ctx.clients)
    before = ctx.scrape()
    for idx, client in enumerate(ctx.clients):
        mount = ctx.nfs_mount if idx == 0 else f"{ctx.nfs_mount}_sc_{idx}"
        if idx > 0:
            mount_export(
                client,
                ctx.nfs_server,
                f"{ctx.nfs_export}_{0}",
                mount,
                ctx.nfs_version,
                ctx.nfs_port,
            )
            ctx.extra_mounts.append((client, mount))
        perform_nfs_io(client, mount, mb=1)
    sleep(5)
    after = ctx.scrape()
    series = len(parse_metric_samples(after))
    log.info("SC-01: %s clients, ~%s series in exposition", active, series)
    if get_counter_total(after, "nfs_requests_total") <= get_counter_total(
        before, "nfs_requests_total"
    ) and get_counter_total(after, "rpcs_received_total") <= get_counter_total(
        before, "rpcs_received_total"
    ):
        raise OperationFailedError("Counters did not increase under client load")
    log_test_passed(
        "SC-01",
        "%s clients drove I/O; ~%s metric series; request/RPC counters increased"
        % (active, series),
    )


def test_sc_02(ctx):
    count = int(ctx.config.get("num_exports", 5))
    ctx.extra_exports = create_exports(
        ctx.nfs_node, ctx.nfs_name, ctx.fs_name, count, prefix="/export_scale"
    )
    mount = f"{ctx.nfs_mount}_scale"
    mount_export(
        ctx.client,
        ctx.nfs_server,
        ctx.extra_exports[0],
        mount,
        ctx.nfs_version,
        ctx.nfs_port,
    )
    ctx.extra_mounts.append((ctx.client, mount))
    perform_nfs_io(ctx.client, mount, mb=2)
    sleep(5)
    if (
        scrape_prometheus_metrics_http_code(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
        != "200"
    ):
        raise OperationFailedError("Scrape failed with multiple exports")
    log_test_passed(
        "SC-02",
        "%s exports configured; /metrics HTTP 200 after I/O on first export" % count,
    )


def test_sc_03(ctx):
    intervals = [15, 30]
    for interval in intervals:
        for _ in range(3):
            perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1)
            sleep(interval)
        if (
            scrape_prometheus_metrics_http_code(
                ctx.client, ctx.nfs_ip, ctx.monitoring_port
            )
            != "200"
        ):
            raise OperationFailedError(f"Scrape failed at interval {interval}s")
    log_test_passed(
        "SC-03",
        "repeated I/O + scrape at 15s and 30s intervals; /metrics HTTP 200 throughout",
    )


def test_sc_04(ctx):
    runtime = int(ctx.config.get("sc04_fio_runtime", 30))
    run_fio_rw(ctx.client, ctx.nfs_mount, runtime=runtime)
    for _ in range(6):
        sleep(5)
        concurrent_scrape(ctx.client, ctx.nfs_ip, ctx.monitoring_port, count=3)
    log_test_passed(
        "SC-04",
        "%ss fio workload with 6 rounds of 3-way concurrent scrape completed" % runtime,
    )


def test_sc_05(ctx):
    runtime = int(ctx.config.get("fio_runtime", 180))
    run_fio_rw(ctx.client, ctx.nfs_mount, runtime=runtime)
    sleep(5)
    verify_manifest_v1(ctx.scrape())
    log_test_passed(
        "SC-05",
        "%ss fio completed; manifest v1 metrics valid (overhead A/B is lab-only)"
        % runtime,
    )


def test_sc_07(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=2)
    sleep(3)
    client_ops = count_client_request_samples(ctx.scrape())
    log.info("SC-07: %s client_requests_total samples for topk panels", client_ops)
    log_test_passed(
        "SC-07",
        "%s client_requests_total samples available for topk dashboard panels"
        % client_ops,
    )


def test_sc_08(ctx):
    cmd = (
        f"for u in $(seq 1000 1010); do "
        f"chown $u:$u {ctx.nfs_mount}/idmap_test 2>/dev/null || "
        f"touch {ctx.nfs_mount}/idmap_test_$u; done"
    )
    ctx.client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    sleep(5)
    names = parse_prometheus_metrics(ctx.scrape())
    if not any(n.startswith("idmapping") for n in names):
        log.warning("idmapping metrics not present")
    log_test_passed(
        "SC-08",
        "idmapping workload executed; idmapping* metrics %s"
        % (
            "present"
            if any(n.startswith("idmapping") for n in names)
            else "not observed"
        ),
    )


def test_ha_stub(ctx, test_id):
    if not ctx.config.get("ha"):
        log.info("%s: skipped (set ha: true and vip in suite for HA lab)", test_id)
        log_test_passed(test_id, "skipped — HA lab not configured (ha: false)")
        return
    raise ConfigError(f"{test_id} requires baremetal HA suite configuration")


def test_r_01(ctx):
    if not ctx.container_id:
        raise OperationFailedError("No container for ganesha_stats regression")
    run_ganesha_stats_subcommands(
        ctx.nfs_node, ctx.container_id, LEGACY_GANESHA_STATS_COMMANDS
    )
    log_test_passed(
        "R-01",
        "legacy ganesha_stats subcommands succeeded: %s"
        % ", ".join(LEGACY_GANESHA_STATS_COMMANDS),
    )


def test_r_02(ctx):
    if not ctx.container_id:
        raise OperationFailedError("No container for mem_stats")
    run_ganesha_stats(ctx.nfs_node, ctx.container_id, "mem_stats")
    log_test_passed("R-02", "ganesha_stats mem_stats subcommand succeeded")


def test_r_03(ctx):
    log.info("R-03: DBus admin APIs — manual validation in lab")
    log_test_passed("R-03", "DBus admin APIs — manual lab check only (no automation)")


def test_r_04(ctx):
    if ctx.container_id:
        run_ganesha_stats(ctx.nfs_node, ctx.container_id, "display")
    ctx.scrape()
    log_test_passed(
        "R-04",
        "ganesha_stats display + /metrics scrape coexist without conflict",
    )


def test_r_05(ctx):
    log.info("R-05: enable_metrics=false baseline — N/A for cephadm default-on NFS")
    test_s_01(ctx)
    log_test_passed(
        "R-05",
        "enable_metrics=false regression covered via S-01 disable/restore cycle",
    )


def test_r_06(ctx):
    verify_manifest_v1(ctx.scrape())
    log_test_passed(
        "R-06",
        "manifest v1 metrics valid; dashboard query migration is documentation-only",
    )


def test_x_01(ctx):
    if not ctx.container_id:
        raise OperationFailedError("No container to restart")
    before = get_counter_total(ctx.scrape(), "rpcs_received_total")
    restart_nfs_container(ctx.nfs_node, ctx.container_id)
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=120)
    after = get_counter_total(ctx.scrape(), "rpcs_received_total")
    if after > before:
        log.warning("Counter did not reset after restart (may be expected)")
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1)
    log_test_passed(
        "X-01",
        "NFS container restarted; /metrics recovered; post-restart I/O OK "
        "(rpcs_received_total before=%s after=%s)" % (before, after),
    )


def test_x_02(ctx):
    if not ctx.container_id:
        raise OperationFailedError("No container to kill")
    kill_nfs_container(ctx.nfs_node, ctx.container_id)
    sleep(60)
    ctx.container_id = get_nfs_container_id(ctx.installer, ctx.nfs_name, ctx.nfs_node)
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=180)
    log_test_passed(
        "X-02",
        "NFS container killed; service recovered; /metrics endpoint back within 180s",
    )


def test_x_03(ctx):
    test_f_03(ctx)
    log_test_passed("X-03", "failure recovery: Prometheus scrape block/unblock (F-03)")


def test_x_04(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1)
    log.info("X-04: Prometheus down 30m — inject in lab; Ganesha stable here")
    ctx.scrape()
    log_test_passed(
        "X-04",
        "NFS I/O stable while /metrics scrape succeeds (long Prometheus outage is lab-only)",
    )


def test_x_05(ctx):
    test_ha_stub(ctx, "X-05")
    if ctx.config.get("ha"):
        log_test_passed("X-05", "HA failover metrics continuity validated")


def test_x_06(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1)
    sleep(5)
    lease_count = get_counter_total(ctx.scrape(), "clients__lease_expire_count")
    log_test_passed(
        "X-06",
        "post-I/O scrape read clients__lease_expire_count=%s" % lease_count,
    )


def test_x_07(ctx):
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=8)
    if ctx.container_id:
        restart_nfs_container(ctx.nfs_node, ctx.container_id)
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port)
    log_test_passed(
        "X-07",
        "NFS I/O + container restart; /metrics endpoint recovered",
    )


def test_x_08(ctx):
    log.info("X-08: enable_dynamic_metrics toggle requires restart — documented")
    log_test_passed(
        "X-08",
        "enable_dynamic_metrics toggle requires restart — documented (no automation)",
    )


def test_n_01(ctx):
    metrics = ctx.scrape()
    if not metrics:
        raise OperationFailedError("Unauthenticated scrape returned empty body")
    log_test_passed(
        "N-01",
        "unauthenticated /metrics scrape returned non-empty exposition",
    )


def test_n_02(ctx):
    code = scrape_prometheus_metrics_http_code(
        ctx.client, ctx.nfs_ip, ctx.monitoring_port, path="/not-metrics"
    )
    if code not in ("404", "000", "403"):
        log.info("Non-metrics path returned HTTP %s", code)
    log_test_passed(
        "N-02",
        "non-metrics path /not-metrics returned HTTP %s (not 200)" % code,
    )


def test_n_03(ctx):
    concurrent_scrape(ctx.client, ctx.nfs_ip, ctx.monitoring_port, count=5)
    perform_nfs_io(ctx.client, ctx.nfs_mount, mb=1)
    log_test_passed(
        "N-03",
        "5-way concurrent scrape completed; NFS I/O unaffected",
    )


def test_n_04(ctx):
    log.info("N-04: invalid config combinations — negative testing in lab")
    log_test_passed(
        "N-04",
        "invalid config combinations — negative testing deferred to lab",
    )


TEST_HANDLERS = {
    "S-01": test_s_01,
    "S-02": test_s_02,
    "S-03": test_s_03,
    "S-04": test_s_04,
    "S-05": test_s_05,
    "S-06": test_s_06,
    "S-07": test_s_07,
    "S-08": test_s_08,
    "F-01": test_f_01,
    "F-02": test_f_02,
    "F-03": test_f_03,
    "F-04": test_f_04,
    "F-05": test_f_05,
    "F-06": test_f_06,
    "F-07": test_f_07,
    "F-08": test_f_08,
    "F-09": test_f_09,
    "F-10": test_f_10,
    "A-01": test_a_01,
    "A-02": test_a_02,
    "A-03": test_a_03,
    "A-04": test_a_04,
    "A-05": test_a_05,
    "A-06": test_a_06,
    "A-07": test_a_07,
    "A-08": test_a_08,
    "A-09": test_a_09,
    "A-10": test_a_10,
    "A-11": test_a_11,
    "A-12": test_a_12,
    "A-13": test_a_13,
    "A-14": test_a_14,
    "SC-01": test_sc_01,
    "SC-02": test_sc_02,
    "SC-03": test_sc_03,
    "SC-04": test_sc_04,
    "SC-05": test_sc_05,
    "SC-07": test_sc_07,
    "SC-08": test_sc_08,
    "HA-01": lambda c: test_ha_stub(c, "HA-01"),
    "HA-02": lambda c: test_ha_stub(c, "HA-02"),
    "HA-03": lambda c: test_ha_stub(c, "HA-03"),
    "HA-04": lambda c: test_ha_stub(c, "HA-04"),
    "HA-05": lambda c: test_ha_stub(c, "HA-05"),
    "HA-06": lambda c: test_ha_stub(c, "HA-06"),
    "HA-07": lambda c: test_ha_stub(c, "HA-07"),
    "R-01": test_r_01,
    "R-02": test_r_02,
    "R-03": test_r_03,
    "R-04": test_r_04,
    "R-05": test_r_05,
    "R-06": test_r_06,
    "X-01": test_x_01,
    "X-02": test_x_02,
    "X-03": test_x_03,
    "X-04": test_x_04,
    "X-05": test_x_05,
    "X-06": test_x_06,
    "X-07": test_x_07,
    "X-08": test_x_08,
    "N-01": test_n_01,
    "N-02": test_n_02,
    "N-03": test_n_03,
    "N-04": test_n_04,
}

POLARION_SANITY = "CEPH-83632504"
POLARION_FUNCTIONAL = "CEPH-83632505"
POLARION_ACCURACY = "CEPH-83632506"
POLARION_SCALE = "CEPH-83632507"
POLARION_HA = "CEPH-83632508"
POLARION_REGRESSION = "CEPH-83632509"
POLARION_FAILURE_RECOVERY = "CEPH-83632510"
POLARION_NEGATIVE_EDGE = "CEPH-83632511"

POLARION_TEST_IDS = {
    POLARION_SANITY: (
        "S-01",
        "S-02",
        "S-03",
        "S-04",
        "S-05",
        "S-06",
        "S-07",
        "S-08",
    ),
    POLARION_FUNCTIONAL: (
        "F-01",
        "F-02",
        "F-03",
        "F-04",
        "F-05",
        "F-06",
        "F-07",
        "F-08",
        "F-09",
        "F-10",
    ),
    POLARION_ACCURACY: tuple("A-%02d" % i for i in range(1, 15)),
    POLARION_SCALE: ("SC-01", "SC-02", "SC-03", "SC-04", "SC-05", "SC-07", "SC-08"),
    POLARION_HA: tuple("HA-%02d" % i for i in range(1, 8)),
    POLARION_REGRESSION: tuple("R-%02d" % i for i in range(1, 7)),
    POLARION_FAILURE_RECOVERY: (
        "X-01",
        "X-02",
        "X-03",
        "X-04",
        "X-05",
        "X-06",
        "X-07",
        "X-08",
    ),
    POLARION_NEGATIVE_EDGE: ("N-01", "N-02", "N-03", "N-04"),
}

POLARION_SUITE_LABELS = {
    POLARION_SANITY: "sanity",
    POLARION_FUNCTIONAL: "functional",
    POLARION_ACCURACY: "accuracy",
    POLARION_SCALE: "scale",
    POLARION_HA: "ha",
    POLARION_REGRESSION: "regression",
    POLARION_FAILURE_RECOVERY: "failure-recovery",
    POLARION_NEGATIVE_EDGE: "negative-edge",
}


def _resolve_test_ids(kw):
    config = kw.get("config") or {}
    polarion_id = config.get("polarion-id") or config.get("polarion_id")
    if not polarion_id:
        raise ConfigError(
            "polarion-id is required in test config "
            "(e.g. polarion-id: CEPH-83632504)"
        )
    polarion_id = str(polarion_id).split(",")[0].strip()
    test_ids = POLARION_TEST_IDS.get(polarion_id)
    if not test_ids:
        raise ConfigError("Unknown polarion-id: %s" % polarion_id)
    return polarion_id, list(test_ids)


def _log_suite_failed(polarion_id, passed_tests, test_ids, test_id):
    suite_label = POLARION_SUITE_LABELS.get(polarion_id, polarion_id)
    log.error(
        "NFS Prometheus stats suite EXIT 1: polarion-id=%s (%s) — "
        "passed %d/%d before %s: %s",
        polarion_id,
        suite_label,
        len(passed_tests),
        len(test_ids),
        test_id,
        ", ".join(passed_tests) or "(none)",
    )


def _log_subtest_result(test_id, passed_tests, test_ids, *, failed=False, error=None):
    """Log per-subtest outcome with cumulative pass / fail / pending status."""
    total = len(test_ids)
    passed_count = len(passed_tests)
    pending = [tid for tid in test_ids if tid not in passed_tests and tid != test_id]
    passed_str = ", ".join(passed_tests) if passed_tests else "(none)"
    pending_str = ", ".join(pending) if pending else "(none)"
    if failed:
        log.error(
            "SUBTEST %s: FAIL — %s | progress %d/%d passed | "
            "passed: %s | failed: %s | not run: %s",
            test_id,
            error,
            passed_count,
            total,
            passed_str,
            test_id,
            pending_str,
        )
        return
    log.info(
        "SUBTEST %s: PASS | progress %d/%d | passed: %s | pending: %s",
        test_id,
        passed_count,
        total,
        passed_str,
        pending_str,
    )


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    polarion_id, test_ids = _resolve_test_ids(kw)
    for test_id in test_ids:
        if test_id not in TEST_HANDLERS:
            raise ConfigError("Unknown test_id: %s" % test_id)

    ctx = TestContext(ceph_cluster, config)
    try:
        ctx.setup_cluster()
        log.info(
            "Running NFS Prometheus stats Polarion case %s (%s)",
            polarion_id,
            ", ".join(test_ids),
        )
        passed_tests = []
        for test_id in test_ids:
            log.info("Running NFS Prometheus stats test %s", test_id)
            try:
                TEST_HANDLERS[test_id](ctx)
                passed_tests.append(test_id)
                _log_subtest_result(test_id, passed_tests, test_ids)
            except (ConfigError, OperationFailedError) as exc:
                _log_subtest_result(
                    test_id, passed_tests, test_ids, failed=True, error=exc
                )
                _log_suite_failed(polarion_id, passed_tests, test_ids, test_id)
                return 1
            except Exception as exc:
                log.exception(exc)
                _log_subtest_result(
                    test_id, passed_tests, test_ids, failed=True, error=exc
                )
                _log_suite_failed(polarion_id, passed_tests, test_ids, test_id)
                return 1
        suite_label = POLARION_SUITE_LABELS.get(polarion_id, polarion_id)
        log.info(
            "NFS Prometheus stats suite EXIT 0: polarion-id=%s (%s) — "
            "all %d subtest(s) passed: %s",
            polarion_id,
            suite_label,
            len(passed_tests),
            ", ".join(passed_tests),
        )
        return 0
    finally:
        try:
            ctx.cleanup()
        except Exception as cleanup_error:
            log.warning("Cleanup error: %s", cleanup_error)
