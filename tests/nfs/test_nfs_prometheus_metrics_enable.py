"""
NFS Prometheus Metrics Enablement (PME) — CephCI automation.

Covers CLI ``--enable-nfs-metrics`` and NFS orch ``spec.enable_nfs_metrics``
(IBMCEPH-14515 / Polarion CEPH-83633168).

Does **not** use ganesha template patching as the enable path; that is the
legacy workaround covered by ``test_nfs_prometheus_stats.py``.
"""

from __future__ import annotations

import json
import re
import time

import yaml

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    create_nfs_via_file_and_verify,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)
from tests.nfs.nfs_prometheus_stats_utils import (
    DEFAULT_MONITORING_PORT,
    firewall_allow_port,
    firewall_remove_port,
    get_monitoring_port,
    get_nfs_container_id,
    prepare_cluster_ceph_access,
    scrape_prometheus_metrics,
    scrape_prometheus_metrics_http_code,
    verify_metrics_endpoint_not_accessible,
    verify_monitoring_port_listening,
    wait_metrics_recovery,
)
from utility.log import Log

log = Log(__name__)

RE_ENABLE_METRICS_TRUE = re.compile(r"(?im)^\s*Enable_Metrics\s*=\s*true\s*;")
RE_ENABLE_METRICS_ANY = re.compile(r"(?im)^\s*Enable_Metrics\s*=\s*(\w+)\s*;")
RE_MONITORING_PORT = re.compile(r"(?im)^\s*Monitoring_Port\s*=\s*(\d+)\s*;")

# Polarion IDs → TC-PME-*
POLARION_A1 = "CEPH-83633169"
POLARION_A2 = "CEPH-83633170"
POLARION_A3 = "CEPH-83633171"
POLARION_F1 = "CEPH-83633172"
POLARION_F2 = "CEPH-83633173"
POLARION_R1 = "CEPH-83633174"
POLARION_R2 = "CEPH-83633175"
POLARION_U1 = "CEPH-83633176"
POLARION_U2 = "CEPH-83633177"
POLARION_N1 = "CEPH-83633178"
POLARION_N2 = "CEPH-83633179"
POLARION_N3 = "CEPH-83633180"
POLARION_D1 = "CEPH-83633181"
POLARION_D2 = "CEPH-83633182"

POLARION_TEST_IDS = {
    POLARION_A1: ("A1",),
    POLARION_A2: ("A2",),
    POLARION_A3: ("A3",),
    POLARION_F1: ("F1",),
    POLARION_F2: ("F2",),
    POLARION_R1: ("R1",),
    POLARION_R2: ("R2",),
    POLARION_U1: ("U1",),
    POLARION_U2: ("U2",),
    POLARION_N1: ("N1",),
    POLARION_N2: ("N2",),
    POLARION_N3: ("N3",),
    POLARION_D1: ("D1",),
    POLARION_D2: ("D2",),
}


def _resolve_test_ids(kw):
    config = kw.get("config") or {}
    polarion_id = config.get("polarion-id") or config.get("polarion_id")
    if not polarion_id:
        raise ConfigError(
            "polarion-id is required in test config "
            "(e.g. polarion-id: CEPH-83633169)"
        )
    polarion_id = str(polarion_id).split(",")[0].strip()
    test_ids = POLARION_TEST_IDS.get(polarion_id)
    if not test_ids:
        raise ConfigError("Unknown polarion-id for PME: %s" % polarion_id)
    return polarion_id, list(test_ids)


class PmeContext:
    """Shared NFS / scrape context for PME cases."""

    def __init__(self, ceph_cluster, config: dict):
        self.ceph_cluster = ceph_cluster
        self.config = config
        self.installer = ceph_cluster.get_nodes(role="installer")[0]
        self.clients = ceph_cluster.get_nodes(role="client")
        self.nfs_nodes = ceph_cluster.get_nodes(role="nfs")
        if not self.clients:
            raise ConfigError("PME tests require at least one client node")
        if not self.nfs_nodes:
            raise ConfigError("PME tests require at least one nfs node")
        self.client = self.clients[0]
        self.nfs_node = self.nfs_nodes[0]
        self.nfs_name = config.get("nfs_name", "cephfs-nfs-pme")
        self.nfs_export = config.get("nfs_export", "/export_pme")
        self.nfs_mount = config.get("nfs_mount", "/mnt/nfs_pme")
        self.fs_name = config.get("fs_name", "cephfs")
        self.nfs_version = config.get("nfs_version", 4.1)
        self.nfs_port = int(config.get("port", 2049))
        self.monitoring_port = int(
            config.get("monitoring_port", DEFAULT_MONITORING_PORT)
        )
        self.nfs_ip = self.nfs_node.ip_address
        self.nfs_server = self.nfs_node.hostname
        self.service_wait_timeout = int(config.get("service_wait_timeout", 300))
        self.redeploy_wait = int(config.get("redeploy_wait", 15))
        self.runtime_firewall_ports = set()
        self.mounted = False
        self.cluster_created = False
        self.extra_clusters = []

    def enable_mgr_nfs(self):
        Ceph(self.client).mgr.module.enable(module="nfs", force=True)
        time.sleep(3)

    def prepare_access(self):
        prepare_cluster_ceph_access(
            self.ceph_cluster,
            self.installer,
            clients=self.clients[:1],
            nfs_nodes=self.nfs_nodes,
        )

    def allow_metrics_firewall(self, port=None):
        port = int(port or self.monitoring_port)
        firewall_allow_port(self.nfs_node, port)
        self.runtime_firewall_ports.add(port)

    def create_cli(self, enable_metrics=False, name=None, check_ec=True):
        """Create NFS cluster via client ``ceph`` CLI (ceph-common).

        Kept local to this module so we do not touch shared
        ``cli/ceph/nfs/cluster/cluster.py`` (different OWNERS).
        """
        name = name or self.nfs_name
        self.enable_mgr_nfs()
        cmd = "ceph nfs cluster create %s '%s'" % (name, self.nfs_server)
        if enable_metrics:
            cmd += " --enable-nfs-metrics"
        out, err = self.client.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)
        if check_ec is False:
            return out, err
        verify_nfs_ganesha_service(
            node=self.installer,
            timeout=self.service_wait_timeout,
            service_id=name,
        )
        self.cluster_created = True
        if name != self.nfs_name:
            self.extra_clusters.append(name)
        self.refresh_endpoints(name)
        return name

    def nfs_spec(
        self,
        enable_metrics=None,
        name=None,
        hosts=None,
        port=None,
        monitoring_port=None,
        placement=None,
    ):
        name = name or self.nfs_name
        nfs_port = int(port if port is not None else self.nfs_port)
        mon_port = int(
            monitoring_port if monitoring_port is not None else self.monitoring_port
        )
        if placement is None:
            placement = {"hosts": hosts or [self.nfs_server]}
        spec = {
            "service_type": "nfs",
            "service_id": name,
            "placement": placement,
            "spec": {
                "port": nfs_port,
                "monitoring_port": mon_port,
            },
        }
        if enable_metrics is not None:
            spec["spec"]["enable_nfs_metrics"] = bool(enable_metrics)
        return spec

    def apply_spec(
        self,
        enable_metrics=None,
        name=None,
        placement=None,
        hosts=None,
        port=None,
        monitoring_port=None,
    ):
        name = name or self.nfs_name
        self.enable_mgr_nfs()
        mon_port = int(
            monitoring_port if monitoring_port is not None else self.monitoring_port
        )
        objs = [
            self.nfs_spec(
                enable_metrics=enable_metrics,
                name=name,
                hosts=hosts,
                port=port,
                monitoring_port=mon_port,
                placement=placement,
            )
        ]
        ok = create_nfs_via_file_and_verify(
            self.installer,
            objs,
            timeout=self.service_wait_timeout,
            nfs_nodes=self.nfs_nodes,
            service_id=name,
        )
        if not ok:
            raise OperationFailedError(
                "Failed to apply NFS spec for %s (enable_nfs_metrics=%s)"
                % (name, enable_metrics)
            )
        self.cluster_created = True
        if name != self.nfs_name:
            self.extra_clusters.append(name)
        self.refresh_endpoints(name, monitoring_port=mon_port)
        return name

    def refresh_endpoints(self, name=None, monitoring_port=None):
        name = name or self.nfs_name
        cfg = {}
        if monitoring_port is not None:
            cfg["monitoring_port"] = int(monitoring_port)
        elif self.monitoring_port:
            cfg["monitoring_port"] = int(self.monitoring_port)
        self.monitoring_port = get_monitoring_port(self.nfs_node, name, cfg)
        self.allow_metrics_firewall(self.monitoring_port)

    def read_ganesha_conf(self, name=None):
        name = name or self.nfs_name
        container_id = get_nfs_container_id(self.installer, name, self.nfs_node)
        if not container_id:
            raise OperationFailedError(
                "No NFS container_id for %s on %s" % (name, self.nfs_node.hostname)
            )
        conf, _ = self.nfs_node.exec_command(
            sudo=True,
            cmd="podman exec %s cat /etc/ganesha/ganesha.conf" % container_id,
        )
        return conf or ""

    def metrics_core_snippet(self, conf=None, name=None):
        conf = conf if conf is not None else self.read_ganesha_conf(name)
        enable = None
        match = RE_ENABLE_METRICS_ANY.search(conf)
        if match:
            enable = match.group(1).lower()
        port = None
        match = RE_MONITORING_PORT.search(conf)
        if match:
            port = match.group(1)
        return enable, port

    def assert_metrics_enabled(self, name=None):
        name = name or self.nfs_name
        conf = self.read_ganesha_conf(name)
        if not RE_ENABLE_METRICS_TRUE.search(conf):
            raise OperationFailedError(
                "Enable_Metrics = true; missing in ganesha.conf for %s" % name
            )
        enable, port = self.metrics_core_snippet(conf)
        log.info(
            "Metrics enabled for %s: Enable_Metrics=%s Monitoring_Port=%s",
            name,
            enable,
            port,
        )
        verify_monitoring_port_listening(self.nfs_node, self.monitoring_port)
        code = scrape_prometheus_metrics_http_code(
            self.client, self.nfs_ip, self.monitoring_port
        )
        if code != "200":
            raise OperationFailedError(
                "/metrics HTTP %s on %s:%s (expected 200)"
                % (code, self.nfs_ip, self.monitoring_port)
            )
        body = scrape_prometheus_metrics(self.client, self.nfs_ip, self.monitoring_port)
        if "# HELP" not in body and "# TYPE" not in body:
            raise OperationFailedError(
                "/metrics response is not Prometheus text on %s:%s"
                % (self.nfs_ip, self.monitoring_port)
            )
        log.info(
            "Scraped /metrics OK on %s:%s (%d bytes)",
            self.nfs_ip,
            self.monitoring_port,
            len(body),
        )

    def assert_metrics_disabled(self, name=None, require_no_enable_true=True):
        name = name or self.nfs_name
        conf = self.read_ganesha_conf(name)
        if require_no_enable_true and RE_ENABLE_METRICS_TRUE.search(conf):
            raise OperationFailedError(
                "Enable_Metrics = true unexpectedly present for metrics-off %s" % name
            )
        verify_metrics_endpoint_not_accessible(
            self.client, self.nfs_node, self.nfs_ip, self.monitoring_port
        )
        log.info("Metrics disabled for %s (endpoint not served)", name)

    def setup_with_mount(self, enable_metrics=False):
        """Full setup_nfs_cluster path (export + mount) with optional CLI flag."""
        # setup_nfs_cluster does not yet expose enable_nfs_metrics; use CLI create
        # for metrics-on, and stock setup for metrics-off + mount.
        if enable_metrics:
            self.create_cli(enable_metrics=True)
            Ceph(self.client).nfs.export.create(
                fs_name=self.fs_name,
                nfs_name=self.nfs_name,
                nfs_export=self.nfs_export,
                fs=self.fs_name,
            )
            self.client.exec_command(sudo=True, cmd="mkdir -p %s" % self.nfs_mount)
            self.client.exec_command(
                sudo=True,
                cmd=(
                    "mount -t nfs -o nfsvers=%s,port=%s %s:%s %s"
                    % (
                        self.nfs_version,
                        self.nfs_port,
                        self.nfs_ip,
                        self.nfs_export,
                        self.nfs_mount,
                    )
                ),
            )
            self.mounted = True
        else:
            setup_nfs_cluster(
                clients=[self.client],
                nfs_server=self.nfs_server,
                port=str(self.nfs_port),
                version=self.nfs_version,
                nfs_name=self.nfs_name,
                nfs_mount=self.nfs_mount,
                fs_name=self.fs_name,
                export=self.nfs_export,
                fs=self.fs_name,
                ceph_cluster=self.ceph_cluster,
            )
            self.cluster_created = True
            self.mounted = True
        self.prepare_access()
        self.refresh_endpoints()

    def delete_cluster(self, name=None):
        name = name or self.nfs_name
        try:
            Ceph(self.client).nfs.cluster.delete(name)
        except Exception as exc:
            log.warning("Delete NFS cluster %s: %s", name, exc)

    def cleanup(self):
        for port in list(self.runtime_firewall_ports):
            try:
                firewall_remove_port(self.nfs_node, port)
            except Exception as exc:
                log.warning("Firewall cleanup %s: %s", port, exc)
        self.runtime_firewall_ports.clear()
        if self.mounted:
            try:
                cleanup_cluster(
                    self.client,
                    self.nfs_mount,
                    self.nfs_name,
                    self.nfs_export,
                    nfs_nodes=self.nfs_nodes,
                )
                self.mounted = False
                self.cluster_created = False
            except Exception as exc:
                log.warning("cleanup_cluster: %s", exc)
        elif self.cluster_created:
            self.delete_cluster(self.nfs_name)
            self.cluster_created = False
        for extra in list(self.extra_clusters):
            self.delete_cluster(extra)
        self.extra_clusters.clear()


def test_a1(ctx: PmeContext):
    """TC-PME-A1: Enable via CLI --enable-nfs-metrics."""
    log.info("TC-PME-A1: enable via CLI")
    ctx.create_cli(enable_metrics=True)
    ctx.prepare_access()
    ctx.assert_metrics_enabled()


def test_a2(ctx: PmeContext):
    """TC-PME-A2: Enable via NFS service spec."""
    log.info("TC-PME-A2: enable via orch spec enable_nfs_metrics: true")
    ctx.apply_spec(enable_metrics=True)
    ctx.prepare_access()
    ctx.assert_metrics_enabled()


def test_a3(ctx: PmeContext):
    """TC-PME-A3: Default create keeps metrics off."""
    log.info("TC-PME-A3: default create metrics off")
    ctx.create_cli(enable_metrics=False)
    ctx.prepare_access()
    ctx.assert_metrics_disabled()


def test_f1(ctx: PmeContext):
    """TC-PME-F1: CLI ↔ spec metrics config parity."""
    log.info("TC-PME-F1: CLI vs spec metrics NFS_CORE_PARAM parity")
    # Avoid port clashes when both clusters land on the same NFS host:
    # create CLI cluster first (default 2049/9587), then spec cluster on alt ports.
    # If a second NFS node exists, place the spec cluster there with defaults.
    cli_name = "%s-cli" % ctx.nfs_name
    spec_name = "%s-spec" % ctx.nfs_name
    ctx.create_cli(enable_metrics=True, name=cli_name)

    alt_hosts = None
    alt_port = None
    alt_mon = None
    if len(ctx.nfs_nodes) > 1:
        alt_hosts = [ctx.nfs_nodes[1].hostname]
    else:
        alt_port = int(ctx.config.get("f1_alt_port", 2050))
        alt_mon = int(ctx.config.get("f1_alt_monitoring_port", 9588))

    ctx.apply_spec(
        enable_metrics=True,
        name=spec_name,
        hosts=alt_hosts,
        port=alt_port,
        monitoring_port=alt_mon,
    )
    ctx.prepare_access()

    cli_enable, cli_mon = ctx.metrics_core_snippet(name=cli_name)
    # Switch scrape node for second cluster when on another host
    if alt_hosts:
        ctx.nfs_node = ctx.nfs_nodes[1]
        ctx.nfs_ip = ctx.nfs_node.ip_address
    if alt_mon:
        ctx.monitoring_port = alt_mon
        ctx.allow_metrics_firewall(alt_mon)
    spec_enable, spec_mon = ctx.metrics_core_snippet(name=spec_name)
    if cli_enable != "true" or spec_enable != "true":
        raise OperationFailedError(
            "Enable_Metrics mismatch: cli=%s spec=%s" % (cli_enable, spec_enable)
        )
    if not cli_mon or not spec_mon:
        raise OperationFailedError(
            "Monitoring_Port missing: cli=%s spec=%s" % (cli_mon, spec_mon)
        )
    # Same enablement semantics; Monitoring_Port may differ when colocated.
    log.info(
        "CLI/spec parity OK: Enable_Metrics=true "
        "(CLI Monitoring_Port=%s, spec Monitoring_Port=%s)",
        cli_mon,
        spec_mon,
    )


def test_f2(ctx: PmeContext):
    """TC-PME-F2: Enable/disable after cluster create via orch apply."""
    log.info("TC-PME-F2: toggle enable_nfs_metrics post-create")
    ctx.apply_spec(enable_metrics=False)
    ctx.prepare_access()
    ctx.assert_metrics_disabled()

    ctx.apply_spec(enable_metrics=True)
    ctx.assert_metrics_enabled()

    ctx.apply_spec(enable_metrics=False)
    # Give exporter time to drop after redeploy
    time.sleep(ctx.redeploy_wait)
    ctx.assert_metrics_disabled()


def test_r1(ctx: PmeContext):
    """TC-PME-R1: Create without flag still healthy / NFS usable."""
    log.info("TC-PME-R1: create without flag remains healthy")
    ctx.setup_with_mount(enable_metrics=False)
    out, _ = ctx.client.exec_command(
        sudo=True,
        cmd="ceph nfs cluster info %s --format json" % ctx.nfs_name,
    )
    info = json.loads(out or "{}")
    if not info:
        raise OperationFailedError("Empty nfs cluster info for %s" % ctx.nfs_name)
    probe = "%s/pme_r1.txt" % ctx.nfs_mount
    ctx.client.exec_command(sudo=True, cmd="echo pme-r1 > %s" % probe)
    read_out, _ = ctx.client.exec_command(sudo=True, cmd="cat %s" % probe)
    if "pme-r1" not in (read_out or ""):
        raise OperationFailedError("NFS mount I/O failed for R1")
    ctx.assert_metrics_disabled()


def test_r2(ctx: PmeContext):
    """TC-PME-R2: Redeploy does not flip metrics-off → on."""
    log.info("TC-PME-R2: redeploy must not enable metrics accidentally")
    ctx.create_cli(enable_metrics=False)
    ctx.prepare_access()
    ctx.assert_metrics_disabled()

    CephAdm(ctx.installer).ceph.orch.redeploy("nfs.%s" % ctx.nfs_name)
    time.sleep(ctx.redeploy_wait)
    verify_nfs_ganesha_service(
        node=ctx.installer,
        timeout=ctx.service_wait_timeout,
        service_id=ctx.nfs_name,
    )
    ctx.refresh_endpoints()
    ctx.assert_metrics_disabled()


def test_u1(ctx: PmeContext):
    """TC-PME-U1: Enable metrics post N-1 → 10.0 upgrade (post-upgrade phase)."""
    operation = str(ctx.config.get("operation", "after_upgrade")).lower()
    if operation in ("before_upgrade", "pre_upgrade"):
        log.info("TC-PME-U1 pre-upgrade: create metrics-off cluster for later enable")
        # Omit enable_nfs_metrics on N-1 (field may not exist yet); custom ports via spec.
        ctx.apply_spec(enable_metrics=None)
        ctx.prepare_access()
        ctx.assert_metrics_disabled()
        # Leave cluster in place for upgrade suite (skip full cleanup)
        ctx.cluster_created = False
        return

    log.info("TC-PME-U1 post-upgrade: apply enable_nfs_metrics: true")
    # Cluster should already exist from pre-upgrade; apply enable.
    ctx.apply_spec(enable_metrics=True)
    ctx.prepare_access()
    ctx.assert_metrics_enabled()


def test_u2(ctx: PmeContext):
    """TC-PME-U2: Metrics-disabled stays disabled after upgrade."""
    operation = str(ctx.config.get("operation", "after_upgrade")).lower()
    if operation in ("before_upgrade", "pre_upgrade"):
        log.info("TC-PME-U2 pre-upgrade: create metrics-off baseline")
        # Omit enable_nfs_metrics on N-1 so create matches "normal" pre-feature path.
        ctx.apply_spec(enable_metrics=None)
        ctx.prepare_access()
        ctx.assert_metrics_disabled()
        ctx.cluster_created = False
        return

    log.info("TC-PME-U2 post-upgrade: assert metrics still off")
    verify_nfs_ganesha_service(
        node=ctx.installer,
        timeout=ctx.service_wait_timeout,
        service_id=ctx.nfs_name,
    )
    ctx.prepare_access()
    ctx.refresh_endpoints()
    ctx.assert_metrics_disabled()
    # Do not delete — upgrade suite owns lifecycle when reuse_cluster set
    if ctx.config.get("reuse_cluster"):
        ctx.cluster_created = False


def test_n1(ctx: PmeContext):
    """TC-PME-N1: Invalid --enable-nfs-metrics value rejected."""
    log.info("TC-PME-N1: invalid CephBool value")
    ctx.enable_mgr_nfs()
    name = "%s-bad" % ctx.nfs_name
    cmd = "ceph nfs cluster create %s '%s' --enable-nfs-metrics=invalid" % (
        name,
        ctx.nfs_server,
    )
    out, err = ctx.client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    combined = "%s\n%s" % (out or "", err or "")
    log.info("N1 create output: %s", combined.strip())
    clusters = Ceph(ctx.client).nfs.cluster.ls() or []
    if name in clusters:
        ctx.delete_cluster(name)
        raise OperationFailedError(
            "Invalid --enable-nfs-metrics=invalid created leftover cluster %s" % name
        )
    if "invalid" not in combined.lower() and "bool" not in combined.lower():
        # Some builds only return non-zero without rich text; require failure at least.
        # Re-run capturing return code via bash.
        rc_out, _ = ctx.client.exec_command(
            sudo=True,
            cmd="bash -lc %s" % json.dumps("%s; echo EXIT:$?" % cmd),
            check_ec=False,
        )
        if "EXIT:0" in (rc_out or ""):
            raise OperationFailedError(
                "CLI accepted invalid --enable-nfs-metrics=invalid"
            )
    log.info("N1: invalid flag rejected; no leftover cluster")


def test_n2(ctx: PmeContext):
    """TC-PME-N2: Scrape /metrics when disabled."""
    log.info("TC-PME-N2: scrape when metrics disabled")
    ctx.create_cli(enable_metrics=False)
    ctx.prepare_access()
    ctx.assert_metrics_disabled()


def test_n3(ctx: PmeContext):
    """TC-PME-N3: Spec with metrics + invalid placement."""
    log.info("TC-PME-N3: enable_nfs_metrics with empty placement")
    ctx.enable_mgr_nfs()
    name = "%s-badplace" % ctx.nfs_name
    bad_spec = {
        "service_type": "nfs",
        "service_id": name,
        "placement": {"hosts": []},
        "spec": {
            "port": ctx.nfs_port,
            "monitoring_port": ctx.monitoring_port,
            "enable_nfs_metrics": True,
        },
    }
    remote = "/tmp/cephci_pme_n3_invalid.yaml"
    fp = ctx.installer.remote_file(sudo=True, file_name=remote, file_mode="wb")
    try:
        fp.write(
            yaml.dump(
                bad_spec,
                sort_keys=False,
                indent=2,
                default_flow_style=False,
            ).encode("utf-8")
        )
        fp.flush()
    finally:
        fp.close()

    out, err = ctx.installer.exec_command(
        sudo=True,
        cmd="ceph orch apply -i %s" % remote,
        check_ec=False,
    )
    combined = "%s\n%s" % (out or "", err or "")
    log.info("N3 apply output: %s", combined.strip())
    ctx.installer.exec_command(sudo=True, cmd="rm -f %s" % remote, check_ec=False)

    lowered = combined.lower()
    if "traceback" in lowered and "enable_nfs_metrics" in lowered:
        raise OperationFailedError(
            "N3 raised metrics-related traceback instead of placement error"
        )
    if not any(
        token in lowered
        for token in ("placement", "host", "invalid", "empty", "error", "failed")
    ):
        # orch may accept YAML then fail reconcile — ensure service never healthy
        time.sleep(10)
        raw, _ = ctx.installer.exec_command(
            sudo=True,
            cmd="ceph orch ps --service_name nfs.%s -f json" % name,
            check_ec=False,
        )
        try:
            daemons = json.loads(raw or "[]")
        except json.JSONDecodeError:
            daemons = []
        running = [
            d for d in daemons if (d.get("status_desc") or "").lower() == "running"
        ]
        if running:
            ctx.delete_cluster(name)
            raise OperationFailedError(
                "N3 invalid placement unexpectedly started NFS daemons"
            )
    # Best-effort cleanup if a partial service was registered
    ctx.delete_cluster(name)
    log.info("N3: invalid placement rejected/failed without metrics traceback")


def test_d1(ctx: PmeContext):
    """TC-PME-D1: MGR restart while metrics enabled."""
    log.info("TC-PME-D1: MGR fail with metrics enabled")
    ctx.create_cli(enable_metrics=True)
    ctx.prepare_access()
    ctx.assert_metrics_enabled()

    ctx.client.exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)
    for w in WaitUntil(timeout=120, interval=5):
        out, _ = ctx.client.exec_command(
            sudo=True, cmd="ceph mgr stat -f json", check_ec=False
        )
        try:
            stat = json.loads(out or "{}")
        except json.JSONDecodeError:
            stat = {}
        if stat.get("available"):
            break
    else:
        raise OperationFailedError("MGR did not become available after fail")

    time.sleep(10)
    verify_nfs_ganesha_service(
        node=ctx.installer,
        timeout=ctx.service_wait_timeout,
        service_id=ctx.nfs_name,
    )
    ctx.refresh_endpoints()
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=120)
    ctx.assert_metrics_enabled()


def test_d2(ctx: PmeContext):
    """TC-PME-D2: NFS daemon redeploy with metrics enabled."""
    log.info("TC-PME-D2: orch redeploy nfs with metrics enabled")
    ctx.create_cli(enable_metrics=True)
    ctx.prepare_access()
    ctx.assert_metrics_enabled()

    CephAdm(ctx.installer).ceph.orch.redeploy("nfs.%s" % ctx.nfs_name)
    time.sleep(ctx.redeploy_wait)
    verify_nfs_ganesha_service(
        node=ctx.installer,
        timeout=ctx.service_wait_timeout,
        service_id=ctx.nfs_name,
    )
    ctx.refresh_endpoints()
    wait_metrics_recovery(ctx.client, ctx.nfs_ip, ctx.monitoring_port, timeout=180)
    ctx.assert_metrics_enabled()


TEST_HANDLERS = {
    "A1": test_a1,
    "A2": test_a2,
    "A3": test_a3,
    "F1": test_f1,
    "F2": test_f2,
    "R1": test_r1,
    "R2": test_r2,
    "U1": test_u1,
    "U2": test_u2,
    "N1": test_n1,
    "N2": test_n2,
    "N3": test_n3,
    "D1": test_d1,
    "D2": test_d2,
}


def run(ceph_cluster, **kw):
    config = kw.get("config", {}) or {}
    polarion_id, test_ids = _resolve_test_ids(kw)
    ctx = PmeContext(ceph_cluster, config)
    try:
        log.info(
            "Running PME Polarion %s (%s)",
            polarion_id,
            ", ".join(test_ids),
        )
        for test_id in test_ids:
            handler = TEST_HANDLERS.get(test_id)
            if not handler:
                raise ConfigError("Unknown PME test_id: %s" % test_id)
            log.info("=== PME %s ===", test_id)
            handler(ctx)
            log.info("PME %s: PASS", test_id)
        return 0
    except (ConfigError, OperationFailedError) as exc:
        log.error("PME %s FAIL: %s", polarion_id, exc)
        return 1
    finally:
        try:
            ctx.cleanup()
        except Exception as cleanup_error:
            log.warning("PME cleanup error: %s", cleanup_error)
