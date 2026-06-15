"""NFS Multi-Active ingress negative orchestrator apply tests (NEG-1..NEG-5)."""

from datetime import datetime

from cli.exceptions import ConfigError
from tests.nfs.lib.multi_active import init_crash_monitoring, wait_for_ceph_health
from tests.nfs.lib.multi_active.config import NfsMultiActiveConfig
from tests.nfs.lib.multi_active.export_restriction import (
    NfsMultiActiveExportRestriction,
)
from tests.nfs.lib.multi_active.ingress_negative import NfsMultiActiveIngressNegative
from utility.log import Log

log = Log(__name__)

NFS_SERVICE_ID_PREFIX = "cephfs-nfs-neg"
MIN_NFS_NODES = 2
DEFAULT_FRONTEND_PORT = 17601
DEFAULT_MONITOR_PORT = 17701
NEG_APPLY_TIMEOUT = 120

NEGATIVE_WORKFLOWS = {
    "neg1_ingress_without_backend_service": {
        "tc": "NEG-1",
        "desc": "Ingress spec without backend_service must fail validation.",
        "mutator": "_mutate_neg1_missing_backend_service",
        "validator": "validation_failure",
        "expected_errors": ("Cannot add ingress: No backend_service specified",),
        "vip": 2,
        "frontend_port": 17601,
        "monitor_port": 17701,
    },
    "neg2_backend_service_nonexistent": {
        "tc": "NEG-2",
        "desc": "Ingress with backend_service nfs.nonexistent must fail or stay unhealthy.",
        "mutator": "_mutate_neg2_nonexistent_backend",
        "validator": "apply_failed_or_unhealthy",
        "vip": 1,
        "frontend_port": 17602,
        "monitor_port": 17702,
    },
    "neg3_ingress_schema_typo": {
        "tc": "NEG-3",
        "desc": "Ingress spec with invalid service_type / unknown keys must fail schema validation.",
        "mutator": "_mutate_neg3_schema_typo",
        "validator": "validation_failure",
        "expected_errors": (
            "ServiceSpec: __init__() got an unexpected keyword argument 'backend_service'",
        ),
        "vip": 2,
        "frontend_port": 17603,
        "monitor_port": 17703,
    },
    "neg4_ip_export_restriction_haproxy_failover": {
        "tc": "NEG-4",
        "desc": (
            "IP-based export restrictions with HAProxy protocol; background IO "
            "resumes across ingress and NFS failover; remount and IO re-check "
            "after each phase."
        ),
        "validator": "ip_export_restriction_failover",
        "vip": 1,
    },
    "neg5_ganesha_crash_io_resume": {
        "tc": "NEG-5",
        "desc": (
            "SIGKILL ganesha on the pinned NFS backend while background IO runs; "
            "IO continues, the NFS service restarts, then remount and IO re-check. "
            "Reuses NEG-4 cluster state when run in sequence, or deploys its own "
            "cluster when run standalone."
        ),
        "validator": "ganesha_crash_io_resume",
        "vip": 1,
    },
}


def _mutate_neg1_missing_backend_service(spec):
    spec["spec"].pop("backend_service", None)


def _mutate_neg2_nonexistent_backend(spec):
    spec["spec"]["backend_service"] = "nfs.nonexistent"


def _mutate_neg3_schema_typo(spec):
    spec["service_type"] = "ingress_typo"
    spec["unknown_spec_key"] = "not-allowed"


_MUTATORS = {
    "_mutate_neg1_missing_backend_service": _mutate_neg1_missing_backend_service,
    "_mutate_neg2_nonexistent_backend": _mutate_neg2_nonexistent_backend,
    "_mutate_neg3_schema_typo": _mutate_neg3_schema_typo,
}


def _resolve_vip(config, workflow):
    vips = NfsMultiActiveConfig.get_vips(config)
    slot = int(workflow.get("vip", config.get("vip", 1)))
    if slot < 1 or slot > len(vips):
        raise ConfigError(
            f"Negative test vip slot must be 1..{len(vips)}, got {slot!r}"
        )
    return vips[slot - 1]


def _ingress_hosts(ceph_cluster, config):
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    min_nodes = int(config.get("min_ingress_hosts", MIN_NFS_NODES))
    if len(nfs_nodes) < min_nodes:
        raise ConfigError(
            f"Ingress negative tests need at least {min_nodes} NFS-capable nodes, "
            f"found {len(nfs_nodes)}"
        )
    count = int(config.get("ingress_host_count", 2))
    return [node.hostname for node in nfs_nodes[:count]]


def _build_negative_spec(config, ceph_cluster, workflow_key):
    workflow = NEGATIVE_WORKFLOWS[workflow_key]
    nfs_name = (
        f"{NFS_SERVICE_ID_PREFIX}-{workflow_key}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    vip = _resolve_vip(config, workflow)
    ingress_hosts = _ingress_hosts(ceph_cluster, config)
    frontend_port = int(
        workflow.get(
            "frontend_port", config.get("frontend_port", DEFAULT_FRONTEND_PORT)
        )
    )
    monitor_port = int(
        workflow.get("monitor_port", config.get("monitor_port", DEFAULT_MONITOR_PORT))
    )

    spec = NfsMultiActiveIngressNegative.build_base_ingress_spec(
        nfs_name,
        vip,
        ingress_hosts,
        frontend_port,
        monitor_port,
    )
    mutator = _MUTATORS[workflow["mutator"]]
    mutator(spec)
    return nfs_name, spec


def _run_negative_workflow(
    ceph_cluster, config, workflow_key, phase_step, phase_total, neg_export_session
):
    workflow = NEGATIVE_WORKFLOWS[workflow_key]
    installer = ceph_cluster.get_nodes("installer")[0]

    log.info(
        "\n" + "=" * 70 + "\n" "Negative phase %s/%s | %s | %s\n" "%s\n" + "=" * 70,
        phase_step,
        phase_total,
        workflow["tc"],
        workflow_key,
        workflow["desc"],
    )

    if workflow["validator"] == "ip_export_restriction_failover":
        neg_export_session[0] = (
            NfsMultiActiveExportRestriction.run_neg4_ip_export_restriction_haproxy_failover(
                ceph_cluster,
                config,
                workflow["tc"],
            )
        )
        log.info("%s (%s) passed", workflow_key, workflow["tc"])
        return

    if workflow["validator"] == "ganesha_crash_io_resume":
        neg_export_session[0] = (
            NfsMultiActiveExportRestriction.run_neg5_ganesha_crash_with_io(
                ceph_cluster,
                config,
                workflow["tc"],
                session=neg_export_session[0],
            )
        )
        log.info("%s (%s) passed", workflow_key, workflow["tc"])
        return

    nfs_name, spec = _build_negative_spec(config, ceph_cluster, workflow_key)
    service_name = f"ingress.nfs.{nfs_name}"

    result = NfsMultiActiveIngressNegative.apply_specs(installer, [spec])
    try:
        if workflow["validator"] == "validation_failure":
            NfsMultiActiveIngressNegative.assert_apply_validation_failure(
                result,
                workflow["tc"],
                expected_errors=workflow.get("expected_errors", ()),
            )
        elif workflow["validator"] == "apply_failed_or_unhealthy":
            NfsMultiActiveIngressNegative.assert_apply_failed_or_ingress_unhealthy(
                installer,
                result,
                service_name,
                workflow["tc"],
                timeout=int(config.get("ingress_unhealthy_timeout", NEG_APPLY_TIMEOUT)),
                expected_errors=workflow.get("expected_errors", ()),
            )
        else:
            raise ConfigError(
                f"Unsupported negative validator {workflow['validator']!r}"
            )
    finally:
        if result.exit_code == 0:
            NfsMultiActiveIngressNegative.remove_ingress_service(
                installer, service_name
            )

    log.info("%s (%s) passed", workflow_key, workflow["tc"])


def run(ceph_cluster, **kw):
    """Run NEG-1..NEG-5 ingress negative workflows in one session."""
    config = kw.get("config") or {}
    init_crash_monitoring(config)
    phases = config.get("workflows") or list(NEGATIVE_WORKFLOWS)
    for workflow_key in phases:
        if workflow_key not in NEGATIVE_WORKFLOWS:
            raise ConfigError(f"Unknown negative workflow {workflow_key!r}")

    neg4_key = "neg4_ip_export_restriction_haproxy_failover"
    neg5_key = "neg5_ganesha_crash_io_resume"
    if neg4_key in phases and neg5_key in phases:
        if phases.index(neg4_key) >= phases.index(neg5_key):
            raise ConfigError(
                "NEG-5 must run after NEG-4 when both are in the workflow list"
            )

    log.info(
        "NFS Multi-Active ingress negative tests | %d phase(s)",
        len(phases),
    )

    neg_export_session = [None]
    try:
        for phase_idx, workflow_key in enumerate(phases, start=1):
            _run_negative_workflow(
                ceph_cluster,
                config,
                workflow_key,
                phase_idx,
                len(phases),
                neg_export_session,
            )
            workflow = NEGATIVE_WORKFLOWS[workflow_key]
            try:
                wait_for_ceph_health(
                    ceph_cluster, config, context=f"post-{workflow['tc']}"
                )
            except ConfigError as exc:
                log.error("Post-%s health check failed: %s", workflow["tc"], exc)
                return 1

        try:
            wait_for_ceph_health(ceph_cluster, config, context="post-suite")
        except ConfigError as exc:
            log.error("Post-suite health check failed: %s", exc)
            return 1
        log.info("Ingress negative test session completed successfully")
        return 0
    except Exception as exc:
        log.error("Ingress negative test session failed: %s", exc)
        return 1
    finally:
        if neg_export_session[0] is not None:
            try:
                neg_export_session[0].cleanup()
            except Exception as exc:
                log.warning("NEG-4/5 shared cluster cleanup failed: %s", exc)
