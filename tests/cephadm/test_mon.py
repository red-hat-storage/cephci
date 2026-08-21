from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.mon import Mon
from utility.log import Log
from utility.odf_defaults import (
    APPLY_ODF_DEFAULTS_KEY,
    apply_v2_only_mon_addrs,
    overrides_enabled,
)

log = Log(__name__)

CLUSTER_STATE = [
    "ceph status",
    "ceph orch ls mon -f json-pretty",
    "ceph orch ps '' --service_name mon -f json-pretty",
    "ceph health detail -f yaml",
]


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage monitor service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.mon for test config
    """
    log.info("Running Ceph-admin Monitor test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing MON %s service" % command)
    monitor = Mon(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(monitor, command)
        method(config)
        # New/replaced mons may reappear with dual v1+v2; re-apply v2-only
        # when ODF defaults were requested via --custom-config.
        overrides = (
            config.get("overrides")
            or kw.get("test_data", {}).get("custom_config_dict")
            or {}
        )
        if command == "apply" and overrides_enabled(overrides, APPLY_ODF_DEFAULTS_KEY):
            log.info(
                "Re-applying v2-only mon addrs after mon apply "
                "(--custom-config apply-odf-defaults=true)"
            )
            monitor.check_service(
                service_name=monitor.SERVICE_NAME, timeout=300, interval=10
            )
            apply_v2_only_mon_addrs(monitor.shell)
    finally:
        # Get cluster state
        get_cluster_state(monitor, CLUSTER_STATE)
    return 0
