from ceph.ceph_admin.alert_manager import AlertManager
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.grafana import Grafana
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.node_exporter import NodeExporter
from ceph.ceph_admin.prometheus import Prometheus
from utility.log import Log

log = Log(__name__)

MONITORING = {
    "prometheus": Prometheus,
    "alertmanager": AlertManager,
    "grafana": Grafana,
    "node-exporter": NodeExporter,
}


def run(ceph_cluster, **kw):
    """Ceph-admin module to manage ceph-iscsi service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.iscsi for test config
    """
    log.info("Running Ceph-admin Provisioning test")

    config = kw.get("config")
    config["overrides"] = kw.get("test_data", {}).get("custom_config_dict")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    service = config.pop("service")
    _monitoring = MONITORING[service]

    log.info("Executing %s %s service" % (service, command))

    monitoring = _monitoring(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(monitoring, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(monitoring)

    return 0
