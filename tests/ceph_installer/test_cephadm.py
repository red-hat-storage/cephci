"""
Test suite that verifies the deployment of RedHat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""
import logging

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.alert_manager import AlertManager
from ceph.ceph_admin.cephfs_mirror import CephfsMirror
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.crash import Crash
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.grafana import Grafana
from ceph.ceph_admin.helper import get_cluster_state, validate_log_file_after_enable
from ceph.ceph_admin.host import Host
from ceph.ceph_admin.iscsi import ISCSI
from ceph.ceph_admin.mds import MDS
from ceph.ceph_admin.mgr import Mgr
from ceph.ceph_admin.mon import Mon
from ceph.ceph_admin.nfs import NFS
from ceph.ceph_admin.node_exporter import NodeExporter
from ceph.ceph_admin.orch import Orch
from ceph.ceph_admin.osd import OSD
from ceph.ceph_admin.prometheus import Prometheus
from ceph.ceph_admin.rbd_mirror import RbdMirror
from ceph.ceph_admin.rgw import RGW

LOG = logging.getLogger()


SERVICE_MAP = dict(
    {
        "alertmanager": AlertManager,
        "cephadm": CephAdmin,
        "crash": Crash,
        "grafana": Grafana,
        "host": Host,
        "iscsi": ISCSI,
        "mds": MDS,
        "mgr": Mgr,
        "mon": Mon,
        "nfs": NFS,
        "node-exporter": NodeExporter,
        "osd": OSD,
        "prometheus": Prometheus,
        "rgw": RGW,
        "orch": Orch,
        "rbd-mirror": RbdMirror,
        "cephfs-mirror": CephfsMirror,
        "daemon": Daemon,
    }
)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Return the status of the test execution run with the provided keyword arguments.

    Unlike other test suites, "steps" has been introduced to support workflow style
    execution along with customization.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: cluster deployment
            desc: Deploy a minimal cluster
            config:
                verify_cluster_health: true | false
                steps:
                    - config:
                        command: bootstrap
                        service: cephadm
                        base_cmd_args:
                        verbose: true
                        args:
                            mon-ip: node1
                    - config:
                        command: add_hosts
                        service: host
                        args:
                            attach_ip_address: true
                            labels: apply-all-labels
                    - config:
                        command: mon
                        service: mon
                        args:
                            placement:
                                label: mon
                    - config:
                        command: mgr
                        service: mgr
                        args:
                            placement:
                                label: mgr
                    - config:
                        command: apply
                        service: osd
                        args:
                            all-available-devices: true
                    - config:
                        command: shell
                        args:
                            - ceph osd pool create <pool_name> 3 3 replicated
    """
    LOG.info("Starting Ceph cluster deployment.")
    config = kwargs["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    try:
        steps = config.get("steps", [])
        for step in steps:
            cfg = step["config"]

            if cfg["command"] == "shell":
                cephadm.shell(base_cmd_args=cfg.get("base_cmd_args"), args=cfg["args"])
                continue

            obj = SERVICE_MAP[cfg["service"]](cluster=ceph_cluster, **config)
            func = fetch_method(obj, cfg["command"])
            func(cfg)

        if config.get("verify_cluster_health"):
            cephadm.cluster.check_health(
                rhbuild=config.get("rhbuild"), client=cephadm.installer
            )
        if config.get("verify_log_files"):
            isvalid = validate_log_file_after_enable(cephadm)
            if not isvalid:
                LOG.error("Log file validation failure")
                return 1

    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    finally:
        # Get cluster state
        get_cluster_state(cephadm)
    return 0
