"""
Test suite that verifies the deployment of RedHat Ceph Storage via the cephadm CLI.

The intent of the suite is to simulate a standard operating procedure expected by a
customer.
"""
import logging

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.alert_manager import AlertManager
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.grafana import Grafana
from ceph.ceph_admin.host import Host
from ceph.ceph_admin.iscsi import ISCSI
from ceph.ceph_admin.mds import MDS
from ceph.ceph_admin.mgr import Mgr
from ceph.ceph_admin.mon import Mon
from ceph.ceph_admin.nfs import NFS
from ceph.ceph_admin.node_exporter import NodeExporter
from ceph.ceph_admin.osd import OSD
from ceph.ceph_admin.prometheus import Prometheus
from ceph.ceph_admin.rgw import RGW

LOG = logging.getLogger()
SERVICE_MAP = dict(
    {
        "alertmanager": AlertManager,
        "cephadm": CephAdmin,
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
                steps:
                    - config:
                        command: bootstrap
                        service: cephadm
                        base_cmd_args:
                        verbose: true
                        args:
                            mon-ip: node1
                    - config:
                        command: add
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

    try:
        config = kwargs["config"]
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        steps = config.get("steps")

        for step in steps:
            cfg = step["config"]

            if cfg["command"] == "shell":
                cephadm.shell(args=cfg["args"])
                continue

            obj = SERVICE_MAP[cfg["service"]](cluster=ceph_cluster, **config)
            func = fetch_method(obj, cfg["command"])
            func(cfg)

        return 0
    except BaseException as be:  # noqa
        LOG.error(be)
        return 1
