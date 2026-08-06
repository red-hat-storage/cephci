"""Preflight checks for Regional File Mount / Virtual Server multi-route tests."""

from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify dual-homed NFS and client networking before scenario tests.

    Fails fast when the wrong global conf was used or OpenStack VMs are
    single-homed.
    """
    config = kw.get("config") or {}
    try:
        vs.preflight_multi_route_environment(ceph_cluster, config)
        log.info("Virtual server network preflight passed")
        return 0
    except Exception as exc:
        log.error("Virtual server network preflight failed: %s", exc)
        return 1
