import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.helper import get_cluster_state

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Executing the daemon operation using cephadm
        ceph orch daemon restart|start|stop|reconfigure|redeploy  <service name>.

    Args:
        ceph_cluster
        kw: command and service are passed from the test case.

    Example:
          Testing ceph orch daemon restart mon

    config:
      command: restart
      service: mon

      Returns:
          output, error   returned by the command.
    """
    config = kw.get("config")

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing service %s service" % command)
    daemon = Daemon(cluster=ceph_cluster, **config)

    try:
        method = fetch_method(daemon, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(daemon)
    return 0
