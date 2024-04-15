from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.orch import Orch
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Executing the service operation using cephadm
        ceph orch remove|restart|start|stop|reconfigure|redeploy  <service name>.

    Args:
        ceph_cluster
        kw: command and service are passed from the test case.

    Example:
          Testing ceph orch restart mon

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
    orch = Orch(cluster=ceph_cluster, **config)

    try:
        method = fetch_method(orch, command)
        method(config)
    finally:
        # Get cluster state
        if kw.get("no_cluster_state", True):
            get_cluster_state(orch)
    return 0
