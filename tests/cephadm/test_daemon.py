from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.helper import get_cluster_state
from utility.log import Log

log = Log(__name__)


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
    tests = config.get("steps", [{"config": config}])

    for test in tests:
        # Manage Ceph daemon using ceph-admin daemon orchestration commands
        _test_cfg = test["config"]
        command = _test_cfg.pop("command")
        log.info("Executing service %s service" % command)
        daemon = Daemon(cluster=ceph_cluster, **_test_cfg)

        try:
            method = fetch_method(daemon, command)
            method(_test_cfg)
        except Exception as err:
            log.error(err)
        finally:
            # Get cluster state
            get_cluster_state(daemon)
    return 0
