import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.device import Device
from ceph.ceph_admin.helper import get_cluster_state

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Executing the device operation using cephadm
        ceph orch device ls
        ceph orch device zap <hostname> <path> [--force]

    Args:
        ceph_cluster
        kw: command and service are passed from the test case.

    Example:
          Testing ceph orch device zap node3:/dev/vdb

    cconfig:
        command: zap
        base_cmd_args:
          verbose: true
        pos_args:
          - "node3"
          - "/dev/vdb"

      Returns:
          output, error   returned by the command.
    """
    config = kw.get("config")

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing service %s service" % command)
    device = Device(cluster=ceph_cluster, **config)

    try:
        method = fetch_method(device, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(device)
    return 0
