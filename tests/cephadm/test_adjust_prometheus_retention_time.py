from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config
from utility.log import Log

log = Log(__name__)


def get_retention_time(node, promethus_api_host):
    """
    Get the retention_time set for prometheus daemon
    """
    cmd = f"curl -s {promethus_api_host}/api/v1/status/flags | grep -A2 retention"
    out = loads(node.exec_command(sudo=True, cmd=cmd)[0])["data"][
        "storage.tsdb.retention.time"
    ]
    return out


def run(ceph_cluster, **kw):
    """
    Validate the change in retention time for Prometheus
    """
    config = kw.get("config", {})
    admin = ceph_cluster.get_ceph_object("installer")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    installer_host = installer.hostname
    specs = config.get("specs")
    retention_time = config.get("retention_time")

    # Get prometheus api host
    kw = {"get-prometheus-api-host": ""}
    prometheus = CephAdm(installer).ceph.dashboard(**kw)

    # Validate the default retention time for prometheus using curl
    # it should be 15 days <to-do>
    _retention_time = get_retention_time(installer, prometheus)
    if _retention_time != "15d":
        raise OperationFailedError("retention_time for Prometheus is not as expected")

    # Set a new retention time using spec file
    file = create_yaml_config(installer, specs)
    c = {"pos_args": [], "input": file}
    CephAdm(installer, mount=file).ceph.orch.apply(check_ec=True, **c)

    # Redeploy the prometheus daemon to implement the changes
    out = CephAdm(installer).ceph.orch.redeploy("prometheus")
    if "Scheduled" not in out:
        raise OperationFailedError("Fail to redeploy Prometheus daemon")

    # Validate the changes using curl command
    timeout, interval = 300, 30
    for w in WaitUntil(timeout=timeout, interval=interval):
        _retention_time = get_retention_time(installer, prometheus)
        if _retention_time == "1y":
            log.info("retention_time for Prometheus is changed successfully")
            break
        log.error("retention_time is not as expected, rechecking!")
    if w.expired:
        raise OperationFailedError(
            "Failed to adjust the Prometheus retention time using spec"
        )

    # Get cluster FSID
    fsid = CephAdm(admin).ceph.fsid()
    if not fsid:
        raise OperationFailedError("Failed to get cluster FSID")

    # Set a new retention time by modifying the unit.run file
    file_path = f"/var/lib/ceph/{fsid}/prometheus.{installer_host}/unit.run"
    cmd = (
        f'sed -i -e " s#--storage.tsdb.retention.time=1y#--storage.tsdb.retention.time='
        f'{retention_time}#; " {file_path}'
    )
    installer.exec_command(sudo=True, cmd=cmd)

    # Validate the changes using curl command
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        _retention_time = get_retention_time(installer, prometheus)
        if _retention_time != retention_time:
            log.info("retention_time for Prometheus is changed successfully")
            break
        log.error("retention_time is not as expected, rechecking!")
    if w.expired:
        raise OperationFailedError(
            "Failed to adjust the Prometheus retention time using unit.run"
        )
    return 0
