from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config
from utility.log import Log

log = Log(__name__)


def get_retention_size(node, promethus_api_host):
    """
    Get the retention_size set for prometheus daemon
    """
    cmd = f"curl -s {promethus_api_host}/api/v1/status/flags | grep -A2 retention"
    out = loads(node.exec_command(sudo=True, cmd=cmd)[0])["data"][
        "storage.tsdb.retention.size"
    ]
    return out


def run(ceph_cluster, **kw):
    """
    Validate the change in retention size for Prometheus
    """
    config = kw.get("config", {})
    installer = ceph_cluster.get_nodes(role="installer")[0]
    specs = config.get("specs")

    # Get prometheus api host
    kw = {"get-prometheus-api-host": ""}
    prometheus = CephAdm(installer).ceph.dashboard(**kw)

    # Validate the default retention size for prometheus using curl
    # it should be 0B
    _retention_size = get_retention_size(installer, prometheus)
    if _retention_size != "0B":
        raise OperationFailedError("retention_size for Prometheus is not as expected")

    # Set a new retention size using spec file
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
        _retention_size = get_retention_size(installer, prometheus)
        if _retention_size == "50MiB":
            log.info("retention_size for Prometheus is changed successfully")
            break
        log.error("retention_size is not as expected, rechecking!")
    if w.expired:
        raise OperationFailedError(
            "Failed to adjust the Prometheus retention size using spec"
        )
    return 0
