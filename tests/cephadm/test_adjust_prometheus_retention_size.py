from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config
from utility.log import Log

log = Log(__name__)

PROMETHEUS_DEFAULT_PORT = 9090


def get_prometheus_api_url(installer, ceph_cluster):
    """
    Get Prometheus API base URL from dashboard; if not set, derive from orch.
    """
    kw = {"get-prometheus-api-host": ""}
    url = (CephAdm(installer).ceph.dashboard(**kw) or "").strip()
    if url:
        return url.rstrip("/")
    # Dashboard has no Prometheus API host set; get from orch and use default port
    out = CephAdm(installer).ceph.orch.ps(daemon_type="prometheus", format="json")
    daemons = loads(out) if isinstance(out, str) else out
    if not daemons:
        raise OperationFailedError("No Prometheus daemon found in cluster")
    hostname = daemons[0]["hostname"]
    for node in ceph_cluster.get_nodes():
        if node.hostname == hostname:
            return f"http://{node.ip_address}:{PROMETHEUS_DEFAULT_PORT}"
    raise OperationFailedError(
        f"Node for Prometheus hostname {hostname} not found in cluster"
    )


def get_retention_size(node, prometheus_api_url):
    """
    Get the retention_size set for prometheus daemon via /api/v1/status/flags.
    """
    url = f"{prometheus_api_url}/api/v1/status/flags"
    cmd = f"curl -s '{url}'"
    out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=True)
    data = loads(out)["data"]
    key = "storage.tsdb.retention.size"
    if key not in data:
        raise OperationFailedError(
            f"Prometheus flags response missing '{key}'; keys: {list(data.keys())}"
        )
    return data[key]


def run(ceph_cluster, **kw):
    """
    Validate the change in retention size for Prometheus
    """
    config = kw.get("config", {})
    installer = ceph_cluster.get_nodes(role="installer")[0]
    specs = config.get("specs")

    # Get prometheus API URL (from dashboard or from orch if not set)
    prometheus_url = get_prometheus_api_url(installer, ceph_cluster)

    # Validate the default retention size for prometheus using curl (should be 0B)
    _retention_size = get_retention_size(installer, prometheus_url)
    if _retention_size != "0B":
        raise OperationFailedError(
            "retention_size for Prometheus is not as expected"
        )

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
        _retention_size = get_retention_size(installer, prometheus_url)
        if _retention_size == "50MiB":
            log.info("retention_size for Prometheus is changed successfully")
            break
        log.error("retention_size is not as expected, rechecking!")
    if w.expired:
        raise OperationFailedError(
            "Failed to adjust the Prometheus retention size using spec"
        )
    return 0
