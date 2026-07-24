import json

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm


class RedeployServiceError(Exception):
    pass


def verify_redeploy_demon(node, daemon):
    """
    Verify redeploy demon.
    Args:
        node (str): installer node object
        daemon (str): reploy demon
    """
    timeout, interval = 60, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        result = CephAdm(node).ceph.orch.ps(format="json")
        all_running_daemons = [
            data.get("daemon_name") for data in json.loads(result[node[0].hostname][0])
        ]
        if (item == daemon for item in all_running_daemons):
            break
    if w.expired:
        raise RedeployServiceError("Redeploy deamons check failed")


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="installer")
    config = kw.get("config")
    exp_out = config.get("result")
    service_name = config.get("service_name")
    if not service_name:
        raise RedeployServiceError("service_name value not present in config")
    image_name = config.get("image")
    if not image_name:
        raise RedeployServiceError("image value not present in config")
    result = CephAdm(node).ceph.orch.ps(format="json")
    daemon_name = list(
        filter(
            lambda data: service_name in data.get("daemon_name"),
            json.loads(result[node[0].hostname][0]),
        )
    )[0].get("daemon_name")
    result = CephAdm(node).ceph.orch.daemon.redeploy(daemon_name, image=image_name)
    result = list(result.values())[0][0].strip()
    if exp_out not in result:
        raise RedeployServiceError("Fail to redeploy deamon")
    verify_redeploy_demon(node, daemon_name)
    return 0
