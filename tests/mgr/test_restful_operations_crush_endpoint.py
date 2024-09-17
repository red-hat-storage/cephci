import json

from api.exceptions import ConfigError, OperationFailedError
from api.restful.crush import Crush
from cli.cephadm.cephadm import CephAdm

# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}


def _get(url, endpoint, username, key, status_code):
    _code, _ = Crush(url=url, api=endpoint).get(
        username=username, key=key, check_sc=True
    )
    return _code


def run(ceph_cluster, **kw):
    config = kw.get("config", {})

    # Get the MGR node
    node = ceph_cluster.get_nodes(role="mgr")[0]
    mgr_ip = node.ip_address

    # Read the test configs
    username = config.get("username")
    endpoint = config.get("endpoint")
    status_code = config.get("status_code")
    if not (username and endpoint and status_code):
        raise ConfigError("Mandatory config is missing")

    # Get key for API user
    key = json.loads(CephAdm(node).ceph.restful.list_key()).get(username, None)
    if not key:
        raise OperationFailedError(f"Failed to get key for API user {username}")

    # Declare the URL for the request
    url = f"https://{mgr_ip}:8003"

    # Validate GET method on /crush/rule endpint
    out = _get(url, endpoint, username, key, status_code)
    if out != status_code:
        raise OperationFailedError(
            f"RESTful GET method for {endpoint} failed with status code:{out}"
        )
    return 0
