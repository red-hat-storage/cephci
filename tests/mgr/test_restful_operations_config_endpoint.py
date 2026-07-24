import json

from api.exceptions import ConfigError, OperationFailedError
from api.restful.config import Config
from cli.cephadm.cephadm import CephAdm

# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}


def _get(config, url, endpoint, username, key):
    # Check if any <args> are present
    if config.get("args"):
        endpoint += f"/{config.get('args')}"
    _code, _ = Config(url=url, api=endpoint).get(
        username=username, key=key, check_sc=True
    )
    return _code


def _patch(config, url, endpoint, username, key):
    data = config.get("data")
    if not data:
        raise ConfigError("Mandatory config 'data' is missing")
    # Use PATCH method on the given pool_id of the RESTful module
    _code, _ = Config(url=url, api=endpoint).patch(
        username=username, key=key, data=data, check_sc=True
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
    method = config.get("method")
    status_code = config.get("status_code")
    if not (username and endpoint and method and status_code):
        raise ConfigError("Mandatory config is missing")

    # Get key for API user
    key = json.loads(CephAdm(node).ceph.restful.list_key()).get(username, None)
    if not key:
        raise OperationFailedError(f"Failed to get key for API user {username}")

    # Declare the URL for the request
    url = f"https://{mgr_ip}:8003"

    # Validate GET method on the endpoint
    if method == "GET":
        out = _get(config, url, endpoint, username, key)
        if out != status_code:
            raise OperationFailedError(
                f"RESTful GET method for {endpoint} failed with status code:{out}"
            )

    # Validate PATCH method on the endpoint
    elif method == "PATCH":
        out = _patch(config, url, endpoint, username, key)
        if out != status_code:
            raise OperationFailedError(
                f"RESTful PATCH method for {endpoint} failed with status code:{out}"
            )

    return 0
