import json

from api.exceptions import ConfigError, OperationFailedError
from api.restful.request import Request
from cli.cephadm.cephadm import CephAdm

# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}

# Missing config parameter err msg
CONFIG_ERR_MSG = "Mandatory config parameter '{}' not provided"


def _get(url, endpoint, username, key, status_code, check_sc):
    # Use GET method on the /request or /request/<arg> endpoint
    if check_sc:
        response, _ = Request(url=url, api=endpoint).get(
            username=username, key=key, check_sc=True
        )
        if response == status_code:
            return 0
    else:
        response = Request(url=url, api=endpoint).get(
            username=username, key=key, check_sc=False
        )
    if not response:
        raise OperationFailedError(
            f"RESTful GET method for {endpoint} failed with invalid response"
        )
    return response


def _post(config, url, endpoint, username, key, status_code):
    data = config.get("data")
    if not data:
        raise ConfigError(CONFIG_ERR_MSG.format("data"))
    # Use POST method on the /request endpoint of the RESTful module
    _code, _ = Request(url=url, api=endpoint).post(
        username=username, key=key, data=data, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful POST method for {endpoint} failed with status code:{_code}"
    )


def _delete(config, url, endpoint, username, key, status_code):
    # Check if any <args> are present
    if config.get("args"):
        endpoint += f'/{config.get("args")}'
    # Use DELETE method on the /request or or /request/<arg>
    _code, _ = Request(url=url, api=endpoint).delete(
        username=username, key=key, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful DELETE method for {endpoint} failed with status code:{_code}"
    )


def run(ceph_cluster, **kw):
    """
    Valiate all suported methods on the RESTful request endpoint
    """
    config = kw.get("config", {})

    # Get the MGR node
    node = ceph_cluster.get_nodes(role="mgr")[0]
    mgr_ip = node.ip_address

    # Read the test configs
    username = config.get("username")
    if not username:
        raise ConfigError(CONFIG_ERR_MSG.format("username"))

    endpoint = config.get("endpoint")
    if not endpoint:
        raise ConfigError(CONFIG_ERR_MSG.format("endpoint"))

    method = config.get("method")
    if not method:
        raise ConfigError(CONFIG_ERR_MSG.format("method"))

    status_code = config.get("status_code")
    if not status_code:
        raise ConfigError(CONFIG_ERR_MSG.format("status_code"))

    # Get key for API user
    key = json.loads(CephAdm(node).ceph.restful.list_key()).get(username, None)
    if not key:
        raise OperationFailedError(f"Failed to get key for API user {username}")

    # Declare the URL for the request
    url = f"https://{mgr_ip}:8003"

    if method == "GET":
        request_id = _get(url, endpoint, username, key, status_code, check_sc=False)
        _ids = [_request["id"] for _request in request_id.json()]
        # Use the _ids in GET method for /request/<args> endppoint
        for id in _ids:
            _endpoint = endpoint + f"/{id}"
            _get(url, _endpoint, username, key, status_code, check_sc=True)
    elif method == "POST":
        return _post(config, url, endpoint, username, key, status_code)
    elif method == "DELETE":
        return _delete(config, url, endpoint, username, key, status_code)
    return 0
