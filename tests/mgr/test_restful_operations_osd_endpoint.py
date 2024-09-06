import json

from api.exceptions import ConfigError, OperationFailedError
from api.restful.osd import Osd
from cli.cephadm.cephadm import CephAdm

# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}

# Missing config parameter err msg
CONFIG_ERR_MSG = "Mandatory config parameter '{}' not provided"


def _get(config, url, endpoint, username, key, status_code):
    # Check if any <args> are present
    if config.get("args"):
        endpoint += f'/{config.get("args")}'
    # Check if the endpoint needs to have "command"
    if config.get("command"):
        endpoint += "/command"
    # Use GET method on the /osd or or /osd/<arg> or /osd/<arg>/command
    # endpoint of the RESTful module
    _code, _ = Osd(url=url, api=endpoint).get(username=username, key=key, check_sc=True)
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful GET method for {endpoint} failed with status code:{_code}"
    )


def _post(config, url, endpoint, username, key, status_code):
    args = config.get("args")
    if not args:
        raise ConfigError(CONFIG_ERR_MSG.format("args"))
    data = config.get("data")
    if not data:
        raise ConfigError(CONFIG_ERR_MSG.format("data"))
    # Use POST method on the /osd/<arg>/command endpoint of the RESTful module
    endpoint += f"/{args}/command"
    _code, _ = Osd(url=url, api=endpoint).post(
        username=username, key=key, data=data, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful POST method for {endpoint} failed with status code:{_code}"
    )


def _patch(config, url, endpoint, username, key, status_code):
    args = config.get("args")
    if not args:
        raise ConfigError(CONFIG_ERR_MSG.format("args"))
    data = config.get("data")
    if not data:
        raise ConfigError(CONFIG_ERR_MSG.format("data"))
    # Use PATCH method on /osd/<arg> endpoint of the RESTful module
    endpoint += f"/{args}"
    _code, _ = Osd(url=url, api=endpoint).patch(
        username=username, key=key, data=data, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful PATCH method for {endpoint} failed with status code:{_code}"
    )


def run(ceph_cluster, **kw):
    """
    Valiate all suported methods on the RESTful osd endpoint
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
        return _get(config, url, endpoint, username, key, status_code)
    elif method == "POST":
        return _post(config, url, endpoint, username, key, status_code)
    elif method == "PATCH":
        return _patch(config, url, endpoint, username, key, status_code)
    return 0
