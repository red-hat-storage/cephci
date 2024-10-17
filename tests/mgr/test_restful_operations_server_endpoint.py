import json

from api.exceptions import ConfigError, OperationFailedError
from api.restful.server import Server
from ceph.utils import get_node_by_id
from cli.cephadm.cephadm import CephAdm

# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}

# Missing config parameter err msg
CONFIG_ERR_MSG = "Mandatory config parameter '{}' not provided"


def _get(config, ceph_cluster, url, endpoint, username, key):
    # Check if any <args> are present
    if config.get("args"):
        server_fqdn = get_node_by_id(ceph_cluster, config.get("args")).hostname
        endpoint += f"/{server_fqdn}"
    # Use GET method on the /server or or /server/<arg>
    _code, _ = Server(url=url, api=endpoint).get(
        username=username, key=key, check_sc=True
    )
    return _code


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

    status_code = config.get("status_code")
    if not status_code:
        raise ConfigError(CONFIG_ERR_MSG.format("status_code"))

    # Get key for API user
    key = json.loads(CephAdm(node).ceph.restful.list_key()).get(username, None)
    if not key:
        raise OperationFailedError(f"Failed to get key for API user {username}")

    # Declare the URL for the request
    url = f"https://{mgr_ip}:8003"

    # Validate the GET method
    out = _get(config, ceph_cluster, url, endpoint, username, key)
    if out != status_code:
        raise OperationFailedError(
            f"RESTful GET method for {endpoint} failed with status code:{out}"
        )
    return 0
