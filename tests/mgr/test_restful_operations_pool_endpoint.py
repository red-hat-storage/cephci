import json

from api.exceptions import ConfigError, OperationFailedError
from api.restful.pool import Pool
from cli.cephadm.cephadm import CephAdm

# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}

# Missing config parameter err msg
CONFIG_ERR_MSG = "Mandatory config parameter '{}' not provided"


def _get(config, osd_node, url, endpoint, username, key, status_code):
    # Check if any <args> are present
    if config.get("args"):
        # Get pool name from config
        pool_name = config.get("args")
        # Get ID for the given pool name
        out = json.loads(CephAdm(osd_node).ceph.osd.pool.ls(format="json"))
        if not out:
            raise OperationFailedError("Failed to fetch the pools")
        pool_id = None
        for pool in out:
            if pool["pool_name"] == pool_name:
                pool_id = pool["pool_id"]
                break
        if not pool_id:
            raise OperationFailedError(f"Failed to get pool_id for {pool_name}")
        endpoint += f"/{pool_id}"
    _code, _ = Pool(url=url, api=endpoint).get(
        username=username, key=key, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful GET method for {endpoint} failed with status code:{_code}"
    )


def _post(config, url, endpoint, username, key, status_code):
    data = config.get("data")
    if not data:
        raise ConfigError(CONFIG_ERR_MSG.format("data"))
    # Use POST method on the /pool endpoint of the RESTful module
    _code, _ = Pool(url=url, api=endpoint).post(
        username=username, key=key, data=data, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful POST method for {endpoint} failed with status code:{_code}"
    )


def _patch(config, osd_node, url, endpoint, username, key, status_code):
    data = config.get("data")
    if not data:
        raise ConfigError(CONFIG_ERR_MSG.format("data"))
    # Get pool name from config
    pool_name = config.get("args")
    # Get ID for the given pool name
    out = json.loads(CephAdm(osd_node).ceph.osd.pool.ls(format="json"))
    if not out:
        raise OperationFailedError("Failed to fetch the pools")
    pool_id = None
    for pool in out:
        if pool["pool_name"] == pool_name:
            pool_id = pool["pool_id"]
            break
    if not pool_id:
        raise OperationFailedError(f"Failed to get pool_id for {pool_name}")
    endpoint += f"/{pool_id}"
    # Use PATCH method on the given pool_id of the RESTful module
    _code, _ = Pool(url=url, api=endpoint).patch(
        username=username, key=key, data=data, check_sc=True
    )
    if _code == status_code:
        return 0
    raise OperationFailedError(
        f"RESTful PATCH method for {endpoint} failed with status code:{_code}"
    )


def run(ceph_cluster, **kw):
    config = kw.get("config", {})

    # Get the MGR node
    node = ceph_cluster.get_nodes(role="mgr")[0]
    mgr_ip = node.ip_address

    # Get OSD node to get pool ID
    osd_node = ceph_cluster.get_nodes(role="osd")[0]

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
        return _get(config, osd_node, url, endpoint, username, key, status_code)

    elif method == "POST":
        return _post(config, url, endpoint, username, key, status_code)

    elif method == "PATCH":
        return _patch(config, osd_node, url, endpoint, username, key, status_code)
    return 0
