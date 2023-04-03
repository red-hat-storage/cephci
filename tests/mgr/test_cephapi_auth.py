from api import BadRequestError
from api.cephapi.auth.auth import Auth
from utility.log import Log

# LOG constant
LOG = Log(__name__)

# Missing config parameter err msg
CONFIG_ERR_MSG = "Mandatory config parameter '{}' not provided"


# Request header
HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}


# Mandatory test config exception
class ConfigNotFoundError(Exception):
    pass


# Unexpected response status code exception
class UnexpectedStatusCodeError(Exception):
    pass


# Unexpected response exception
class UnexpectedResponseError(Exception):
    pass


# Missing resource exception
class ResourceNotFoundError(Exception):
    pass


def validate_configs(config):
    """Validate test config parameters"""
    cephapi = config.get("cephapi")
    if not cephapi:
        raise ConfigNotFoundError(CONFIG_ERR_MSG.format("cephapi"))

    api = cephapi.get("api")
    if not api:
        raise ConfigNotFoundError(CONFIG_ERR_MSG.format("api"))

    data = cephapi.get("data")
    if api in ["auth", "auth.check"] and not data:
        raise ConfigNotFoundError(CONFIG_ERR_MSG.format("data"))

    method = cephapi.get("method")
    if not method:
        raise ConfigNotFoundError(CONFIG_ERR_MSG.format("method"))

    status_code = cephapi.get("status_code")
    if not status_code:
        raise ConfigNotFoundError(CONFIG_ERR_MSG.format("status_code"))


def test_auth(url, method, status_code, data):
    try:
        _code, _json = getattr(Auth(url=url, header=HEADER), method)(**data)
        LOG.debug(f"Recieved status code '{_code}'")

        token = _json.get("token")
        if _code == 201 and token:
            LOG.debug(f"Recieved expected token in response json - '{token}'")
            return 0

        LOG.error("Failed to get token in response json")

    except BadRequestError:
        if int(status_code) == 400:
            LOG.info("Recieved 'Bad Request' response as expected")
            return 0

        raise UnexpectedResponseError("Recieved unexpected response 'Bad Request'")

    except Exception as e:
        raise UnexpectedStatusCodeError(f"Unexpected error '{str(e)}'")

    return 1


def test_auth_check(url, method, data):
    _code, _json = getattr(Auth(url=url, header=HEADER), method)(**data)
    LOG.debug(f"Recieved status code '{_code}'")

    token = _json.get("token")
    if _code == 201:
        LOG.debug(f"Recieved token from 'auth' API - {token}")

        _code, _json = getattr(Auth(url=url, header=HEADER).check, method)(
            token, check_sc=True
        )
        LOG.debug(f"Recieved status code '{_code}'")

        if _code == 200:
            LOG.info("Validation successfull for token")
            return 0

    raise UnexpectedStatusCodeError(f"Unexpected status code '{_code}'")


def test_auth_logout(url, method, data):
    _code, _ = getattr(Auth(url=url, header=HEADER).logout, method)(**data)
    LOG.debug(f"Recieved status code '{_code}'")

    if _code == 200:
        LOG.info("Validation successfull for logout")
        return 0

    raise UnexpectedStatusCodeError(f"Unexpected status code '{_code}'")


def run(ceph_cluster, **kwargs):
    """Validate cephapi endpoints"""
    config = kwargs.get("config", {})
    validate_configs(config)

    # Get dashboard URL from mgr service
    url = ceph_cluster.get_mgr_services().get("dashboard")
    if not url:
        raise ResourceNotFoundError("Dashboard not configured")

    # Read cephapi test configs
    cephapi = config.get("cephapi")
    api = cephapi.get("api")
    method = cephapi.get("method")
    data = cephapi.get("data", {})

    # Log test config
    info = f"Validating Ceph Api '{api}' with method '{method}'"
    if data:
        info += f" for data '{data}'"
    LOG.info(info)

    # Set check for request reponse
    data["check_sc"] = True

    # Validate cephapi endpoints
    if api == "auth":
        status_code = cephapi.get("status_code")
        return test_auth(url, method, status_code, data)

    elif api == "auth.check":
        return test_auth_check(url, method, data)

    elif api == "auth.logout":
        return test_auth_logout(url, method, data)
