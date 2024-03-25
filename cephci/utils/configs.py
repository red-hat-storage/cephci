import os

import yaml

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cephci.yaml")
CONFIG = None


def get_configs(config=None):
    """Read configurations from yaml

    Args:
        config (str): Config file path
    """
    global CONFIG
    if CONFIG:
        return CONFIG

    # Check for default config
    config = config if config else DEFAULT_CONFIG_PATH

    log.info(f"Loading config file - {config}")
    with open(config, "r") as _stream:
        try:
            CONFIG = yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid configuration file '{config}'")


def _get_credentials():
    """Get credentials from config"""
    if not CONFIG:
        raise ConfigError("Configuration is not passed")

    try:
        return CONFIG["credentials"]
    except KeyError:
        raise ConfigError("Credentials configurations are missing from config")


def get_subscription_credentials(server):
    """Get subscription manager details from config

    Args:
        server (str): Subscription manager server
    """
    _dict = {}
    log.info(f"Loading subscription manager details for server '{server}'")
    try:
        _subscription = _get_credentials()["subscription"]
        _server = _subscription[server]
        _serverurl = _dict["serverurl"] = _server["serverurl"]
        _dict["baseurl"] = _server["baseurl"]
        _dict["username"] = _server["username"]
        _dict["password"] = _server["password"]
        _dict["timeout"] = int(_server.get("timeout", 60))
        _dict["retry"] = int(_server.get("retry", 6))

        log.info(f"Loaded details for server - {_serverurl}")
    except KeyError:
        raise ConfigError(f"Insufficient config for '{server}' in subscription")

    return _dict


def get_registry_credentials(server, build):
    """Get container registry credentials from config

    Args:
        server (str): Container registry server
        build (str): Build type
    """
    _dict = {}
    log.info(f"Loading registry credentials for server '{server}' and build '{build}'")
    try:
        _registry = _get_credentials()["registry"]
        _build = _registry[build]
        _server = _build[server]
        _serverurl = _dict["registry"] = _server["registry"]
        _dict["username"] = _server["username"]
        _dict["password"] = _server["password"]

        log.info(f"Loaded registry credentials for server - {_serverurl}")
    except KeyError:
        raise ConfigError(f"Insufficient config for '{server}' & '{build}' registry")

    return _dict


def get_cloud_credentials(cloud):
    """Get cloud credentials from config

    Args:
        cloud (str): Cloud type
    """
    _dict, msg = {}, f"Loaded {cloud} cloud details for server"
    log.info(f"Loading cloud credentials for '{cloud}'")
    try:
        # Get configs from file
        _cloud = _get_credentials()["cloud"]
        _server = _cloud[cloud]

        # Get baremetal configs
        if cloud == "baremetal":
            server = _dict["server"] = _server["server"]
            _dict["env"] = _server["env"]
            _dict["ssh_key"] = _server.get("ssh_key")

            msg += server

        # Get openstack configs
        elif cloud == "openstack":
            _authurl = _dict["auth-url"] = _server["auth-url"]
            _dict["auth-version"] = _server["auth-version"]
            _dict["tenant-name"] = _server["tenant-name"]
            _dict["service-region"] = _server["service-region"]
            _dict["domain"] = _server["domain"]
            _dict["tenant-domain-id"] = _server["tenant-domain-id"]

            msg += _authurl

        # Set common configs
        _dict["username"] = _server.get("username")
        _dict["password"] = _server.get("password")
        _dict["timeout"] = int(_server.get("timeout", 1800))
        _dict["retry"] = int(_server.get("retry", 60))

        # DEBUG
        log.info(msg)
    except KeyError:
        raise ConfigError(f"Insufficient config for '{cloud}' in cloud")

    return _dict


def get_vault_credentials():
    _dict = {}
    _agent = {}
    log.info("Loading cloud credentials for vault server")
    try:
        _vault = _get_credentials()["vault"]
        _dict["url"] = _vault["url"]
        _agent["auth"] = _vault["agent"]["auth"]
        _agent["engine"] = _vault["agent"]["engine"]
        _agent["role-id"] = _vault["agent"]["role-id"]
        _agent["secret-id"] = _vault["agent"]["secret-id"]
        _agent["prefix"] = _vault["agent"]["prefix"]
        _dict["agent"] = _agent
    except KeyError:
        raise ConfigError("Insufficient config for vault server")

    return _dict


def _get_repos():
    """Get repositories from config file"""
    if not CONFIG:
        raise ConfigError("Configuration is not passed")

    try:
        return CONFIG["repos"]
    except KeyError:
        raise ConfigError("Repo configurations are missing from config")


def get_repos(server, version):
    """Get repositories from config for server and version

    Args:
        server (str): Subscription server
        version (str): Distro version
    """
    log.info(
        f"Loading repositories for server '{server}' and distro version '{version}'"
    )
    try:
        _repo = _get_repos()[server]
        return _repo[version]
    except KeyError:
        raise ConfigError(f"Insufficient config for {server} & '{version}' in repo")


def _get_packages():
    """Get packages from config file"""
    if not CONFIG:
        raise ConfigError("Configuration is not passed")

    try:
        return CONFIG["packages"]
    except KeyError:
        raise ConfigError("Packages configurations are missing from config")


def _get_images():
    """Get images from config file"""
    if not CONFIG:
        raise ConfigError("Configuration is not passed")

    try:
        return CONFIG["images"]
    except KeyError:
        raise ConfigError("Image configurations are missing from config")


def get_packages(version=None):
    """Get packages from config for version

    Args:
        version (str): Distro version
    """
    log.info(f"Loading packages distro version '{version}'")
    try:
        _packages = _get_packages()
        packages = _packages["all"]
        if version:
            packages.extend(_packages[version])
        return packages
    except KeyError:
        raise ConfigError(f"Insufficient config for '{version}' in package")


def get_images(build_type):
    """Get images from config for build type

    Args:
        build_type (str): Ceph build version (pacific, quincy)
    """
    log.info(f"Loading images for build '{build_type}'")
    try:
        return _get_images()[build_type]
    except KeyError:
        raise ConfigError(f"Insufficient config for '{build_type}' in images")


def _get_reports():
    """Get report details from config file"""
    if not CONFIG:
        raise ConfigError("Configuration is not passed")

    try:
        return CONFIG["reports"]
    except KeyError:
        raise ConfigError("Reports configurations are missing from config")


def get_reports(service):
    """Get reporting details from config for service

    Args:
        service (str): Service name
    """
    _dict = {}
    log.info(f"Loading credentials for reporting service '{service}'")
    try:
        _service = _get_reports()[service]
        _dict["url"] = _service["url"]
        _dict["svn_repo"] = _service["svn_repo"]
        _dict["user"] = _service["user"]
        _dict["token"] = _service["token"]
        _dict["default_project"] = _service.get("default_project", "CEPH")
        _dict["cert_path"] = _service["cert_path"]
        return _dict
    except KeyError:
        raise ConfigError(f"Insufficient config for '{service}'")


def get_database_credentials(config, database):
    """
    Gets the credentials of the database
    Args:
        config (conf): User specified config
        database (str): Name of the database
    """
    get_configs(config)
    if not CONFIG:
        raise ConfigError("Configuration is not passed")
    _dict = {}
    try:
        log.info(f"Loading credentials for reporting service '{database}'")
        credentials = CONFIG["reports"][database]
        _dict["name"] = credentials["name"]
        _dict["user"] = credentials["user"]
        _dict["pwd"] = credentials["pwd"]
        _dict["host"] = credentials["host"]
        _dict["port"] = credentials["port"]
        return _dict
    except KeyError:
        raise ConfigError(f"Insufficient config for '{database}'")
