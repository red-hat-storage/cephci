import os

import yaml

from cli.exceptions import ConfigError
from utility.utils import (
    registry_host_from_image,
    resolve_registry_host,
    resolve_registry_login,
)


def get_cephci_config():
    """Get data from ~/.cephci.yaml"""
    home_dir = os.path.expanduser("~")
    cfg_file = os.path.join(home_dir, ".cephci.yaml")

    try:
        with open(cfg_file, "r") as yml:
            return yaml.safe_load(yml)
    except ConfigError:
        raise ConfigError("Failed to read ~/.cephci.yaml")


def get_registry_details(ibm_build=False, registry=None, image=None):
    """Get registry credentials by host from the ``registries:`` section.

    Args:
        ibm_build (bool): Unused; retained for call-site compatibility.
        registry (str): Registry hostname. Preferred when set.
        image (str): Container image; host is derived when ``registry`` is unset.

    Raises:
        ConfigError: when the host cannot be resolved or is missing from config.
    """
    _ = ibm_build  # retained for API compatibility
    registry_host = resolve_registry_host(explicit=registry, image=image)
    if not registry_host:
        raise ConfigError(
            "Registry host is required. Pass registry=, image=, or "
            "--custom-config bootstrap-registry=<host>."
        )
    try:
        return resolve_registry_login(registry_host)
    except KeyError as err:
        raise ConfigError(str(err)) from err


__all__ = [
    "get_cephci_config",
    "get_registry_details",
    "registry_host_from_image",
    "resolve_registry_host",
    "resolve_registry_login",
]
