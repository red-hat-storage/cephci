"""Bootstrap the ceph-volume suite with optional runtime SSH key-file paths."""

from copy import deepcopy

from cli.exceptions import ConfigError
from tests.cephadm.test_cephadm_bootstrap import run as run_bootstrap


def run(ceph_cluster, **kwargs):
    """Apply suite-local SSH overrides before delegating standard bootstrap.

    custom_config_dict accepts bootstrap-ssh-private-key and
    bootstrap-ssh-public-key as paths to key files on the installer.
    """
    config = deepcopy(kwargs.get("config"))
    if not config.get("bootstrap"):
        raise ConfigError("Mandatory config 'bootstrap' not provided")

    overrides = kwargs.get("test_data", {}).get("custom_config_dict") or {}
    for option in ("ssh-private-key", "ssh-public-key"):
        runtime_key = f"bootstrap-{option}"
        if runtime_key in overrides:
            value = overrides[runtime_key]
            if not isinstance(value, str) or not value.strip():
                raise ConfigError(
                    f"Runtime config '{runtime_key}' must be a key-file path"
                )
            config["bootstrap"][option] = value

    return run_bootstrap(ceph_cluster, **{**kwargs, "config": config})
