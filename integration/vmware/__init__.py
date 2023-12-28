from integration.vmware import VMWare
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


class CloudProvider:
    """Interface for common cloud operations"""

    def __init__(self, cloud, **config):
        log.info(f"Initiating instance for cloud '{cloud}'")

        self._type = cloud.lower()
        if self._type == "vmware":
            self._cloud = VMWare(**config)

        else:
            raise ConfigError(f"Unsupported cloud provider '{cloud}'")

    def __getattr__(self, name):
        return self._cloud.__getattribute__(name)

    def __repr__(self):
        return self._type
