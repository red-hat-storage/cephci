from cli.cloudproviders.openstack import Openstack
from cli.exceptions import ConfigError


class CloudProvider:
    def __init__(self, cloud, **config):
        self._cloud = self._get_cloud(cloud, **config)

    def _get_cloud(self, cloud, **config):
        """Get cloud object"""
        if cloud.lower() == "openstack":
            return Openstack(**config)

        elif cloud.lower() == "ibmc":
            pass

        else:
            raise ConfigError(f"Unsupported cloud provider '{cloud}'")

    def nodes(self, prefix):
        """Get nodes with prefix"""
        return self._cloud.get_nodes_by_prefix(prefix)

    def volumes(self, prefix):
        """Get volumes with prefix"""
        return self._cloud.get_volumes_by_prefix(prefix)
