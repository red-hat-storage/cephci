"""Registry of the infrastructure backends supported by CephCI.

Adding support for a cloud means adding a module here and one entry to
:data:`PROVIDERS`; the orchestration itself does not know the backends by name.
"""

from typing import Optional

from cephci.providers.aws import AWSProvider
from cephci.providers.baremetal import BaremetalProvider
from cephci.providers.base import CloudProvider, NodeLogin
from cephci.providers.ibmc import IBMCProvider
from cephci.providers.ocpvirt import OCPVirtProvider
from cephci.providers.onecloud import OneCloudProvider
from cephci.providers.openstack import OpenStackProvider

__all__ = [
    "AWSProvider",
    "BaremetalProvider",
    "CloudProvider",
    "IBMCProvider",
    "NodeLogin",
    "OCPVirtProvider",
    "OneCloudProvider",
    "OpenStackProvider",
    "PROVIDERS",
    "get_provider",
]

PROVIDERS = {
    "openstack": OpenStackProvider,
    "ibmc": IBMCProvider,
    "aws": AWSProvider,
    "onecloud": OneCloudProvider,
    "ocpvirt": OCPVirtProvider,
    "baremetal": BaremetalProvider,
}


def get_provider(cloud_type: Optional[str]) -> Optional[CloudProvider]:
    """Return the provider that serves the given value of --cloud.

    Args:
        cloud_type (str): The requested infrastructure.

    Returns:
        CloudProvider: An instance of the matching provider, or None when the
            infrastructure is not supported. Callers decide whether an unsupported
            cloud is fatal.
    """
    if not cloud_type:
        return None

    if cloud_type in PROVIDERS:
        return PROVIDERS[cloud_type]()

    # Historically any cloud whose name contains baremetal is user provided.
    if "baremetal" in cloud_type:
        return BaremetalProvider()

    return None
