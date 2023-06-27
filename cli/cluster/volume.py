from cli.cloudproviders import CloudProvider
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


class Volume(CloudProvider):
    """Interface to perform volume operations"""

    def __init__(self, name, cloud="openstack", **config):
        """Initialize instance with provided details

        Args:
            name (str): Volume name
            cloud (str): Cloud type [openstack|ibmc|baremetal]

        **kwargs:
            <key-val> for cloud credentials
        """
        super(Volume, self).__init__(cloud, **config)

        self._volume = self._cloud.get_volume_by_name(name)
        self._id = self._cloud.get_volume_id(self._volume)

        self._name = name

    @property
    def name(self):
        """Volume name"""
        return self._name

    @property
    def id(self):
        """Volume id"""
        return self._id

    @property
    def state(self):
        """Volume state"""
        return self._cloud.get_volume_state_by_id(self.id) if self._volume else None

    def delete(self, timeout=300, interval=10):
        """Delete volume

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        if not self._volume:
            msg = f"Volume with name '{self.name}' doesn't exists"
            log.error(msg)
            raise OperationFailedError(msg)

        self.detach_volume(timeout, interval)
        self.destroy_volume(timeout, interval)

        self._volume = None
        return True

    def detach_volume(self, timeout=300, interval=10):
        """Detach volume from node

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        self._cloud.detach_volume(self._volume, timeout, interval)

    def destroy_volume(self, timeout=300, interval=10):
        """Destroy node

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        self._cloud.destroy_volume(self._volume, timeout, interval)
