from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


class Volume:
    """Interface to perform volume operations"""

    def __init__(self, name, cloud):
        """Initialize instance with provided details

        Args:
            name (str): Volume name
            cloud (str): Cloud type [openstack|ibmc|baremetal]

        **kwargs:
            <key-val> for cloud credentials
        """
        self._cloud, self._name = cloud, name

    @property
    def cloud(self):
        """Cloud provider object"""
        return self._cloud

    @property
    def name(self):
        """Volume name"""
        return self._name

    @property
    def id(self):
        """Volume id"""
        return self.cloud.get_volume_id(self.name)

    @property
    def state(self):
        """Volume state"""
        return self.cloud.get_volume_state_by_name(self.name)

    def create(self, size, timeout=300, interval=10):
        """Create volume on cloud

        Args:
            size (int): Volume size
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        if self.id:
            msg = f"Volume with name '{self.name}' already exists"
            log.error(msg)
            raise OperationFailedError(msg)

        self.cloud.create_volume(self.name, size, timeout, interval)

        return True

    def delete(self, timeout=300, interval=10):
        """Delete volume

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        if not self.id:
            msg = f"Volume with name '{self.name}' doesn't exists"
            log.error(msg)
            raise OperationFailedError(msg)

        self.cloud.detach_volume(self.name, timeout, interval)
        self.cloud.destroy_volume(self.name, timeout, interval)

        return True

    def detach_volume(self, timeout=300, interval=10):
        """Detach volume from node

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        volume = self.get_volume_by_id(self.id)
        self.cloud.detach_volume(volume, timeout, interval)

    def destroy_volume(self, timeout=300, interval=10):
        """Destroy node

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        volume = self.get_volume_by_id(self.id)
        self.cloud.destroy_volume(volume, timeout, interval)
