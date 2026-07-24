from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Schedule(Cli):
    """
    This module provides CLI interface to manage the mirror snapshot scheduling.
    """

    def __init__(self, nodes, base_cmd):
        super(Schedule, self).__init__(nodes)
        self.base_cmd = base_cmd + " schedule"

    def add_(self, **kw):
        """
        This method is used to create a mirror-snapshot schedule.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool(str): name of the pool.
              image(str): name of the image.
              start-time(str): can be specified using the ISO 8601 time format(Optional).
              interval(str): schedule interval
              See rbd help mirror snapshot schedule add for more supported keys
        """
        kw_copy = deepcopy(kw)
        interval = kw_copy.pop("interval", "")
        start_time = kw_copy.pop("start-time", "")
        cmd = f"{self.base_cmd} add {interval} {start_time} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def ls(self, **kw):
        """
        This method is used to list all snapshot schedules for a specific level (global, pool, or image).
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool(str): name of the pool(Optional).
              image(str): name of the image(Optional).
              recursive(bool): set this to true to list all schedules at the specified level and below.
              See rbd help mirror snapshot schedule ls for more supported keys
        """
        cmd = f"{self.base_cmd} ls {build_cmd_from_args(**kw)}"

        return self.execute(cmd=cmd)

    def status(self, **kw):
        """
        This method is used to view the status for when the next snapshots will be created for snapshot based mirroring.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool(str): name of the pool(Optional).
              image(str): name of the image(Optional).
              See rbd help mirror snapshot schedule status for more supported keys
        """
        cmd = f"{self.base_cmd} status {build_cmd_from_args(**kw)}"

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """
        This method is used to remove a mirror-snapshot schedule.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool(str): name of the pool.
              image(str): name of the image.
              start-time(str): can be specified using the ISO 8601 time format(Optional).
              interval(str): schedule interval
              See rbd help mirror snapshot schedule remove for more supported keys
        """
        kw_copy = deepcopy(kw)
        interval = kw_copy.pop("interval", "")
        start_time = kw_copy.pop("start-time", "")
        cmd = f"{self.base_cmd} remove {interval} {start_time} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
