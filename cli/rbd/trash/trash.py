from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args

from .trash_purge_schedule import TrashPurgeSchedule


class Trash(Cli):
    """
    This module provides CLI interface to manage snapshots from images in pool.
    """

    def __init__(self, nodes, base_cmd):
        super(Trash, self).__init__(nodes)
        self.base_cmd = base_cmd + " trash"
        self.trash_purge_schedule = TrashPurgeSchedule(nodes, self.base_cmd)

    def list(self, **kw):
        """
        Lists images in trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    See rbd help trash list for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} list {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def ls(self, **kw):
        """
        Lists images in trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    See rbd help trash ls for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} ls {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def move(self, **kw):
        """
        Moves image to trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    image(str) : name of the image
                    expires-at(str) : set the expiration time of an image so it can be
                          purged when it is stale
                    image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                    See rbd help trash move for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} move {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def mv(self, **kw):
        """
        Moves image to trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    image(str) : name of the image
                    expires-at(str) : set the expiration time of an image so it can be
                          purged when it is stale
                    image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                    See rbd help trash move for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} mv {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def purge(self, **kw):
        """
        Purges all images in a pool to trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    expired-before(str) : purges images that expired before the
                        given date
                    threshold(str) : purges images until the current pool data usage
                        is reduced to X%, value range: 0.0-1.0
                    See rbd help trash purge for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} purge {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def purge_schedule_add(self, **kw):
        """
        Add trash purge schedule to all images in a pool.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    interval(str) : schedule interval
                    start-time(str) : schedule start time
                    See rbd help trash purge schedule add for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} purge schedule add {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def purge_schedule_list(self, **kw):
        """
        List trash purge schedule to all images in a pool.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    recursive(str) : list all schedules
                    See rbd help trash purge schedule list for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} purge schedule list {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def purge_schedule_ls(self, **kw):
        """
        List trash purge schedule to all images in a pool.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    recursive(str) : list all schedules
                    See rbd help trash purge schedule ls for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} purge schedule ls {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def purge_schedule_remove(self, **kw):
        """
        Remove trash purge schedule to all images in a pool.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    interval(str) : schedule interval
                    start-time(str) : schedule start time
                    See rbd help trash purge schedule add for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} purge schedule remove {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def purge_schedule_rm(self, **kw):
        """
        Remove trash purge schedule to all images in a pool.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    interval(str) : schedule interval
                    start-time(str) : schedule start time
                    See rbd help trash purge schedule add for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} purge schedule rm {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def remove(self, **kw):
        """
        Remove an image from trash
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    image-id(str) : image id
                    image(str) : image name
                    See rbd help trash remove for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} remove {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def rm(self, **kw):
        """
        Remove an image from trash
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    image-id(str) : image id
                    image(str) : image name
                    See rbd help trash rm for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} rm {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def restore(self, **kw):
        """
        Restore an image from trash
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    image-id(str) : image id
                    image(str) : image name
                    See rbd help trash restore for more supported keys
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} restore {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)
