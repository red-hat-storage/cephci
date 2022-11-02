from copy import deepcopy

from cli.utilities.utils import build_cmd_args

from .purge import Purge


class Trash:
    """This module provides CLI interface to manage block device images from/in trash."""

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " trash"
        self.purge = Purge(nodes, self.base_cmd)

    def mv(self, **kw):
        """
        Moves an image to the trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image_name(str) :  name of image to be moved to trash
                pool_name(str)  : name of the pool
                expires_at(str) : Expiration time to defer the image (optional)
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        image_name = kw_copy.pop("image_name", "")
        pool_name = kw_copy.pop("pool_name", "")
        cmd = (
            self.base_cmd + f" mv {pool_name}/{image_name}" + build_cmd_args(kw=kw_copy)
        )

        return self.execute(cmd=cmd)

    def rm(self, **kw):
        """
        Removes an image from the trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image_id(int)  :  id of image to be removed from trash
                pool_name(str) : name of the pool
                force(bool)    :  To determine whether its a forced operation or not(optional)
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        image_id = kw_copy.pop("image_id", "")
        pool_name = kw_copy.pop("pool_name", "")
        cmd = self.base_cmd + f" rm {pool_name}/{image_id}" + build_cmd_args(kw=kw_copy)

        return self.execute(cmd=cmd)

    def restore(self, **kw):
        """
        Restores an image from the trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image_id(int) :  id of image to be restored from trash
                pool_name(str) : name of the pool
                image(int) :  name of image to be renamed(Optional)
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        image_id = kw_copy.pop("image_id", "")
        pool_name = kw_copy.pop("pool_name", "")
        cmd = (
            self.base_cmd
            + f" restore {pool_name}/{image_id}"
            + build_cmd_args(kw=kw_copy)
        )

        return self.execute(cmd=cmd)

    def ls(self, **kw):
        """
        Lists imageids from the trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str) : name of the pool
                all(bool) : whether to list all trashed images(Optional)
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        cmd = self.base_cmd + f" ls {pool_name}" + build_cmd_args(kw=kw_copy)

        return self.execute(cmd=cmd)
