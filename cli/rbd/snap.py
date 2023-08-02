from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Snap(Cli):
    """
    This module provides CLI interface to manage snapshots from images in pool.
    """

    def __init__(self, nodes, base_cmd):
        super(Snap, self).__init__(nodes)
        self.base_cmd = base_cmd + " snap"

    def create(self, **kw):
        """
        Creates a snapshot.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    snap(str)  : name of the snapshot
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    snap-spec(str) : <pool>/<image>@<snap>
                    See rbd help snap create for more supported keys
        """
        kw_copy = deepcopy(kw)
        snap_spec = kw_copy.pop("snap-spec", "")
        cmd = f"{self.base_cmd} create {snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def ls(self, **kw):
        """
        Lists snapshots.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool
                    image(str): name of the image
                    image-spec(str) : <pool>/<image>
                    See rbd help snap ls for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} ls {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def rollback(self, **kw):
        """
        Rollback a snapshot.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    snap(str)  : name of the snapshot
                    image(str) : name of the image
                    snap-spec(str) : <pool>/<image>@<snap>
                    See rbd help snap rollback for more supported keys
        """
        kw_copy = deepcopy(kw)
        snap_spec = kw_copy.pop("snap-spec", "")
        cmd = f"{self.base_cmd} rollback {snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def rm(self, **kw):
        """
        Removes the snapshot.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    snap(str)  : name of the snapshot
                    image(str) : name of the image
                    snap-spec(str) : <pool>/<image>@<snap>
                    See rbd help snap rm for more supported keys
        """
        kw_copy = deepcopy(kw)
        snap_spec = kw_copy.pop("snap-spec", "")
        cmd = f"{self.base_cmd} rm {snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def purge(self, **kw):
        """
        Purges the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)  : name of the pool
                image(str) : name of the image
                image-spec(str) : <pool>/<image>
                See rbd help snap purge for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} purge {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def protect(self, **kw):
        """
        Protects the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)  : name of the pool
                snap(str)  : name of the snapshot
                image(str) : name of the image
                snap-spec(str) : <pool>/<image>@<snap>
                See rbd help snap protect for more supported keys
        """
        kw_copy = deepcopy(kw)
        snap_spec = kw_copy.pop("snap-spec", "")
        cmd = f"{self.base_cmd} protect {snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def unprotect(self, **kw):
        """
        Unprotects the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)  : name of the pool
                image(str) : name of the image
                snap(str)  : name of the snapshot
                snap-spec(str) : <pool>/<image>@<snap>
                See rbd help snap unprotect for more supported keys
        """
        kw_copy = deepcopy(kw)
        snap_spec = kw_copy.pop("snap-spec", "")
        cmd = f"{self.base_cmd} unprotect {snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
