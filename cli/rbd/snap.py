class Snap:
    """
    This module provides CLI interface to manage snapshots from images in pool.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " snap"

    def create(self, **kw):
        """
        Creates a snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                snap_name(str)  : name of the snapshot
                image_name(str) : name of the image
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        snap_name = kw.get("snap_name")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + f" create {pool_name}/{image_name}@{snap_name}"

        return self.execute(cmd=cmd)

    def ls(self, **kw):
        """
        Lists snapshots.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str) : name of the pool
                image_name(str): name of the image
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + f" ls {pool_name}/{image_name}"

        return self.execute(cmd=cmd)

    def rollback(self, **kw):
        """
        Rollback a snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                snap_name(str)  : name of the snapshot
                image_name(str) : name of the image
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        snap_name = kw.get("snap_name")
        cmd = self.base_cmd + f" rollback {pool_name}/{image_name}@{snap_name}"

        return self.execute(cmd=cmd)

    def rm(self, **kw):
        """
        Removes the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                snap_name(str)  : name of the snapshot
                image_name(str) : name of the image
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        snap_name = kw.get("snap_name")
        cmd = self.base_cmd + f" rm {pool_name}/{image_name}@{snap_name}"

        return self.execute(cmd=cmd)

    def purge(self, **kw):
        """
        Purges the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                image_name(str) : name of the image
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + f" purge {pool_name}/{image_name}"

        return self.execute(cmd=cmd)

    def protect(self, **kw):
        """
        Protects the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                snap_name(str)  : name of the snapshot
                image_name(str) : name of the image
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        snap_name = kw.get("snap_name")
        cmd = self.base_cmd + f" protect {pool_name}/{image_name}@{snap_name}"

        return self.execute(cmd=cmd)

    def unprotect(self, **kw):
        """
        Unprotects the snapshot.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                image_name(str) : name of the image
                snap_name(str)  : name of the snapshot
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        snap_name = kw.get("snap_name")
        cmd = self.base_cmd + f" unprotect {pool_name}/{image_name}@{snap_name}"

        return self.execute(cmd=cmd)
