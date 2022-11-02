from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Image:
    """This module provides CLI interface to manage rbd mirror image commands."""

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " image"

    def demote(self, **kw):
        """Wrapper for rbd mirror image demote.

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            image_spec: poolname/[namespace]/imagename
            pool_name: Name of the pool.
            namespace: Name of the namespace.
            image: Name of the image.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} demote {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def disable(self, **kw):
        """Wrapper for rbd mirror image disable.

        Args
            kw: Key value pair of method arguments

        Example::
        Supported keys:
            image_spec: poolname/[namespace]/imagename
            pool_name: Name of the pool.
            namespace: Name of the namespace.
            image: Name of the image.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} disable {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def enable(self, **kw):
        """Wrapper for rbd mirror image enable.

        Args
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image_spec: poolname/[namespace]/imagename
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                mode: Mode of mirroring(journal/snapshot) [default: journal]
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        mode = kw_copy.pop("mode", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} enable {image_spec} {mode}{cmd_args}"

        return self.execute(cmd=cmd)

    def promote(self, **kw):
        """Wrapper for rbd mirror image promote.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image_spec: poolname/[namespace]/imagename
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                force(bool): True - To promote image forcefully
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} promote {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def resync(self, **kw):
        """Wrapper for rbd mirror image resync.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image_spec: poolname/[namespace]/imagename
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} resync {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def snapshot(self, **kw):
        """Wrapper for rbd mirror image snapshot.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image_spec: poolname/[namespace]/imagename
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} snapshot {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def status(self, **kw):
        """Wrapper for rbd mirror image status.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                format: output format (plain, json, or xml) [default: plain]
                pretty-format: true - pretty formatting (json and xml)
                image_spec: poolname/[namespace]/imagename
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} status {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)
