from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Image(Cli):
    """This module provides CLI interface to manage rbd mirror image commands."""

    def __init__(self, nodes, base_cmd):
        super(Image, self).__init__(nodes)
        self.base_cmd = base_cmd + " image"

    def demote(self, **kw):
        """Wrapper for rbd mirror image demote.
        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            image-spec: poolname/[namespace]/imagename
            pool: Name of the pool.
            namespace: Name of the namespace.
            image: Name of the image.
            See rbd help mirror image demote for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} demote {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def disable(self, **kw):
        """Wrapper for rbd mirror image disable.
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            image-spec: poolname/[namespace]/imagename
            pool: Name of the pool.
            namespace: Name of the namespace.
            image: Name of the image.
            See rbd help mirror image disable for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} disable {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def enable(self, **kw):
        """Wrapper for rbd mirror image enable.
        Args
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image-spec: poolname/[namespace]/imagename
                pool: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                mode: Mode of mirroring(journal/snapshot) [default: journal]
                See rbd help mirror image enable for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        mode = kw_copy.pop("mode", "")
        cmd = f"{self.base_cmd} enable {image_spec} {mode} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def promote(self, **kw):
        """Wrapper for rbd mirror image promote.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image-spec: poolname/[namespace]/imagename
                pool: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                force(bool): True - To promote image forcefully
                See rbd help mirror image promote for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} promote {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def resync(self, **kw):
        """Wrapper for rbd mirror image resync.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image-spec: poolname/[namespace]/imagename
                pool: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                See rbd help mirror image resync for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} resync {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def snapshot(self, **kw):
        """Wrapper for rbd mirror image snapshot.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                image-spec: poolname/[namespace]/imagename
                pool: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                See rbd help mirror image snapshot for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} snapshot {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def status(self, **kw):
        """Wrapper for rbd mirror image status.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                format: output format (plain, json, or xml) [default: plain]
                pretty-format: true - pretty formatting (json and xml)
                image-spec: poolname/[namespace]/imagename
                pool: Name of the pool.
                namespace: Name of the namespace.
                image: Name of the image.
                See rbd help mirror image status for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} status {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
