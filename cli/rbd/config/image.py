from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Image(Cli):
    """
    This module provides CLI interface to manage rbd config image commands for images in pool.
    """

    def __init__(self, nodes, base_cmd):
        super(Image, self).__init__(nodes)
        self.base_cmd = base_cmd + " image"

    def get(self, **kw):
        """
        Get image config value for given key.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    key(str)   : image meta key
                    See rbd help image-meta get for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        key = kw_copy.pop("key", "")
        cmd = f"{self.base_cmd} get {image_spec} {key} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def list(self, **kw):
        """
        List image config values.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    See rbd help image-meta get for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} list {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def ls(self, **kw):
        """
        List image config values.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    See rbd help image-meta get for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} ls {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def remove(self, **kw):
        """
        Remove image config value for given key.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    key(str)   : image meta key
                    See rbd help image-meta get for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        key = kw_copy.pop("key", "")
        cmd = f"{self.base_cmd} remove {image_spec} {key} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def rm(self, **kw):
        """
        Remove image config value for given key.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    key(str)   : image meta key
                    See rbd help image-meta get for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        key = kw_copy.pop("key", "")
        cmd = f"{self.base_cmd} rm {image_spec} {key} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def set(self, **kw):
        """
        Set image config value for given key.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    key(str)   : image meta key
                    value(str) : image meta value
                    See rbd help image-meta get for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        key = kw_copy.pop("key", "")
        value = kw_copy.pop("value", "")
        cmd = f"{self.base_cmd} set {image_spec} {key} {value} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
