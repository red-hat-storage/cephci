from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Feature(Cli):
    """
    This module provides CLI interface to manage snapshots from images in pool.
    """

    def __init__(self, nodes, base_cmd):
        super(Feature, self).__init__(nodes)
        self.base_cmd = base_cmd + " feature"

    def enable(self, **kw):
        """
        Enable an RBD feature on an image.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    features(str): <space separated list of features to be enabled>
                    See rbd help feature enable for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        features = kw_copy.pop("features", "")
        cmd = f"{self.base_cmd} enable {image_spec} {features} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def disable(self, **kw):
        """
        Disable an RBD feature on an image.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str)  : name of the pool
                    image(str) : name of the image
                    namespace(str) : name of the namespace
                    image-spec(str) : <pool>/<image>
                    features(str): <space separated list of features to be disabled>
                    See rbd help feature enable for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        features = kw_copy.pop("features", "")
        cmd = f"{self.base_cmd} disable {image_spec} {features} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
