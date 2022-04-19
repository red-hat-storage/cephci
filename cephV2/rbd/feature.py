from utility.log import Log

log = Log(__name__)


class Feature:
    def __init__(self, parent_base_cmd):
        self.base_cmd = parent_base_cmd + " feature"

    def enable(self, args):
        """CLI wrapper for rbd feature enable command

        Enables mentioned features on specified image.

        Args:
            pool_name: Poolname of the image
            image_name: Name of the image for which feature needs to be enabled.
            feature: Name of the feature that needs to be enabled on the image.
        """
        cmd = self.base_cmd + f' enable {args["pool_name"]}/{args["image_name"]} {args["feature"]}'
        return self.exec_cmd(cmd)

    def disable(self):
        pass
