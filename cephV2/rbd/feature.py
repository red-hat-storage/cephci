from utility.log import Log

log = Log(__name__)


class Feature:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " feature"
        self.node = node

    def enable(self, args):
        """CLI wrapper for rbd feature enable command

        Enables mentioned features on specified image.

        Args:
            pool_name: Poolname of the image
            image_name: Name of the image for which feature needs to be enabled.
            feature: Name of the feature that needs to be enabled on the image.
        """
        cmd = (
            self.base_cmd
            + f' enable {args["pool_name"]}/{args["image_name"]} {args["features"]}'
        )
        return self.node.exec_command(cmd=cmd)

    def disable(self):
        pass
