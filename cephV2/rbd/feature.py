from cephV2.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


class Feature(Rbd):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Feature, self).__init__(nodes=nodes)

    def enable(self, args):
        """CLI wrapper for rbd feature enable command

        Enables mentioned features on specified image.

        Args:
            pool_name: Poolname of the image
            image_name: Name of the image for which feature needs to be enabled.
            feature: Name of the feature that needs to be enabled on the image.
        """
        cmd = f'rbd feature enable {args["pool_name"]}/{args["image_name"]} {args["feature"]}'
        self.exec_cmd(cmd)

    def disable(self):
        pass
