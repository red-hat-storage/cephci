from utility.log import Log

log = Log(__name__)


class Snap:
    def __init__(self, parent_base_cmd):
        self.base_cmd = parent_base_cmd + " snap"

    def create(self, args):
        """
        Creates a snap of an image in a specified pool name and image name
        Args:
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as
            snap_name  : name of the snapshot
        """
        snap_name = args["snap_name"]
        pool_name = args["pool_name"]
        image_name = args["image_name"]

        cmd = self.base_cmd + f" create {pool_name}/{image_name}@{snap_name}"
        return self.exec_cmd(cmd=cmd)

    def protect(self, args):
        """
        Protects the provided snapshot
        Args:
            snap_name : snapshot name in pool/image@snap format
        """
        snap_spec = args["snapshot_spec"]
        cmd = self.base_cmd + f" protect {snap_spec}"
        return self.exec_cmd(cmd=cmd)

    def unprotect():
        pass

    def flatten():
        pass

    def list_children():
        pass

    def purge():
        pass

    def list():
        pass

    def rollback():
        pass

    def remove():
        pass
