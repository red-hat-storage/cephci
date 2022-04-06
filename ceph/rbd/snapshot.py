from ceph.rbd.rbd import Rbd
from tests.rbd.exceptions import ProtectSnapError, SnapCreateError
from utility.log import Log

log = Log(__name__)


class Snapshot(Rbd):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Snapshot, self).__init__(nodes=nodes)

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
        cmd = f"rbd snap create {pool_name}/{image_name}@{snap_name}"
        if not self.exec_cmd(cmd=cmd):
            raise SnapCreateError("Creating the snapshot failed")

    def protect(self, args):
        """
        Protects the provided snapshot
        Args:
            snap_name : snapshot name in pool/image@snap format
        """
        snap_spec = args["snapshot_spec"]
        cmd = f"rbd snap protect {snap_spec}"
        if self.exec_cmd(cmd=cmd):
            raise ProtectSnapError("Protecting the snapshot Failed")

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
