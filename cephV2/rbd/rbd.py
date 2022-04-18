from cephV2.cli import CephCLI
from utility.log import Log

log = Log(__name__)


class Rbd(CephCLI):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Rbd, self).__init__(nodes=nodes)

    def create(self, args):
        """CLI wrapper for `rbd create`

        rbd create is used to create an rbd image on the specified pool
        provisioning specified size with mentioned features.

        Args:
            pool_name: Exiting pool_name in which image needs to be created
            image_name: Name of the image to be created
            size: size of the image
            features: Comma separated list of features to be enabled if default
                      values to be ovveriden
            data_pool: Name of the EC pool if data needs to be placed in a
                       separate EC pool.
                       Note: allow_ec_overwrites must be set on the EC pool.
        """

        cmd = (
            f'rbd create {args["pool_name"]}/{args["image_name"]} --size {args["size"]}'
        )
        if args.get("features", None):
            cmd = cmd + f' --image-features {args["features"]}'
        return self.exec_cmd(cmd)

    def clone(self, args):
        """CLI wrapper for 'rbd clone'

        Clones a RBD snapshot into a clone image in specified pool

        Args:
            source_spec: Source snapshot spec (pool/image@snap).
            destination_spec: destination_pool/clone image name.
        """
        cmd = f'rbd clone {args["source_spec"]} {args["destination_spec"]}'
        return self.exec_cmd(cmd)

    def bench():
        pass

    def rbd_import(self, args):
        """
        Imports a file as an image to specified pool name and image name
        Args:
            filename   : name of the file to be imported
                         Note: File must be present on instance.
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as
        """
        filename = args["file_name"]
        pool_name = args["pool_name"]
        image_name = args["image_name"]
        cmd = f"rbd import {filename} {pool_name}/{image_name}"

        return self.exec_cmd(cmd=cmd, long_running=True)
