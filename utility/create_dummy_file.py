"""Creates a dummy file in provided path.

Some of the tests require a dummy file to be created on instances,
This module creates a dumy file at provided path.

Todo: Implement option to create on provided node number
"""
from utility.log import Log

log = Log(__name__)


class CreateDummyFile:
    @staticmethod
    def run(kw, args):
        """Creates dummy file on first client node using dd tool.

        args:
            kw: Cluster configs
            args: Configs required for this method

        args needs to have -
            cluster_name: Name of the cluster on which file needs to be created
            path: file name with abs path
            size: number of blocks that needs to be created by dd (K, M, G)
            long_running: True or False - Required for bug files
        """
        nodes = kw["ceph_cluster_dict"][args["cluster_name"]]

        target = nodes.get_ceph_object("client")

        log.info(f"Creating a dummy file at provided {args['path']}")
        cmd = f"dd if=/dev/urandom of={args['path']} bs=4 count="
        cmd = cmd + str(args["size"]) if args.get("size", None) else "5M"

        return target.exec_command(
            cmd=cmd, long_running=args.get("long_running", False)
        )
