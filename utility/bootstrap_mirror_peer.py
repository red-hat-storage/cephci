from cephV2.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


class BootstrapMirrorPeer:
    @staticmethod
    def run(kw, args):
        """Create bootstrap peer and import in other cluster.

        Peer bootstrap create on one cluster and import on another cluster.

        Usage
        - botstrap_mirror_peer:
          args:
            primary: ceph-rbd1
            primary_site_name: primary
            secondary: ceph-rbd2
            secondary_site_name: secondary
            direction: rx-tx
            pool_name: poolname
        """

        cluster = kw["ceph_cluster_dict"][args["primary"]]

        bootstrap_instance = Rbd(cluster)

        arg = {}
        arg["pool_name"] = args["pool_name"]
        arg["site_name"] = args["site_name"]

        out, err = bootstrap_instance.create(arg)

        cmd = f"echo {out} > token_file"

        cluster = kw["ceph_cluster_dict"][args["secondary"]]
        cluster.get_ceph_object("client").exec_command(cmd)

        bootstrap_instance = Rbd(cluster)

        arg["file_path"] = args["file_path"]

        bootstrap_instance.key_import(arg)
