from ceph.ceph_admin import CephAdmin
from cephV2.rbd.mirror.bootstrap import Bootstrap
from utility.log import Log
from utility.copy_file import CopyFile
from cephV2.rbd.mirror.bootstrap import Bootstrap
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

        bootstrap_instance = Bootstrap(cluster)
        
        arg = {}
        arg ["pool_name"] = args["pool_name"]
        arg ["site_name"] = args["site_name"]

        out, err = bootstrap_instance.create(arg)
        
        cmd = f"echo {out} > token_file"

        cluster = kw["ceph_cluster_dict"][args["secondary"]]
        bootstrap_instance = Bootstrap(cluster)
        
        arg ["file_path"] = args["file_path"]
        
        bootstrap_instance.key_import(arg)
