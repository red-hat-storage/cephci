"""Test case covered -
CEPH-9511 - Attempt creating mirror in same cluster. This should be prevented

Test Case Flow:
1. Follow the latest official Block device Doc to configure RBD Mirroring
2. Attempt creating mirror in same cluster as primary and secondary this should be prevented
3. CLI should not provide any option to specify same cluster source and target
or it detects the condition where source and target cluster for the mirror same and prints an error message.
"""

from ceph.parallel import parallel
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    Method to validate configuring mirror on same cluster source and target
    got prevented or not.

    Args:
        **kw: test data
        Example::
            config:
                imagesize: 2G

    Returns:
        int: The return value - 0 for success, 1 for failure
    """
    try:
        log.info(
            "Starting CEPH-9511 - Attempt creating mirror in same cluster. This should be prevented"
        )
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw["ceph_cluster_dict"].values()
        ]
        poolname = mirror1.random_string() + "_tier_2_rbd_mirror_pool"
        primary_cluster = kw.get("ceph_cluster")
        primary_cluster_name = primary_cluster.name
        with parallel() as p:
            p.spawn(mirror1.create_pool, poolname=poolname)
            p.spawn(mirror2.create_pool, poolname=poolname)
        mirror1.enable_mirroring(mirror_level="pool", specs=poolname, mode="pool")
        mirror2.enable_mirroring(mirror_level="pool", specs=poolname, mode="pool")
        mirror1.bootstrap_peers(poolname=poolname, cluster_name=primary_cluster_name)
        mirror1.copy_file(
            file_name="/root/bootstrap_token_primary",
            src=mirror1.ceph_client,
            dest=mirror2.ceph_client,
        )
        out, err = mirror1.import_bootstrap(
            poolname=poolname,
            cluster_name=primary_cluster_name,
            all=True,
            check_ec=False,
        )
        log.debug(err)
        if "cannot import token for local cluster" in err:
            log.info("importing bootstrap token with same cluster got prevented")
        else:
            log.error("importing bootstrap token with same cluster not prevented")
            return 1
        return 0
    except Exception as e:
        log.exception(e)
        return 1
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
