from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Configure RBD mirroring over pool.

    This module enable RBD mirroring over provided pool,
    which could be used for Regional DR scenario.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    try:
        config = kw.get("config")
        clusters = kw.get("ceph_cluster_dict")
        primary = rbdmirror.RbdMirror(clusters[config.get("primary")], config)
        secondary = rbdmirror.RbdMirror(clusters[config.get("secondary")], config)
        pool_name = config["pool-name"]
        log.info(f"Configure RBD mirroring over {pool_name} pool")
        primary.create_pool(poolname=pool_name)
        secondary.create_pool(poolname=pool_name)
        primary.config_mirror(secondary, poolname=pool_name, mode="image")
        return 0
    except Exception as e:  # noqa
        log.exception(e)
        return 1
