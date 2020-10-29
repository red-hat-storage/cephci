import logging
log = logging.getLogger(__name__)

def run(ceph_cluster, **kw):
    """
    Add nodes to existing clusters using orchestrator
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
    """
    log.info("")