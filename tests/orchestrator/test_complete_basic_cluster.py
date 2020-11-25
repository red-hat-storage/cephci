import logging
log = logging.getLogger(__name__)

def run(ceph_cluster, **kw):
    """
    Add 2 mons, 3 OSDs, 2 RGWs, 1 MDS, 1 nfs daemons to cluster
    bootstrapped using cephadm
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
    """
    log.info("")

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    config = kw.get('config')

    if config.get('add'):

        for to_be_added in config.get('add'):
            ceph_installer.orch_add_node(to_be_added.get("node-name")
            ceph_installer.orch_add_daemon(to_be_added.get("node-name"), to_be_added.get("demon"))




