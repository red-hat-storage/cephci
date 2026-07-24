from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


class AdminNodeRemovalError(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    Verify removal of _admin node acting as a bootstrap node.

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """

    config = kw.get("config")
    # Removing _admin node from bootstrp node
    instance = CephAdmin(cluster=ceph_cluster, **config)
    admin_node = instance.cluster.get_nodes()[0].hostname
    cmd = f"cephadm shell ceph orch host rm {admin_node}"
    try:
        instance.installer.exec_command(cmd, sudo=True)
        raise AdminNodeRemovalError("Test case failed, Admin node removed")
    except CommandFailed:
        log.info("As expected unable to remove _admin node from bootstrap node")
        return 0
