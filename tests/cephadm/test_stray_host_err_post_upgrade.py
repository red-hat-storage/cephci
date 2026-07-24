from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get cephadm health
    ceph_health = CephAdm(installer).ceph.health()
    if "CEPHADM_STRAY_HOST" in ceph_health:
        raise OperationFailedError(
            "Post upgrade CEPHADM_STRAY_HOST error is seen. #2115184"
        )

    if "HEALTH_WARN" in ceph_health:
        raise OperationFailedError("Health warning is present post upgrade.")

    log.info("No CEPHADM_STRAY_HOST / HEALTH_WARN is seen post upgrade.")
    return 0
