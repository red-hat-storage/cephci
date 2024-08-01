from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    installer = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(installer)

    # List the blocklisted clients
    out = cephadm.ceph.osd.blocklist.ls()
    if "listed" in str(out):
        log.info("OSD blockist clients listed successfully")
    else:
        raise OperationFailedError("Failed to list OSD blocklist clients")
    return 0
