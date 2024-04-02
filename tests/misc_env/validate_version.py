import traceback
from distutils.version import LooseVersion

from utility.log import Log
from utility.utils import get_ceph_version_from_cluster, get_ceph_version_from_repo

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        log.info(kw.get("config"))
        ceph_version_installed = get_ceph_version_from_cluster(clients[0])
        ceph_version = get_ceph_version_from_repo(clients[0], config)
        if LooseVersion(ceph_version) <= LooseVersion(ceph_version_installed):
            log.info(
                f"Upgrade should not proceeded as installed versions is {ceph_version_installed} "
                f"greater than or equal to latest Version i.e., {ceph_version}"
            )
            return 1
        log.info(
            f"Upgrade should be proceeded as installed versions is {ceph_version_installed} "
            f"lesser than latest Version i.e., {ceph_version}"
        )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
