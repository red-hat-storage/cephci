from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OsdOperationError
from cli.utilities.utils import get_process_id
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify ceph orch ps output does show an incorrect amount of used memory for OSDs

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    node = ceph_cluster.get_nodes("osd")[0]

    osd_details = loads(CephAdm(node).ceph.orch.ps(daemon_type="osd", format="json"))
    mem_usage_from_ceph = []
    mem_usage_from_ps = []

    # Get Memory usage value from ceph orch ps command
    for item in osd_details:
        if item.get("hostname") == node.hostname:
            mem = int(item.get("memory_usage")) / (1024 * 1024)
            mem_usage_from_ceph.append(int(mem))
    mem_usage_from_ceph.sort()

    # Get Memory usage value from system ps command
    pids = get_process_id(node, "ceph-osd")
    for pid in pids.split(" "):
        cmd = f"ps -p {pid.strip()} -o rss="  # Get RSS value
        rss, _ = node.exec_command(cmd=cmd, sudo=True)
        rss = int(rss) / 1024
        mem_usage_from_ps.append(int(rss))

    # Compare the mem usage data
    if not (mem_usage_from_ceph == mem_usage_from_ps):
        log.error(
            f"Memory usages returned by ceph orch ps {mem_usage_from_ceph} and ps {mem_usage_from_ps}"
        )
        raise OsdOperationError(
            "Error!!! There is a mismatch in the Memory usage returned by ps and ceph orch ps. Refer Bz: #2259884"
        )

    log.info("There is no mismatch in the Memory usage returned by ps and ceph orch ps")
    return 0
