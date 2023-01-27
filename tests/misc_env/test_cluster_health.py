import json

from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster, **kwargs) -> int:
    """
    Validate the Ceph Helath Status
    Returns:
        0 -> on success
        1 -> on failure

    Raises:
        CommandError
    """
    LOG.info("Validating Ceph Health")
    clients = ceph_cluster.get_ceph_objects("client")
    out, rc = clients[0].exec_command(sudo=True, cmd="ceph -s -f json")
    cluster_info = json.loads(out)
    if cluster_info.get("health").get("status") != "HEALTH_OK":
        LOG.error(f"Cluster helath is in : {cluster_info.get('health').get('status')}")
        out, rc = clients[0].exec_command(sudo=True, cmd="ceph health detail -f json")
        error_summary = json.loads(out)
        LOG.error(f"{error_summary}")
        return 1
    return 0
