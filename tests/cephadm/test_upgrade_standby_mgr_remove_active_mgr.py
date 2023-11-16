import re

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError


def run(ceph_cluster, **kwargs):
    """Upgrade standby daemons and remove active daemons"""
    # Get config
    config = kwargs.get("config")

    # Get container image
    image = config.get("container_image")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get list of active and standby mgr
    out = CephAdm(installer).ceph.status()
    match = re.search(r"mgr: (.+?)\((.+?)\), standbys: (.+?)$", out, re.MULTILINE)
    if not match:
        raise CephadmOpsExecutionError("No mgr avaiable in cluster")
    mgr_daemons = {"active": match.group(1), "standby": match.group(3).split(", ")}

    # Upgrade standby mgr
    for daemon in mgr_daemons["standby"]:
        out = CephAdm(installer).ceph.orch.daemon.redeploy(f"mgr.{daemon}", image=image)
        if "Scheduled" not in out:
            raise CephadmOpsExecutionError("Fail to redeploy deamon")

    # Remove active mgr
    placement = ""
    for deamon in mgr_daemons["standby"]:
        node = deamon.split(".")[0]
        placement = f"{node} {placement}"
    nodes = placement.strip()
    conf = {"pos_args": [], "placement": f'"{nodes}"'}
    out = CephAdm(installer).ceph.orch.apply(service_name="mgr", **conf)
    if "Scheduled" not in out:
        raise CephadmOpsExecutionError("Fail to remove active mgr")

    return 0
