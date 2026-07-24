import json

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


class FailedToRemoveService(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    Verifies removal of a given service from existing cluster

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data
            e.g:
                - test:
                    abort-on-fail: true
                    config:
                        service_name: mon
                    name: Delete mon from an existing cluster
                    desc: Delete mon service from an existing cluster and verify status
                    polarion-id: CEPH-83573743
                    module: test_remove_service_from_existing_cluster.py
    """
    config = kw.get("config")

    node = ceph_cluster.get_nodes(role="mon")[0]

    # Get the service to be removed from cluster
    service = config.get("service_name")

    # Initial service details of the service in the cluster
    out = CephAdm(node).ceph.orch.ls(service_type=service, format="json-pretty")
    initial_service_details = json.loads(out)[0]

    # Make the service unmanaged
    c = {"pos_args": [], "unmanaged=": "True"}
    CephAdm(node).ceph.orch.apply(service_name=service, **c)

    # Remove the service form the cluster
    CephAdm(node).ceph.orch.rm(service_name=service, force=True)

    # Perform ceph orch apply after rm
    placement_nodes = []
    for n in ceph_cluster.get_nodes(role=service):
        if n != node and n.role != "client":
            placement_nodes.append(n.hostname)

    conf = {"pos_args": [], "placement=": f"\"{','.join(placement_nodes)}\""}
    CephAdm(node).ceph.orch.apply(service_name=service, **conf)

    # Get final service details
    timeout, interval = 20, 2
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(node).ceph.orch.ls(service_type=service, format="json-pretty")
        final_service_details = json.loads(out)[0]
        if final_service_details != initial_service_details:
            log.info("The rm operation succeeded. The service list has been updated")
            return 0
    if w.expired:
        raise FailedToRemoveService(
            "rm operation failed to remove service from existing cluster"
        )
