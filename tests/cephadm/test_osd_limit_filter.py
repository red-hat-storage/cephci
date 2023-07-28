from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.cephadm.exceptions import CephadmOpsExecutionError
from cli.exceptions import ResourceNotFoundError, UnexpectedStateError
from cli.utilities.operations import wait_for_cluster_health
from cli.utilities.utils import get_disk_list, get_service_id, get_service_state


def run(ceph_cluster, **kw):
    """Verify OSDs deployed using limit filter in the spec file
    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    admin = ceph_cluster.get_nodes(role="_admin")[0]
    client = ceph_cluster.get_nodes(role="client")[0]
    nodes = ceph_cluster.get_nodes(role="osd")

    # Verify healthy cluster
    if not client:
        raise ResourceNotFoundError("Client node is missing, add a client node")
    health = wait_for_cluster_health(client, "HEALTH_OK", 120, 10)
    if not health:
        raise UnexpectedStateError("Cluster is not in HEALTH_OK state")

    # Verify if there are 12 OSDs listed as per the limit filter spec deployment
    expected_ids = list(range(0, 12))
    actual_ids = []
    timeout, interval = 120, 2
    for w in WaitUntil(timeout=timeout, interval=interval):
        osd_ps = loads(CephAdm(admin).ceph.orch.ps(daemon_type="osd", format="json"))
        for key in osd_ps:
            actual_ids.append(int(key["daemon_id"]))
            actual_ids.sort()
        if len(osd_ps) == 12 and expected_ids == actual_ids:
            break
    if w.expired:
        raise ResourceNotFoundError(
            "The count of OSDs does not match the expected count"
        )

    for node in nodes:
        # Verify if OSD disks are being listed
        osd_disks, _ = get_disk_list(sudo=True, node=node, expr="osd")
        if not osd_disks:
            raise CephadmOpsExecutionError("OSD disks not listed")

        # Verify that the OSD services are not inactive
        osd_id = get_service_id(node, "osd")
        for id in osd_id:
            status = get_service_state(node, id)
            if "inactive" in status:
                raise UnexpectedStateError(f"OSD {id} not in active state")

    return 0
