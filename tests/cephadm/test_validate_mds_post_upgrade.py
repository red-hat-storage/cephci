from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """
    Verify max mds value is not changed post upgrade

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.
    """

    config = kw.get("config")
    expected_max_mds = config.get("max_mds")
    node = ceph_cluster.get_nodes("installer")[0]

    # Get ceph fs dump
    fs_dump = loads(CephAdm(node).ceph.fs.get("cephfs", "json"))

    # Validate the max_mds value is same as the given
    max_mds = fs_dump["mdsmap"]["max_mds"]

    if max_mds != expected_max_mds:
        raise OperationFailedError(
            "Unexpected! The max mds value mismatch post upgrade. #2188270"
        )
    return 0
