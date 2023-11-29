from cli.exceptions import OperationFailedError
from cli.utilities.utils import get_pid_limit


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    # Check for mgr nodes
    for service in ["rgw", "mon", "mds"]:
        node = ceph_cluster.get_nodes(role=service)[0]
        pid_limit = get_pid_limit(node, service)
        if pid_limit != "-1":
            raise OperationFailedError(
                f"The pid limit is not set to -1 for {service} but instead to {pid_limit}"
            )
    return 0
