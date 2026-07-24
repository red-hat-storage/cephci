from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """Verify prepare host command
    Args:
        kw: test data
            e.g:
            - test:
                name: test_prepare-host_command
                desc: Check-host/prepare-host for cephadm use/host configuration
                polarion-id: CEPH-83573789
                module: test_prepare_host.py
                config:
                    result: Host looks OK
    """
    # Get test configs
    config = kw.get("config")

    # Get mon node
    node = ceph_cluster.get_nodes(role="mon")[0]

    # Prepare host
    out, _ = CephAdm(node).prepare_host()
    out = out.split("\n")[-2]

    # Validate result
    if config.get("result") != out:
        raise OperationFailedError("CephAdm perpare-host command failed")

    return 0
