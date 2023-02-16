from cli.cephadm.cephadm import CephAdm


class PrepareHostError(Exception):
    pass


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
    config = kw.get("config")
    node = ceph_cluster.get_nodes(role="mon")[0]
    result = CephAdm(node).prepare_host()
    out = result[1].split("\n")[-2]
    if config.get("result") != out:
        raise PrepareHostError("cephadm perpare-host command fail")
    return 0
