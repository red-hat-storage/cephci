from ceph.ceph import CommandFailed
from cli.cephadm.cephadm import CephAdm


class CollocateRGWNodeCountError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Deploy collocated RGW with just node count
    Args:
        kw: test data
            e.g:
            - test:
                name: test_deploy_collocated_RGW_with_node_count
                desc: Deploy collocated RGW with just node count
                config:
                    pos_args:
                    - foo
                    result: count-per-host must be combined with label or hosts or host_pattern
    """
    config = kw.get("config")
    node = ceph_cluster.get_nodes(role="mon")[0]
    conf = {"pos_args": config.get("pos_args"), "placement": "2,count-per-host:2"}
    try:
        result = CephAdm(node).ceph.orch.apply(
            service_name="rgw", check_ec=True, **conf
        )
        if config.get("result") not in result:
            raise CollocateRGWNodeCountError(
                "apply command did not fail with the expected error"
            )
    except CommandFailed as err:
        if config.get("result") not in str(err):
            raise CollocateRGWNodeCountError(
                "apply command did not fail with the expected error"
            )
    return 0
