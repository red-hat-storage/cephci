from cli.cephadm.cephadm import CephAdm


class CephmgrDashboardPluginError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify dashboard cannot start on non-active ceph-mgrs
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    nodes = ceph_cluster.get_nodes()
    # non-active mgr node
    node = [node for node in nodes if not node.role == "mgr"][0]

    # Enable dashboard plug-in in non-active mgr
    out = CephAdm(node).ceph.mgr.module(action="enable", module="dashboard")
    exp_error = kw.get("config").get("error")
    if exp_error not in out:
        CephmgrDashboardPluginError("Dashboard plug-in enable in non-active mgr")
    return 0
