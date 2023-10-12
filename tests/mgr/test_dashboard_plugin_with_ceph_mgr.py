from cli.cephadm.cephadm import CephAdm


class DashboardPluginWithCephMgr(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify dashboard plugin works fine with ceph-mgr
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    node = ceph_cluster.get_nodes(role="mgr")[0]

    # Enable dashboard plug-in in non-active mgr
    out = CephAdm(node).ceph.mgr.module.enable("dashboard")
    exp_out = "enabled"
    if exp_out not in out:
        DashboardPluginWithCephMgr("Dashboard plug-in not enable")

    # Verification dashboard plug-in
    out = CephAdm(node).ceph.mgr.module.ls()
    exp_out = "dashboard"
    if exp_out not in out:
        DashboardPluginWithCephMgr("Dashboard plug-in not listed under enabled_modules")

    # Verification mgr service
    out = CephAdm(node).ceph.mgr.services()
    if exp_out not in out:
        DashboardPluginWithCephMgr("Dashboard plug-in not listed under enabled_modules")
    return 0
