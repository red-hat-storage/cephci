from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """Verify Ceph Mgr plugins
    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="mgr")[0]
    plugins = [
        "influx",
        "k8sevents",
        "prometheus",
        "restful",
        "rook",
        "stats",
        "telegraf",
        "telemetry",
        "zabbix",
    ]

    for plugin in plugins:
        # Enable ceph-mgr plugin
        if CephAdm(node).ceph.mgr.module.enable(plugin, force=True):
            OperationFailedError(f"Failed to enable {plugin} plugin")

        # Verify ceph-mgr plugin being listed under enabled modules
        out = CephAdm(node).ceph.mgr.module.ls()
        if plugin not in out:
            OperationFailedError(f"{plugin} plug-in not listed under enabled_modules")

        # Disable ceph-mgr plugin
        if CephAdm(node).ceph.mgr.module.disable(plugin):
            OperationFailedError(f"Failed to disable {plugin} plugin")

    return 0
