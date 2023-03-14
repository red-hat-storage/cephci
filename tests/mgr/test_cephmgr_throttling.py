from cli.cephadm.cephadm import CephAdm


class CephmgrThrottlingError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify cephmgr throttling
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
        e.g
        test:
            name: Verify cephmgr throttling
            desc: Verify cephmgr throttling
            polarion-id: CEPH-83573248
            module: test_cephmgr_throttling
            config:
                operation: enable
                module: balancer
                key: target_max_misplaced_ratio
                value: .07
    """
    node = ceph_cluster.get_nodes(role="mon")[0]
    action = kw.get("config").get("operation")
    if not action:
        CephmgrThrottlingError("operation value not present in config")
    module = kw.get("config").get("module")
    if not module:
        CephmgrThrottlingError("module value not present in config")
    key = kw.get("config").get("key")
    if not key:
        CephmgrThrottlingError("key value not present in config")
    value = kw.get("config").get("value")
    if not value:
        CephmgrThrottlingError("value not present in config")
    # Enable balancer module
    if CephAdm(node).ceph.mgr.module(action, module):
        CephmgrThrottlingError(f"Failed to {action} module {module}")
    exp_out = '"active": true'
    # Balancer module status
    result = CephAdm(node).ceph.balancer.status()
    if exp_out not in result:
        CephmgrThrottlingError(f"Module {module} not active")
    # Throttling
    if CephAdm(node).ceph.config_key.set(key, value):
        CephmgrThrottlingError(f"Failed to set {key} with value {value}")
    return 0
