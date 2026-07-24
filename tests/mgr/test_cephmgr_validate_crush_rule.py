from cli.cephadm.cephadm import CephAdm


class CrushConfigFailureError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify Ceph Mgr Crush rule
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="mgr")[0]

    # create a rule
    args = ["create-replicated", "fast", "default", "host", "ssd"]
    CephAdm(node).ceph.osd.crush.rule(*args)

    # set set_chooseleaf_vary_r 1 as per the bz : 1874866
    CephAdm(node).ceph.osd.crush.set("set_chooseleaf_vary_r", "1")

    # Get all pools
    pools = CephAdm(node).ceph.osd.lspools()

    # Set pg_autoscale on for pool
    CephAdm(node).ceph.osd.pool.set(
        pools.split("\n")[1], key="pg_autoscale_mode", value="on"
    )

    # Verify ceph health doesn't show Module 'pg_autoscaler' has failed: 'op' error
    health_data = CephAdm(node).ceph.health()
    if "Module 'pg_autoscaler' has failed: 'op' error" in health_data:
        raise CrushConfigFailureError(
            "pg_autoscaler failed error is seen in ceph health"
        )
    return 0
