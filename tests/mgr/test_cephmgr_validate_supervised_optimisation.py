from cli.cephadm.cephadm import CephAdm


class CephMgrBalancerFailError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify Ceph Mgr Supervised Optimisation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="mgr")[0]
    mode = kw.get("config").get("mode")
    # Turn off automatic balancing
    if CephAdm(node).ceph.balancer.set_state(state="off"):
        raise CephMgrBalancerFailError("Failed to turn off automatic balancing")

    # Set the balancer mode to crush-compat
    if CephAdm(node).ceph.balancer.mode(mode=mode):
        raise CephMgrBalancerFailError(
            "Failed to change the balance mode to crush-compat"
        )

    # Evaluate the current distribution before creating Plan
    if not CephAdm(node).ceph.balancer.eval():
        raise CephMgrBalancerFailError(
            "Failed to evaluate the distribution before creating plan"
        )

    # Generate a Plan
    if CephAdm(node).ceph.balancer.optimize(plan="TestPlan"):
        raise CephMgrBalancerFailError("Failed to create plan")

    # Evaluate the newly created plan
    if not CephAdm(node).ceph.balancer.eval():
        raise CephMgrBalancerFailError(
            "Failed to evaluate the distribution after creating plan"
        )

    # Execute the plan
    if CephAdm(node).ceph.balancer.execute_plan(plan="TestPlan"):
        raise CephMgrBalancerFailError("Failed to execute newly created plan")

    return 0
