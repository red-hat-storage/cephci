from ceph.utils import get_node_by_id
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.ops.host import host_maintenance_enter, host_maintenance_exit


def run(ceph_cluster, **kw):
    """
    Create a pool for rbd application that has
    size and min_size 2 and pool_type replicated.
    Force a host with OSDs to enter maintenance mode
    using flags --force and --yes-i-really-mean-it
    Validate the logs for no errors
    Remove the host from maintenance
    """
    config = kw["config"]
    instance = config.get("node")
    pool_name = config.get("pool_name")
    min_size = config.get("min_size")
    application = config.get("app_name")
    pool_config = config.get("pool_config")
    instance_name = get_node_by_id(ceph_cluster, instance).hostname
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Create a pool
    out = CephAdm(installer).ceph.osd.pool.create(pool_name, **pool_config)
    print(out)
    if not out:
        raise OperationFailedError("Failed to create pool")

    # Set the pool min_size to 2
    CephAdm(installer).ceph.osd.pool.set(pool_name, "min_size", min_size)

    # Enable application on the pool
    CephAdm(installer).ceph.osd.pool.application(pool_name, application, "enable")

    # Put the host into maintainence
    out = host_maintenance_enter(
        installer, instance_name, force=True, yes_i_really_mean_it=True
    )
    if not out:
        raise OperationFailedError(
            f"Failed to add host {instance_name} into maintenance"
        )

    # Validate Cephadm logs for no OrchestratorError
    out = CephAdm(installer).ceph.logs(num="5", channel="cephadm")
    if (
        f"maintenance mode request for {instance_name} has SET the noout group"
        not in str(out)
    ):
        raise OperationFailedError("Cephadm log validation failed")

    # Remove host from maintenance
    out = host_maintenance_exit(installer, instance_name)
    if not out:
        raise OperationFailedError(
            f"Failed to remove host {instance_name} from maintenance"
        )
    return 0
