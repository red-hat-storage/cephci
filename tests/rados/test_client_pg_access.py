"""
test Module to :
1. Create pool
2. write objects
3. get objects and verify checksum
4. Create snapshots
5. Delete snapshots
6. Delete objects
7. Delete pools
"""

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.test_9281 import do_rados_get, do_rados_put
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create pool, then add , get , delete objects & Snapshots.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    global num_objects
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_target_configs = config["verify_client_pg_access"]["configurations"]
    num_snaps = config["verify_client_pg_access"]["num_snapshots"]
    try:
        num_objects = config["verify_client_pg_access"]["num_objects"]
    except KeyError:
        print("Variable num_objects is not defined.Assigning to the default value")
        num_objects = 250
    # Check if num of objects variable is assigned
    if num_objects is None or not num_objects or num_objects < 0:
        log.info(
            "The number of objects are not defined properly and assigned to the default value 250 "
        )
        num_objects = 250
    log.debug(
        "Verifying the effects of rados put, get, snap & delete on pool with single PG"
    )

    # Creating pools and starting the test
    for entry in pool_target_configs.values():
        try:
            pool_name = entry["pool_name"]
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {pool_name}"
            )
            if entry.get("pool_type", "replicated") == "erasure":
                method_should_succeed(
                    rados_obj.create_erasure_pool, name=pool_name, **entry
                )
            else:
                method_should_succeed(
                    rados_obj.create_pool,
                    **entry,
                )

            # Creating and reading objects
            with parallel() as p:
                p.spawn(do_rados_put, client_node, pool_name, num_objects)
                p.spawn(do_rados_get, client_node, pool_name, 1)

            # Creating and deleting snapshots on the pool
            snapshots = []
            for _ in range(num_snaps):
                snap = pool_obj.create_pool_snap(pool_name=pool_name)
                if snap:
                    snapshots.append(snap)
                else:
                    log.error("Could not create snapshot on the pool")
                    return 1

            if not pool_obj.delete_pool_snap(pool_name=pool_name):
                log.error("Could not delete the snapshots created")
                return 1

            # Deleting the objects created on the pool
            if not pool_obj.do_rados_delete(pool_name=pool_name):
                log.error("Could not delete the objects present on pool")
                return 1
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # removal of rados pool
            rados_obj.delete_pool(pool=pool_name)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(f"Completed all operations on pool {pool_name}")

    log.info(
        "Completed testing effects of rados put, get, snap & delete on pool with single PG"
    )
    return 0
