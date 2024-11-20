"""
Method to create object snaps using pool snapshot and ensure object snaps are deleted
when pool snapshot is removed.
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83602685
    Covers:
        - Pacific: BZ-2272361
        - Quincy: BZ-2272362
        - Reef: BZ-2263169
    Test to verify object snap deletion when parent pool snapshot is deleted.
    1. Create a replicated pool with default config
    2. Use rados put to write a single object to the pool
    3. Create a pool snapshot
    4. Write another object on the pool using rados put
    5. Create another pool snapshot
    6. Log rados df, rados lssnap, and rados listsnaps outputs
    7. Remove pool snapshot 1 using ceph osd rmsnap cmd
    8. Validate that object snap for obj1 could not be deleted
    9. Upgrade the cluster to fixed version.
    10. Run the force-snap-removal cmd and log rados df, rados lssnap, and rados listsnaps outputs
    11. Remove all RADOS pools from the cluster
    12. Re-run steps 1-7 on the fixed version
    13. Validate that object snap for obj1 was removed alongside pool snapshot
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]
    _pool_name = "test-obj-snap"
    snap_1 = ""
    log.info("Running test case to verify deletion of object snap with pool snap")

    def verify_radosdf_out(num_obj, num_obj_clone, num_obj_cop, timeout):
        """
        Module to implement smart wait and verify the o/p of rados df
        Args:
            num_obj: total number of objects in the pool
            num_obj_clone: number of object clones
            num_obj_cop: number of object copies
            timeout: timeout for smart wait
        Return:
            True -> all expected values match rados df o/p
            False -> all expected values do not match rados df o/p
        """
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while datetime.datetime.now() < timeout_time:
            try:
                df_out, _ = rados_obj.client.exec_command(cmd="rados df", sudo=True)
                log.info(f"rados df o/p after object clone creation: \n{df_out}")
                rados_df = rados_obj.get_rados_df(pool_name=_pool_name)
                assert (
                    int(rados_df["num_objects"]) == num_obj
                ), f"obj count for {_pool_name} is not {num_obj}"
                assert (
                    int(rados_df["num_object_clones"]) == num_obj_clone
                ), f"obj clones for {_pool_name} is not {num_obj_clone}"
                assert (
                    int(rados_df["num_object_copies"])
                ) == num_obj_cop, f"obj copies for {_pool_name} is not {num_obj_clone}"
                break
            except AssertionError as AE:
                log.exception(AE)
                log.error(
                    "rados df o/p is not as expected, sleeping for 30 secs and trying again"
                )
                time.sleep(30)
        else:
            log.error("rados df o/p is incorrect even after 100 secs")
            return False
        return True

    def common_workflow():
        """
        Module to execute common test workflow:
        1. Create a replicated pool
        2. Write single object to the pool
        3. Create a pool snapshot
        4. Write another object to the pool
        5. Log o/p of rados df
        6. Create another pool snapshot
        7. Create obj clones
        8. Check if rados df o/p reports the correct number of clones
        9. List snapshots at pool, and object level
        10 Delete first pool snapshot
        """

        log.info(common_workflow.__doc__)
        # create pool with given config
        log.info("Creating a replicated pool with default config")
        assert rados_obj.create_pool(pool_name=_pool_name), "pool creation failed"

        # write single object to the pool
        pool_obj.do_rados_put(client=client, pool=_pool_name, obj_name="obj1")

        # log rados df o/p
        time.sleep(10)
        df_out, _ = rados_obj.client.exec_command(cmd="rados df", sudo=True)
        log.info(f"rados df o/p after object addition to the pool: \n {df_out}")

        # create a pool snapshot
        log.info(f"Creating pool snapshot of pool {_pool_name}")
        snap_1 = pool_obj.create_pool_snap(pool_name=_pool_name)
        log.info(f"First Pool snapshot created successfully: {snap_1}")

        # write another single object to the pool
        pool_obj.do_rados_put(client=client, pool=_pool_name, obj_name="obj2")

        # log rados df o/p
        time.sleep(10)
        df_out, _ = rados_obj.client.exec_command(cmd="rados df", sudo=True)
        log.info(
            f"rados df o/p after another object was added to the pool: \n {df_out}"
        )

        # create another pool snapshot
        log.info(f"Creating pool snapshot of pool {_pool_name}")
        snap_2 = pool_obj.create_pool_snap(pool_name=_pool_name)
        log.info(f"Second Pool snapshot created successfully: {snap_2}")

        # create object clones for obj1 and obj2
        for obj in ["obj1", "obj2"]:
            pool_obj.do_rados_put(client=client, pool=_pool_name, obj_name=obj)

        # log rados df o/p and ensure number of reported clones is 2
        if not verify_radosdf_out(
            num_obj=4, num_obj_clone=2, num_obj_cop=12, timeout=100
        ):
            log.error("rados df o/p check failed")
            raise Exception("rados df o/p is incorrect even after 100 secs")

        # list snaps for each obj
        log.info("Listing snaps for objects obj1 and obj2")
        for obj in ["obj1", "obj2"]:
            out = rados_obj.list_obj_snaps(pool_name=_pool_name, obj_name=obj)
            log.info(f"Object snapshot for {obj}: {out}")

        # list pool level snapshots
        out = rados_obj.list_pool_snaps(pool_name=_pool_name)
        log.info(f"Listing pool level snapshots: \n{out}")

        # delete pool snapshots
        for snap in [snap_1, snap_2]:
            if not pool_obj.delete_pool_snap(pool_name=_pool_name, snap_name=snap):
                log.error(f"Pool snapshot deletion failed for snap {snap}")
                raise Exception(f"Pool snapshot deletion failed for {snap}")

    try:
        if config.get("issue_reproduction"):
            common_workflow()

            # object snap should linger even after pool snapshot deletion
            if not verify_radosdf_out(
                num_obj=4, num_obj_clone=2, num_obj_cop=12, timeout=100
            ):
                log.error("rados df o/p check failed")
                raise Exception("rados df o/p is incorrect even after 100 secs")

            # list snaps for obj1
            log.info(f"Listing snaps for objects obj1 after {snap_1} deletion")
            out = rados_obj.list_obj_snaps(pool_name=_pool_name, obj_name="obj1")
            log.info(f"Object snaps for obj1 after pool snap deletion: {out}")
            assert (
                len(out["clones"][0]["snapshots"]) == 2
            ), "obj1 clone snaps should have been 2"

            log.info(
                "Completed reproducing issue with unfixed build, test will resume after upgrade"
            )
        if config.get("verify_fix"):
            # check if test pool exists on the cluster
            if rados_obj.get_pool_details(pool=_pool_name):
                log.info(
                    f"Pool {_pool_name} exists, proceeding with force removal of snaps"
                )
                log.info("Force remove the undeleted lingering object snaps for obj1")
                _cmd = f"ceph osd pool force-remove-snap {_pool_name}"
                out, err = rados_obj.client.exec_command(cmd=_cmd, sudo=True)
                log.info(f"Force removal o/p: \n {out+err}")
                assert (
                    "removed snapids" in out + err
                ), "force-remove-snap o/p not as expected"

                time.sleep(10)
                # list snaps for obj1 after force removal
                log.info("Listing snaps for objects obj1 after deletion")
                out = rados_obj.list_obj_snaps(pool_name=_pool_name, obj_name="obj1")
                log.info(f"Object snaps for obj1 after pool snap deletion: {out}")
                assert (
                    len(out["clones"][0]["snapshots"]) == 0
                ), "obj1 clone snaps should have been 0"

                # list pool snapshot after force removal
                out = rados_obj.list_pool_snaps(pool_name=_pool_name)
                log.info(f"Pool snaphots for {_pool_name} after force removal:\n {out}")
                assert (
                    "0 snaps" in out
                ), f"pool snapshots for {_pool_name} should have been 0"

                # remove all rados pools and perform the test steps again
                rados_obj.rados_pool_cleanup()

            log.info(
                "\n\n -----------------------------------------"
                "Testing the same workflow again with fixed version"
                "---------------------------------------------\n\n"
            )
            common_workflow()

            # object snap should NOT linger after pool snapshot deletion
            if not verify_radosdf_out(
                num_obj=2, num_obj_clone=0, num_obj_cop=6, timeout=120
            ):
                log.error("rados df o/p check failed")
                raise Exception("rados df o/p is incorrect even after 120 secs")

            # list snaps for obj1
            log.info("Listing snaps for objects obj1 after pool snap deletion")
            out = rados_obj.list_obj_snaps(pool_name=_pool_name, obj_name="obj1")
            log.info(f"Object snaps for obj1 after pool snap deletion: {out}")
            assert (
                len(out["clones"][0]["snapshots"]) == 0
            ), "obj1 clone snaps should have been 0"

            log.info(
                "Test verification completed for object snapshot and pool snapshot removal"
            )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info("*********** Execution of finally block starts ***********")
        if not config.get("issue_reproduction"):
            # delete the created osd pool
            rados_obj.delete_pool(pool=_pool_name)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
