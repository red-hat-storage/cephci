import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83571710
    Test to verify concurrent IOPs on an object in a pool
    1. Create a replicated pool
    2. Perform the below two tasks concurrently:
        - Use rados put to write 1MB data to an object (object size/2) times
          with initial offset 0 and incremental offset of 2MB from client node
        - Use rados put to write 1MB data to an object (object size/2) times
          with initial offset 1MB and incremental offset of 2MB from installer node
    3. Perform the below two tasks parallely:
        - Use rados put to write 1MB data to an object (object size/2) times
          with initial offset 0 and incremental offset of 2MB from client node
        - Use rados put to write 4MB data to an object 15 (object size/2) times
          with initial offset 4MB and incremental offset of 8MB from installer node
    4. Verify the ceph df stats and ensure the pool has only 1 object
        and stored data is equal to input Object size
    5. Delete the created pools
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    object_name = "obj_parallel_io"
    log.info("Running test case to verify parallel I/O operations on an Object")
    o_sizes = config["obj_sizes"]

    try:
        for pool_name in ["re_pool_concurrent_io", "re_pool_parallel_io"]:
            o_stored = 1
            d_stored = 0
            if "parallel" in pool_name:
                log.info("----- Starting workflow for Parallel IOPS -----\n")
            else:
                log.info("----- Starting workflow for Concurrent IOPS -----\n")
            # create pool with given config
            rados_obj.create_pool(pool_name=pool_name)
            for o_size in o_sizes:
                # reduce object size to nearest multiple of 2
                if o_size % 2 != 0:
                    o_size = o_size - 1
                    log.info(
                        "Object size needs to be multiple of 2 for parallel writes, "
                        f"Input object size has been changed to {o_size}"
                    )
                (
                    rados_obj.run_parallel_io(
                        pool_name=pool_name, obj_name=object_name, obj_size=o_size
                    )
                    if "parallel" in pool_name
                    else rados_obj.run_concurrent_io(
                        pool_name=pool_name, obj_name=object_name, obj_size=o_size
                    )
                )
                d_stored += o_size

                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=35)
                while datetime.datetime.now() <= timeout_time:
                    try:
                        stats_post_iops = rados_obj.get_cephdf_stats(
                            pool_name=pool_name
                        )
                        log.info(
                            f"ceph df stats post iops on object {object_name} in pool {pool_name} - "
                            f"{stats_post_iops}"
                        )

                        # validation of object and data stored in the pool
                        objs_stored = int(stats_post_iops["stats"]["objects"])
                        data_stored = int(stats_post_iops["stats"]["stored"] / 1048576)
                        log.info(
                            f"Objects in the pool: {objs_stored} | Data stored in the pool: {data_stored} MB"
                        )

                        assert objs_stored == o_stored and (
                            d_stored - 1
                        ) <= data_stored <= (d_stored + 1)
                        o_stored = objs_stored + 1
                        log.info(
                            f"----- Verification completed for object size {o_size}MB ------"
                        )
                        break
                    except Exception:
                        time.sleep(8)
                        log.info("Sleeping for 8 seconds and checking pool stats again")
                        if datetime.datetime.now() >= timeout_time:
                            log.error("Pool stats are incorrect even after 35 seconds")
                            raise
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.delete_pool(pool="re_pool_concurrent_io")
        rados_obj.delete_pool(pool="re_pool_parallel_io")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        "Verification of concurrent and parallel IOPS on an object completed successfully"
    )
    return 0
