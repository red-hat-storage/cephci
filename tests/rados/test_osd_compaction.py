"""
Tier-3 test module to perform various operations on large OMAPS
and execute OSD compaction during IOPS with and without OSD failure
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-11681
    This test is to perform various operations on OMAPS and execute
    OSD compaction during IOPS with and without OSD failure
    1. Create cluster with default configuration
    2. Create a pool and write large number of OMAPS to few objects
    3. Perform various operations on the OMAP entries, list, get,
     add new, delete, clear, etc
    4. Trigger background IOPS using rados bench
    5. Execute OSD compaction for a healthy OSD
    6. Stop the OSD using systemctl
    7. Execute OSD compaction on the down OSD, expected to fail
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    omap_config = config["omap_config"]
    bench_config = config["bench_config"]

    # Creating pools and starting the test
    try:
        log.debug(
            f"Creating replicated pool on the cluster with name {omap_config['pool_name']}"
        )
        method_should_succeed(rados_obj.create_pool, **omap_config)
        pool_name = omap_config.pop("pool_name")
        normal_objs = omap_config["normal_objs"]
        bench_pool_name = bench_config["pool_name"]
        if normal_objs > 0:
            # create n number of objects without any omap entry
            rados_obj.bench_write(
                pool_name=pool_name,
                **{
                    "rados_write_duration": 600,
                    "byte_size": "4096KB",
                    "max_objs": normal_objs,
                    "verify_stats": False,
                },
            )

        # calculate objects to be written with omaps and begin omap creation process
        omap_obj_num = omap_config["obj_end"] - omap_config["obj_start"]
        log.debug(
            f"Created the pool. beginning to create omap entries on the pool. Count : {omap_obj_num}"
        )
        if not pool_obj.fill_omap_entries(pool_name=pool_name, **omap_config):
            log.error(f"Omap entries not generated on pool {pool_name}")
            raise Exception(f"Omap entries not generated on pool {pool_name}")

        assert pool_obj.check_large_omap_warning(
            pool=pool_name,
            obj_num=omap_obj_num,
            check=omap_config["large_warn"],
            obj_check=False,
        )

        # getting list of omap objects present in the pool
        out, _ = cephadm.shell([f"rados ls -p {pool_name} | grep 'omap_obj'"])
        omap_obj_list = out.split()
        log.info(f"List of Objects having large omaps in {pool_name}: {omap_obj_list}")

        # performing various operations on large omap objects
        # 1. List omap key entries for an object
        obj_rand = random.choice(omap_obj_list)
        log.info(f"#1 Listing OMAP key entries for obj: {obj_rand}")
        omap_keys, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapkeys"
        )
        log.info(
            f"Printing first 20 OMAP keys present for Object {obj_rand}: \n"
            f"{omap_keys.split()[:20]}"
        )

        # 2. List omap keys & values entries for an object
        obj_rand = random.choice(omap_obj_list)
        log.info(f"#2 Listing OMAP key & value entries for obj: {obj_rand}")
        omap_key_val, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapvals"
        )
        omap_kv_trim = "\n".join(omap_key_val.split("\n")[-21:-1])
        log.info(
            f"Printing last few OMAP keys and values present for Object {obj_rand}: "
            f"{omap_kv_trim}"
        )

        # 3. create a custom omap key & value entry for an object
        obj_rand = random.choice(omap_obj_list)
        log.info(f"#3 Creating a custom OMAP key & value entry for obj: {obj_rand}")
        custom_val = random.randint(pow(10, 5), pow(10, 6) - 1)
        custom_key = f"key_{custom_val}"
        pool_obj.perform_omap_operation(
            pool=pool_name,
            obj=obj_rand,
            ops="setomapval",
            key=custom_key,
            val=custom_val,
        )
        time.sleep(5)

        # verifying addition of custom OMAP key
        # 4. Fetch value of a particular omap key
        log.info(f"#4 Fetching value of a particular OMAP key obj: {obj_rand}")
        omap_val, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="getomapval", key=custom_key
        )
        assert str(custom_val) in omap_val
        log.info(f"Value of OMAP key {custom_key} is: {omap_val}")

        # 5. Remove the newly added OMAP entry
        log.info(f"#5 Removing a custom OMAP entry for obj: {obj_rand}")
        pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="rmomapkey", key=custom_key
        )
        time.sleep(5)

        # verifying successful removal of custom OMAP key
        # below validation does not help as failure is not getting
        # caught correctly
        # try:
        #     _, err = pool_obj.perform_omap_operation(
        #         pool=pool_name, obj=obj_rand, ops="listomapkeys", key=custom_key
        #     )
        # except Exception as ex:
        #     log.info(f"Expected to fail | STDERR:\n {ex}")
        # if "No such key" not in ex:
        #     raise AssertionError(f"{custom_key} should have been deleted, expected error msg not found")
        omap_keys, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapkeys"
        )
        assert custom_key not in omap_keys
        log.info(f"{custom_key} key successfully removed from OMAP entries")

        # 6. Clear all OMAP entries from an object
        obj_rand = random.choice(omap_obj_list)
        omap_obj_list.remove(obj_rand)
        log.info(f"#6 Clearing all OMAP entries for obj: {obj_rand}")
        pool_obj.perform_omap_operation(pool=pool_name, obj=obj_rand, ops="clearomap")
        time.sleep(5)

        # verifying removal of all omap entries for the obj
        omap_keys, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapkeys"
        )
        assert len(omap_keys) == 0
        log.info(f"All OMAP entries succesfully removed for obj: {obj_rand}")

        log.info("OMAP operations have been successfully executed")

        # Fetch an Object with OMAPs and its primary OSD to perform OSD compaction
        obj_compact = random.choice(omap_obj_list)
        osd_id = rados_obj.get_osd_map(pool=pool_name, obj=obj_compact)[
            "acting_primary"
        ]
        log.info(
            f"Primary OSD of the object {obj_compact} to be used for compaction: {osd_id}"
        )

        # fetch the initial use % for the chosen OSD
        log.debug("Fetching the initial use % for the chosen OSD before compaction")
        osd_df_init = rados_obj.get_osd_df_stats(filter=f"osd.{osd_id}")["nodes"][0]
        osd_util_init = osd_df_init["utilization"]

        # without IOPS in-progress perform OSD compaction of chosen OSD
        log.info(f"Starting OSD compaction for OSD {osd_id}")
        out, _ = cephadm.shell([f"ceph tell osd.{osd_id} compact"])
        log.info(out)
        assert "elapsed_time" in out
        time.sleep(10)

        # fetch the final use % for the chosen OSD after compaction
        log.debug("Fetching the final use % for the chosen OSD after compaction")
        osd_df_final = rados_obj.get_osd_df_stats(filter=f"osd.{osd_id}")["nodes"][0]
        osd_util_final = osd_df_final["utilization"]

        log.info(f"Initial OSD utilization before compaction: {osd_util_init}")
        log.info(f"Final OSD utilization after compaction: {osd_util_final}")
        assert osd_util_final <= osd_util_init

        # Fetch an Object with OMAPs and its primary OSD to perform OSD compaction
        obj_compact = random.choice(omap_obj_list)
        osd_id = rados_obj.get_osd_map(pool=pool_name, obj=obj_compact)[
            "acting_primary"
        ]
        log.info(
            f"Primary OSD of the object {obj_compact} to be used for compaction: {osd_id}"
        )

        # create another pool for background IOPS
        assert rados_obj.create_pool(**bench_config)

        # Initiating background IOPS using rados bench for 5 mins
        log.info("Initiating background IOPS using rados bench for 5 mins")
        rados_obj.bench_write(
            pool_name=bench_pool_name, rados_write_duration=300, background=True
        )

        # with IOPS in-progress perform OSD compaction of chosen OSD
        log.info(f"Starting OSD compaction for OSD {osd_id}")
        out, _ = cephadm.shell([f"ceph tell osd.{osd_id} compact"])
        log.info(out)
        assert "elapsed_time" in out
        time.sleep(10)

        # stop the OSD to check compaction failure
        log.info(f"Stopping OSD {osd_id} to perform compaction again")
        rados_obj.change_osd_state(action="stop", target=osd_id)
        try:
            _, err = cephadm.shell([f"ceph tell osd.{osd_id} compact"])
        except Exception as e:
            log.info("expected to fail with below error:")
            log.info(e)
            assert "problem getting command descriptions" in str(e)

        log.info(
            "OSD compaction with and without failure has been successfully verified."
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.delete_pool(pool=pool_name)
        rados_obj.delete_pool(pool=bench_pool_name)
        rados_obj.change_osd_state(action="start", target=osd_id)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed testing effects of large number of omap entries on pools ")
    return 0
