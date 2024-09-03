import json

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    #CEPH-83571714
    Capture the block stats of an object using ceph-objectstore-tool utility
    Verify the decrease in number of blocks when the amount of data in an object
    is reduced by overwrite
    1. Create pool and write data to an object
    2. Fetch the acting pg set for the object and corresponding osd nodes
    3. Stop any one of the OSDs from the acting pg set using systemctl
    4. Capture the object dump using ceph-objectstore-tool
    5. Start the OSD which was previously stopped
    6. Overwrite the data in the object with a smaller chunk
    7. Stop the same OSD and fetch object dump using ceph-objectstore-tool
    8. Verify the difference in block stats and decrease in the number of blocks
    should be proportional to the decrease in data in the object

    # Test currently does not pass on RHCS 6.0 because the acting PG set
     changes for the object as soon as one of the OSD is brought down even if
     no-recovery flags are set. Needs further investigation
     Execution with RHCS 5.3 is fine, passes 10/10 times
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    iterations = config.get("write_iteration")
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)

    pool_name = config["pool_name"]
    object_name = config.get("object_name", "obj-objectstore")

    log.info(
        "Running test case to verify changes in number "
        "of blocks when data is written to an object and removed from an object"
    )

    def fetch_primary_osd():
        osd_map = rados_obj.get_osd_map(pool=pool_name, obj=object_name)
        acting_pg_set = osd_map["acting"]
        log.info(f"Acting pg set for {object_name} in {pool_name}: {acting_pg_set}")
        if not acting_pg_set:
            log.error("Failed to retrieve acting pg set")
            raise Exception("Failed to retrieve acting pg set")
        return acting_pg_set[0]

    try:
        # create pool with given config
        if config["create_pool"]:
            rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)

        bluestore_min_alloc_size = int(
            mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_hdd")
        )
        log.info(f"bluestore_min_alloc_size: {bluestore_min_alloc_size}")

        # create single object and perform IOPs
        pool_obj.do_rados_put(client=client_node, pool=pool_name, obj_name=object_name)

        pool_obj.do_rados_append(
            client=client_node,
            pool=pool_name,
            obj_name=object_name,
            nobj=(iterations - 1),
        )

        try:
            primary_osd = fetch_primary_osd()
            obj_json = objectstore_obj.list_objects(
                osd_id=primary_osd, obj_name=object_name
            ).strip()
            if not obj_json:
                log.error(
                    f"Object {object_name} could not be listed in OSD {primary_osd}"
                )
                return 1
            log.info(obj_json)

            primary_osd = fetch_primary_osd()
            out = objectstore_obj.fetch_object_dump(osd_id=primary_osd, obj=obj_json)
            log.info(out)
            obj_dump = json.loads(out)
            obj_dump_stats = obj_dump["stat"]
            obj_dump_extents = obj_dump["onode"]["extents"]
        except (KeyError, ValueError) as e:
            log.error(
                f"Object dump json for {object_name} could not be fetched correctly"
            )
            log.error(f"{e.__doc__}")
            log.exception(e)

        log.debug(f"Object dump for {object_name} after initial write operation")
        log.debug(obj_dump)
        log.info(
            "Verify the block size, total size and number of blocks after write operation"
        )
        try:
            log.info(f"{obj_dump_stats['blksize']} == {bluestore_min_alloc_size}")
            assert obj_dump_stats["blksize"] == bluestore_min_alloc_size
            log.info(f"{obj_dump_stats['size']} == {iterations * 4 * 1048576}")
            assert obj_dump_stats["size"] == (iterations * 4 * 1048576)
            log.info(
                f"{obj_dump_stats['blocks']} == {(iterations * 4 * 1048576) / bluestore_min_alloc_size}"
            )
            assert obj_dump_stats["blocks"] == int(
                (iterations * 4 * 1048576) / bluestore_min_alloc_size
            )
            l_offset = obj_dump_extents[-1]["logical_offset"]
            length = obj_dump_extents[-1]["length"]
            log.info(f"{l_offset} + {length} == {(iterations * 4 * 1048576)}")
            assert l_offset + length == (iterations * 4 * 1048576)
            blocks_l_extent = int(
                length / obj_dump_stats["blksize"]
            )  # number of blocks in a logical extent
            log.info(
                f"{len(obj_dump_extents)} == {int(obj_dump_stats['blocks']/blocks_l_extent)}"
            )
            assert len(obj_dump_extents) == int(
                obj_dump_stats["blocks"] / blocks_l_extent
            )
            log.info("PASS")
        except (AssertionError, KeyError) as e:
            log.info("^FAIL")
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise

        method_should_succeed(
            wait_for_clean_pg_sets,
            rados_obj,
            timeout=500,
            sleep_interval=30,
            test_pool=pool_name,
        )

        # Perform single write operation on the same object again to overwrite all previous data
        pool_obj.do_rados_put(client=client_node, pool=pool_name, obj_name=object_name)

        primary_osd = fetch_primary_osd()
        out = objectstore_obj.fetch_object_dump(osd_id=primary_osd, obj=obj_json)
        obj_dump_single = json.loads(out)
        obj_dump_single_stats = obj_dump_single["stat"]
        obj_dump_single_extents = obj_dump_single["onode"]["extents"]

        log.debug(f"Object dump for {object_name} after overwrite operation")
        log.debug(obj_dump_single)
        log.info(
            "Verify change in block size, total size and number of blocks after data removal"
        )
        try:
            log.info(
                f"{obj_dump_single_stats['blksize']} == {bluestore_min_alloc_size}"
            )
            assert obj_dump_single_stats["blksize"] == bluestore_min_alloc_size
            log.info(
                f"{obj_dump_single_stats['size']} "
                f"== {obj_dump_stats['size']} - {(iterations - 1) * (4 << 20)}"
            )
            assert obj_dump_single_stats["size"] == obj_dump_stats["size"] - (
                iterations - 1
            ) * (4 << 20)
            log.info(
                f"{obj_dump_single_stats['blocks']} "
                f"== {obj_dump_stats['blocks']} "
                f"- {((iterations - 1) * (4 << 20)) / bluestore_min_alloc_size}"
            )
            assert obj_dump_single_stats["blocks"] == obj_dump_stats["blocks"] - int(
                ((iterations - 1) * (4 << 20)) / bluestore_min_alloc_size
            )
            l_offset = obj_dump_single_extents[-1]["logical_offset"]
            length = obj_dump_single_extents[-1]["length"]
            log.info(f"{l_offset} + {length} == {(4 << 20)}")
            assert l_offset + length == (4 << 20)
            blocks_l_extent = int(
                length / obj_dump_single_stats["blksize"]
            )  # number of blocks in a logical extent
            log.info(
                f"{len(obj_dump_single_extents)} == {int(obj_dump_single_stats['blocks']/blocks_l_extent)}"
            )
            assert len(obj_dump_single_extents) == int(
                obj_dump_single_stats["blocks"] / blocks_l_extent
            )
            log.info("PASS")
        except (AssertionError, KeyError) as e:
            log.info("^FAIL")
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise
    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Delete the created osd pool
        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
