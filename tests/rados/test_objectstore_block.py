import json
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

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

    pool_name = config["pool_name"]
    object_name = config.get("object_name", "obj-objectstore")

    log.info(
        "Running test case to verify changes in number "
        "of blocks when data is written to an object and removed from an object"
    )

    try:
        # create pool with given config
        if config["create_pool"]:
            rados_obj.create_pool(pool_name=pool_name)

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

        osd_map = rados_obj.get_osd_map(pool=pool_name, obj=object_name)
        acting_pg_set = osd_map["acting"]
        log.info(f"Acting pg set for {object_name} in {pool_name}: {acting_pg_set}")
        if not acting_pg_set:
            log.error("Failed to retrieve acting pg set")
            raise Exception("Failed to retrieve acting pg set")

        acting_osd_id = acting_pg_set[0]
        acting_osd_node = rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=acting_osd_id
        )
        osd_data_path = ceph_cluster.get_osd_metadata(acting_osd_id).get("osd_data")
        log.info(f"osd_data_path: {osd_data_path}")

        assert rados_obj.change_osd_state(action="stop", target=acting_osd_id)
        assert rados_obj.change_osd_state(action="disable", target=acting_osd_id)

        cmd_base = f"cephadm shell --name osd.{acting_osd_id} -- "
        cmd_fetch_obj_dump = f"{cmd_base} ceph-objectstore-tool --data-path {osd_data_path} {object_name} dump"

        try:
            obj_dump_, _ = acting_osd_node.exec_command(
                sudo=True, cmd=cmd_fetch_obj_dump
            )
            obj_dump = json.loads(obj_dump_)
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

        assert rados_obj.change_osd_state(action="start", target=acting_osd_id)
        assert rados_obj.change_osd_state(action="enable", target=acting_osd_id)

        for i in range(2):
            time.sleep(3)
            new_acting_pg_set = rados_obj.get_osd_map(pool=pool_name, obj=object_name)[
                "acting"
            ]
            log.info(new_acting_pg_set)
            if new_acting_pg_set == acting_pg_set:
                break
            elif i == 1:
                log.error(
                    f"{acting_osd_id} could not return to PG {new_acting_pg_set} within timeout. "
                    f"New acting pg set({new_acting_pg_set} and old acting pg set({acting_pg_set}) are not same"
                )
                raise Exception("acting pg set has changed")

        # Perform single write operation on the same object again to overwrite all previous data
        pool_obj.do_rados_put(client=client_node, pool=pool_name, obj_name=object_name)

        assert rados_obj.change_osd_state(action="stop", target=acting_osd_id)
        assert rados_obj.change_osd_state(action="disable", target=acting_osd_id)

        obj_dump_single_, _ = acting_osd_node.exec_command(
            sudo=True, cmd=cmd_fetch_obj_dump
        )
        obj_dump_single = json.loads(obj_dump_single_)
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
                f"== {obj_dump_stats['size']} - {(iterations - 1) * 4 * 1048576}"
            )
            assert (
                obj_dump_single_stats["size"]
                == obj_dump_stats["size"] - (iterations - 1) * 4 * 1048576
            )
            log.info(
                f"{obj_dump_single_stats['blocks']} "
                f"== {obj_dump_stats['blocks']} "
                f"- {((iterations - 1) * 4 * 1048576) / bluestore_min_alloc_size}"
            )
            assert obj_dump_single_stats["blocks"] == obj_dump_stats["blocks"] - int(
                ((iterations - 1) * 4 * 1048576) / bluestore_min_alloc_size
            )
            l_offset = obj_dump_single_extents[-1]["logical_offset"]
            length = obj_dump_single_extents[-1]["length"]
            log.info(f"{l_offset} + {length} == {4 * 1048576}")
            assert l_offset + length == (4 * 1048576)
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
        rados_obj.change_osd_state(action="start", target=acting_osd_id)
        rados_obj.change_osd_state(action="enable", target=acting_osd_id)

        # Delete the created osd pool
        if config.get("delete_pool"):
            rados_obj.detete_pool(pool=pool_name)

    return 0
