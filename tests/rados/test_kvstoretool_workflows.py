"""
Test Module to perform specific functionalities of ceph-kvstore-tool.
ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> command [args…]
Possible operations:
    list [prefix]
    list-crc [prefix]
    dump [prefix]
    exists <prefix> [key]
    get <prefix> <key> [out <file>]
    crc <prefix> <key>
    get-size [<prefix> <key>]
    set <prefix> <key> [ver <N>|in <file>]
    rm <prefix> <key>
    rm-prefix <prefix>
    store-copy <path> [num-keys-per-tx]
    store-crc <path>
    compac
    compact-prefix <prefix>
    compact-range <prefix> <start> <end>
    destructive-repair
    stats
    histogram
"""

import datetime
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.kvstoretool_workflows import kvstoreToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83629905
    # CEPH-83629283
    Test to perform +ve workflows for the ceph-kvstore-tool utility
    with health and ENOSPC OSDs
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    kvstore_obj = kvstoreToolWorkflows(node=cephadm, nostart=True)
    bench_obj_size_kb = 4096
    prefix = "C"

    all_ops = [
        "print_usage",
        "list_kvpairs",
        "list_crc",
        "dump",
        "check_existence",
        "get_value",
        "get_crc",
        "get_size",
        "set_value",
        "remove",
        "remove_prefix",
        "store_copy",
        "store_crc",
        "compact",
        "compact_prefix",
        "compact_range",
        "destructive_repair",
        "print_stats",
        "print_histogram",
    ]
    readonly_ops = [
        "list_kvpairs",
        "list_crc",
        "dump",
        "check_existence",
        "get_value",
        "get_crc",
        "get_size",
        "store_crc",
        "print_stats",
        "print_histogram",
    ]

    def execute_ops(op):
        if operation == "print_usage":
            # Execute ceph-kvstore-tool --help
            intro = (
                f"\n ----------------------------------"
                f"\n Printing CKT Usage/help for OSD {_osd_id}"
                f"\n ----------------------------------"
            )
            log.info(intro)
            out = kvstore_obj.help(osd_id=_osd_id)
            log.info(out)

        if op == "list_kvpairs":
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> list <prefix>
            intro = (
                f"\n ------------------------------------------"
                f"\n List all KV Pairs for prefix {prefix} within an OSD {_osd_id} "
                f"\n ------------------------------------------"
            )
            log.info(intro)
            out = kvstore_obj.list(osd_id=_osd_id, prefix=prefix)
            log.info("List of KV pairs in OSD %s with prefix %s", _osd_id, prefix)
            log.info(out)
            assert "_head" in out or "meta" in out

        if op == "list_crc":
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> list-crc <prefix>
            intro = (
                f"\n ------------------------------------------"
                f"\n List CRCs for prefix {prefix} within an OSD {_osd_id} "
                f"\n ------------------------------------------"
            )
            log.info(intro)
            crc_list = kvstore_obj.list_crc(osd_id=_osd_id, prefix=prefix)
            log.info("List of CRCs in OSD : ", _osd_id)
            log.info(crc_list)

        if op == "dump":
            # Use the ceph-kvstore-tool to dump KV pair
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> dump <prefix>
            intro = (
                f"\n --------------------"
                f"\n Executing KVStore dump for OSD {_osd_id}"
                f"\n --------------------"
            )
            log.info(intro)
            out = kvstore_obj.dump(osd_id=_osd_id, file_redirect=True)

            log.info(out)

        if op == "check_existence":
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> exists <prefix> [<key>]
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Check existence of KV pairs with prefix {prefix} in OSD {_osd_id} "
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            out = kvstore_obj.check_existence(osd_id=_osd_id, prefix=prefix)
            log.info(out)
            assert prefix in out and "exists" in out

        if op == "get_value":
            # Use the ceph-kvstore-tool utility to get value of a key.
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> get <prefix> <key> [out <file>]
            intro = (
                f"\n --------------------------------"
                f"\n Get value of key 'meta' with prefix {prefix} for OSD {_osd_id}"
                f"\n --------------------------------"
            )
            log.info(intro)

            _key = "meta"
            out = kvstore_obj.get_value(osd_id=_osd_id, prefix=prefix, key=_key)
            log.info(out)
            assert f"({prefix}, {_key})" in out
            assert f"({prefix}, {_key}) does not exist" not in out

        if op == "get_crc":
            # Use the ceph-kvstore-tool utility to get CRC of a key.
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> crc <prefix> <key>
            intro = (
                f"\n --------------------------------"
                f"\n Get CRC of key 'meta' with prefix {prefix} for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            _key = "meta"
            out = kvstore_obj.get_crc(osd_id=_osd_id, prefix=prefix, key=_key)
            log.info(out)
            assert f"({prefix}, {_key})  crc" in out
            assert f"({prefix}, {_key}) does not exist" not in out

        if operation == "set_value":
            # Use the ceph-kvstore-tool utility to set value of a key.
            # preserve the original value of key 'meta'
            # Use the ceph-kvstore-tool utility to get value of a key.
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> get <prefix> <key> [out <file>]
            log.info("Preserving the original value of key 'meta' before modification")
            intro = (
                f"\n --------------------------------"
                f"\n Get value of key 'meta' with prefix {prefix} for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            _key = "meta"
            out = kvstore_obj.get_value(
                osd_id=_osd_id, prefix=prefix, key=_key, out_file="/mnt/org_val"
            )
            log.info(out)
            assert f"({prefix}, {_key})" in out

            log.info("Proceed to modify value of key 'meta' with randon data")
            _cmd = "echo 'random data' > /tmp/rand_data"
            osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=_osd_id)
            osd_host.exec_command(sudo=True, cmd=_cmd)

            # ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> set <prefix> <key> [ver <N>|in <file>]
            intro = (
                f"\n --------------------------------"
                f"\n Set value of key 'meta' with prefix {prefix} for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            kvstore_obj.set_value(
                osd_id=_osd_id, prefix=prefix, key=_key, in_file="/mnt/rand_data"
            )

            # retrieve modified value and validate the change
            out = kvstore_obj.get_value(osd_id=_osd_id, prefix=prefix, key=_key)
            log.info("Modified value of key %s : \n" % _key)
            log.info(out)
            assert "random data" in out

            # restore the value of key 'meta' to its original value
            kvstore_obj.set_value(
                osd_id=_osd_id, prefix=prefix, key=_key, in_file="/mnt/org_val"
            )

        if op == "get_size":
            # Use the ceph-kvstore-tool utility to get value of a key.
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> get-size <prefix> <key>
            intro = (
                f"\n --------------------------------"
                f"\n Get size of key 'meta' with prefix {prefix} for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            _key = "meta"
            out = kvstore_obj.get_size(osd_id=_osd_id, prefix=prefix, key=_key)
            log.info(out)
            assert "estimated store size" in out
            assert f"({prefix},{_key}) size" in out

        if op == "store_crc":
            # Use the ceph-kvstore-tool utility to store crc
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> store-crc <path>
            intro = (
                f"\n --------------------------------"
                f"\n Store crc for OSD {_osd_id}"
                f"\n --------------------------------"
            )
            log.info(intro)

            _path = "./store_crc"
            out = kvstore_obj.store_crc(osd_id=_osd_id, path=_path)
            log.info(out)
            assert f"store at '{_path}' crc " in out

        if op == "compact":
            # Use the ceph-kvstore-tool utility to compact all data of kvstore
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> compact
            intro = (
                f"\n --------------------------------"
                f"\n Compact kvstore data for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            out, err = kvstore_obj.compact(osd_id=_osd_id, exclude_stderr=False)
            log.info(out + err)
            assert "compact starting" in err and "compact completed" in err

        if op == "compact-prefix":
            # Use the ceph-kvstore-tool utility to compact data of kvstore with specific prefix
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> compact-prefix <prefix>
            intro = (
                f"\n --------------------------------"
                f"\n Compact kvstore data with prefix {prefix} for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            out, err = kvstore_obj.compact_prefix(
                osd_id=_osd_id, prefix=prefix, exclude_stderr=False
            )
            log.info(out + err)
            assert "compact starting" in err and "compact completed" in err

        if op == "compact-range":
            # Use the ceph-kvstore-tool utility to compact range of kvstore data with specific prefix
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> compact
            intro = (
                f"\n --------------------------------"
                f"\n Compact range of kvstore data with prefix {prefix} for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            out, err = kvstore_obj.compact_range(
                osd_id=_osd_id, prefix=prefix, start=1, end=10, exclude_stderr=False
            )
            log.info(out + err)
            assert "compact starting" in err and "compact completed" in err

        if op == "destructive-repair":
            # Use the ceph-kvstore-tool utility to perform destructive repair
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> destructive-repair
            intro = (
                f"\n --------------------------------"
                f"\n Perform destructive repair for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            out = kvstore_obj.destructive_repair(osd_id=_osd_id)
            log.info(out)
            assert "destructive-repair completed without reporting an error" in out

        if op == "print_stats":
            # Use the ceph-kvstore-tool utility to print stats
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> stats
            intro = (
                f"\n --------------------------------"
                f"\n Print KVStore Stats for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            out = kvstore_obj.print_stats(osd_id=_osd_id)
            log.info(out)
            entries = [
                "db_statistics",
                "Compaction Stats",
                "Cumulative compaction",
                "Cumulative writes",
                "Interval writes",
            ]
            for entry in entries:
                assert entry in out

        if op == "print_histogram":
            # Use the ceph-kvstore-tool utility to print histogram
            # Execute ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> histogram
            intro = (
                f"\n --------------------------------"
                f"\n Print KVStore Histogram for OSD {_osd_id} "
                f"\n --------------------------------"
            )
            log.info(intro)

            out = kvstore_obj.print_histogram(osd_id=_osd_id)
            log.info(out)
            entries = [
                "prefix",
                "rocksdb_value_distribution",
                "rocksdb_key_value_histogram",
                "key_hist",
                "value_hist",
                "build_size_histogram finished in",
            ]
            for entry in entries:
                assert entry in out

    try:
        if config.get("bluestore-enospc"):
            log.info(
                "\n\n Execution begins for CKT Bluestore ENOSPC scenarios ************ \n\n"
            )

            # create a data pool with single pg
            _pool_name = "ckt-enospc"
            assert rados_obj.create_pool(pool_name=_pool_name, pg_num=1, pg_num_max=1)
            log.info("Pool %s with single PG created successfully" % _pool_name)

            # retrieving the size of each osd part of acting set for the pool
            acting_set = rados_obj.get_pg_acting_set(pool_name=_pool_name)
            osd_sizes = {}
            for osd_id in acting_set:
                osd_df_stats = rados_obj.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"osd.{osd_id}"
                )
                osd_sizes[osd_id] = osd_df_stats["nodes"][0]["kb"]
            primary_osd_size = osd_sizes[acting_set[0]]

            log.info(
                "Write OMAP entries to the pool using librados, 200 objects with 5 omap entries each"
            )
            assert pool_obj.fill_omap_entries(
                pool_name=_pool_name, obj_start=0, obj_end=200, num_keys_obj=5
            )
            time.sleep(30)

            # set nearfull, backfill-full and full-ratio to 100%
            # set noout and norebalance flags
            cmds = [
                "ceph osd set-full-ratio 1",
                "ceph osd set-backfillfull-ratio 1",
                "ceph osd set-nearfull-ratio 1",
                "ceph osd set noout",
                "ceph osd set norebalance",
            ]

            [cephadm.shell(args=[cmd]) for cmd in cmds]

            # determine the number of objects to be written to the pool
            # to achieve ENOPSC state
            objs_enospc = int(primary_osd_size / bench_obj_size_kb * 0.9)

            # perform rados bench to fill OSDs till 90%
            rados_obj.bench_write(
                pool_name=_pool_name,
                rados_write_duration=600,
                max_objs=objs_enospc,
                byte_size=f"{bench_obj_size_kb}KB",
                verify_stats=False,
                check_ec=False,
            )
            time.sleep(30)

            # calculate the number of 3KB objects needed to utilize 15% of the OSD
            objs_enospc = int(primary_osd_size / 3 * 0.15)
            init_time, _ = rados_obj.client.exec_command(
                cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
            )

            osd_down = False
            for _ in range(3):
                # perform rados bench to trigger ENOSPC warning
                rados_obj.bench_write(
                    pool_name=_pool_name,
                    rados_write_duration=600,
                    max_objs=objs_enospc,
                    byte_size="3072",
                    verify_stats=False,
                    check_ec=False,
                )
                time.sleep(30)

                # log the cluster and pool fill %
                cluster_fill = (
                    int(rados_obj.get_cephdf_stats()["stats"]["total_used_raw_ratio"])
                    * 100
                )
                pool_fill = (
                    int(
                        rados_obj.get_cephdf_stats(pool_name=_pool_name)["stats"][
                            "percent_used"
                        ]
                    )
                    * 100
                )

                log.info("Cluster fill percentage: %d" % cluster_fill)
                log.info("Pool %s fill percentage: %d" % (_pool_name, pool_fill))

                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
                # wait for 300 secs to let OSDs crash
                while datetime.datetime.now() < timeout_time:
                    for _osd_id in acting_set:
                        if "down" == rados_obj.fetch_osd_status(_osd_id):
                            log.info("OSD %s is down, as expected" % _osd_id)
                            osd_down = True
                            break  # exit for-loop if even a single OSD is down
                    else:
                        log.warning(
                            "OSDs %s are up and running, re-check after 30 secs"
                            % acting_set
                        )
                        time.sleep(30)
                    if osd_down:
                        break  # exit while-loop if even a single OSD is down
                else:
                    log.warning(
                        "None of the OSDs went down with ENOPSC, rerun rados bench"
                    )
                    time.sleep(10)

                if osd_down:
                    down_osds = rados_obj.get_osd_list(status="down")
                    log.info("Down OSDs on the cluster: %s" % down_osds)
                    # _osd_id = random.choice(down_osds)
                    break  # exit outer for-loop if even a single OSD is down
            else:
                log.error("Could not generate ENOSPC on OSDs after 3 attempts")
                raise Exception("Could not generate ENOSPC on OSDs after 3 attempts")

            end_time, _ = rados_obj.client.exec_command(
                cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
            )
            # check for entries of "ceph_abort_msg" and "bluefs enospc" in OSD log
            log_lines = rados_obj.get_journalctl_log(
                start_time=init_time,
                end_time=end_time,
                daemon_type="osd",
                daemon_id=_osd_id,
            )

            for line in log_lines.splitlines():
                if "bluefs enospc" in line:
                    log.info("Expected BlueFS ENOSPC log entry found:\n %s" % line)
                    break
            else:
                err_msg = (
                    "Expected BluesFS ENOSPC log entries not found for OSD %s" % _osd_id
                )
                log.error(err_msg)
                raise Exception(err_msg)

            # execute CKT commands that are now feasible in read-only mode
            for operation in readonly_ops:
                log.info("Next operation will be performed for ENOSPC OSD %s" % _osd_id)
                execute_ops(op=operation)

            log.info(
                "ceph-kvstore-tool read-only functionalities verified successfully"
            )
        else:
            osd_list = rados_obj.get_osd_list(status="up")
            log.info(f"List of OSDs: \n{osd_list}")
            _osd_id = random.choice(osd_list)

            log.info("Create a data pool with default config")
            assert rados_obj.create_pool(pool_name="ckt-pool")

            log.info("Write data to the pool using rados bench, 500 objects")
            assert rados_obj.bench_write(
                pool_name="ckt-pool",
                rados_write_duration=200,
                max_objs=500,
                verify_stats=False,
            )

            log.info(
                "Write OMAP entries to the pool using librados, 200 objects with 5 omap entries each"
            )
            assert pool_obj.fill_omap_entries(
                pool_name="ckt-pool", obj_start=0, obj_end=200, num_keys_obj=5
            )

            for operation in all_ops:
                log.info("Next operation will be performed on UP OSD %s" % _osd_id)
                execute_ops(op=operation)

            log.info("ceph-kvstore-tool functionalities verified successfully")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info("\n\n\n*********** Execution of finally block starts ***********\n\n")
        # start stopped OSD
        if (
            "_osd_id" in globals()
            or "_osd_id" in locals()
            and not config.get("bluestore-enospc")
        ):
            rados_obj.change_osd_state(action="start", target=_osd_id)

        # reset nearfull, backfill-full and full-ratio to 100%
        # reset noout and norebalance flags
        cmds = [
            "ceph osd set-full-ratio 0.95",
            "ceph osd set-backfillfull-ratio 0.8",
            "ceph osd set-nearfull-ratio 0.75",
            "ceph osd unset noout",
            "ceph osd unset norebalance",
        ]
        [cephadm.shell(args=[cmd]) for cmd in cmds]

        rados_obj.rados_pool_cleanup()
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status() and not config.get("bluestore-enospc"):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed verification of Ceph-kvstore-Tool commands.")
    return 0
