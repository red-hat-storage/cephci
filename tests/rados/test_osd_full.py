"""
Module to verify scenarios related to OSD being nearfull, backfill-full
and completely full
"""
import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_bench import RadosBench
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    #CEPH-83571715
    Verify cluster behaviour when OSDs are nearfull, backfill-full, and completely full.
    Autoscale PGs, ensure proper backfilling and rebalancing
    1. Create replicated pool with single PG and autoscaling disabled
    2. Retrieve the acting set for the pool
    3. Calculate the amount of data to be written to pools in
        order to trigger near-full, backfill-full, and completely
        full OSD health warnings
    4. Perform IOPS using rados bench to fill up the pool
        up till nearfull, backfillfull and completely full capacity
    5. Verify the number of objects written to the pool and the cluster
        health warning after each IOPS iteration
    6. Perform IOPS again and ensure IOPS was unsuccessful as OSDs are full
    7. Increase the set-full-ratio to a higher value and set-backfillfull-ratio
        to a value higher than the previous value of set-full-ratio
    8. Ensure that completely full and backfillfull warning disappear and
        nearfull warning remains
    9. Enable autoscaling and observe the acting set OSD's 'used capacity' decreasing
    10. Eventually nearfull warning should disappear, wait for PGs to achieve active+clean state
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    bench_obj = RadosBench(mon_node=cephadm, clients=client_node)
    osd_sizes = {}
    bench_obj_size_kb = 16384
    # object size for bench ops is chosen as 16 MB as this is the safe max value
    # as per RHCS Administration Guide | benchmarking-ceph-performance_admin (https://red.ht/3zyuIrp)

    log.info("Running pg rebalance with full OSDs test case")
    pool_target_configs = config["pool_config"]

    # Creating pools and starting the test
    for entry in pool_target_configs.values():
        try:
            pool_name = entry["pool_name"]
            log.info(
                f"Creating {entry['pool_type']} pool on the cluster with name {pool_name}"
            )
            if entry.get("pool_type", "replicated") == "erasure":
                method_should_succeed(
                    rados_obj.create_erasure_pool, name=pool_name, **entry
                )
            else:
                method_should_succeed(rados_obj.create_pool, **entry)

            log.info(f"Pool {pool_name} created successfully")

            # retrieving the size of each osd part of acting set for the pool
            acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
            for osd_id in acting_set:
                osd_df_stats = rados_obj.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"osd.{osd_id}"
                )
                osd_sizes[osd_id] = osd_df_stats["nodes"][0]["kb"]

            primary_osd_size = osd_sizes[acting_set[0]]
            for osd_id in acting_set:
                if osd_sizes[osd_id] != primary_osd_size:
                    log.error(
                        f"All OSDs of the acting set {acting_set} are not of same size"
                    )
                    raise Exception("Total capacity of OSDs is not same")

            log.info(
                f"All OSDs part of acting set {acting_set} are of same size {primary_osd_size} KB"
            )

            # determine the number of objects to be written to the pool
            # to achieve nearfull, backfillfull and full state.
            """ although nearfull warning should get triggered at > 85% capacity,
            it has been observed that filling the OSDs upto borderline 85% resulted in
            false failures, therefore, 85.5% OSD capacity is being utilized
            to trigger nearfull warning """
            max_obj_nearfull = int(primary_osd_size * 0.855 / bench_obj_size_kb) + 1
            subseq_obj_backfillfull = subseq_obj_full = (
                int(primary_osd_size * 0.05 / bench_obj_size_kb) + 1
            )
            # it has been observed that rados bench writes one object more than the
            # number given as max-object during the first iteration
            total_objs = (
                max_obj_nearfull + subseq_obj_backfillfull + subseq_obj_full + 1
            )

            # perform rados bench to trigger nearfull warning
            nearfull_config = {
                "seconds": 150,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
                "max-objects": max_obj_nearfull,
            }
            bench_obj.write(client=client_node, pool_name=pool_name, **nearfull_config)

            time.sleep(7)  # blind sleep to let all objs show up in ceph df stats
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            if pool_stat["stats"]["objects"] != max_obj_nearfull + 1:
                log.error(
                    f"Pool {pool_name} is not having {max_obj_nearfull+1} objects"
                )
                raise Exception(
                    f"{pool_name} expected to have {max_obj_nearfull+1} objects,"
                    f" had {pool_stat['stats']['objects']} objects"
                )

            log.info(
                f"{max_obj_nearfull+1} objects have been written to pool {pool_name}. \n"
                f"Pool has {pool_stat['stats']['objects']} objs | Expected: {max_obj_nearfull+1}"
            )

            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=25)
            while datetime.datetime.now() < timeout_time:
                try:
                    health_detail, _ = cephadm.shell(args=["ceph health detail"])
                    log.info(f"Health warning: \n {health_detail}")
                    for osd_id in acting_set:
                        assert f"osd.{osd_id} is near full" in health_detail
                    assert f"pool '{pool_name}' is nearfull" in health_detail
                    break
                except AssertionError:
                    time.sleep(6)
                    if datetime.datetime.now() >= timeout_time:
                        raise

            log.info("Verification completed for OSD nearfull scenario")
            log.debug(f"Pool stat post nearfull write Ops: \n {pool_stat}")

            # perform rados bench to trigger backfill-full warning
            backfillfull_config = {
                "seconds": 80,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
                "max-objects": subseq_obj_backfillfull,
            }
            bench_obj.write(
                client=client_node, pool_name=pool_name, **backfillfull_config
            )

            time.sleep(7)  # blind sleep to let all objs show up in ceph df stats
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.debug(pool_stat)
            if (
                pool_stat["stats"]["objects"]
                != max_obj_nearfull + subseq_obj_backfillfull + 1
            ):
                log.error(
                    f"Pool {pool_name} is not having {max_obj_nearfull + subseq_obj_backfillfull + 1} objects"
                )
                raise Exception(
                    f"{pool_name} expected to have {max_obj_nearfull + subseq_obj_backfillfull + 1} objects,"
                    f" had {pool_stat['stats']['objects']} objects"
                )

            log.info(
                f"{subseq_obj_backfillfull} objects have been additionally written to pool {pool_name}. \n"
                f"Pool has {pool_stat['stats']['objects']} objs | "
                f"Expected: {max_obj_nearfull + subseq_obj_backfillfull + 1}"
            )

            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=25)
            while datetime.datetime.now() < timeout_time:
                try:
                    health_detail, _ = cephadm.shell(args=["ceph health detail"])
                    log.info(f"Health warning: \n {health_detail}")
                    for osd_id in acting_set:
                        assert f"osd.{osd_id} is backfill full" in health_detail
                    assert f"pool '{pool_name}' is backfillfull" in health_detail
                    break
                except AssertionError:
                    time.sleep(6)
                    if datetime.datetime.now() > timeout_time:
                        raise

            log.info("Verification completed for OSD backfillfull scenario")
            log.debug(f"Pool stat post backfillfull write Ops: \n {pool_stat}")

            # perform rados bench to trigger osd full warning
            osdfull_config = {
                "seconds": 80,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
                "max-objects": subseq_obj_full,
            }
            bench_obj.write(client=client_node, pool_name=pool_name, **osdfull_config)

            time.sleep(7)  # blind sleep to let all objs show up in ceph df stats
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.debug(pool_stat)
            if pool_stat["stats"]["objects"] != total_objs:
                log.error(f"Pool {pool_name} is not having {total_objs} objects")
                raise Exception(
                    f"{pool_name} expected to have {total_objs} objects,"
                    f" had {pool_stat['stats']['objects']} objects"
                )

            log.info(
                f"{subseq_obj_backfillfull} objects have been additionally written to pool {pool_name}. \n"
                f"Pool has {pool_stat['stats']['objects']} objs | Expected: {total_objs}"
            )

            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=25)
            while datetime.datetime.now() < timeout_time:
                try:
                    health_detail, _ = cephadm.shell(args=["ceph health detail"])
                    log.info(f"Health warning: \n {health_detail}")
                    for osd_id in acting_set:
                        assert f"osd.{osd_id} is full" in health_detail
                    assert f"pool '{pool_name}' is full (no space)" in health_detail
                    break
                except AssertionError:
                    time.sleep(6)
                    if datetime.datetime.now() > timeout_time:
                        raise

            log.info("Verification completed for OSD full scenario")
            log.debug(f"Pool stat post osd full write Ops: \n {pool_stat}")

            # perform rados put to check if write ops is still possible
            pool_obj.do_rados_put(
                client=client_node, pool=pool_name, nobj=10, timeout=15
            )

            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.debug(pool_stat)
            if pool_stat["stats"]["objects"] != total_objs:
                log.error(
                    "Write ops should not have been possible, number of objects in the pool have changed"
                )
                raise Exception(
                    f"Pool {pool_name} has {pool_stat['stats']['objects']} objs | Expected {total_objs} objs"
                )

            log.info("Write operation did not take effect as OSDs are full")
            log.info(
                f"Pool has {pool_stat['stats']['objects']} objs | Expected {total_objs} objs"
            )

            """ change the full-ratio and backfillfull-ratio to 0.97 and 0.96 respectively,
            this is to enable the cluster to backfill to other pgs when autoscaling
            is enabled for the pool """
            cmds = [
                "ceph osd set-full-ratio 0.97",
                "ceph osd set-backfillfull-ratio 0.96",
            ]

            [cephadm.shell(args=[cmd]) for cmd in cmds]

            time.sleep(10)
            health_detail, _ = cephadm.shell(args=["ceph health detail"])
            assert "backfill full" not in health_detail and "near full" in health_detail
            log.info("backfill full and osd full health warnings disappeared")
            log.debug(f"Health warning: \n {health_detail}")

            # proceed to remove the pg autoscaling restriction for the pool
            # and check ceph health warning should disappear
            rados_obj.set_pool_property(
                pool=pool_name, props="pg_autoscale_mode", value="on"
            )

            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=420)
            while datetime.datetime.now() < timeout_time:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                if "nearfull" not in health_detail and "near full" not in health_detail:
                    break
                log.info("PG rebalancing and OSD backfilling is still in-progress")
                log.info(
                    "OSD 'used size' is yet to drop below nearfull threshold \n"
                    "Sleeping for 25 seconds"
                )
                time.sleep(25)

            if datetime.datetime.now() > timeout_time and "nearfull" in health_detail:
                log.error("OSD nearfull health warning still active after 7 mins")
                log.info(f"Cluster health:\n {health_detail}")
                raise Exception(
                    "Cluster still has nearfull health warning after 7 mins"
                )

            log.info(f"Cluster health:\n {health_detail}")
            log.info("nearfull health warnings have disappeared")

            # Waiting for cluster to get clean state after PG auto-scaling is enabled
            if not wait_for_clean_pg_sets(rados_obj):
                log.error("PG's in cluster are not active + Clean state.. ")
                raise Exception("PG's in cluster are not active + Clean state.. ")
            log.info("Cluster reached clean state after PG autoscaling")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            # deleting the pool created after the test
            rados_obj.detete_pool(pool=pool_name)

        log.info(
            "Verification of PG autoscaling and cluster behavior with nearfull,"
            " backfill full and completely full OSD has been completed"
        )
    return 0
