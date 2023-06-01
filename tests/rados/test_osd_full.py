"""
Module to verify scenarios related to OSD being nearfull, backfill-full
and completely full
"""
import datetime
import json
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_bench import RadosBench
from ceph.utils import get_node_by_id
from tests.ceph_installer.test_cephadm import run as add_osd
from tests.cephadm.test_host import run as deploy_host
from tests.rados.rados_test_util import get_device_path
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

    def create_pool_per_config():
        # Creating pools
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
        return True

    def perform_write_after_full(full_obj):
        # perform rados put to check if write ops is still possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=10, timeout=15)

        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(pool_stat)
        if pool_stat["stats"]["objects"] != full_obj:
            log.error(
                "Write ops should not have been possible, number of objects in the pool have changed"
            )
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs | Expected {full_obj}"
            )

        log.info("Write operation did not take effect as OSDs are full")
        log.info(
            f"Pool has {pool_stat['stats']['objects']} objs | Expected {full_obj} objs"
        )

    def check_full_warning(type, a_set):
        if type == "nearfull":
            osd_warn = "is near full"
            pool_warn = "is nearfull"
        elif type == "backfillfull":
            osd_warn = "is backfill full"
            pool_warn = "is backfillfull"
        elif type == "full":
            osd_warn = "is full"
            pool_warn = "is full (no space)"
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=125)
        while datetime.datetime.now() < timeout_time:
            try:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                log.info(f"Health warning: \n {health_detail}")
                if "stray daemon" in health_detail:
                    cephadm.shell(args=["ceph orch restart mgr"])
                for osd_id in a_set:
                    assert f"osd.{osd_id} {osd_warn}" in health_detail
                assert f"pool '{pool_name}' {pool_warn}" in health_detail
                break
            except AssertionError:
                time.sleep(10)
                if datetime.datetime.now() >= timeout_time:
                    raise

    if config.get("pg_autoscaling"):
        doc = (
            "\n#CEPH-83571715"
            "\n Verify cluster behaviour when OSDs are nearfull, backfill-full, and completely full."
            "\n Autoscale PGs, ensure proper backfilling and rebalancing"
            "\n\t 1. Create replicated pool with single PG and autoscaling disabled"
            "\n\t 2. Retrieve the acting set for the pool"
            "\n\t 3. Calculate the amount of data to be written to pools in"
            " order to trigger near-full, backfill-full, and completely full OSD health warnings"
            "\n\t 4. Perform IOPS using rados bench to fill up the pool up till nearfull,"
            " backfillfull and completely full capacity"
            "\n\t 5. Verify the number of objects written to the pool and the cluster"
            " health warning after each IOPS iteration"
            "\n\t 6. Perform IOPS again and ensure IOPS was unsuccessful as OSDs are full"
            "\n\t 7. Increase the set-full-ratio to a higher value and set-backfillfull-ratio"
            " to a value higher than the previous value of set-full-ratio"
            "\n\t 8. Ensure that completely full and backfillfull warning disappear and "
            "nearfull warning remains"
            "\n\t 9. Enable autoscaling and observe the acting set OSD's 'used capacity' decreasing"
            "\n\t 10. Eventually nearfull warning should disappear, wait for PGs to achieve active+clean state"
        )

        log.info(doc)
        config = config["pg_autoscaling"]
        log.info("Running pg rebalance with full OSDs test case")
        pool_target_configs = config["pool_config"]

        for entry in pool_target_configs.values():
            try:
                rados_obj.configure_pg_autoscaler(**{"default": "off"})
                pool_name = entry["pool_name"]
                assert create_pool_per_config()
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
                bench_obj.write(
                    client=client_node, pool_name=pool_name, **nearfull_config
                )

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

                check_full_warning(type="nearfull", a_set=acting_set)

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

                check_full_warning(type="backfillfull", a_set=acting_set)

                log.info("Verification completed for OSD backfillfull scenario")
                log.debug(f"Pool stat post backfillfull write Ops: \n {pool_stat}")

                # perform rados bench to trigger osd full warning
                osdfull_config = {
                    "seconds": 80,
                    "b": f"{bench_obj_size_kb}KB",
                    "no-cleanup": True,
                    "max-objects": subseq_obj_full,
                }
                bench_obj.write(
                    client=client_node, pool_name=pool_name, **osdfull_config
                )

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

                check_full_warning(type="full", a_set=acting_set)

                log.info("Verification completed for OSD full scenario")
                log.debug(f"Pool stat post osd full write Ops: \n {pool_stat}")

                curr_pool_objs = rados_obj.get_cephdf_stats(pool_name=pool_name)[
                    "stats"
                ]["objects"]
                perform_write_after_full(full_obj=curr_pool_objs)

                """ change the full-ratio and backfillfull-ratio to 0.97 and 0.96 respectively,
                this is to enable the cluster to backfill to other pgs when autoscaling
                is enabled for the pool """
                cmds = [
                    "ceph osd set-full-ratio 0.97",
                    "ceph osd set-backfillfull-ratio 0.96",
                ]

                [cephadm.shell(args=[cmd]) for cmd in cmds]

                time.sleep(30)
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                assert (
                    "backfill full" not in health_detail
                    and "near full" in health_detail
                )
                log.info("backfill full and osd full health warnings disappeared")
                log.debug(f"Health warning: \n {health_detail}")

                # proceed to remove the pg autoscaling restriction for the pool
                # and check ceph health warning should disappear
                rados_obj.configure_pg_autoscaler(**{"default": "on"})
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_autoscale_mode", value="on"
                )

                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=480)
                while datetime.datetime.now() < timeout_time:
                    health_detail, _ = cephadm.shell(args=["ceph health detail"])
                    if (
                        "nearfull" not in health_detail
                        and "near full" not in health_detail
                    ):
                        break
                    log.info("PG rebalancing and OSD backfilling is still in-progress")
                    log.info(
                        "OSD 'used size' is yet to drop below nearfull threshold \n"
                        "Sleeping for 25 seconds"
                    )
                    time.sleep(25)

                if (
                    datetime.datetime.now() > timeout_time
                    and "nearfull" in health_detail
                ):
                    log.error("OSD nearfull health warning still active after 8 mins")
                    log.info(f"Cluster health:\n {health_detail}")
                    raise Exception(
                        "Cluster still has nearfull health warning after 8 mins"
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
                # reset full ratios
                cmds = [
                    "ceph osd set-nearfull-ratio 0.85",
                    "ceph osd set-backfillfull-ratio 0.90",
                    "ceph osd set-full-ratio 0.95",
                ]
                [cephadm.shell(args=[cmd]) for cmd in cmds]

            log.info(
                "Verification of PG autoscaling and cluster behavior with nearfull,"
                " backfill full and completely full OSD has been completed"
            )
        return 0

    if config.get("osd_addition"):
        doc = (
            "\n# CEPH-9323"
            "\n Verify cluster rebalacing when cluster is full and"
            "\n new OSDs are added"
            "\n\t 1. Create replicated pool with single PG and autoscaling disabled"
            "\n\t 2. Retrieve the acting set for the pool"
            "\n\t 3. Calculate the amount of data to be written to pools in"
            " order to completely full OSD health warnings"
            "\n\t 4. Perform IOPS using rados bench to fill up the pool up till completely full capacity"
            "\n\t 5. Verify the number of objects written to the pool and the cluster"
            "\n\t 6. Perform IOPS again and ensure IOPS was unsuccessful as OSDs are full"
            "\n\t 7. Add new OSD host and deploy new OSDs"
            "\n\t 8. Enable autoscaling and observe the acting set OSD's 'used capacity' decreasing"
            "\n\t 10. Eventually cluster full warning should disappear, wait for PGs to achieve active+clean state"
            "\n\t 11. Remove the newly add OSD host and wait for PGs to achieve active+clean state"
        )

        log.info(doc)
        config = config["osd_addition"]
        log.info(
            "Running test for PG rebalancing with full OSDs when new OSDs are added"
        )
        pool_target_configs = config["pool_config"]

        for entry in pool_target_configs.values():
            try:
                rados_obj.configure_pg_autoscaler(**{"default": "off"})
                pool_name = entry["pool_name"]
                assert create_pool_per_config()
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

                # change the nearfull, backfill-full and full-ratio to 0.65, 0.69, and 0.7 respectively
                cmds = [
                    "ceph osd set-nearfull-ratio 0.65",
                    "ceph osd set-backfillfull-ratio 0.69",
                    "ceph osd set-full-ratio 0.70",
                ]
                [cephadm.shell(args=[cmd]) for cmd in cmds]

                # determine the number of objects to be written to the pool
                # to achieve nearfull, backfillfull and full state.
                full_objs = int(primary_osd_size * 0.7 / bench_obj_size_kb) + 1

                # perform rados bench to trigger nearfull warning
                full_config = {
                    "seconds": 200,
                    "b": f"{bench_obj_size_kb}KB",
                    "no-cleanup": True,
                    "max-objects": full_objs,
                }
                bench_obj.write(client=client_node, pool_name=pool_name, **full_config)

                time.sleep(7)  # blind sleep to let all objs show up in ceph df stats
                pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
                if pool_stat["stats"]["objects"] != full_objs + 1:
                    log.error(f"Pool {pool_name} is not having {full_objs+1} objects")
                    raise Exception(
                        f"{pool_name} expected to have {full_objs+1} objects,"
                        f" had {pool_stat['stats']['objects']} objects"
                    )

                log.info(
                    f"{full_objs+1} objects have been written to pool {pool_name}. \n"
                    f"Pool has {pool_stat['stats']['objects']} objs | Expected: {full_objs+1}"
                )

                check_full_warning(type="full", a_set=acting_set)

                log.info("Verification completed for OSD full scenario")
                log.debug(f"Pool stat post osd full write Ops: \n {pool_stat}")

                curr_pool_objs = rados_obj.get_cephdf_stats(pool_name=pool_name)[
                    "stats"
                ]["objects"]
                perform_write_after_full(full_obj=curr_pool_objs)

                cephadm.shell(args=["ceph osd set-full-ratio 0.75"])
                cephadm.shell(args=["ceph osd set-backfillfull-ratio 0.73"])
                # proceed to remove the pg autoscaling restriction for the pool
                # and check ceph health warning should disappear
                rados_obj.configure_pg_autoscaler(**{"default": "on"})
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_autoscale_mode", value="on"
                )

                # adding node13 as a new host to the cluster while rebalancing in-progress
                add_args = {
                    "service": "host",
                    "command": "add",
                    "args": {
                        "node": "node13",
                        "attach_address": True,
                        "labels": "apply-all-labels",
                    },
                }
                add_args.update(config)

                ncount_pre = len(rados_obj.run_ceph_command(cmd="ceph orch host ls"))
                deploy_host(ceph_cluster=ceph_cluster, config=add_args)

                if not ncount_pre <= len(
                    rados_obj.run_ceph_command(cmd="ceph orch host ls")
                ):
                    log.error("New hosts are not added to the cluster")
                    raise Exception("Execution error")

                log.info(
                    "New hosts added to the cluster successfully, Proceeding to deploy OSDs on the same."
                )
                # Deploying OSDs on the new nodes.
                osd_args = {
                    "steps": [
                        {
                            "config": {
                                "command": "apply_spec",
                                "service": "orch",
                                "validate-spec-services": True,
                                "specs": [
                                    {
                                        "service_type": "osd",
                                        "service_id": "new_osds",
                                        "encrypted": "true",
                                        "placement": {"label": "osd-bak"},
                                        "spec": {"data_devices": {"all": "true"}},
                                    }
                                ],
                            }
                        }
                    ]
                }
                osd_args.update(config)
                ocount_pre = rados_obj.run_ceph_command(cmd="ceph osd stat")["num_osds"]
                add_osd(ceph_cluster=ceph_cluster, config=osd_args)
                if (
                    not ocount_pre
                    <= rados_obj.run_ceph_command(cmd="ceph osd stat")["num_osds"]
                ):
                    log.error("New OSDs were not added into the cluster")
                    raise Exception("Execution error")

                log.info("Deployed new hosts and deployed OSDs on them")

                # new OSD host RAW USE(kb) before rebalaning
                new_host = get_node_by_id(ceph_cluster, "node13")
                raw_use_org = rados_obj.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"{new_host.hostname}"
                )["summary"]["total_kb_used"]

                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
                while datetime.datetime.now() < timeout_time:
                    health_detail, _ = cephadm.shell(args=["ceph health detail"])
                    if (
                        "is near full" not in health_detail
                        and "is nearfull" not in health_detail
                    ):
                        break
                    log.info("PG rebalancing and OSD backfilling is still in-progress")
                    log.info(
                        "OSD 'used size' is yet to drop below nearfull threshold \n"
                        "Sleeping for 25 seconds"
                    )
                    time.sleep(25)

                if (
                    datetime.datetime.now() > timeout_time
                    and "near full" in health_detail
                ):
                    log.error("OSD nearfull health warning still active after 10 mins")
                    log.info(f"Cluster health:\n {health_detail}")
                    log.debug(f"ceph osd df stats: \n {rados_obj.get_osd_df_stats}")
                    raise Exception(
                        "Cluster still has OSD nearfull health warning after 10 mins"
                    )

                log.info(f"Cluster health:\n {health_detail}")
                log.info("OSD nearfull health warnings have disappeared")

                # Waiting for cluster to get clean state after rebalancing was triggered
                if not wait_for_clean_pg_sets(rados_obj):
                    log.error("PG's in cluster are not active + Clean state.. ")
                    raise Exception("PG's in cluster are not active + Clean state.. ")
                log.info(
                    "Cluster reached clean state after PG autoscaling and new OSDs addition"
                )

                # check ODS host utilization post rebalancing
                raw_use_after = rados_obj.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"{new_host.hostname}"
                )["summary"]["total_kb_used"]

                assert raw_use_after > raw_use_org
                log.info("Data successfully backfilled to newly added OSDs")

                # check fragmentation score for acting set osds
                for _osd_id in acting_set:
                    assert rados_obj.check_fragmentation_score(osd_id=_osd_id)
            except Exception as e:
                log.error(f"Failed with exception: {e.__doc__}")
                log.exception(e)
                return 1
            finally:
                # deleting the created pool
                rados_obj.detete_pool(pool=pool_name)

                # reset full ratios
                cmds = [
                    "ceph osd set-nearfull-ratio 0.85",
                    "ceph osd set-backfillfull-ratio 0.90",
                    "ceph osd set-full-ratio 0.95",
                ]
                [cephadm.shell(args=[cmd]) for cmd in cmds]

                # Removing newly added OSD host and checking status
                rm_host_id = config.get("remove_host", "node13")
                rm_host = get_node_by_id(ceph_cluster, rm_host_id)
                log.info(
                    f"Identified host : {rm_host.hostname} to be removed from the cluster"
                )

                osd_hosts = rados_obj.get_osd_hosts()
                if rm_host.hostname in osd_hosts:
                    # get list of osd_id on the host to be removed
                    rm_osd_list = rados_obj.get_osd_df_stats(
                        tree=True, filter_by="name", filter=rm_host.hostname
                    )["nodes"][0]["children"]
                    dev_path_list = []
                    for osd_id in rm_osd_list:
                        dev_path_list.append(
                            get_device_path(host=rm_host, osd_id=osd_id)
                        )

                    # Starting to drain the host
                    drain_cmd = f"ceph orch host drain {rm_host.hostname}"
                    cephadm.shell([drain_cmd])
                    # Sleeping for 2 seconds for removal to have started
                    time.sleep(2)
                    log.debug(f"Started drain operation on node : {rm_host.hostname}")

                    status_cmd = "ceph orch osd rm status -f json"
                    end_time = datetime.datetime.now() + datetime.timedelta(
                        seconds=3000
                    )
                    flag = False
                    while end_time > datetime.datetime.now():
                        out, err = cephadm.shell([status_cmd])
                        try:
                            drain_ops = json.loads(out)
                            for entry in drain_ops:
                                log.debug(
                                    f"Drain operations are going on host {rm_host.hostname} \nOperations: {entry}"
                                )
                        except json.JSONDecodeError:
                            log.info(
                                f"Drain operations completed on host : {rm_host.hostname}"
                            )
                            flag = True
                            break
                        except Exception as error:
                            log.error(f"Hit issue during drain operations: {error}")
                            raise Exception(error)
                        log.debug("Sleeping for 10 seconds and checking again....")
                        time.sleep(10)

                    if not flag:
                        log.error(
                            "Drain operation not completed on the cluster even after 3000 seconds"
                        )
                        raise Exception("Execution Error")

                    log.info(
                        f"Completed drain operation on the host. {rm_host.hostname}\n Removing host from the cluster"
                    )

                    for dev_path in dev_path_list:
                        assert utils.zap_device(
                            ceph_cluster, host=rm_host.hostname, device_path=dev_path
                        )

                    time.sleep(5)
                    rm_cmd = f"ceph orch host rm {rm_host.hostname} --force"
                    cephadm.shell([rm_cmd])
                    time.sleep(5)

                    # Checking if the host still exists on the cluster
                    ls_cmd = "ceph orch host ls"
                    hosts = rados_obj.run_ceph_command(cmd=ls_cmd)
                    for host in hosts:
                        if host["hostname"] == rm_host.hostname:
                            log.error(
                                f"Host : {rm_host.hostname} still present on the cluster"
                            )
                            raise Exception("Host not removed error")
                    log.info(
                        f"Successfully removed host : {rm_host.hostname} from the cluster."
                        f" Checking status after removal"
                    )

                    # Waiting for recovery post OSD host removal
                    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
                    log.info("PG's are active + clean post OSD host removal")

            log.info(
                "Verification of OSD host addition during rebalancing and cluster full has been completed"
            )
        return 0
