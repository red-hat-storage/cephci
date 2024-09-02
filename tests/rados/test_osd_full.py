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
    Module to verify scenarios related to OSD being nearfull, backfill-full
    and completely full

    Currently, covers the following tests:
    - CEPH-83571715: Verify cluster behaviour when OSDs are nearfull, backfill-full, and completely full.
    Autoscale PGs, ensure proper backfilling and rebalancing
    - CEPH-9323: Verify cluster rebalacing when cluster is full and new OSDs are added
    - CEPH-83572758(BZ-2214864): Verify IOs are possbile on a cluster after one OSD reaches FULL state
    """

    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config["rhbuild"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    bench_obj = RadosBench(mon_node=cephadm, clients=client_node)
    osd_sizes = {}
    bench_obj_size_kb = 16384
    # object size for bench ops is chosen as 16 MB as this is the safe max value
    # as per RHCS Administration Guide | benchmarking-ceph-performance_admin (https://red.ht/3zyuIrp)

    def create_pool_per_config(**entry):
        # Creating pools
        log.info(
            f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
        )
        if entry.get("pool_type", "replicated") == "erasure":
            method_should_succeed(
                rados_obj.create_erasure_pool, entry["pool_name"], **entry
            )
        else:
            method_should_succeed(rados_obj.create_pool, **entry)

        log.info(f"Pool {entry['pool_name']} created successfully")
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

    def match_osd_size(a_set):
        for osd_id in a_set:
            osd_df_stats = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter=f"osd.{osd_id}"
            )
            osd_sizes[osd_id] = osd_df_stats["nodes"][0]["kb"]

        primary_osd_size = osd_sizes[a_set[0]]
        for osd_id in a_set:
            if osd_sizes[osd_id] != primary_osd_size:
                log.error(f"All OSDs of the acting set {a_set} are not of same size")
                raise Exception("Total capacity of OSDs is not same")

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
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=210)
        while datetime.datetime.now() < timeout_time:
            try:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                log.info(f"Health warning: \n {health_detail}")
                if "stray daemon" in health_detail:
                    cephadm.shell(args=["ceph orch restart mgr"])
                for osd_id in a_set:
                    if type == "full":
                        if f"osd.{osd_id} {osd_warn}" in health_detail:
                            break
                    else:
                        assert f"osd.{osd_id} {osd_warn}" in health_detail
                assert f"pool '{pool_name}' {pool_warn}" in health_detail
                break
            except AssertionError:
                time.sleep(25)
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
                rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
                pool_name = entry["pool_name"]
                assert create_pool_per_config(**entry)
                # retrieving the size of each osd part of acting set for the pool
                acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                match_osd_size(a_set=acting_set)
                primary_osd_size = osd_sizes[acting_set[0]]

                log.info(
                    f"All OSDs part of acting set {acting_set} are of same size {primary_osd_size} KB"
                )

                # determine how much % cluster is already filled
                current_fill_ratio = rados_obj.get_cephdf_stats()["stats"][
                    "total_used_raw_ratio"
                ]

                # determine the number of objects to be written to the pool
                # to achieve nearfull, backfillfull and full state.
                """ although nearfull warning should get triggered at > 85% capacity,
                it has been observed that filling the OSDs upto borderline 85% resulted in
                false failures, therefore, 85.5% OSD capacity is being utilized
                to trigger nearfull warning """
                max_obj_nearfull = (
                    int(
                        primary_osd_size
                        * (0.855 - current_fill_ratio)
                        / bench_obj_size_kb
                    )
                    + 1
                )
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

                time.sleep(10)  # blind sleep to let all objs show up in ceph df stats
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
                # [update] Aug 2023: It has been observed that backfilling takes too much time
                # with new OSD QoS mClock, to mitigate longer wait time, mClock parameters are tuned
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
                log.info(f"Health warning: \n {health_detail}")

                # proceed to remove the pg autoscaling restriction for the pool
                # and check ceph health warning should disappear
                rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_autoscale_mode", value="on"
                )

                # tune mclock recovery settings
                if rhbuild and rhbuild.split(".")[0] >= "6":
                    rados_obj.set_mclock_profile(profile="high_recovery_ops")
                    rados_obj.set_mclock_parameter(param="osd_max_backfills", value=8)

                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
                # recovery time was previously 480 secs, increased as recovery with mClock
                # could be slower with default settings
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
                        "Sleeping for 45 seconds"
                    )
                    time.sleep(45)

                if (
                    datetime.datetime.now() > timeout_time
                    and "nearfull" in health_detail
                ):
                    log.error("OSD nearfull health warning still active after 8 mins")
                    log.info(f"Cluster health:\n {health_detail}")
                    raise Exception(
                        "Cluster still has nearfull health warning after 10 mins"
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
                log.info(
                    "\n \n ************** Execution of finally block begins here *************** \n \n"
                )
                # deleting the pool created after the test
                rados_obj.delete_pool(pool=pool_name)
                # reset full ratios
                cmds = [
                    "ceph osd set-nearfull-ratio 0.85",
                    "ceph osd set-backfillfull-ratio 0.90",
                    "ceph osd set-full-ratio 0.95",
                ]
                [cephadm.shell(args=[cmd]) for cmd in cmds]
                if rhbuild and rhbuild.split(".")[0] >= "6":
                    rados_obj.set_mclock_profile(reset=True)

                # log cluster health
                rados_obj.log_cluster_health()
                # check for crashes after test execution
                if rados_obj.check_crash_status():
                    log.error("Test failed due to crash at the end of test")
                    return 1

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
                rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
                pool_name = entry["pool_name"]
                assert create_pool_per_config(**entry)
                # retrieving the size of each osd part of acting set for the pool
                acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                match_osd_size(a_set=acting_set)
                primary_osd_size = osd_sizes[acting_set[0]]

                log.info(
                    f"All OSDs part of acting set {acting_set} are of same size {primary_osd_size} KB"
                )

                # determine how much % cluster is already filled
                current_fill_ratio = rados_obj.get_cephdf_stats()["stats"][
                    "total_used_raw_ratio"
                ]

                # change the nearfull, backfill-full and full-ratio to 0.70, 0.79, and 0.8 respectively
                cmds = [
                    "ceph osd set-nearfull-ratio 0.7",
                    "ceph osd set-backfillfull-ratio 0.75",
                    "ceph osd set-full-ratio 0.80",
                ]
                [cephadm.shell(args=[cmd]) for cmd in cmds]

                # determine the number of objects to be written to the pool
                # to achieve nearfull, backfillfull and full state.
                full_objs = (
                    int(
                        primary_osd_size
                        * (0.805 - current_fill_ratio)
                        / bench_obj_size_kb
                    )
                    + 1
                )

                # perform rados bench to trigger full warning
                full_config = {
                    "seconds": 200,
                    "b": f"{bench_obj_size_kb}KB",
                    "no-cleanup": True,
                    "max-objects": full_objs,
                }
                bench_obj.write(client=client_node, pool_name=pool_name, **full_config)

                time.sleep(20)  # blind sleep to let all objs show up in ceph df stats
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

                # increase backfill-full and full ratio to enable backfilling
                cephadm.shell(args=["ceph osd set-full-ratio 0.83"])
                cephadm.shell(args=["ceph osd set-backfillfull-ratio 0.81"])
                # proceed to remove the pg autoscaling restriction for the pool
                # and check ceph health warning should disappear
                rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_autoscale_mode", value="on"
                )

                # tune mclock recovery settings
                if rhbuild and rhbuild.split(".")[0] >= "6":
                    rados_obj.set_mclock_profile(profile="high_recovery_ops")
                    rados_obj.set_mclock_parameter(param="osd_max_backfills", value=8)

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
                """
                This check fails because ceph osd df stats shows incorrect
                data for an OSD host if OSDs are yet to be deployed or if
                the host has been removed from the cluster
                This only happens if ceph osd df stats are fetched with name
                filter, instead of showing 0, it shows the total cluster summary stats
                BZ -
                raw_use_org = rados_obj.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"{new_host.hostname}"
                )["summary"]["total_kb_used"]
                """

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

                # check OSD host utilization post rebalancing
                raw_use_after = rados_obj.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"{new_host.hostname}"
                )["summary"]["total_kb_used"]

                log.info(
                    f"Checking if new OSD host got backfilled,"
                    f"assert {raw_use_after} > 0"
                )
                # assert raw_use_after > raw_use_org
                assert raw_use_after > 0
                log.info("Data successfully backfilled to newly added OSDs")

                # check fragmentation score for acting set osds
                for _osd_id in acting_set:
                    assert rados_obj.check_fragmentation_score(osd_id=_osd_id)
            except Exception as e:
                log.error(f"Failed with exception: {e.__doc__}")
                log.exception(e)
                return 1
            finally:
                log.info(
                    "\n \n ************** Execution of finally block begins here *************** \n \n"
                )
                # deleting the created pool
                rados_obj.delete_pool(pool=pool_name)

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
                    if rhbuild and rhbuild.split(".")[0] >= "6":
                        rados_obj.set_mclock_profile(reset=True)

                    # log cluster health
                    rados_obj.log_cluster_health()
                    # check for crashes after test execution
                    if rados_obj.check_crash_status():
                        log.error("Test failed due to crash at the end of test")
                        return 1
            log.info(
                "Verification of OSD host addition during rebalancing and cluster full has been completed"
            )
        return 0

    if config.get("iops_with_full_osds"):
        doc = (
            "\n # CEPH-83572758 | BZ-2214864"
            "\n Verify IOs are possbile on a cluster after one OSD reaches FULL state"
            "\n\t 1. Create a replicated pool with single PG and autoscaling disabled"
            "\n\t 2. Retrieve the acting set for pool 1"
            "\n\t 3. Reweight the OSDs part of acting set of Pool 1 to 0"
            "\n\t 4. Create another replicated pool and EC pool with single PG and autoscaling disabled"
            "\n\t 5. Retrieve the acting set for pool 2 and 3, should not have any OSD from acting set of Pool 1"
            "\n\t 6. Reweight the OSDs part of acting set of Pool 1 back to 1"
            "\n\t 7. Perform IOPS using rados bench to fill up Pool 1 up till completely full capacity"
            "\n\t 8. Perform IOPS on Pool 2 and Pool 3 and ensure IOPS is successful as full "
            "OSDs are not part of acting set"
        )

        log.info(doc)
        config = config["iops_with_full_osds"]
        log.info("Running test for IOPs with cluster having full OSDs")

        try:
            rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
            pool1_name = pool_name = config["pool-1"]["pool_name"]
            assert create_pool_per_config(**config["pool-1"])

            # retrieving the size of each osd part of acting set for the pool
            acting_set1 = rados_obj.get_pg_acting_set(pool_name=pool1_name)
            match_osd_size(a_set=acting_set1)
            primary_osd_size = osd_sizes[acting_set1[0]]

            log.info(
                f"All OSDs part of acting set {acting_set1} are of same size {primary_osd_size} KB"
            )

            # reweight osds to 0
            for osd_id in acting_set1:
                cephadm.shell([f"ceph osd reweight osd.{osd_id} 0"])

            pool2_name = config["pool-2"]["pool_name"]
            assert create_pool_per_config(**config["pool-2"])
            acting_set2 = rados_obj.get_pg_acting_set(pool_name=pool2_name)

            pool3_name = config["pool-3"]["pool_name"]
            assert create_pool_per_config(**config["pool-3"])
            acting_set3 = rados_obj.get_pg_acting_set(pool_name=pool3_name)

            for osd_id in acting_set1:
                assert osd_id not in acting_set2
                assert osd_id not in acting_set3

            log.info(
                "None of the OSDs part of acting set of Pool 1 are part of acting set of Pool 2 and 3"
            )
            log.info(
                f"\n Acting set of Pool 1: {acting_set1} | Acting set of Pool 2: {acting_set2} | "
                f"Acting set of Pool 3: {acting_set3}"
            )

            # change the nearfull, backfill-full and full-ratio to 0.60, 0.65, and 0.7 respectively
            cmds = [
                "ceph osd set-nearfull-ratio 0.65",
                "ceph osd set-backfillfull-ratio 0.69",
                "ceph osd set-full-ratio 0.70",
            ]
            [cephadm.shell(args=[cmd]) for cmd in cmds]

            # determine the number of objects to be written to the pool
            # to achieve nearfull, backfillfull and full state.
            full_objs = int(primary_osd_size * 0.7 / bench_obj_size_kb) + 1

            # reweight osds to 1
            for osd_id in acting_set1:
                cephadm.shell([f"ceph osd reweight osd.{osd_id} 1"])

            # check if acting set of pool1 returned after reweight to 0 and 1
            time.sleep(5)
            acting_set1_post_reweight = rados_obj.get_pg_acting_set(
                pool_name=pool1_name
            )
            log.info(f"Acting set of {pool1_name} before reweight: {acting_set1}")
            log.info(
                f"Acting set of {pool1_name} after reweight process: {acting_set1_post_reweight}"
            )
            assert sorted(acting_set1) == sorted(acting_set1_post_reweight)

            # perform rados bench to trigger full warning
            full_config = {
                "seconds": 200,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
                "max-objects": full_objs,
            }
            bench_obj.write(client=client_node, pool_name=pool1_name, **full_config)

            log.info(f"{full_objs + 1} objects have been written to pool {pool1_name}")

            check_full_warning(type="full", a_set=acting_set1)

            # perform rados bench to ensure IOPS is still possible for Pool 2 and Pool 3
            pool2_config = {
                "seconds": 60,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
            }
            bench_obj.write(client=client_node, pool_name=pool2_name, **pool2_config)
            bench_obj.write(client=client_node, pool_name=pool3_name, **pool2_config)

            time.sleep(7)  # blind sleep to let all objs show up in ceph df stats
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool2_name)
            if pool_stat["stats"]["objects"] == 0:
                log.error(f"Pool {pool2_name} is not having any objects")
                raise Exception(
                    f"{pool2_name} expected to have some objects,"
                    f" had {pool_stat['stats']['objects']} objects"
                )
            log.info(
                f"{pool_stat['stats']['objects']} objects have been written to pool {pool2_name}"
            )

            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool3_name)
            if pool_stat["stats"]["objects"] == 0:
                log.error(f"Pool {pool3_name} is not having any objects")
                raise Exception(
                    f"{pool3_name} expected to have some objects,"
                    f" had {pool_stat['stats']['objects']} objects"
                )
            log.info(
                f"{pool_stat['stats']['objects']} objects have been written to pool {pool3_name}"
            )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            # deleting the created pool
            rados_obj.delete_pool(pool=pool1_name)
            rados_obj.delete_pool(pool=pool2_name)
            rados_obj.delete_pool(pool=pool3_name)

            # reset full ratios
            cmds = [
                "ceph osd set-nearfull-ratio 0.85",
                "ceph osd set-backfillfull-ratio 0.90",
                "ceph osd set-full-ratio 0.95",
            ]
            [cephadm.shell(args=[cmd]) for cmd in cmds]
            rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})

            # reweight osds to 1
            for osd_id in acting_set1:
                cephadm.shell([f"ceph osd reweight osd.{osd_id} 1"])

            # tune mclock recovery settings
            if rhbuild and rhbuild.split(".")[0] >= "6":
                rados_obj.set_mclock_profile(profile="high_recovery_ops")
                rados_obj.set_mclock_parameter(param="osd_max_backfills", value=8)

            # Waiting for recovery post test execution completion
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
            log.info("PG's are active + clean post test workflow")
            if rhbuild and rhbuild.split(".")[0] >= "6":
                rados_obj.set_mclock_profile(reset=True)

        log.info(
            "Verification of possible IOPs on a Cluster with full OSDs has been completed"
        )
        return 0
