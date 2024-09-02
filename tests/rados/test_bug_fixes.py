"""
Module to verify scenarios related to RADOS Bug fixes
"""

import datetime
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_bench import RadosBench
from cli.utilities.utils import reboot_node
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to verify scenarios related to fixes provided for generic RADOS BZs
    which do not fit in any other existing segregation

    Currently, covers the following tests:
    - CEPH-83590689 | BZ-2011756: Verify ceph config show and get for all daemons
    - CEPH-83590688 | BZ-2229651: Induce Slow OSD heartbeat warning by controlled network delay
    - CEPH-83590688 | BZ-2229651: Verify auto removal of slow osd heartbeat warning after 15 mins of OSD node restart
    """

    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config["rhbuild"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    bench_obj = RadosBench(mon_node=cephadm, clients=client_node)
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    bench_obj_size_kb = 16384

    if config.get("test-config-show-get"):
        doc = (
            "\n# CEPH-83590689"
            "\n Verify ceph config show and get command for all daemons"
            "\n\t 1. Deploy a cluster with at least 1 mon, mgr, OSD, and RGW each"
            "\n\t 2. Retrieve the ceph config show output for each daemon"
            "\n\t 3. Retrieve the ceph config get output for each daemon"
        )

        log.info(doc)
        log.info("Running test to verify ceph config show and get command outputs")

        try:
            if float(rhbuild.split("-")[0]) < 7.1:
                log.info("Passing without execution, BZ yet to be backported")
                return 0

            daemon_list = ["mon", "mgr", "osd", "rgw", "mds"]
            for daemon in daemon_list:
                # retrieve ceph orch service for the daemon
                orch_services = rados_obj.list_orch_services(service_type=daemon)
                if not orch_services:
                    log.info(f"No Orch services found for the daemon: {daemon}")
                    log.info("Continuing to the next daemon in the list")
                    continue

                log.info(f"List of orch services for daemon {daemon}: {orch_services}")

                # log daemon processes for respective orch services
                for service in orch_services:
                    orch_ps = client_node.exec_command(
                        cmd=f"ceph orch ps --service_name {service} --refresh",
                        client_exec=True,
                    )
                    log.info(
                        f"Orch processes for orch service {service}: \n\n {orch_ps}"
                    )

                for service in orch_services:
                    orch_ps_json = rados_obj.run_ceph_command(
                        cmd=f"ceph orch ps --service_name {service} --refresh",
                        client_exec=True,
                    )
                    rand_orch_ps = random.choice(orch_ps_json)
                    log.info(
                        f"Chosen Orch process's dameon name: {rand_orch_ps['daemon_name']}"
                    )
                    log.info(
                        f"Chosen Orch process's daemon id: {rand_orch_ps['daemon_id']}"
                    )
                    log.info(
                        f"Chosen Orch process's daemon type: {rand_orch_ps['daemon_type']}"
                    )

                    if daemon == "rgw":
                        config_get_op = mon_obj.get_config(
                            daemon_name=f"client.{rand_orch_ps['daemon_name']}"
                        )
                        config_show_op = mon_obj.show_config(
                            daemon="client", id=rand_orch_ps["daemon_name"]
                        )
                    else:
                        config_get_op = mon_obj.get_config(
                            daemon_name=rand_orch_ps["daemon_name"]
                        )
                        config_show_op = mon_obj.show_config(
                            daemon=daemon, id=rand_orch_ps["daemon_id"]
                        )

                    log.info(
                        f"ceph config get output for the daemon {rand_orch_ps['daemon_name']}: "
                        f"\n\n {config_get_op}"
                    )
                    log.info(
                        f"ceph config show output for the daemon {rand_orch_ps['daemon_name']}: "
                        f"\n\n {config_show_op}"
                    )

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(
            "Verification of ceph config show and ceph config get output has been completed"
        )
        return 0

    if config.get("slow-osd-heartbeat"):
        doc = (
            "\n# CEPH-83590688"
            "\n Verify slow osd heartbeat warning being generated when network delay > 1 sec"
            "\n\t 1. Deploy a cluster with at least 2 OSD hosts"
            "\n\t 2. Ensure Cluster health condition should be HEALTH_OK"
            "\n\t 3. Choose an OSD host at random and induce network delay greater than 1 > sec"
            "\n\t 4. Slow OSD heartbeat warning should be displayed for all the OSDs part of the OSD host"
        )

        log.info(doc)
        log.info("Running test to verify slow OSD heartbeat warning")

        try:
            # check the default value of network threshold
            mgr_dump = rados_obj.run_ceph_command(cmd="ceph mgr dump", client_exec=True)
            active_mgr = mgr_dump["active_name"]
            _cmd = f"ceph tell mgr.{active_mgr} dump_osd_network"
            dump_osd_network_out = rados_obj.run_ceph_command(
                cmd=_cmd, client_exec=True
            )
            threshold = int(dump_osd_network_out["threshold"])

            log.info(f"Current threshold for Slow OSD network is: {threshold}")
            if threshold != 1000:
                log.error(
                    "Default threshold value for network delay is not 1000, aborting the test"
                )
                raise Exception(
                    "Default threshold value for network delay is not 1000, aborting the test"
                )

            # fetch list of OSD hosts
            osd_hosts = rados_obj.get_osd_hosts()
            rand_host = random.choice(osd_hosts)
            log.info(f"Chosen OSD host to add network delay: {rand_host}")

            _cmd = f"ceph osd ls-tree {rand_host}"
            osd_list = rados_obj.run_ceph_command(cmd=_cmd, client_exec=True)
            log.info(f"List of OSDs on the host: {osd_list}")

            assert rados_obj.add_network_delay_on_host(
                hostname=rand_host, delay="1100ms", set_delay=True
            )

            # smart wait for 120 secs to check Slow OSD heartbeat warning
            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=120)
            while datetime.datetime.now() < timeout_time:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                log.info(f"Health warning: \n {health_detail}")
                if "Slow OSD heartbeats on back" not in health_detail:
                    log.error("Slow OSD heartbeat warning yet to be generated")
                    log.info("sleeping for 30 secs")
                    time.sleep(30)
                    continue
                break
            else:
                log.error("Expected slow OSD heartbeat did not show up within timeout")
                raise Exception(
                    "Expected slow OSD heartbeat did not show up within timeout"
                )

            log.info(
                f"Expected slow OSD heartbeat warning found on the cluster: {health_detail}"
            )

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )

            if "rand_host" in globals() or "rand_host" in locals():
                # removing network delay
                rados_obj.add_network_delay_on_host(
                    hostname=rand_host, delay="1100ms", set_delay=False
                )

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info(
            "Verification of Slow OSD heartbeat with default timeout of 1 sec has been completed"
        )
        return 0

    if config.get("slow-osd-heartbeat-baremetal"):
        doc = (
            "\n# CEPH-83590688"
            "\n Verify auto removal of slow osd heartbeat warning after 15 mins of OSD node restart"
            "\n\t 1. Deploy a cluster with at least 2 OSD hosts"
            "\n\t 2. Ensure Cluster health condition should be HEALTH_OK"
            "\n\t 3. Choose an OSD host at random and reboot the node"
            "\n\t 4. Slow OSD heartbeat warning may appear but should not linger around post 15 mins"
        )

        log.info(doc)
        log.info("Running test to verify slow OSD heartbeat warning on baremetal")

        try:
            # check the default value of network threshold
            mgr_dump = rados_obj.run_ceph_command(cmd="ceph mgr dump", client_exec=True)
            active_mgr = mgr_dump["active_name"]
            _cmd = f"ceph tell mgr.{active_mgr} dump_osd_network"
            dump_osd_network_out = rados_obj.run_ceph_command(
                cmd=_cmd, client_exec=True
            )
            threshold = int(dump_osd_network_out["threshold"])

            log.info(f"Current threshold for Slow OSD network is: {threshold}")
            if threshold != 1000:
                log.error(
                    "Default threshold value for network delay is not 1000, aborting the test"
                )
                raise Exception(
                    "Default threshold value for network delay is not 1000, aborting the test"
                )

            # fetch list of OSD hosts
            osd_hosts = rados_obj.get_osd_hosts()
            rand_host = random.choice(osd_hosts)
            log.info(f"Chosen OSD host to be restarted: {rand_host}")
            osd_nodes = ceph_cluster.get_nodes(role="osd")

            # get the ceph node for chosen osd host
            for node in osd_nodes:
                if node.hostname == rand_host:
                    osd_node = node
                    break

            # restart the osd host
            reboot_node(node=osd_node)

            # smart wait for 15 mins and check health status on regular intervals
            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=900)
            while datetime.datetime.now() < timeout_time:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                log.info(f"Health warning: \n {health_detail}")
                time.sleep(120)

            # Checking if slow osd heartbeat warning exists on the cluster 15 mins post restart
            health_detail, _ = cephadm.shell(args=["ceph health detail"])
            log.info(f"Health warning: \n {health_detail}")

            if "Slow OSD heartbeats on back" in health_detail:
                log.error(
                    "Slow OSD heartbeat still exists on the cluster after 15 mins"
                )
                raise Exception(
                    "Slow OSD heartbeat still exists on the cluster after 15 mins"
                )

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info(
            "Verification of automatic removal of Slow OSD heartbeat warning has been completed "
        )
        return 0

    if config.get("lower_bluefs_shared_alloc_size"):
        doc = (
            "\n# CEPH-83591092"
            "\n Bugzilla trackers:"
            "\n\t- Pacific: 2264054"
            "\n\t- Quincy: 2264053"
            "\n\t- Reef: 2264052 & 2260306"
            "\n Verify OSDs do not crash terminally when custom 'bluefs_shared_alloc_size' "
            "is in use and it's below 'bluestore_min_alloc_size' persistent for BlueStore on deployment"
            "\n\t 1. Deploy a cluster with no default values of 'bluestore_min_alloc_size' and "
            "'bluefs_shared_alloc_size'"
            "\n\t 2. Fetch the current active value of 'bluestore_min_alloc_size'"
            "\n\t 3. Choose one OSD from each OSD node at random"
            "\n\t 4. Set the value of 'bluefs_shared_alloc_size' to half of 'bluestore_min_alloc_size' "
            "for each chosen OSD"
            "\n\t 5. Restart Orch OSD service"
            "\n\t 6. Create a replicated pool and fill it till 70%"
            "\n\t 7. Ensure restart of Orch OSD service is successful"
        )

        log.info(doc)
        log.info(
            "Running test to verify OSD resiliency when 'bluefs_shared_alloc_size'"
            " is below 'bluestore_min_alloc_size'"
        )

        try:
            _pool_name = "test-bluefs-shared"

            # get the list of OSDs on the cluster
            osd_list = rados_obj.run_ceph_command(cmd="ceph osd ls")
            log.debug(f"List of active OSDs: \n{osd_list}")

            # check the current value of 'bluestore_min_alloc_size'
            min_alloc_size = int(
                mon_obj.show_config(
                    daemon="osd", id=osd_list[0], param="bluestore_min_alloc_size_hdd"
                )
            )
            log.info(
                f"Current persistent value of 'bluestore_min_alloc_size_hdd' is {min_alloc_size}"
            )
            min_alloc_size_ssd = int(
                mon_obj.show_config(
                    daemon="osd", id=osd_list[0], param="bluestore_min_alloc_size_ssd"
                )
            )
            log.info(
                f"Current persistent value of 'bluestore_min_alloc_size_ssd' is {min_alloc_size}"
            )

            # cherry-pick one OSD ID from each osd node
            test_osd_list = [
                rados_obj.collect_osd_daemon_ids(osd_node=node)[0] for node in osd_nodes
            ]

            log.info(
                f"OSDs for which bluefs_shared_alloc_size will be modified: {test_osd_list}"
            )
            shared_alloc_size = int(min_alloc_size / 2)
            log.info(
                f"bluefs_shared_alloc_size parameter will be set to - {shared_alloc_size}"
            )

            for osd_id in test_osd_list:
                mon_obj.set_config(
                    section=f"osd.{osd_id}",
                    name="bluefs_shared_alloc_size",
                    value=shared_alloc_size,
                )

            time.sleep(5)

            # restart OSD orch service
            assert rados_obj.restart_daemon_services(daemon="osd")

            # determine how much % cluster is already filled
            current_fill_ratio = rados_obj.get_cephdf_stats()["stats"][
                "total_used_raw_ratio"
            ]

            # determine the number of objects to be written to the pool
            # to achieve 70% utilization
            osd_df_stats = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter="osd.0"
            )
            osd_size = osd_df_stats["nodes"][0]["kb"]
            num_objs = int(
                (osd_size * (0.7 - current_fill_ratio) / bench_obj_size_kb) + 1
            )

            # create a sample pool to write data
            assert rados_obj.create_pool(pool_name=_pool_name)

            # fill the cluster till 70% capacity
            bench_config = {
                "seconds": 300,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
                "max-objects": num_objs,
            }
            bench_obj.write(client=client_node, pool_name=_pool_name, **bench_config)

            time.sleep(10)

            # log the cluster and pool fill %
            cluster_fill = (
                int(rados_obj.get_cephdf_stats()["stats"]["total_used_raw_ratio"]) * 100
            )
            pool_fill = (
                int(
                    rados_obj.get_cephdf_stats(pool_name=_pool_name)["stats"][
                        "percent_used"
                    ]
                )
                * 100
            )

            log.info(f"Cluster fill %: {cluster_fill}")
            log.info(f"Pool {_pool_name} fill %: {pool_fill}")

            # restart OSD orch service
            assert rados_obj.restart_daemon_services(daemon="osd")

            # check for any crashes reported during the test
            crash_list = rados_obj.run_ceph_command("ceph crash ls-new")
            if len(crash_list) != 0:
                log.error(f"Crash reported on the cluster: {crash_list}")
                raise Exception(f"Crash reported on the cluster: {crash_list}")

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # delete pool
            rados_obj.delete_pool(pool=_pool_name)

            # remove bluefs_shared_alloc_size
            if "test_osd_list" in locals() or "test_osd_list" in globals():
                for osd_id in test_osd_list:
                    mon_obj.remove_config(
                        section=f"osd.{osd_id}", name="bluefs_shared_alloc_size"
                    )

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info(
            "Verification of OSD resiliency when 'bluefs_shared_alloc_size' "
            "is below 'bluestore_min_alloc_size' has been completed "
        )
        return 0
