"""
Module to verify scenarios related to RADOS Bug fixes
"""

import datetime
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_bench import RadosBench
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from cli.utilities.utils import reboot_node
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import (
    get_device_path,
    wait_for_daemon_status,
    wait_for_device_rados,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

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
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
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
            # log cluster health
            rados_obj.log_cluster_health()
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
            osd_hosts = ceph_cluster.get_nodes(role="osd")
            rand_host = random.choice(osd_hosts)
            log.info(f"Chosen OSD host to add network delay: {rand_host.hostname}")

            osd_list = rados_obj.collect_osd_daemon_ids(osd_node=rand_host)
            log.info(f"List of OSDs on the host: {osd_list}")

            assert rados_obj.add_network_delay_on_host(
                hostname=rand_host.hostname, delay="1100ms", set_delay=True
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
            # log cluster health
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )

            if "rand_host" in globals() or "rand_host" in locals():
                # removing network delay
                rados_obj.add_network_delay_on_host(
                    hostname=rand_host.hostname, delay="1100ms", set_delay=False
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
            osd_hosts = ceph_cluster.get_nodes(role="osd")
            rand_host = random.choice(osd_hosts)
            log.info(f"Chosen OSD host to be restarted: {rand_host.hostname}")

            # restart the osd host
            reboot_node(node=rand_host)

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
            # log cluster health
            rados_obj.log_cluster_health()
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
            osd_list = rados_obj.get_osd_list(status="up")
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
            # log cluster health
            rados_obj.log_cluster_health()
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

    if config.get("stray_daemon_warning"):
        doc = (
            "\n# CEPH-83615076"
            "\n Bugzilla trackers:"
            "\n\t- Quincy: 2355037"
            "\n\t- Reef: 2355044"
            "\n\t- Squid: 2269003"
            "\n Verify that there are no CEPHADM_STRAY_DAEMON warnings while replacing the OSD"
            "\n\t This test validates the OSD lifecycle by executing a defined sequence of actions:"
            "\n\t 1. Mark the selected OSD down and out."
            "\n\t 2. Stop the OSD process."
            "\n\t 3. Remove the OSD using `ceph orch osd rm` with --zap, --replace, and --force options."
            "\n\t 4. Ensure the OSD no longer appears in the cluster via `get_osd_list()` output."
            "\n\t 5. Verify no `CEPHADM_STRAY_DAEMON` warnings are raised post-removal."
            "The test ensures the OSD is cleanly removed from the cluster's control plane and "
            "that no residual daemon artifacts or health warnings persist."
            "\nCluster health is verified at the end of the test to confirm it returns to `HEALTH_OK`."
        )

        log.info(doc)
        log.info(
            "Running test to verify that there are no CEPHADM_STRAY_DAEMON warnings while replacing the OSD"
        )

        try:
            osd_devices = {}

            test_map = [
                {
                    "desc": "Scenario 1: OSD Orchestrator service is managed | OSD replaced without zap",
                    "unmanaged": False,
                    "zap": False,
                    "expectation": "As the device is not being zapped, the concerned OSD will be removed from"
                    " the cluster, but its crush entry would be retained with a status change to "
                    "'destroyed'. This OSD's entry will be removed from `ceph node ls` output. "
                    "No 'CEPHADM_STRAY_DAEMON' warning should be generated on the cluster.",
                },
                {
                    "desc": "Scenario 2: OSD Orchestrator service is unmanaged | OSD replaced without zap",
                    "unmanaged": True,
                    "zap": False,
                    "expectation": "As the device is not being zapped, the concerned OSD will be removed from "
                    "the cluster, but its crush entry would be retained with a status change to"
                    " 'destroyed'. This OSD's entry will be removed from `ceph node ls` output."
                    " No 'CEPHADM_STRAY_DAEMON' warning should be generated on the cluster.",
                },
                {
                    "desc": "Scenario 3: OSD Orchestrator service is managed | OSD replaced with zap",
                    "unmanaged": False,
                    "zap": True,
                    "expectation": "As the device is being zapped and replaced, the concerned OSD will be "
                    "removed from the cluster, but its crush entry would be retained with a "
                    "status change to 'destroyed' and device will be available for OSD "
                    "re-deployment, hence the OSD will be added back and won't remain in "
                    "destroyed state. This OSD's entry will NOT be removed from `ceph node ls` "
                    "output. No 'CEPHADM_STRAY_DAEMON' warning should be generated on the cluster.",
                },
                {
                    "desc": "Scenario 4: OSD Orchestrator service is unmanaged | OSD replaced with zap",
                    "unmanaged": True,
                    "zap": True,
                    "expectation": "As the device is being zapped and replaced, the concerned OSD will be "
                    "removed from the cluster, but its crush entry would be retained with a "
                    "status change to 'destroyed' and device will be available for OSD "
                    "re-deployment, however, as the service is unmanaged, the OSD would remain "
                    "in 'destroyed' state. This OSD's entry will be removed from `ceph node ls` "
                    "output. No 'CEPHADM_STRAY_DAEMON' warning should be generated on the cluster.",
                },
            ]

            log.debug("Test map: \n", test_map)

            for test in test_map:
                log.info(test["desc"])
                log.info(test["expectation"])

                if (
                    rhbuild
                    and rhbuild.split(".")[0] >= "8"
                    and not (test["unmanaged"] or test["zap"])
                ):
                    log.info(
                        "Skipping this Scenario as it fails in Squid and Tentacle. Bug: 2368108"
                    )
                    continue

                assert rados_obj.set_service_managed_type(
                    service_type="osd", unmanaged=test["unmanaged"]
                )

                # get the list of OSDs on the cluster
                osd_list = rados_obj.get_osd_list(status="up")
                log.debug(f"List of active OSDs: \n{osd_list}")

                # choose an OSD at random
                osd_id = random.choice(osd_list)
                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                assert utils.set_osd_out(ceph_cluster, osd_id=osd_id)
                if not test["zap"]:
                    dev_path = get_device_path(host=osd_host, osd_id=osd_id)
                    if osd_devices.get(osd_host):
                        osd_devices[osd_host].append(dev_path)
                    else:
                        osd_devices[osd_host] = [dev_path]
                utils.osd_replace(ceph_cluster, osd_id=osd_id, zap=test["zap"])
                time.sleep(15)

                if test["zap"] and not test["unmanaged"]:
                    """
                    Scenario - 3
                    As the device is being zapped and replaced, the concerned OSD will be removed from the cluster,
                    but its crush entry would be retained with a status change to "destroyed" and device will be
                    available for OSD re-deployment, hence the OSD will be added back and won't remain in destroyed
                    state. This OSD's entry will NOT be removed from `ceph node ls` output. No "CEPHADM_STRAY_DAEMON"
                    warning should be generated on the cluster.
                    """
                    endtime = datetime.datetime.now() + datetime.timedelta(seconds=300)
                    while datetime.datetime.now() < endtime:
                        if rados_obj.fetch_osd_status(_osd_id=osd_id) == "up":
                            break
                        log.info("OSD.%s yet to come up, sleeping for 30 secs" % osd_id)
                        time.sleep(30)
                    else:
                        log.error(
                            "OSD.%s did not get re-deployed within timeout" % osd_id
                        )
                        raise Exception(
                            "OSD.%s did not get re-deployed within timeout" % osd_id
                        )

                    # destroyed OSD should get re-deployed and should be part of ceph node ls output
                    node_ls_op = rados_obj.run_ceph_command(
                        cmd="ceph node ls osd", client_exec=True
                    )
                    log.debug("ceph node ls osd output: ", node_ls_op)
                    nodels_osd_list = [
                        item for entry in node_ls_op.values() for item in entry
                    ]
                    assert int(osd_id) in nodels_osd_list, (
                        "OSD.%s is expected to be part of ceph node ls" % osd_id
                    )
                else:
                    endtime = datetime.datetime.now() + datetime.timedelta(seconds=100)
                    while datetime.datetime.now() < endtime:
                        # check status of OSD from ceph osd tree output
                        destroyed_osds = rados_obj.get_osd_list(status="destroyed")
                        if osd_id in destroyed_osds:
                            log.info(
                                "OSD.%s is in destroyed state. Proceeding to check ceph node ls"
                                % osd_id
                            )
                            break
                        log.error(
                            "OSD.%s is not in destroyed state. Sleeping for 20 secs"
                            % osd_id
                        )
                        time.sleep(20)
                    else:
                        log.error(
                            "OSD.%s is not in destroyed state after 100 secs" % osd_id
                        )
                        raise Exception(
                            "OSD.%s is not in destroyed state after 100 secs" % osd_id
                        )

                    # destroyed OSD should not be part of ceph node ls output
                    node_ls_op = rados_obj.run_ceph_command(
                        cmd="ceph node ls osd", client_exec=True
                    )
                    log.debug("ceph node ls osd output: ", node_ls_op)

                    nodels_osd_list = [
                        item for entry in node_ls_op.values() for item in entry
                    ]
                    assert int(osd_id) not in nodels_osd_list, (
                        "OSD.%s not expected to be part of ceph node ls" % osd_id
                    )

                # ceph health should not contain 'CEPHADM_STRAY_DAEMON' warning
                endtime = datetime.datetime.now() + datetime.timedelta(seconds=240)
                while datetime.datetime.now() < endtime:
                    health_detail = rados_obj.log_cluster_health()
                    assert (
                        "CEPHADM_STRAY_DAEMON" not in health_detail
                    ), "CEPHADM_STRAY_DAEMON warning found"
                    time.sleep(20)

                log.info("Verification completed for " + test["desc"])

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            # log cluster health
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            if not rhbuild.startswith("8"):
                # zap all OSD device paths whose OSD was removed
                for key, val in osd_devices.items():
                    for dev in val:
                        utils.zap_device(
                            ceph_cluster=ceph_cluster,
                            host=key.hostname,
                            device_path=dev,
                        )

            time.sleep(60)
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info(
            "Verification of CEPHADM_STRAY_DAEMON warnig for all combination completed"
        )
        return 0

    if config.get("test_osd_spec_update"):
        doc = (
            "\n# bug : https://bugzilla.redhat.com/show_bug.cgi?id=2304314"
            "\n Verify that OSDs can be added/moved to new spec"
            "\n\t 1. Deploy a cluster with at least 1 mon, mgr, OSD, and RGW each"
            "\n\t 2. Remove 1 OSD daemon, note the daemon ID"
            "\n\t 3. Add back the OSD via ceph orch daemon add command"
            "\n\t 4. Note the OSD service created for the new OSD is unmanaged"
            "\n\t 5. Move the OSD to the older managed service"
        )

        log.info(doc)
        log.info("Running test to Verify that OSDs can be added/moved to new spec")

        try:
            if float(rhbuild.split("-")[0]) < 7.1:
                log.info("Passing without execution, BZ yet to be backported")
                return 0

            target_osd = random.choice(rados_obj.get_osd_list(status="UP"))
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
            osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)
            dev_path = get_device_path(host, target_osd)
            log_msg = (
                f"\nSelected OSD for test : \n"
                f"osd device path  : {dev_path},\n osd_id : {target_osd},\n host.hostname : {host.hostname}\n"
                f"OSD spec : {osd_spec_name}\n"
            )
            log.info(log_msg)
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            utils.osd_remove(ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
            method_should_succeed(
                utils.zap_device, ceph_cluster, host.hostname, dev_path
            )
            method_should_succeed(
                wait_for_device_rados, host, target_osd, action="remove"
            )

            # Adding the removed OSD back and checking the cluster status
            utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)
            method_should_succeed(wait_for_device_rados, host, target_osd, action="add")
            method_should_succeed(
                wait_for_daemon_status,
                rados_obj=rados_obj,
                daemon_type="osd",
                daemon_id=target_osd,
                status="running",
                timeout=60,
            )
            log.debug("Completed addition of the OSD back to cluster.")

            # Checking if the newly added OSD is without placement
            if not service_obj.unmanaged_osd_service_exists():
                log.info(
                    "No un-manage-able placement spec present on the cluster"
                    "Proceeding to change the OSD spec to original one if the "
                    "placement has been changed during the OSD add"
                )
            else:
                log.info(
                    "un-manage-able placement spec based OSD present on the cluster post addition"
                )
            time.sleep(10)
            new_osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)
            log.info("OSD spec post replacement is %s", new_osd_spec_name)

            if osd_spec_name != new_osd_spec_name:
                log.info(
                    "OSD default spec changed post removal & addition."
                    "Moving the service back to original placement"
                )
                # Trying to set the OSD back to managed
                assert service_obj.add_osds_to_managed_service(
                    osds=[target_osd], spec=osd_spec_name
                )
                time.sleep(10)
                new_osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)
                log.info(
                    "OSD spec post replacement and subsequent movement is %s",
                    new_osd_spec_name,
                )
                # Currently OSD metadta is not updated with the spec change. Open discussion in progress.
                # if osd_spec_name != new_osd_spec_name:
                #     log.error("Spec could not be updated for the OSD : %s", target_osd)
                #     return 1

            log.info("The OSD was successfully moved to original spec.")

            # Checking cluster health after OSD removal
            method_should_succeed(rados_obj.run_pool_sanity_check)
            log.info(
                f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
            )
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            # log cluster health
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(
            "Verification that OSDs can be added/moved to new spec has been completed"
        )
        return 0
