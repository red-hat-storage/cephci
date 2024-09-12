"""
This file contains various tests/ validations related to ceph df stats.
Tests included:
1. Verification of ceph df output upon creation & deletion of objects
2. MAX_AVAIL value should not change to an invalid value
   upon addition of osd with weight 0
"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.utils import get_node_by_id
from tests.misc_env.lvm_deployer import create_lvms
from tests.rados.rados_test_util import get_device_path, wait_for_device_rados
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Performs tests related to ceph df stats
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    client = ceph_cluster.get_nodes(role="client")[0]

    if config.get("verify_cephdf_stats"):
        desc = (
            "\n#CEPH-83571666 \n"
            "This test is to verify whether ceph df displays the \n"
            "correct number of objects in a pool upon creation \n"
            "and deletion \n"
            "Script covers the following steps- \n"
            "1. Creating a pool with default pg number \n"
            "2. Create 'n' number of objects using rados put \n"
            "3. Verify ceph df stats \n"
            "4. Remove all the objects from the given pool \n"
            "5. Verify ceph df stats updated with 0 objects \n"
        )

        log.info(desc)
        counter = 1
        df_config = config.get("verify_cephdf_stats")
        pool_name = df_config["pool_name"]
        obj_nums = df_config["obj_nums"]

        try:
            # create pool with given config
            if df_config["create_pool"]:
                rados_obj.create_pool(pool_name=pool_name)

            for obj_num in obj_nums:
                # create 'obj_num' number of objects
                pool_obj.do_rados_put(client=client, pool=pool_name, nobj=obj_num)

                """ [upd] Jul'23: it now takes more time than before for ceph df stats
                 to account for all the objects in a pool
                 blind sleep in being increased from 5 to 30,
                 cannot implement smart wait along because it would pass the test even
                 if the objects are displayed correctly momentarily and change later on,
                 the number of objects should be stable and correct after a while"""
                time.sleep(30)  # blind sleep to let all the objs show up in ceph df

                # Verify Ceph df output post object creation
                try:
                    while counter < 5:
                        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
                        ceph_df_obj = pool_stat["stats"]["objects"]
                        if ceph_df_obj == obj_num:
                            log.info(
                                f"ceph df stats display correct objects {ceph_df_obj} for {pool_name}"
                            )
                            break
                        else:
                            log.error(
                                f"ceph df stats display incorrect objects {ceph_df_obj} for {pool_name}"
                            )
                            log.info(
                                f"Validation failed in round #{counter}, retying after 30 secs.."
                            )
                            counter += 1
                            time.sleep(30)
                    else:
                        log.error(
                            f"ceph df stats display incorrect objects {ceph_df_obj} for {pool_name} even after"
                            f" 60 secs"
                        )
                        return 1
                except KeyError:
                    log.error(
                        f"No stats found on the cluster for the requested pool {pool_name}"
                    )
                    return 1

                # delete all objects from the pool
                pool_obj.do_rados_delete(pool_name=pool_name)
                time.sleep(30)  # blind sleep to let all the objs get removed from stats

                # Verify Ceph df output post objects deletion
                try:
                    counter = 1
                    while counter < 5:
                        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
                        ceph_df_obj = pool_stat.get("stats")["objects"]
                        if ceph_df_obj == 0:
                            log.info(f"ceph df stats display 0 objects for {pool_name}")
                            break
                        else:
                            log.error(
                                f"ceph df stats display incorrect objects {ceph_df_obj} for {pool_name}"
                            )
                            log.info(
                                f"Validation failed in round #{counter}, retying after 30 secs.."
                            )
                            counter += 1
                            time.sleep(30)
                    else:
                        log.error(
                            f"ceph df stats display incorrect objects {ceph_df_obj} for {pool_name} even after "
                            f"120 secs"
                        )
                        return 1
                except KeyError:
                    log.error(
                        f"No stats found on the cluster for the requested pool {pool_name}"
                    )
                    return 1

                log.info(f"ceph df stats verification completed for {obj_num} objects")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            if df_config.get("delete_pool"):
                rados_obj.delete_pool(pool=pool_name)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("ceph df stats verification completed")
        return 0

    if config.get("verify_cephdf_max_avail"):
        desc = (
            "\n#CEPH-10312 \n"
            "This test is to verify that ceph df MAX AVAIL does not \n"
            "become '0' when an OSD is added with initial weight '0' \n"
            "Script covers the following steps- \n"
            "1. Creating a pool with default pg number \n"
            "2. Create 'n' number of objects using rados put(perform IOPS) \n"
            "3. Capture ceph df stats \n"
            "4. Remove the last osd from the cluster osd list \n"
            "5. Set the param 'osd_crush_initial_weight' to 0 \n"
            "6. Add the removed osd \n"
            "7. Verify that the weight of the added osd is 0 using ceph osd tree \n"
            "8. Capture ceph df stats, MAX AVAIL should decrease by \n"
            "the size of the concerned osd and should not change to 0 \n"
            "9. Re-weight the OSD to its initial value \n"
            "10. Capture ceph df stats, MAX AVAIL should be same as \n"
            "initial value and not equal to 0 or decreased value \n"
        )

        log.info(desc)
        df_config = config.get("verify_cephdf_max_avail")
        pool_name = df_config["pool_name"]
        obj_nums = df_config["obj_nums"]

        try:
            # create pool with given config
            if df_config["create_pool"]:
                rados_obj.create_pool(pool_name=pool_name)

            # create 'obj_num' number of objects
            pool_obj.do_rados_put(client=client, pool=pool_name, nobj=obj_nums)

            time.sleep(5)  # blind sleep to let all the objs show up in ceph df

            initial_pool_stat = rados_obj.get_cephdf_stats()["pools"]

            # obtain the last osd id
            out, _ = cephadm.shell(args=["ceph osd ls"])
            osd_id = out.strip().split("\n")[-1]

            osd_df_stats = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter=f"osd.{osd_id}"
            )

            org_weight = osd_df_stats["nodes"][0]["crush_weight"]
            osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
            osd_size = osd_df_stats["nodes"][0]["kb"]
            dev_path = get_device_path(host=osd_host, osd_id=osd_id)
            assert utils.set_osd_out(ceph_cluster, osd_id=osd_id)
            utils.osd_remove(ceph_cluster, osd_id=osd_id)
            time.sleep(5)
            assert utils.zap_device(
                ceph_cluster, host=osd_host.hostname, device_path=dev_path
            )
            assert wait_for_device_rados(
                host=osd_host, osd_id=osd_id, action="remove", timeout=1000
            )

            out, _ = cephadm.shell(["ceph config set osd osd_crush_initial_weight 0"])
            assert wait_for_device_rados(host=osd_host, osd_id=osd_id, action="add")
            assert utils.set_osd_in(ceph_cluster, all=True)
            time.sleep(8)  # blind sleep to let osd stats show up

            # ensure weight of newly added OSD is 0
            zero_weight = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter=f"osd.{osd_id}"
            )["nodes"][0]["crush_weight"]
            assert zero_weight == 0

            org_max_avail = initial_pool_stat[0]["stats"]["max_avail"]
            upd_max_avail = org_max_avail - osd_size * 1024 / 3
            zero_weight_pool_stat = rados_obj.get_cephdf_stats()["pools"]
            log.info("Verifying that the reduced max_avail is within range and != 0")
            for pool in zero_weight_pool_stat:
                log.info(f"POOL: {pool['name']}")
                if (
                    rados_obj.get_pool_property(pool=pool["name"], props="crush_rule")
                    != "replicated_rule"
                ):
                    log.info(f"{pool['name']} is an Erasure-Coded pool")
                    log.info(f"{pool['stats']['max_avail']} != 0")
                    assert pool["stats"]["max_avail"] != 0
                else:
                    log.info(
                        f"{upd_max_avail} <= {pool['stats']['max_avail']} < {org_max_avail}"
                    )
                    assert upd_max_avail <= pool["stats"]["max_avail"] < org_max_avail
                log.info("PASS")

            assert rados_obj.reweight_crush_items(
                name=f"osd.{osd_id}", weight=org_weight
            )

            time.sleep(30)  # blind sleep to let stats get updated post crush re-weight
            assert wait_for_clean_pg_sets(rados_obj, timeout=900)
            log.info("Verifying max_avail value is same as before")
            reweight_pool_stat = rados_obj.get_cephdf_stats()["pools"]
            for pool in reweight_pool_stat:
                if (
                    rados_obj.get_pool_property(pool=pool["name"], props="crush_rule")
                    == "replicated_rule"
                ):
                    log.info(f"POOL: {pool['name']}")
                    log.info(
                        f"{int(pool['stats']['max_avail']/1073741824)} GB == {int(org_max_avail/1073741824)} GB"
                    )
                    assert int(pool["stats"]["max_avail"] / 1073741824) == int(
                        org_max_avail / 1073741824
                    )
                    log.info("pass")
        except AssertionError as AE:
            log.error(f"Failed with exception: {AE.__doc__}")
            log.exception(AE)
            return 1
        finally:
            log.info("\n ************* Executing finally block **********\n")
            assert rados_obj.reweight_crush_items(
                name=f"osd.{osd_id}", weight=org_weight
            )
            if df_config.get("delete_pool"):
                rados_obj.delete_pool(pool=pool_name)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("ceph df MAX AVAIL stats verification completed")
        return 0

    if config.get("cephdf_max_avail_osd_expand"):
        desc = (
            "\n#CEPH-83595780"
            "\nReef: BZ-2296248"
            "\nSquid: BZ-2296247"
            "\nThis test is to verify that ceph df MAX AVAIL is displayed correctly for all the pools"
            " when OSD size is increased"
            "\nSteps- \n"
            "1. Creating a pool with default config\n"
            "2. Log pool stats and verify max_avail \n"
            "3. Create LVMs on backup nodes \n"
            "4. Add backup hosts to the cluster and deploy OSDs on them \n"
            "5. Log pool stats and verify max_avail \n"
            "6. Expand OSD size on one backup host \n"
            "7. Log pool stats and verify max_avail \n"
            "8. Write data to a pool to fill the cluster \n"
            "9. Log pool stats and verify max_avail \n"
            "10. Expand OSD size on other backup host \n"
            "11. Log pool stats and verify max_avail"
        )

        log.info(desc)
        df_config = config.get("cephdf_max_avail_osd_expand")
        pool_name = "test-osd-expand"
        lvm_list = {"node12": [], "node13": []}

        def bytes_to_gb(val):
            return round(val / (1 << 30), 1)

        try:
            # pass without execution for Squid
            if not rhbuild.startswith("7"):
                log.info("Test is currently valid only for RHCS 7.x")
                return 0

            # create default pool with given name
            rados_obj.create_pool(pool_name=pool_name)

            initial_pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.info(f"{pool_name} pool stat: {initial_pool_stat}")

            # execute max_avail check across the cluster
            if not rados_obj.verify_max_avail(variance=0.25):
                log.error("MAX_AVAIL deviates on the cluster more than expected")
                raise Exception("MAX_AVAIL deviates on the cluster more than expected")
            log.info("MAX_AVAIL on the cluster are as per expectation")

            # add backup hosts to the cluster
            service_obj.add_new_hosts(add_nodes=["node12", "node13"], deploy_osd=False)
            node12_obj = get_node_by_id(ceph_cluster, "node12")
            node13_obj = get_node_by_id(ceph_cluster, "node13")
            time.sleep(15)

            for node in ["node12", "node13"]:
                # create 10G LVMs on backup nodes
                node_obj = node12_obj if node == "node12" else node13_obj
                empty_devices = rados_obj.get_available_devices(
                    node_name=node_obj.hostname, device_type="hdd"
                )
                if len(empty_devices) < 1:
                    log.error(
                        f"Need at least 1 spare disks available on host {node_obj.hostname}"
                    )
                    raise Exception(
                        f"One spare disk not available on host {node_obj.hostname}"
                    )

                # for dev in empty_devices:
                lvm_list[node] = create_lvms(
                    node=node_obj, count=1, size="10G", devices=[empty_devices[0]]
                )
                log.info(
                    f"List of LVMs created on {node_obj.hostname}: {lvm_list[node]}"
                )

            init_osd_list = rados_obj.get_active_osd_list()
            log.info(f"Active OSD list before OSD addition: {init_osd_list}")
            init_osd_count = len(init_osd_list)
            # deploy single OSD on lvm devices created
            for node in ["node12", "node13"]:
                node_obj = node12_obj if node == "node12" else node13_obj
                log.info(f"Proceeding to deploy OSDs on {node_obj.hostname}")
                add_cmd = f"ceph orch daemon add osd {node_obj.hostname}:data_devices={lvm_list[node][0]}"
                out, err = cephadm.shell(args=[add_cmd])

                if not (
                    "Created osd" in out and f"on host '{node_obj.hostname}" in out
                ):
                    log.error(f"OSD addition on {node_obj.hostname} failed")
                    log.error(err)
                    raise Exception(f"OSD addition on {node_obj.hostname} failed")

            post_osd_list = rados_obj.get_active_osd_list()
            log.info(f"Active OSD list after OSD addition: {post_osd_list}")
            post_osd_count = len(post_osd_list)
            if post_osd_count - init_osd_count < 2:
                log.error(
                    f"Expected 2 OSD to get deployed. New osd count {post_osd_count} | Old osd count {init_osd_count}"
                )
                raise Exception("Required no of OSDs not added to the cluster")

            _pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.info(f"{pool_name} pool stat: {_pool_stat}")

            rados_obj.change_recovery_threads(config=config, action="set")
            if not wait_for_clean_pg_sets(rados_obj, timeout=900):
                log.error("Cluster cloud not reach active+clean state within 900 secs")
                raise Exception("Cluster cloud not reach active+clean state")

            # execute max_avail check across the cluster
            if not rados_obj.verify_max_avail():
                log.error("MAX_AVAIL deviates on the cluster more than expected")
                raise Exception("MAX_AVAIL deviates on the cluster more than expected")
            log.info("MAX_AVAIL on the cluster are as per expectation")

            # expand the OSD LVMs on backup node12
            for lvm in lvm_list["node12"]:
                _cmd = f"lvextend -L 15G {lvm}"
                node12_obj.exec_command(cmd=_cmd, sudo=True)

            log.info("LVM size on node12 increased from 10G to 15G")
            node12_osds = rados_obj.collect_osd_daemon_ids(osd_node=node12_obj)

            # for each OSD on node 12, execute bluefs-bdev-expand
            for osd_id in node12_osds:
                out = bluestore_obj.block_device_expand(osd_id=osd_id)
                log.info(out)
                assert "device size" in out and "Expanding" in out
            log.info(f"OSDs {node12_osds} should now be 15G each")

            osd_node12_meta = rados_obj.get_daemon_metadata(
                daemon_type="osd", daemon_id=node12_osds[0]
            )
            bdev_size = bytes_to_gb(int(osd_node12_meta["bluestore_bdev_size"]))
            if not bdev_size == 15:
                log.error(
                    f"{node12_obj.hostname}'s OSD size post expansion does not match expected value of 15"
                )
                log.error(
                    f"Actual OSD {node12_osds[0]} size: {bdev_size} | Expected 15"
                )
                raise Exception(f"OSD.{node12_osds[0]} size post expansion incorrect")

            _pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.info(f"{pool_name} pool stat: {_pool_stat}")

            time.sleep(30)
            # execute max_avail check across the cluster
            if not rados_obj.verify_max_avail():
                log.error("MAX_AVAIL deviates on the cluster more than expected")
                raise Exception("MAX_AVAIL deviates on the cluster more than expected")
            log.info("MAX_AVAIL on the cluster are as per expectation")

            # write data to the pool and expand OSD LVMs on backup node13
            assert rados_obj.bench_write(pool_name=pool_name, rados_write_duration=300)

            _pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.info(f"{pool_name} pool stat: {_pool_stat}")

            time.sleep(30)
            # execute max_avail check across the cluster
            if not rados_obj.verify_max_avail():
                log.error("MAX_AVAIL deviates on the cluster more than expected")
                raise Exception("MAX_AVAIL deviates on the cluster more than expected")
            log.info("MAX_AVAIL on the cluster are as per expectation")

            # expand the OSD LVMs on backup node13
            for lvm in lvm_list["node13"]:
                _cmd = f"lvextend -L 13G {lvm}"
                node13_obj.exec_command(cmd=_cmd, sudo=True)

            log.info("LVM size on node13 increased from 10G to 13G")
            node13_osds = rados_obj.collect_osd_daemon_ids(osd_node=node13_obj)

            # for each OSD on node 12, execute bluefs-bdev-expand
            for osd_id in node13_osds:
                out = bluestore_obj.block_device_expand(osd_id=osd_id)
                log.info(out)
                assert "device size" in out and "Expanding" in out

            log.info(f"OSDs {node13_osds} should now be 13G each")
            osd_node13_meta = rados_obj.get_daemon_metadata(
                daemon_type="osd", daemon_id=node13_osds[0]
            )
            bdev_size = bytes_to_gb(int(osd_node13_meta["bluestore_bdev_size"]))
            if not bdev_size == 13:
                log.error(
                    f"{node13_obj.hostname}'s OSD size post expansion does not match expected value of 13"
                )
                log.error(
                    f"Actual OSD {node13_osds[0]} size: {bdev_size} | Expected 13"
                )
                raise Exception(f"OSD.{node13_osds[0]} size post expansion incorrect")

            _pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.info(f"{pool_name} pool stat: {_pool_stat}")

            time.sleep(30)
            # execute max_avail check across the cluster
            if not rados_obj.verify_max_avail():
                log.error("MAX_AVAIL deviates on the cluster more than expected")
                raise Exception("MAX_AVAIL deviates on the cluster more than expected")
            log.info("MAX_AVAIL on the cluster are as per expectation")

        except AssertionError as AE:
            log.error(f"Failed with exception: {AE.__doc__}")
            log.exception(AE)
            return 1
        finally:
            log.info("\n ************* Executing finally block **********\n")
            rados_obj.delete_pool(pool=pool_name)
            # remove backup hosts
            if "node12_obj" in locals() or "node12_obj" in globals():
                service_obj.remove_custom_host(host_node_name=node12_obj.hostname)
            if "node13_obj" in locals() or "node13_obj" in globals():
                service_obj.remove_custom_host(host_node_name=node13_obj.hostname)
            if not wait_for_clean_pg_sets(rados_obj, timeout=900):
                log.error("Cluster cloud not reach active+clean state")
                return 1
            rados_obj.change_recovery_threads(config=config, action="rm")

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(
            "ceph df MAX AVAIL stats verification upon expansion of OSD size"
            "completed successfully"
        )
        return 0
