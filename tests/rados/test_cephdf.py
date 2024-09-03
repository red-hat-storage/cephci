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
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
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
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
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
