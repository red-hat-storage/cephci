"""
Module to Create, test, re-balance objects and delete pool where each object has large number of KW pairs attached,
thereby increasing the OMAP entries generated on the pool.
Verifying the Bug#2249003 - Observing client.admin crash in thread_name 'rados' on executing 'rados clearomap'
for a rados pool different than the one where the object is present.
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create a large number of omap entries on the single PG pool and test osd resiliency
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    omap_target_configs = config["omap_config"]
    if config.get("crash_config"):
        crash_pool_name = config["crash_config"]["pool_name"]
    else:
        crash_pool_name = "test_crash_pool"
    if not rados_obj.create_pool(pool_name=crash_pool_name):
        log.error(f"Failed to create pool-{crash_pool_name}")
        return 1
    # put the object
    object_name = "obj1"
    cmd_put_obj = f"rados put -p {crash_pool_name} {object_name} /etc/hosts"
    client_node.exec_command(cmd=cmd_put_obj, sudo=True)
    cmd_clearomap = f"rados clearomap -p {crash_pool_name} {object_name}"
    pools = []

    try:
        # Creating pools and starting the test
        for omap_config_key in omap_target_configs.keys():
            omap_config = omap_target_configs[omap_config_key]
            log.debug(
                f"Creating replicated pool on the cluster with name {omap_config['pool_name']}"
            )
            # omap entries cannot exist on EC pools. Removing the code for EC pools,
            # And testing only on RE pools.
            method_should_succeed(rados_obj.create_pool, **omap_config)
            pool_name = omap_config.pop("pool_name")
            pools.append(pool_name)
            normal_objs = omap_config["normal_objs"]
            if normal_objs > 0:
                # create n number of objects without any omap entry
                rados_obj.bench_write(
                    pool_name=pool_name,
                    **{
                        "rados_write_duration": 500,
                        "byte_size": "4096KB",
                        "max_objs": normal_objs,
                    },
                )
            # calculate objects to be written with omaps and begin omap creation process
            omap_obj_num = omap_config["obj_end"] - omap_config["obj_start"]
            log.debug(
                f"Created the pool. beginning to create omap entries on the pool. Count : {omap_obj_num}"
            )
            if not pool_obj.fill_omap_entries(pool_name=pool_name, **omap_config):
                log.error(f"Omap entries not generated on pool {pool_name}")
                raise Exception(f"Omap entries not generated on pool {pool_name}")

            assert pool_obj.check_large_omap_warning(
                pool=pool_name,
                obj_num=omap_obj_num,
                check=omap_config["large_warn"],
            )

            if omap_config["large_warn"]:
                out, _ = cephadm.shell([f"rados ls -p {pool_name} | grep 'omap_obj'"])
                omap_obj_list = out.split()
                omap_obj = random.choice(omap_obj_list)
                # Fetching the current acting set for the pool
                acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                rados_obj.change_recovery_threads(config={}, action="set")
                log.debug(
                    f"Proceeding to restart OSDs from the acting set {acting_set}"
                )
                for osd_id in acting_set:
                    rados_obj.change_osd_state(action="stop", target=osd_id)
                    # sleeping for 5 seconds for re-balancing to begin
                    time.sleep(5)

                    # Waiting for cluster to get clean state after OSD stopped
                    if not wait_for_clean_pg_sets(rados_obj, test_pool=pool_name):
                        log.error("PG's in cluster are not active + Clean state.. ")
                        raise Exception(
                            "PG's in cluster are not active + Clean state.. "
                        )
                    rados_obj.change_osd_state(action="restart", target=osd_id)
                    log.debug(
                        f"Cluster reached clean state after osd {osd_id} stop and restart"
                    )
                log.info(
                    f"All the OSD's from the acting set {acting_set} were restarted "
                    f"and object movement completed for pool {pool_name}"
                )
        # Verification of the Bug#2249003
        try:
            log.info(f"The omap object name is -{omap_obj}")
            client_node.exec_command(cmd=cmd_clearomap, sudo=True)
            cmd_clearomap = f"rados clearomap -p {crash_pool_name} {omap_obj}"
            client_node.exec_command(cmd=cmd_clearomap, sudo=True)
        except Exception as e:
            log.info(f"Exception hit to execute but this is expected; {e}")
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                "Crashes seen on cluster after executing the clear omap command  on a non-omap pool"
            )
            raise Exception("Service crash on cluster error")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.change_recovery_threads(config={}, action="rm")
        # deleting the pool created after the test
        for pool in pools:
            rados_obj.delete_pool(pool=pool)
        rados_obj.delete_pool(pool=crash_pool_name)

        time.sleep(60)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed testing effects of large number of omap entries on pools ")
    return 0
