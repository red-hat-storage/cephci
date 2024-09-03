"""
Program is for testing the scrub enhancement feature.As part of feature testing creating
corrupted snapshot object.Performing scrub/deep-scrub to replicate the correct SNA_ object
"""

import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.rados_test_util import create_pools
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verification of the scrub enhancement feature
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    global rados_obj, acting_osd_node, config, scrub_obj
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    scrub_obj = RadosScrubber(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    mount_point = "~/automation_dir"
    test_scenarios = ["scrub", "deep-scrub"]
    try:
        log.info("Running scrub enhancement feature testing")
        for test_input in test_scenarios:
            log.info(f"The feature is testing with the {test_input}")
            log.info("Creating the pool with the single PG")
            create_pools(config, rados_obj, client_node)
            log.info("Pool creation completed")
            oname = "UNIQUEOBJECT{i}".format(i=random.randint(0, 10000))
            pool_name = config["create_pools"][0]["create_pool"]["pool_name"]
            log.info("Setting the autoscale mode off")
            rados_obj.set_pool_property(
                pool=pool_name, props="pg_autoscale_mode", value="off"
            )
            log.info(f"The {pool_name} autoscale mode is off")
            log.info(f"creating the object inside the {pool_name}")
            pool_obj.do_rados_put(client=client_node, pool=pool_name, obj_name=oname)
            log.info(f"A object created inside the {pool_name}")
            snapcmd = f"sudo rados mksnap -p {pool_name} snap1"
            client_node.exec_command(cmd=snapcmd, sudo=True)
            log.info("Creating the SNA_ entry by creating object")
            pool_obj.do_rados_put(client=client_node, pool=pool_name, obj_name=oname)
            log.info(f"A snapshot object is created for {pool_name}")
            osd_map_output = rados_obj.get_osd_map(pool=pool_name, obj=oname)
            primary_osd = osd_map_output["acting_primary"]
            log.info(f"The object stored in the primary osd number-{primary_osd}")
            pg_id = osd_map_output["pgid"]
            # setting the noout flag
            scrub_obj.set_osd_flags("set", "noout")
            # Stopping OSD
            if not rados_obj.change_osd_state(action="stop", target=primary_osd):
                log.error(f"Unable to stop the OSD : {primary_osd}")
                raise Exception("Execution error")
            acting_osd_node = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=primary_osd
            )

            cmd_base = f"cephadm shell --name osd.{primary_osd} --"
            cmd_get_snaObj = (
                f"{cmd_base} ceph-kvstore-tool bluestore-kv  /var/lib/ceph/osd/ceph-"
                f"{primary_osd} list p | grep SNA_"
            )
            cmd_no_obj = (
                f"{cmd_base} ceph-kvstore-tool bluestore-kv  /var/lib/ceph/osd/ceph-{primary_osd} "
                f"list p | grep SNA_ | wc -l"
            )
            # Retiving the snapshot object
            sna_obj = acting_osd_node.exec_command(sudo=True, cmd=cmd_get_snaObj)[
                0
            ].strip()
            sna_key_obj_list = sna_obj.split("\t")

            corrupt_key = sna_obj.split(".")[1].strip() + "CORRUPTED"
            correct_key = sna_key_obj_list[1].strip()
            # Creating mount directory in the acting osd to create the object output
            cmd_create_dir = f"mkdir -p {mount_point}"
            acting_osd_node.exec_command(sudo=True, cmd=cmd_create_dir)
            # The Logic is, storing the object with the correct key in a temporary location.
            # Deleting the object and creating the object by using the temporary location with
            # the corrupted SNA_ key entry.
            cmd_str_obj = (
                f"{cmd_base}mount {mount_point}/ -- ceph-kvstore-tool bluestore-kv  /var/lib/ceph/osd/"
                f"ceph-{primary_osd} get p  {correct_key} out /mnt/output"
            )
            # Removing the snapshot object
            acting_osd_node.exec_command(sudo=True, cmd=cmd_str_obj)
            log.info(f"Removing the object in the osd {primary_osd}")
            cmd_rm_obj = (
                f"{cmd_base} ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-{primary_osd} rm p "
                f"{correct_key}"
            )
            acting_osd_node.exec_command(sudo=True, cmd=cmd_rm_obj)
            # Checking the snapshot object is removed
            no_obj_tup = acting_osd_node.exec_command(sudo=True, cmd=cmd_no_obj)
            assert int(no_obj_tup[0].strip()) == 0
            log.info("The snapshot object is deleted")

            # inserting the corrupted snapshot object
            log.info("Creating the corrupted SNA_ entry")
            cmd_insert_obj = (
                f"{cmd_base}mount {mount_point}/ -- ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/"
                f"ceph-{primary_osd} set p {corrupt_key} in /mnt/output"
            )
            acting_osd_node.exec_command(sudo=True, cmd=cmd_insert_obj)
            time.sleep(5)
            no_obj_tup = acting_osd_node.exec_command(sudo=True, cmd=cmd_no_obj)
            assert int(no_obj_tup[0].strip()) == 1
            log.info("The corrupted snapshot object is inserted")
            # Unsetting the noout flag
            scrub_obj.set_osd_flags("unset", "noout")
            if not rados_obj.change_osd_state(action="start", target=primary_osd):
                log.error(f"Unable to start the OSD : {primary_osd}")
                raise Exception("Execution error")

            # Wait until pg active+clean state
            while True:
                pg_report = rados_obj.check_pg_state(pgid=pg_id)
                if "active+clean" in pg_report:
                    break
                log.info(f"The pg-{pg_id} is not in active+clean state")
                time.sleep(10)
            log.info(f"The pg-{pg_id} is in active+clean state")
            # scrubbing/deep scrubbing the pool
            scrub_cmd = f"ceph osd pool {test_input}  {pool_name}"
            old_scrub_time = get_pgid_timestamp(pg_id, test_input)
            rados_obj.run_ceph_command(scrub_cmd)
            time_end = time.time() + 60 * 30

            while time.time() < time_end:
                log.info(f"{test_input} is in progress")

                new_scrub_time = get_pgid_timestamp(pg_id, test_input)
                if old_scrub_time != new_scrub_time:
                    log.info(f"{test_input} is completed")
                    break
                time.sleep(10)

            # setting the noout flag
            scrub_obj.set_osd_flags("set", "noout")
            if not rados_obj.change_osd_state(action="stop", target=primary_osd):
                log.error(f"Unable to stop the OSD : {primary_osd}")
                raise Exception("Execution error")

            cmd_no_correct_obj = (
                f"{cmd_base} ceph-kvstore-tool bluestore-kv  /var/lib/ceph/osd/ceph-{primary_osd} "
                f"list p | grep {correct_key} | wc -l"
            )
            log.info(f"Checking the object is replicated after {test_input}")
            no_obj_tup = acting_osd_node.exec_command(sudo=True, cmd=cmd_no_correct_obj)
            if int(no_obj_tup[0].strip()) == 0:
                log.error(f"The snapshot object is not replicated after {test_input}")
                return 1
            log.info(f"The snapshot object is  replicated after {test_input}")

            # Unsetting the noout flag
            scrub_obj.set_osd_flags("unset", "noout")
            if not rados_obj.change_osd_state(action="start", target=primary_osd):
                log.error(f"Unable to start the OSD : {primary_osd}")
                raise Exception("Execution error")
            node_cleanup(mount_point, primary_osd)
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info("Execution of finally block")
        node_cleanup(mount_point, primary_osd)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def get_pgid_timestamp(pgid, scrub_opr):
    cmd_dump_json = "ceph pg dump_json pgs"
    osd_map_output = rados_obj.run_ceph_command(cmd_dump_json)
    for iterator in osd_map_output["pg_map"]["pg_stats"]:
        if iterator["pgid"] == pgid:
            if scrub_opr == "scrub":
                return iterator["last_scrub_stamp"]
            return iterator["last_deep_scrub_stamp"]


def node_cleanup(rm_dir, primary_osd):
    scrub_obj.set_osd_flags("unset", "noout")
    rados_obj.change_osd_state(action="start", target=primary_osd)
    if config.get("delete_pools"):
        for name in config["delete_pools"]:
            method_should_succeed(rados_obj.delete_pool, name)
        log.info("deleted all the given pools successfully")
    cmd_rmdir = f"rm -rf {rm_dir}"
    acting_osd_node.exec_command(sudo=True, cmd=cmd_rmdir)
    log.info("deleted mount directory successfully")
