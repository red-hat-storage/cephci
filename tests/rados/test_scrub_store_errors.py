"""
This module is to verify the scrub store feature by generating the scrub and deep-scrub errors
"""

import ast
import datetime
import json
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

scrub_error_list = ["size_mismatch"]
deep_scrub_error_list = [
    "size_mismatch",
    "omap_digest_mismatch",
    "data_digest_mismatch",
]


def run(ceph_cluster, **kw):
    """
    Polarion#CEPH-83620444 - Verification of the scrub store by generating the scrb and deep-scrub errors
        Perform the following tests on the replicated pool.
            1. Create an object in the pool
            2. Modify the object to simulate various error conditions
                Case1- Modify the object by using the set-bytes
                Case2- Corrupt the omap key
                Case3 - Remove omap
                Case4- Add additional key to the object
                Case5- Modification of omap header
            3. Verify the detected scrub and deep-scrub errors generated
        Perform the following tests on the EC pool
            1. Create  an object on ec pool
            2. Modify the object to simulate various error conditions
                Case1- Modify the object by using the set-bytes
            3. Verify the detected scrub and deep-scrub errors generated

    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)
    pool_name = config["pool_name"]
    rep_total_cases = [
        "set-bytes",
        "omap_key_corrupt",
        "remove_omap",
        "add_additional_key",
        "set_omap_header",
    ]

    ec_total_cases = [
        "set-bytes",
    ]

    osd_host = ""
    client_node = ceph_cluster.get_nodes(role="client")[0]
    sample_file_name = "/tmp/sample"
    try:
        if config["is_ecpool"]:
            method_should_succeed(
                rados_object.create_erasure_pool,
                name=pool_name,
                **config,
            )
            total_cases = ec_total_cases
        else:
            method_should_succeed(
                rados_object.create_pool,
                name=pool_name,
                **config,
            )
            total_cases = rep_total_cases

        msg_pool_name = f"The {pool_name} is created"
        log.info(msg_pool_name)

        log.info("Setting noscrub and nodeep-scrub flags to the pool")
        if not set_and_unset_scrub_flags(rados_object, pool_name, set_flag=1):
            log.error("Failed to set the noscrub and nodeep-scrub flags")
            return 1

        log.info("Creating the object")
        obj_name = "scrubObj"
        if not create_object(pool_name, client_node, obj_name, config["is_ecpool"]):
            log.error("Failed to create the object")
            return 1
        # Get the acting PG set
        acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
        primary_osd = acting_pg_set[0]
        msg_acting_set = f"Acting set for {pool_name}: {acting_pg_set}"
        log.info(msg_acting_set)
        msg_primary_osd = f"Primary OSD for the pool {pool_name}: {primary_osd}"
        log.info(msg_primary_osd)

        # Get the pool pg_id
        pg_id = rados_object.get_pgid(pool_name=pool_name)
        pg_id = pg_id[0]
        msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
        log.info(msg_pool_id)

        osd_host = rados_object.fetch_host_node(
            daemon_type="osd", daemon_id=primary_osd
        )

        # Create corrupt data file
        log.info("Create the sample file")
        cmd_corrupt_file = f"echo 'This is sample data' > {sample_file_name}"
        out, _ = osd_host.exec_command(sudo=True, cmd=cmd_corrupt_file)

        for value in total_cases:
            only_deep_scrub = False
            method_should_succeed(
                wait_for_clean_pg_sets, rados_object, test_pool=pool_name, timeout=1800
            )

            obj_str = objectstore_obj.list_objects(
                osd_id=primary_osd, pgid=pg_id, obj_name=obj_name
            ).strip()
            obj_pg_id = ast.literal_eval(obj_str)[0]
            json_data = json.dumps(ast.literal_eval(obj_str)[1])

            if value == "set-bytes":
                log.info(
                    "==========1.Verification of scrub and deep-scrub messages after modifying the object "
                    "bytes============"
                )
                objectstore_obj.set_bytes(
                    osd_id=primary_osd,
                    obj=json_data,
                    pgid=obj_pg_id,
                    in_file=sample_file_name,
                )
            elif value == "omap_key_corrupt":
                log.info(
                    "==========2.Verification of deep-scrub messages after omap key corruption ============"
                )
                omap_key = f"key-{obj_name}"
                objectstore_obj.set_omap(
                    osd_id=primary_osd,
                    obj=json_data,
                    pgid=obj_pg_id,
                    key=omap_key,
                    in_file=sample_file_name,
                )
                only_deep_scrub = True
            elif value == "remove_omap":
                log.info(
                    "==========3.Verification of deep-scrub messages after removing omap key ============"
                )
                omap_key = f"key-{obj_name}"
                objectstore_obj.remove_omap(
                    osd_id=primary_osd, pgid=pg_id, obj=json_data, key=omap_key
                )
                only_deep_scrub = True
            elif value == "add_additional_key":
                log.info(
                    "==========4.Verification of deep-scrub messages after addiing additional omap key to "
                    "the object ============"
                )
                omap_key = f"key2-{obj_name}"
                objectstore_obj.set_omap(
                    osd_id=primary_osd,
                    obj=json_data,
                    pgid=obj_pg_id,
                    key=omap_key,
                    in_file=sample_file_name,
                )
                only_deep_scrub = True
            elif value == "set_omap_header":
                log.info(
                    "==========5.Verification of deep-scrub messages after modifying the omap header ============"
                )
                objectstore_obj.set_omap_header(
                    osd_id=primary_osd,
                    obj=json_data,
                    pgid=obj_pg_id,
                    in_file=sample_file_name,
                )
                only_deep_scrub = True
            log.info("===========Performing the scrub operation============")
            if not perform_scrub_operation(
                rados_object, pg_id=pg_id, operation="scrub"
            ):
                log.error("Failed to perform scrub operation")
                return 1
            time.sleep(10)

            scrub_errors = get_error_list(rados_object, pg_id, obj_name)
            if scrub_errors is None and only_deep_scrub is False:
                log.error(
                    "The scrub errors are not generated after performing the scrub operation"
                )
                return 1

            if only_deep_scrub:
                log.info(f"The scrub errors won't generate for the {value} changes")
            else:
                if not check_scrub_store_errors(scrub_errors, operation="scrub"):
                    log.error(
                        "The generated scrub error does not exist in the scrub error list"
                    )
                    return 1
                else:
                    log.info("The scrub errors are generated successfully")

            log.info("===========Performing the deep-scrub operation============")
            if not perform_scrub_operation(
                rados_object, pg_id=pg_id, operation="deep-scrub"
            ):
                log.error("Failed to perform deep-scrub operation")
                return 1
            time.sleep(10)
            combined_errors_list = get_error_list(rados_object, pg_id, obj_name)
            if combined_errors_list is None:
                log.error(
                    "The scrub errors are not generated after performing the scrub operation"
                )
                return 1
            deep_scrub_error = (
                list(set(scrub_error_list) - set(combined_errors_list)) or None
            )

            if deep_scrub_error is not None:
                if not check_scrub_store_errors(
                    deep_scrub_error, operation="deep-scrub"
                ):
                    log.error(
                        "The generated scrub error not exists in the deep-scrub error list"
                    )
                    return 1

            cmd_pg_repair = f"ceph pg repair {pg_id}"
            rados_object.run_ceph_command(cmd=cmd_pg_repair)
            time.sleep(5)
            endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
            while datetime.datetime.now() < endtime:
                obj_list = rados_object.get_inconsistent_object_details(pg_id)
                if not obj_list.get("inconsistents"):
                    log.info("The inconsistent object list is empty. PG is repaired.")
                    break
                log.info("Waiting for the PG to get repaired...")
                time.sleep(10)
            else:
                log.error("The PG is not repaired. Not executing further tests.")
                return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        log.info("Deleting created tmp file")
        delete_file = f"rm -f {sample_file_name}"
        out, _ = osd_host.exec_command(sudo=True, cmd=delete_file)
        method_should_succeed(rados_object.delete_pool, pool_name)
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info(
        "========== Validation of the scrub store errors are completed  ============="
    )
    return 0


def create_object(pool_name, client_node, obj_name, ec_pool_flag):
    """
    Method is used to create the object
    Parameters:
         pool_name: pool name
         client_node: client node object
         rep_pool_flag: Is pool is replicated pool or not
         obj_name: object name
    return: True -> Object created successfully
             False -> Object not created successfully
    """
    try:
        org_file_create = "echo 'The actual message' > /tmp/original"
        client_node.exec_command(cmd=org_file_create, sudo=True)
        # Creating object
        msg_object = f"Creating the {obj_name} object in the pool {pool_name}"
        log.info(msg_object)
        cmd_put_obj = f"rados --pool {pool_name} put {obj_name} /tmp/original"
        client_node.exec_command(cmd=cmd_put_obj, sudo=True)

        if not ec_pool_flag:
            msg_header = f"Setting the hdr-{obj_name} header to {obj_name} object"
            log.info(msg_header)
            cmd_set_omapheader = (
                f"rados --pool {pool_name} setomapheader {obj_name} hdr-{obj_name}"
            )
            msg_omap_val = f"Setting the key-{obj_name} key and  val-{obj_name} value to the  {obj_name} object"
            log.info(msg_omap_val)
            client_node.exec_command(cmd=cmd_set_omapheader, sudo=True)
            cmd_set_omapval = f"rados --pool {pool_name} setomapval {obj_name} key-{obj_name} val-{obj_name}"
            client_node.exec_command(cmd=cmd_set_omapval, sudo=True)
    except Exception as error:
        msg_error = f"Failed to create the object.Exception is -{error.__doc__}"
        log.error(msg_error)
        return False
    return True


def perform_scrub_operation(rados_object, pg_id, operation):
    """
    Method is used to perform the scrub or deep-scrub operation
    Parameters:
        rados_object: rados object
        pg_id: pg id of pool
        operation: is it scrub or deep-scrub
    return: True -> successful completion of operation
            False -> unsuccessful of operation
    """
    wait_time = 900
    try:
        if operation == "scrub":
            rados_object.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=True, wait_time=wait_time
            )
            log.info("The user initiated scrub is completed")
        else:
            rados_object.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=True, wait_time=wait_time
            )
            log.info("The user initiated deep scrub is completed")
    except Exception:
        msg_scrub = f"The user initiated {operation} not started"
        log.error(msg_scrub)
        return False
    return True


def get_error_list(rados_object, pg_id, object_name):
    """
    Method is used to get the error list from inconsistent objects
    Parameters:
        rados_object: rados object
        pg_id: pg id of pool
        object_name: object name
     return: List of error messages or None
    """
    object_list = rados_object.get_inconsistent_object_details(pg_id)
    for obj in object_list.get("inconsistents", []):
        if obj["object"]["name"] == object_name:
            log.info("Inconsistent object exists in the list.")
            return obj["errors"]
    return None


def check_scrub_store_errors(scrub_errors, operation):
    """
    Method is used to check the error list
    scrub_errors:  List of error messages
    operation: Type of operation, either scrub or deep-scrub
    return: True -> If error message exists in the error list
            False -> If error message does not exist in the error list
    """
    if operation == "scrub":
        error_list = scrub_error_list
    else:
        error_list = deep_scrub_error_list

    msg_error_list = f"The {operation} operation error list is {error_list}"
    log.info(msg_error_list)

    for error in scrub_errors:
        msg_error_msg = f"The verification if error message is-- {error} and the list is {error_list}"
        log.info(msg_error_msg)
        if error not in error_list:
            msg_error = f"The error {error} not exists in the {error_list} list"
            log.error(msg_error)
            return False
    return True


def set_and_unset_scrub_flags(rados_object, pool_name, set_flag=0):
    """
    Method is used to set and unset scrub flags
        rados_object: Rados object
        pool_name: pool name
        set_flag:set the scrub flag
        return:True if it successfully set or unset
               False otherwise
    """
    try:
        cmd_scrub = f"ceph osd pool set {pool_name} noscrub {set_flag}"
        cmd_deep_scrub = f"ceph osd pool set {pool_name} nodeep-scrub {set_flag}"
        rados_object.run_ceph_command(cmd=cmd_scrub)
        rados_object.run_ceph_command(cmd=cmd_deep_scrub)
    except Exception as err:
        msg_error = f"Failed to set flag- {set_flag}.Exception is -{err.__doc__}"
        log.error(msg_error)
        return False
    return True
