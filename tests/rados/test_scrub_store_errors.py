"""
This module is to verify the scrub store feature by generating the scrub and deep-scrub errors
"""

import ast
import json
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.rados_scrub import RadosScrubber
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Polarion#CEPH-83620444 - Verification of the scrub store by generating the scrb and deep-scrub errors
        1. Create an object in the pool
        2. Modify the object to simulate various error conditions
            2.1  Modify the object by using the set-bytes
        3. Verify the detected scrub and deep-scrub errors
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)
    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]
    scrub_errors = ""
    combined_errors_list = ""

    scrub_error_list = ["size_mismatch"]
    deep_scrub_error_list = ["omap_digest_mismatch", "data_digest_mismatch"]

    client_node = ceph_cluster.get_nodes(role="client")[0]

    try:
        if not rados_object.create_pool(**replicated_config):
            log.error("Failed to create the replicated Pool")
            return False
        msg_pool_name = f"The {pool_name} is created"

        log.info(msg_pool_name)
        log.info("Creating the object")
        obj_name = "scrubObj"

        create_object(pool_name, client_node, obj_name)

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
        obj_str = objectstore_obj.list_objects(
            osd_id=primary_osd, pgid=pg_id, obj_name=obj_name
        ).strip()

        json_data = json.dumps(ast.literal_eval(obj_str)[1])

        # Create corrupt data file
        log.info("Create the corrupted file")
        cmd_corrupt_file = "echo 'This is corrupted data' > /tmp/corrupt"
        out, _ = osd_host.exec_command(sudo=True, cmd=cmd_corrupt_file)
        objectstore_obj.set_bytes(
            osd_id=primary_osd, obj=json_data, pgid=pg_id, in_file="/tmp/corrupt"
        )
        wait_time = 900
        try:
            rados_object.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=True, wait_time=wait_time
            )
            log.info("The user initiated scrub is completed")
        except Exception:
            log.info("The user initiated scrub operation not started  ")
            return 1

        object_list = rados_object.get_inconsistent_object_details(pg_id)
        for obj in object_list.get("inconsistents", []):
            if obj["object"]["name"] == obj_name:
                log.info(f"Inconsistent object {scrub_object} exists in the list.")
                log.info(
                    f"Checking error messages of inconsistent object {scrub_object}"
                )
                scrub_errors = obj["errors"]
                break

        for error in scrub_errors:
            if error not in scrub_error_list:
                msg_error = f"The error {error} not exists in the scrub list"
                log.error(msg_error)
                return 1

        try:
            rados_object.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=True, wait_time=wait_time
            )
            log.info("The user initiated deep scrub is completed")
        except Exception:
            log.info("The user initiated deep scrub operation not started  ")
            return 1

        object_list = rados_object.get_inconsistent_object_details(pg_id)

        for obj in object_list.get("inconsistents", []):
            if obj["object"]["name"] == obj_name:
                log.info(f"Inconsistent object {scrub_object} exists in the list.")
                log.info(
                    f"Checking error messages of inconsistent object {scrub_object}"
                )
                combined_errors_list = obj["errors"]
                break
        deep_scrub_error = list(set(scrub_error_list) - set(combined_errors_list))
        for error in deep_scrub_error:
            if error not in deep_scrub_error_list:
                msg_error = f"The error {error} not exists in the deep scrub list"
                log.error(msg_error)
                return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        method_should_succeed(rados_object.delete_pool, pool_name)
    log.info(
        "========== Validation of the scrub store errors are completed  ============="
    )
    return 0


def create_object(pool_name, client_node, obj_name):

    org_file_create = "echo 'The actual message' > /tmp/original"
    client_node.exec_command(cmd=org_file_create, sudo=True)
    # Creating object
    cmd_put_obj = f"rados --pool {pool_name} put {obj_name} /tmp/original"
    client_node.exec_command(cmd=cmd_put_obj, sudo=True)

    cmd_set_omapheader = (
        f"rados --pool {pool_name} setomapheader {obj_name} hdr-{obj_name}"
    )
    client_node.exec_command(cmd=cmd_set_omapheader, sudo=True)
    cmd_set_omapval = (
        f"rados --pool {pool_name} setomapval {obj_name} key-{obj_name} val-{obj_name}"
    )
    client_node.exec_command(cmd=cmd_set_omapval, sudo=True)
