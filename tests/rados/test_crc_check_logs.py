"""
The file contains the script to check the "Full-object read crc" error in the OSD logs while reading the same object
with the different sizes
"""

import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83612766 - Verification of read crc error messages in the OSD logs
    Bug Id: https://bugzilla.redhat.com/show_bug.cgi?id=2253735
    1. Create replicated and ec pool with a single PG
    2. set the debug_osd to 20
    3. Put the object into the pool with the different sizes
    4. Get the object after write the file with different sizes
    5. Get the  acting set of the PG
    6. Check the log contain "read crc" error message
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    client = ceph_cluster.get_nodes(role="client")[0]
    pool_obj = PoolFunctions(node=cephadm)
    mon_object = MonConfigMethods(rados_obj=rados_object)

    if not rados_object.enable_file_logging():
        log.error("Error while setting config to enable logging into file")
        return 1

    pool_names = []
    start_time = get_cluster_timestamp(rados_object.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        if config.get("create_pools"):
            pools = config.get("create_pools")
            for each_pool in pools:
                cr_pool = each_pool["create_pool"]
                pool_names.append(cr_pool["pool_name"])
                if cr_pool.get("pool_type") == "erasure":
                    method_should_succeed(
                        rados_object.create_erasure_pool,
                        name=cr_pool["pool_name"],
                        **cr_pool,
                    )
                else:
                    method_should_succeed(rados_object.create_pool, **cr_pool)
        mon_object.set_config(section="osd", name="debug_osd", value="20/20")
        cmd_create_zerokb_obj = "dd if=/dev/zero bs=1k count=0 > /tmp/ZeroKb_file.txt"
        client.exec_command(cmd=cmd_create_zerokb_obj)
        cmd_create_100Kb_obj = "dd if=/dev/zero bs=1k count=100 > /tmp/100Kb_file.txt"
        client.exec_command(cmd=cmd_create_100Kb_obj)
        cmd_create_120mb_obj = "dd if=/dev/zero bs=1M count=120 > /tmp/120MB_file.txt"
        client.exec_command(cmd=cmd_create_120mb_obj)

        init_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()
        for pool_name in pool_names:
            msg_start_pool_test = f"Performing the tests on the--- {pool_name} pool"
            log.info(msg_start_pool_test)
            put_object_different_size(
                pool_name=pool_name, file_input="/tmp/ZeroKb_file.txt", client=client
            )
            log.info("Writing the Object with zero size file")
            get_object_different_size(
                pool_name=pool_name, file_output="/tmp/zero_output.txt", client=client
            )
            log.info("Reading the Object that contains the zero size file")
            put_object_different_size(
                pool_name=pool_name, file_input="/tmp/100Kb_file.txt", client=client
            )
            log.info("Writing the same object with 100kb  file size")
            get_object_different_size(
                pool_name=pool_name, file_output="/tmp/100Kb_output.txt", client=client
            )
            log.info(
                "Reading the same object Object that contains the  100kb file size"
            )
            put_object_different_size(
                pool_name=pool_name, file_input="/tmp/120MB_file.txt", client=client
            )
            log.info("Writing the same object with 120mb  file size")
            get_object_different_size(
                pool_name=pool_name, file_output="/tmp/120MB_output.txt", client=client
            )
            log.info(
                "Reading the same object Object that contains the  120mb file size"
            )
            msg_end_pool_test = (
                f"Writing and reading completed on the--- {pool_name} pool"
            )
            log.info(msg_end_pool_test)

        # A deliberate pause was implemented for log generation
        time.sleep(20)
        end_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()

        for pool_name in pool_names:
            pool_id = pool_obj.get_pool_id(pool_name=pool_name)
            pg_id = f"{pool_id}.0"
            msg_pool_pgid = f"The {pool_name} pool pg id is -{pg_id}"
            log.info(msg_pool_pgid)
            if not check_read_crc_log(rados_object, init_time, end_time, pg_id):
                log.error(
                    "The read crc error line  exists while reading the same object with different size"
                )
                return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("============Execution of finally block==================")
        mon_object.remove_config(section="osd", name="debug_osd")
        rados_object.rados_pool_cleanup()
        tmp_files = [
            "/tmp/ZeroKb_file.txt",
            "/tmp/zero_output.txt",
            "/tmp/100Kb_file.txt",
            "/tmp/100Kb_output.txt",
            "/tmp/120MB_file.txt",
            "/tmp/100Kb_output.txt",
        ]
        log.info("Removing the test files from tmp directory")
        for file in tmp_files:
            cmd_rm_file = f"rm -f {file}"
            client.exec_command(cmd=cmd_rm_file)
        time.sleep(10)
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_object.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_object.check_crash_status(
            start_time=start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def put_object_different_size(pool_name, file_input, client):
    """
    Method is used to put the object in the pool
    Args:
        pool_name: pool name
        file_input: path to the local file  to upload
        client: client object
    Return:
           None
    """
    cmd_put_obj = f"rados -p {pool_name} put obj-1 {file_input}"
    client.exec_command(cmd=cmd_put_obj)


def get_object_different_size(pool_name, file_output, client):
    """
    Method is used to get the object from the pool
    Args:
        pool_name: pool name
        file_input: path to the local file  to upload
        client: client object
    Return:
           None
    """

    cmd_get_obj = f"rados -p {pool_name} get obj-1 {file_output}"
    client.exec_command(cmd=cmd_get_obj)


def check_read_crc_log(rados_object, init_time, end_time, pgid):
    """
    Method to check the read crc error line in the osd logs
    Args:
        rados_object: Rados object
        init_time:  initial time
        end_time:  End time
        pgid: pg id

    Returns: True -> If error lines not present in the logs
             False -> If error line exists in the logs
    """
    log_line = "full-object read crc"
    msg_log_line = f"The log line to check - {log_line}"
    log.info(msg_log_line)
    acting_set = rados_object.get_pg_acting_set(pg_num=pgid)
    msg_acting_set = f"The acting osd set of the pg {pgid} is {acting_set}"
    log.info(msg_acting_set)
    fsid = rados_object.run_ceph_command(cmd="ceph fsid")["fsid"]
    for osd_id in acting_set:
        host = rados_object.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        cmd_get_log_lines = (
            f'awk \'$1 >= "{init_time}" && $1 <= "{end_time}"\' '
            f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log | grep '{log_line}'"
        )
        out, _, exit_code, _ = host.exec_command(
            sudo=True, cmd=cmd_get_log_lines, verbose=True
        )
        log_info_msg = f"exit code is {exit_code}"
        log.info(log_info_msg)
        # if the string "full-object read crc" is found
        # exit code of grep is 0
        # if the "full-object read crc" is not found
        # exit code of grep is 1
        if exit_code == 0:
            msg_err_msg = (
                f" Found the error lines on OSD : {osd_id}"
                f"\n--------output---------"
                f"\n{out}"
            )
            log.error(msg_err_msg)
            return False
    log.info(
        "The read crc error line not generated while reading the same object with different size "
    )
    return True
