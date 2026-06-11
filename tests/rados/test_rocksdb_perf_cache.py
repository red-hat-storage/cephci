import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)


# Exception for command success when
# crash is expected
class HeaderMissing(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    # CEPH-83632416
    Test to verify newly introduced Performance cache counters in rocksDB
    1. Choose an OSD at random
    2. Retrieve "cache-O" counter
    3. Retrieve "cache-default" counter
    4. Log perf dump
    5. Log counter dump
    6. Reset the cache-O counter
    7. Reset the cache-default counter
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    log.info(
        "Running test case to verify Bluestore rocksDB performance cache counters "
    )

    if float(rhbuild.split("-")[0]) < 9.1:
        log.info(
            "Passing test without execution, feature/fix has been merged in RHCS 9.1"
        )
        return 0
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        # choosing an OSD at random
        osd_list = rados_obj.get_osd_list(status="up")
        log.info(f"List of active OSDs on the cluster: {osd_list}")
        osd_id = random.choice(osd_list)
        log.info(f"Random OSD chosen for the test: {osd_id}")

        # list of commands to run
        cmd_list = [
            f"ceph tell osd.{osd_id} rocksdb show cache O",
            f"ceph tell osd.{osd_id} rocksdb show cache default",
            f"ceph tell osd.{osd_id} rocksdb reset cache O",
            f"ceph tell osd.{osd_id} rocksdb reset cache default",
        ]

        # cmd execution
        for _cmd in cmd_list:
            log.info("Command to be executed for OSD %s - %s" % (osd_id, _cmd))
            out, _ = rados_obj.client.exec_command(cmd=_cmd, sudo=True)
            log.debug("Output: \n%s" % out)
            if "show" in _cmd:
                for header in [
                    "shard",
                    "capacity",
                    "usage",
                    "pinned",
                    "elems",
                    "inserts",
                    "lookups",
                    "hits",
                    "misses",
                ]:
                    if header not in out:
                        err_msg = f"{header} not found in the command '{_cmd}' output"
                        log.error(err_msg)
                        raise HeaderMissing(err_msg)

        cmd_list = [
            f"ceph tell osd.{osd_id} perf dump rocksdb",
            f"ceph tell osd.{osd_id} counter dump",
        ]

        for _cmd in cmd_list:
            log.info("Command to be executed for OSD %s - %s" % (osd_id, _cmd))
            out = rados_obj.run_ceph_command(cmd=_cmd, client_exec=True)
            log.debug("Output: \n%s" % out)

        log.info(
            "OSD Bluestore rocksDB Performance Cache counters have been verified"
            f"successfully for OSD osd.{osd_id}"
        )
    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
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
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
