import os
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    This script is a wrapper to Logs enablement and Logs collection module available in cephfs_uitlsV1
    It can be included prior to test case execution to enable debug logs and post testcase execution to collect logs,
    PRETEST: To enable logs
    -----------------------
    - test:
      name: Enable system debug logs
      module: cephfs_logs_util.py
      config:
       ENABLE_LOGS : 1
       daemon_dbg_level : {'mds':5}
    POSTTEST: To collect logs
    -------------------------
    - test:
      name: Collect and upload system logs
      module: cephfs_logs_util.py
      config:
       UPLOAD_LOGS : 1
       daemon_list : ['mds']
    POSTTEST: To disable logs
    -------------------------
    - test:
      name: Disable debug logs
      module: cephfs_logs_util.py
      config:
       DISABLE_LOGS : 1
       daemon_list : ['mds']

    This script will read input params ENABLE_LOGS,UPLOAD_LOGS and DISABLE_LOGS and invoke corresponding
    cephfs_utilsV1 module to perform the task. If UPLOAD_LOGS, script will print the path were logs are uploadded.

    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")

        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        results = []
        daemon_dbg_level = config.get("daemon_dbg_level", {"mds": 5})
        daemon_list = config.get("daemon_list", ["mds"])
        enable_logs = config.get("ENABLE_LOGS", 0)
        disable_logs = config.get("DISABLE_LOGS", 0)
        upload_logs = config.get("UPLOAD_LOGS", 0)
        log_str = (
            f"Test Params : ENABLE_LOGS : {enable_logs}, UPLOAD_LOGS:{upload_logs}"
        )
        log_str += (
            f", daemon_list : {daemon_list}, daemon_dbg_level : {daemon_dbg_level}"
        )
        log.info(log_str)
        if enable_logs == 1:
            log.info(f"Enabling debug logs on daemons : {daemon_dbg_level}")
            if fs_util.enable_logs(client1, daemon_dbg_level):
                assert False, "Enable logs failed"

        if upload_logs == 1:
            log_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
            log.info(f"log path:{log_dir}")
            log.info(f"upload debug logs for daemons : {daemon_list}")
            if fs_util.upload_logs(client1, log_dir, daemons=daemon_list):
                assert False, "Upload logs failed"
        if disable_logs == 1:
            log.info(f"Disabling debug logs on daemons : {daemon_list}")
            if fs_util.disable_logs(client1, daemon_list):
                assert False, "Disable logs failed"

        log.info("Testcase Results:")
        for res in results:
            log.info(res)
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
