import os
import traceback

from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    This script is a wrapper to Crash configuration and Crash check and upload crash files.
    It can be included prior to test case execution to configure crash and post testcase execution
    to collect crash files,
    PRETEST: To configure crash
    -----------------------
    -
    test:
      abort-on-fail: false
      desc: "Setup Crash configuration"
      module: cephfs_crash_util.py
      name: cephfs-crash-setup
      config:
       crash_setup : 1
       daemon_list : ['mds','osd','mgr','mon']
    POSTTEST: To check for crash and upload crash files to logdir
    -------------------------
    -
    test:
      abort-on-fail: false
      desc: "Check for Crash"
      module: cephfs_crash_util.py
      name: cephfs-crash-check
      config:
       crash_check : 1
       daemon_list : ['mds','osd','mgr','mon']

    This script will read input params crash_setup, crash_check and invoke corresponding
    crash module in cephfs_system_utils to perform the task.
    """
    try:
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        client = clients[1]
        log.info("checking Pre-requisites")

        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        daemon_list = config.get("daemon_list", ["mds"])
        crash_setup = config.get("crash_setup", 0)
        crash_check = config.get("crash_check", 0)
        crash_copy = config.get("crash_copy", 1)
        log_str = (
            f"Test Params : Crash Setup : {crash_setup}, Crash check:{crash_check}"
        )
        log_str += f", daemon_list : {daemon_list}"
        log.info(log_str)
        if crash_setup == 1:
            log.info(f"Setup Crash configuration for : {daemon_list}")
            fs_system_utils.crash_setup(client, daemon_list=daemon_list)

        if crash_check == 1:
            log_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
            log.info(f"log path:{log_dir}")
            log.info(f"Check for crash from : {daemon_list}")
            fs_system_utils.crash_check(
                client, crash_copy=crash_copy, daemon_list=daemon_list
            )
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
