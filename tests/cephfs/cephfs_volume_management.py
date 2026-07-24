import traceback
from datetime import datetime, timedelta
from time import sleep

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils
from utility.log import Log

log = Log(__name__)


def wait_for_process(client, process_name, timeout=180, interval=5, ispresent=True):
    """
    Checks for the proccess and returns the status based on ispresent
    :param client:
    :param process_name:
    :param timeout:
    :param interval:
    :param ispresent:
    :return:
    """
    end_time = datetime.now() + timedelta(seconds=timeout)
    log.info("Wait for the process to start or stop")
    while end_time > datetime.now():
        out, _ = client.exec_command(
            sudo=True, cmd=f"ceph orch ps | grep {process_name}", check_ec=False
        )
        log.debug("Running Process: {}".format(out))
        if client.node.exit_status == 0 and ispresent:
            return True
        if client.node.exit_status == 1 and not ispresent:
            return True
        sleep(interval)
    return False


def run(ceph_cluster, **kw):
    """
    Create a volume using:
    -- This creates a CephFS file system and its data and metadata pools.
    It can also try to create MDSes for the filesystem using the enabled ceph-mgr orchestrator module.

    Remove a volume using:
    -- This removes a file system and its data and metadata pool.
    It also tries to remove MDSes using the enabled ceph-mgr orchestrator module.

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1 = client_info["fuse_clients"][0]
        results = []
        tc1 = "83573446"
        log.info(f"Execution of testcase {tc1} started")
        log.info("Create and list a volume")
        commands = [
            "ceph fs volume create cephfs_new",
            "ceph fs ls | grep cephfs_new",
            "ceph osd lspools | grep cephfs.cephfs_new",
            "ceph fs volume ls | grep cephfs_new",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")
        wait_for_process(client1, "cephfs_new")
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            "ceph fs volume rm cephfs_new --yes-i-really-mean-it",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")

        verifyremove_command = [
            "ceph fs ls | grep cephfs_new",
            "ceph osd lspools | grep cephfs.cephfs_new",
        ]
        for command in verifyremove_command:
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
            if client1.node.exit_status == 1:
                results.append(f"{command} successfully executed")
        wait_for_process(client1, "cephfs_new", ispresent=False)
        log.info(f"Execution of testcase {tc1} ended")
        log.info("Testcase Results:")
        for res in results:
            log.info(res)
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
