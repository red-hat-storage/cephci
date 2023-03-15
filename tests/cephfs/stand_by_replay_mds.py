import datetime
import traceback
from time import sleep

from ceph.ceph import CommandFailed
from utility.log import Log

log = Log(__name__)


def wait_for_stable_fs(client, standby_replay, timeout=180, interval=5):
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    log.info("Wait for the command to pass")
    while end_time > datetime.datetime.now():
        try:
            out1, rc = client.exec_command(sudo=True, cmd="ceph fs status")
            print(out1)
            out, rc = client.exec_command(
                sudo=True, cmd="ceph fs status | awk '{print $2}'"
            )
            output = out.splitlines()
            if (
                "active" in output[3]
                and "standby-replay" in output[4]
                and standby_replay == "true"
            ):
                return 0
            if "standby-replay" not in output[4] and standby_replay == "false":
                return 0
            sleep(interval)
        except Exception as e:
            log.info(e)
            log.info(traceback.format_exc())
            raise CommandFailed


def run(ceph_cluster, **kw):
    try:
        """
        CEPH-11258 - OSD node/service add and removal test, with client IO
        Pre-requisites :
        1. Create cephfs volume

        Test Case Flow:
        1. Start IOs
        2. Start removing osd from each osd node
        3. IO's shouldn't fail
        4. Start adding back osd to each osd node
        5. IO's shouldn't fail

        Cleanup:
        1. Remove all the data in cephfs
        2. Remove all the mounts
        """
        tc = "CEPH-83573269"
        log.info("Running cephfs %s test case" % (tc))

        client = ceph_cluster.get_ceph_objects("client")
        client1 = client[0]
        cmd = "ceph fs set cephfs max_mds 1"
        client1.exec_command(sudo=True, cmd=cmd)
        cmd = "ceph fs set cephfs allow_standby_replay true"
        client1.exec_command(sudo=True, cmd=cmd)
        wait_for_stable_fs(client1, standby_replay="true")
        cmd = "ceph fs set cephfs allow_standby_replay false"
        client1.exec_command(sudo=True, cmd=cmd)
        wait_for_stable_fs(client1, standby_replay="false")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
