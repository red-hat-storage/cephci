import json
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
"""
pre-requisites:

Test operation:
1. set cephfs allow_standby_replay 1
2. check if there is standby mds
3. set cephfs allow_standby_replay 0
4. check if standy_replay is removed
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83574291"
        log.info(f"Running CephFS tests for -{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        fs_util.prepare_clients(clients, build)

        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 1"
        )
        out1, _ = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} -f json-pretty"
        )
        output1 = json.loads(out1)

        replay_exist = False
        for mds in output1["mdsmap"]:
            if mds["state"] == "standby-replay":
                replay_exist = True
                break
        if not replay_exist:
            raise CommandFailed("Not able to find standby_replay in mds")

        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 0"
        )
        out2, _ = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} -f json-pretty"
        )
        output2 = json.loads(out2)

        for mds in output2["mdsmap"]:
            if mds["state"] == "stanby-replay":
                raise CommandFailed("Standby replay should not be there")

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
