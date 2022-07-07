import json
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
"""
What it does:
1. Stores ceph versions before upgrade
2. Compares ceph versions after upgrade
3. Stores stat data before upgrade (Permission , Permissions bits and file type, User ID, User Name, Time of birth etc)
4. Compares file stat data after upgrade
5. It is free to add additional file stats as needed

This Script needs 2 times of run:

1. Before upgrade to store ceph version data and file stat data
2. After upgrade to compared those data
"""


def run(ceph_cluster, **kw):
    ceph_version_path = "/tmp/ceph_versions.json"
    log.info("Upgrade checking initiated")
    fs_util = FsUtils(ceph_cluster)
    client = ceph_cluster.get_ceph_objects("client")[0]
    fs_util.auth_list([client])
    check_list = ["mon", "mgr", "osd", "mds", "overall"]
    try:
        out0, err0 = client.exec_command(
            sudo=True, cmd=f"stat {ceph_version_path}", check_ec=False
        )
        if out0:
            log.info("Loading previous ceph versions")
            with open(ceph_version_path, "r") as before_file:
                pre_data = json.loads(json.load(before_file))
            out1, err1 = client.exec_command(
                sudo=True, cmd="ceph versions --format json"
            )
            upgraded_data = json.loads(out1)
            fail_daemon = []
            for daemon in check_list:
                pre_version = str(pre_data[daemon]).split()[2]
                cur_version = str(upgraded_data[daemon]).split()[2]
                log.info(f"Previous ceph {daemon} version is {pre_version}")
                log.info(f"Current ceph {daemon} version is {cur_version}")
            if pre_version >= cur_version:
                fail_daemon.append(daemon)
                log.info(f"Upgrade is not properly done for {daemon}")
            before_file.close()
            if len(fail_daemon) > 0:
                log.error(f"Upgrade failed for these daemons: {str(fail_daemon)}")
                return 1
        else:
            log.info("---------Writing ceph versions--------------")

            client.exec_command(
                sudo=True, cmd=f"ceph versions --format json > {ceph_version_path}"
            )
            out2, err2 = client.exec_command(sudo=True, cmd=f"cat {ceph_version_path}")
            log.info(print(out2))

        file_stat_path = "/tmp/file_stat.txt"
        testing_file_path = "/tmp/stat_testing.txt"
        out0, err0 = client.exec_command(
            sudo=True, cmd=f"stat {file_stat_path}", check_ec=False
        )
        log.info(print(out0))
        if out0:
            log.info("---------Comparing stats of a file after upgrade--------------")
            log.info(
                "File stats to compare: Permission , Permissions bits and file type, User ID, User Name, Time of birth"
            )

            out1, err1 = client.exec_command(
                sudo=True, cmd=f"stat --format=%a,%A,%u,%U,%w {testing_file_path}"
            )
            out2, err2 = client.exec_command(sudo=True, cmd=f"cat {file_stat_path}")
            cur_stat = out1.split(",")
            pre_stat = out2.split(",")
            log.info(print("Current testing file stats: ", cur_stat))
            log.info(print("Previous testing file stats: ", pre_stat))
            for idx in range(4):
                if cur_stat[idx] != pre_stat[idx]:
                    return 1
            log.info("After upgrade file stats are identical")
        else:
            log.info("---------Writing stats of a file before upgrade--------------")
            log.info(
                "File stats to compare: Permission , Permissions bits and file type, User ID, User Name, Time of birth"
            )
            client.exec_command(sudo=True, cmd=f"touch {testing_file_path}")
            out1, err1 = client.exec_command(
                sudo=True,
                cmd=f"stat --format=%a,%A,%u,%U,%w {testing_file_path} > {file_stat_path}",
            )
            log.info(out1)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
