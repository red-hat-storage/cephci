import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
        CEPH-83589266 - Validate Ceph commands when ceph file system is in failed state


        """
        tc = "CEPH-83583721"
        log.info("Running cephfs %s test case" % (tc))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_fail"
        fs_util.create_fs(client1, fs_name)
        fs_util.wait_for_mds_process(clients[0], fs_name)
        client1.exec_command(sudo=True, cmd=f"ceph fs fail {fs_name}")

        with parallel() as p:
            p.spawn(
                client1.exec_command,
                sudo=True,
                cmd=f"ceph fs subvolume create {fs_name} subvol_1;",
                check_ec=False,
                timeout=720,
            )
            p.spawn(client1.exec_command, sudo=True, cmd="ceph fs status")
            p.spawn(client1.exec_command, sudo=True, cmd="ceph mgr stat")
            p.spawn(client1.exec_command, sudo=True, cmd="ceph pg stat")
            for result in p:
                log.info(result)
        return 0

    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(client1, fs_name)
