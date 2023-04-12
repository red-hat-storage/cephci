import json
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisite:
    Steps:
    1. enable mgr stats
    2. check the stats in mgr ls
    3. disable mgr stats
    4. check the stat in mgr ls
    """
    try:
        tc = "CEPH-83573836"
        log.info(f"Running CephFS tests for -{tc}")
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        client1.exec_command(sudo=True, cmd="ceph mgr module enable stats")
        out1, _ = client1.exec_command(
            sudo=True, cmd="ceph mgr module ls --format json-pretty"
        )
        print(out1)
        output1 = json.loads(out1)
        if "stats" not in output1["enabled_modules"]:
            raise CommandFailed("mgr stats module is not enabled")
        client1.exec_command(sudo=True, cmd="ceph mgr module disable stats")
        out2, _ = client1.exec_command(
            sudo=True, cmd="ceph mgr module ls -f json-pretty"
        )
        output2 = json.loads(out2)
        if "stats" in output2["enabled_modules"]:
            raise CommandFailed("mgr stats module is still enabled")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd="ceph mgr module disable stats")
