import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, results=None, **kw):
    """
    An arbitrary pool added to the volume is removed successfully on volume removal.
    Pre-requisites :
    1 Create a Pool
    2 Create fs volume using the created pool.
    3 Remove the volume and verify if pool associated with the volume is also removed.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        results = []
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        if build.startswith("4"):
            clients[0].exec_command(
                sudo=True,
                cmd="ceph fs flag set enable_multiple true",
            )
        log.info("Create FS's and add arbitrary data pool")
        create_fs_pools = [
            "ceph fs volume create cephfs_new",
            "ceph osd pool create cephfs_new-data-ec 64 erasure",
            "ceph osd pool create cephfs_new-metadata 64",
            "ceph osd pool set cephfs_new-data-ec allow_ec_overwrites true",
            "ceph fs new cephfs_new-ec cephfs_new-metadata cephfs_new-data-ec --force",
            "ceph osd pool create cephfs-data-pool-arbitrary",
            "ceph osd pool create cephfs-data-pool-arbitrary-ec 64 erasure",
            "ceph osd pool set cephfs-data-pool-arbitrary-ec allow_ec_overwrites true",
        ]
        for cmd in create_fs_pools:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Add created data pools to each of the filesystem")
        add_pool_to_FS = [
            "ceph fs add_data_pool cephfs_new cephfs-data-pool-arbitrary",
            "ceph fs add_data_pool cephfs_new-ec cephfs-data-pool-arbitrary-ec",
        ]
        for cmd in add_pool_to_FS:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Remove the FS")
        rm_fs = [
            "ceph config set mon mon_allow_pool_delete true",
            "ceph fs volume rm cephfs_new --yes-i-really-mean-it",
            "ceph fs volume rm cephfs_new-ec --yes-i-really-mean-it",
        ]
        for cmd in rm_fs:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info(
            "Verify if arbitrary pool is also removed along with removal of FS Volume"
        )
        verify_fs_removal = [
            "ceph fs ls | grep cephfs",
            "ceph fs ls | grep cephfs-ec",
            "ceph osd lspools | grep cephfs.cephfs-data-pool-arbitrary",
            "ceph osd lspools | grep cephfs.cephfs-data-pool-arbitrary-ec",
        ]
        for cmd in verify_fs_removal:
            clients[0].exec_command(sudo=True, cmd=cmd, check_ec=False)
            if clients[0].node.exit_status == 1:
                results.append(f"{cmd} successfully executed")
        wait_for_process(clients[0], "cephfs_new", ispresent=False)
        wait_for_process(clients[0], "cephfs_new-ec", ispresent=False)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
