import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:
Testing cephfs-journal-tool journal mode

Steps to Reproduce:
1. run "cephfs-journal-tool --rank [fs_name]:0 journal inspect"
2. import journal with empty path
3. import journal with invalid path
4 .import journal with invalid file
5. export the journal
6.import the journal that just exported
7.reset the journal
"""


def run(ceph_cluster, **kw):
    try:
        tc = "83594249"
        log.info(f"Running CephFS tests for ceph tracker - {tc}")
        # Initialize the utility class for CephFS
        fs_util = FsUtils(ceph_cluster)
        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))
        # run  "cephfs-journal-tool --rank [fs_name]:0 journal inspect" command
        inspect_out, ec_1 = client1.exec_command(
            sudo=True, cmd="cephfs-journal-tool --rank cephfs:0 journal inspect"
        )
        log.info(inspect_out)
        if "OK" in inspect_out:
            log.info(
                "cephfs-journal-tool --rank cephfs:0 journal inspect is successful with OK status"
            )
        elif "DAMAGED" in inspect_out:
            log.info(
                "cephfs-journal-tool --rank cephfs:0 journal inspect is successful DAMAGED status"
            )
        else:
            log.error(
                "cephfs-journal-tool --rank cephfs:0 journal inspect should have OK or DAMAGED status"
            )
            return 1
        # import journal with empty path
        import_out1, ec_2 = client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 journal import ",
            check_ec=False,
        )
        log.info(import_out1)
        log.info(ec_2)
        if ec_2 == 0:
            log.error(
                "cephfs-journal-tool --rank cephfs:0 journal import is expected to be failed"
            )
            return 1
        # import journal with invalid path
        import_out2, ec_3 = client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 journal import /test",
            check_ec=False,
        )
        log.info(import_out2)
        log.info(ec_3)
        if ec_3 == 0:
            log.error(
                "cephfs-journal-tool --rank cephfs:0 journal import invalid path is expected to be failed"
            )
            return 1
        # import journal with invalid file
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        client1.exec_command(sudo=True, cmd=f"touch {rand}")
        import_out3, ec_4 = client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 journal import aa",
            check_ec=False,
        )
        log.info(import_out3)
        if ec_4 == 0:
            log.error(
                "cephfs-journal-tool --rank cephfs:0 journal import invalid file is expected to be failed"
            )
            return 1
        # export the journal
        import_out4, ec_5 = client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 journal export /journal",
            check_ec=False,
        )
        # export the journal with invalid path
        log.info(import_out4)
        if import_out4 == 1:
            log.error(
                "cephfs-journal-tool --rank cephfs:0 journal export is expected to succdeed"
            )
            return 1
        log.info("cephfs-journal-tool functional test is successful")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
