import random
import string
import time
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
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client_fs {fs_name}"
        )
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        # getting the mds name
        mdss = ceph_cluster.get_ceph_objects("mds")
        mds_nodes_names = []
        for mds in mdss:
            out, ec = mds.exec_command(
                sudo=True, cmd="systemctl list-units | grep -o 'ceph-.*mds.*\\.service'"
            )
            mds_nodes_names.append((mds, out.strip()))
        log.info(f"NODES_MDS_info :{mds_nodes_names}")
        for mds in mds_nodes_names:
            mds[0].exec_command(sudo=True, cmd=f"systemctl stop {mds[1]}")
        time.sleep(10)
        log.info("All the mds nodes are down")
        health_output = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(health_output[0])
        if "offline" not in health_output[0]:
            return 1
        # run  "cephfs-journal-tool --rank [fs_name]:0 journal inspect" command
        inspect_out, ec_1 = client1.exec_command(
            sudo=True, cmd=f"cephfs-journal-tool --rank {fs_name}:0 journal inspect"
        )
        log.info(inspect_out)
        if "OK" in inspect_out:
            log.info(
                f"cephfs-journal-tool --rank {fs_name}:0 journal inspect is successful with OK status"
            )
        elif "DAMAGED" in inspect_out:
            log.info(
                f"cephfs-journal-tool --rank {fs_name}:0 journal inspect is successful DAMAGED status"
            )
        else:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 journal inspect should have OK or DAMAGED status"
            )
            return 1
        # import journal with empty path
        import_out1, ec_2 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 journal import ",
            check_ec=False,
        )
        log.info(import_out1)
        log.info(ec_2)
        if ec_2 == 0:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 journal import is expected to be failed"
            )
            return 1
        # import journal with invalid path
        import_out2, ec_3 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 journal import /test",
            check_ec=False,
        )
        log.info(import_out2)
        log.info(ec_3)
        if ec_3 == 0:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 journal import invalid path is expected to be failed"
            )
            return 1
        # import journal with invalid file
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        client1.exec_command(sudo=True, cmd=f"touch {rand}")
        import_out3, ec_4 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 journal import aa",
            check_ec=False,
        )
        log.info(import_out3)
        if ec_4 == 0:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 journal import invalid file is expected to be failed"
            )
            return 1
        # export the journal
        import_out4, ec_5 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 journal export /journal",
            check_ec=False,
        )
        # export the journal with invalid path
        log.info(import_out4)
        if import_out4 == 1:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 journal export is expected to succdeed"
            )
            return 1
        log.info("cephfs-journal-tool functional test is successful")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # start mds
        for mds in mds_nodes_names:
            mds_node = mds[0]
            mds_name = mds[1]
            # reset failed counter
            mds_node.exec_command(sudo=True, cmd=f"systemctl reset-failed {mds_name}")
            mds_node.exec_command(sudo=True, cmd=f"systemctl restart {mds_name}")

        time.sleep(10)
        health_output = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(health_output[0])
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
