import json
import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:
Testing cephfs-journal-tool header get and set

Steps to Reproduce:
1. Do "cephfs-journal-tool --rank [fs_name]:0 header get"
2. Check if header has essential metrics
3. change one of the attributes "magic"
4. change one of the attributes "write_pos"
5. change one of the attributes "trimmed_pos"
6. change one of the attributes "expire_pos"
7. check if those attributes are changed to target value
8. move all the values back to original values
"""


def run(ceph_cluster, **kw):
    try:
        tc = "83594266"
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
        # Generate random string for directory names
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
        # test cephfs-journal-tool header get
        header_out1, ec1 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header get",
            check_ec=False,
        )
        mdss = ceph_cluster.get_ceph_objects("mds")
        mds_list = []
        for mds in mdss:
            daemon_name = fs_util.deamon_op(mds, "mds", "stop")
            fs_util.check_deamon_status(mds, "mds", "inactive")
            mds_list.append((mds, daemon_name))
        log.info("All the mds nodes are down")
        log.info(mds_list)
        time.sleep(10)
        health_output = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(health_output[0])
        if "offline" not in health_output[0]:
            return 1
        header_out1 = json.loads(header_out1)
        log.info(header_out1)
        log.info(type(header_out1))
        original_write_pos = header_out1["write_pos"]
        log.info(original_write_pos)
        original_expire_pos = header_out1["expire_pos"]
        log.info(original_expire_pos)
        original_trimmed_pos = header_out1["trimmed_pos"]
        log.info(original_trimmed_pos)
        # Try to change for magic attribute
        header_out2, ec2 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set testing",
            check_ec=False,
        )
        if ec2 == 0:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set should have failed"
            )
            return 1
        target_write_pos = 1024
        target_expire_pos = 2048
        target_trimmed_pos = 4096
        # Try to change for write_pos
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set write_pos {target_write_pos}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set expire_pos {target_expire_pos}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set trimmed_pos {target_trimmed_pos}",
            check_ec=False,
        )
        header_out3, ec3 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header get",
            check_ec=False,
        )
        header_out3 = json.loads(header_out3)
        log.info(header_out3)
        if header_out3["write_pos"] != target_write_pos:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set write_pos failed"
            )
            return 1
        if header_out3["expire_pos"] != target_expire_pos:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set expire_pos failed"
            )
            return 1
        if header_out3["trimmed_pos"] != target_trimmed_pos:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set trimmed_pos failed"
            )
            return 1
        log.info("change of attributes is successful")
        log.info("Resetting the values to the original values")
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set write_pos {original_write_pos}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set expire_pos {original_expire_pos}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set trimmed_pos {original_trimmed_pos}",
            check_ec=False,
        )
        header_out4, ec4 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header get",
            check_ec=False,
        )
        header_out4 = json.loads(header_out4)
        log.info(header_out4)
        if header_out4["write_pos"] != original_write_pos:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set write_pos failed"
            )
            return 1
        if header_out4["expire_pos"] != original_expire_pos:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set expire_pos failed"
            )
            return 1
        if header_out4["trimmed_pos"] != original_trimmed_pos:
            log.error(
                f"cephfs-journal-tool --rank {fs_name}:0 header set trimmed_pos failed"
            )
            return 1

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # Cleanup
        # start mds
        for mds in mds_list:
            fs_util.deamon_op(mds[0], "mds", "reset-failed", service_name=mds[1])
            fs_util.deamon_op(mds[0], "mds", "start", service_name=mds[1])
            fs_util.check_deamon_status(mds[0], "mds", "active")
        time.sleep(10)
        health_output = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(health_output[0])
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set write_pos {original_write_pos}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set expire_pos {original_expire_pos}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 header set trimmed_pos {original_trimmed_pos}",
            check_ec=False,
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
