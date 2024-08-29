import random
import string
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
        Test Cases Covered:
        CEPH-11337 - Mount and unmount CephFS repeatedly in interval of 30 min & check data integrity
        This script doesn't cover 30 min IOs in interval, it covers only data integrity check after remount

        Pre-requisite:
        1. Create CephFS volume

        Test operation:
        1. Mount 1 kernel & 2 fuse clients
        2. Run IOs on 3 clients
        3. Unmount all the clients
        4. Repeat steps 1-3 10 times

        Clean-up:
        1. Remove all the data in Cephfs file system
        2. Remove all the cephfs mounts
        """

        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        client2 = clients[0]
        client3 = clients[0]
        kernel_1_checksum = []
        fuse_1_checksum = []
        fuse_2_checksum = []
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        command = f"mkdir -p {kernel_mounting_dir}kernel_client_1/dir_{{1..11}}"
        client1.exec_command(sudo=True, cmd=command)
        command = f"mkdir -p {kernel_mounting_dir}fuse_client_{{1..2}}/dir_{{1..11}}"
        client1.exec_command(sudo=True, cmd=command)
        for x in range(1, 10):
            try:
                mounting_dir = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(10))
                )
                log.info("Mounting kernel & fuse clients")
                kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
                fs_util.kernel_mount(
                    [client1],
                    kernel_mounting_dir_1,
                    ",".join(mon_node_ips),
                    extra_params=f",fs={fs_name}",
                )
                fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
                mon_node_ips = fs_util.get_mon_node_ips()
                fs_util.fuse_mount(
                    [client2],
                    fuse_mounting_dir_1,
                    extra_params=f" --client_fs {fs_name}",
                )
                fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
                mon_node_ips = fs_util.get_mon_node_ips()
                fs_util.fuse_mount(
                    [client3],
                    fuse_mounting_dir_2,
                    extra_params=f" --client_fs {fs_name}",
                )

                if x != 1:
                    log.info("Verifying the checksum after remounting")
                    new_kernel_1_checksum = fs_util.get_files_and_checksum(
                        client1, f"{kernel_mounting_dir_1}kernel_client_1/dir_{x-1}"
                    )
                    new_fuse_1_checksum = fs_util.get_files_and_checksum(
                        client1, f"{fuse_mounting_dir_1}fuse_client_1/dir_{x-1}"
                    )
                    new_fuse_2_checksum = fs_util.get_files_and_checksum(
                        client1, f"{fuse_mounting_dir_2}fuse_client_2/dir_{x-1}"
                    )
                    if new_kernel_1_checksum != kernel_1_checksum:
                        log.error("Data integrity check failed")
                        return 1
                    if new_fuse_1_checksum != fuse_1_checksum:
                        log.error("Data integrity check failed")
                        return 1
                    if new_fuse_2_checksum != fuse_2_checksum:
                        log.error("Data integrity check failed")
                        return 1
                    log.info("Data integrity check is success")
                rand_size = random.randint(5, 10)
                cmd_dd_k = (
                    f"for n in {{1..20}}; do     dd if=/dev/urandom of={kernel_mounting_dir_1}kernel_client_1/dir_{x}"
                    "/uile$( printf %03d "
                    "$n"
                    f" ) bs={rand_size}k count=1; done"
                )
                cmd_dd_f1 = (
                    f"for n in {{1..20}}; do     dd if=/dev/urandom of={fuse_mounting_dir_1}fuse_client_1/dir_{x}/"
                    "uile$( printf %03d "
                    "$n"
                    f" ) bs={rand_size} count=1; done"
                )
                cmd_fio_f = (
                    f"sudo fio --name=global --rw=readwrite --size={rand_size}k --name=file --directory="
                    f"{fuse_mounting_dir_1}fuse_client_2/dir_{x}/ --numjobs=20"
                )
                with parallel() as p:
                    p.spawn(
                        client1.exec_command,
                        sudo=True,
                        cmd=cmd_dd_k,
                        check_ec=True,
                        timeout=1800,
                    )
                    p.spawn(
                        client1.exec_command,
                        sudo=True,
                        cmd=cmd_dd_f1,
                        check_ec=True,
                        timeout=1800,
                    )
                    p.spawn(
                        client2.exec_command,
                        sudo=True,
                        cmd=cmd_fio_f,
                        check_ec=True,
                        timeout=1800,
                    )
                log.info("Collecting checksum from kernel & fuse mounts")
                kernel_1_checksum = fs_util.get_files_and_checksum(
                    client1, f"{kernel_mounting_dir_1}kernel_client_1/dir_{x}"
                )
                fuse_1_checksum = fs_util.get_files_and_checksum(
                    client1, f"{fuse_mounting_dir_1}fuse_client_1/dir_{x}"
                )
                fuse_2_checksum = fs_util.get_files_and_checksum(
                    client1, f"{fuse_mounting_dir_1}fuse_client_2/dir_{x}"
                )
                client1.exec_command(sudo=True, cmd=f"umount {kernel_mounting_dir_1}")
                client2.exec_command(sudo=True, cmd=f"umount {fuse_mounting_dir_1}")
                client3.exec_command(sudo=True, cmd=f"umount {fuse_mounting_dir_2}")
            except TimeoutError:
                continue
            except Exception as e:
                log.error(e)
                log.error(traceback.format_exc())
                return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mounting_dir}*")
        client1.exec_command(sudo=True, cmd=f"umount {kernel_mounting_dir}")
