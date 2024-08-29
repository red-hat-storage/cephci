import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test steps:
    1. Client 1 with read write and execute permissions on directory 1
    2. Client1 should not write date in directory 2
    3. Client 1 with read for directory 2
    4. Client 1 should write data in first directory 1
    5. Client 2 with read only permissions on directory 1
    6. Client 2 should not write data in second directory
    7. Client 3 with full permissions level on directory 2
    8. Client 3 can read and write on second directory
    """
    try:
        tc = "CEPH-11340"
        log.info(f"Running CephFS tests for {tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)

        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        # prepare 3 mounting dirs
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{mounting_dir}_name1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse_{mounting_dir}_name2/"
        fuse_mounting_dir_3 = f"/mnt/cephfs_fuse_{mounting_dir}_name3/"
        # create 3 directories to mount
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}")
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_2}")
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_3}")
        # create 3 clients with different permissions
        name1 = "name1"
        name2 = "name2"
        name3 = "name3"
        # prepare 3 clients with different permissions
        log.info("create 3 clients with different permissions")
        log.info("give client1 read write permission for /")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs authorize {fs_name} client.{name1} / rw /dir1 rw /dir2 r "
            f"-o /etc/ceph/ceph.client.{name1}.keyring",
        )
        log.info("give client2 only read permission for /dir1")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs authorize {fs_name} client.{name2} / r /dir1 r -o /etc/ceph/ceph.client.{name2}.keyring",
        )
        # give client3 read write permission for /dir2
        log.info("give client3 read write permission for /dir2")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs authorize {fs_name} client.{name3} / r /dir2 rw -o /etc/ceph/ceph.client.{name3}.keyring",
        )
        # mount 3 clients
        log.info("mount 3 clients")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -n client.{name1} {fuse_mounting_dir_1} --client_fs {fs_name}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -n client.{name2} {fuse_mounting_dir_2} --client_fs {fs_name}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -n client.{name3} {fuse_mounting_dir_3} --client_fs {fs_name}",
        )
        log.info("Client1 can read write in /")
        client1.exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}dir1")
        client1.exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}dir2")
        client1.exec_command(sudo=True, cmd=f"touch {fuse_mounting_dir_1}dir1/file1")
        out_ls1 = client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_1}dir1/")
        log.info(f"ls output in {fuse_mounting_dir_1} is {out_ls1[0]}")
        if "file1" not in out_ls1[0]:
            raise CommandFailed(f"write failed in {fuse_mounting_dir_1} with client1")
        # clieent2 read permissions on dir1
        log.info("Client2 can only read in /dir1")
        client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_2}dir1/")
        client1.exec_command(
            sudo=True, cmd=f"touch {fuse_mounting_dir_2}dir1/file2", check_ec=False
        )
        out_ls2 = client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_2}dir1/")
        log.info(f"ls output in {fuse_mounting_dir_2} is {out_ls2[0]}")
        if "file2" in out_ls2[0]:
            raise CommandFailed(
                f"file2 can not be written thru {fuse_mounting_dir_2} with client2"
            )
        # test client3 with read permission on dir2
        log.info("Client3 can read write in /dir2")
        client1.exec_command(sudo=True, cmd=f"touch {fuse_mounting_dir_3}dir2/file3")
        out_ls3 = client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_3}dir2/")
        log.info(f"ls output in {fuse_mounting_dir_3} is {out_ls3[0]}")
        if "file3" not in out_ls3[0]:
            raise CommandFailed(f"write failed in {fuse_mounting_dir_3} with client3")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"ceph auth rm client.{name1}")
        client1.exec_command(sudo=True, cmd=f"ceph auth rm client.{name2}")
        client1.exec_command(sudo=True, cmd=f"ceph auth rm client.{name3}")
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_2
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_3
        )
