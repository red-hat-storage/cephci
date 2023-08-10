import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-11340"
        log.info(f"Running CephFS tests for {tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
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
        client1.exec_command(
            sudo=True,
            cmd=f"ceph auth get-or-create client.{name1} "
            f"mds 'allow *,allow * path=/dir1' mon 'allow r' osd 'allow rw tag cephfs data=cephfs' "
            f"-o /etc/ceph/ceph.client.{name1}.keyring",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph auth get-or-create client.{name2} "
            f"mds 'allow *,allow r path=/dir1' mon 'allow r' osd 'allow rw tag cephfs data=cephfs' "
            f"-o /etc/ceph/ceph.client.{name2}.keyring",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph auth get-or-create client.{name3} "
            f"mds 'allow *,allow * path=/dir2' mon 'allow r' osd 'allow rw tag cephfs data=cephfs' "
            f"-o /etc/ceph/ceph.client.{name3}.keyring",
        )
        # mount 3 clients
        client1.exec_command(
            sudo=True, cmd=f"ceph-fuse -n client.{name1} {fuse_mounting_dir_1}"
        )
        client1.exec_command(
            sudo=True, cmd=f"ceph-fuse -n client.{name2} {fuse_mounting_dir_2}"
        )
        client1.exec_command(
            sudo=True, cmd=f"ceph-fuse -n client.{name3} {fuse_mounting_dir_3}"
        )
        # create 2 dirs
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}dir1")
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}dir2")
        # Test permissions with the clients
        client1.exec_command(sudo=True, cmd=f"touch {fuse_mounting_dir_1}dir1/file1")
        out_ls1 = client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_1}dir1/")
        log.info(f"ls output in {fuse_mounting_dir_1} is {out_ls1[0]}")
        if "file1" not in out_ls1[0]:
            raise CommandFailed(f"write failed in {fuse_mounting_dir_1} with client1")
        client1.exec_command(sudo=True, cmd=f"touch {fuse_mounting_dir_3}dir2/file3")
        out_ls3 = client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_3}dir2/")
        log.info(f"ls output in {fuse_mounting_dir_3} is {out_ls3[0]}")
        if "file3" not in out_ls3[0]:
            raise CommandFailed(f"write failed in {fuse_mounting_dir_3} with client3")
        client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_3}dir2/")
        client1.exec_command(sudo=True, cmd=f"touch {fuse_mounting_dir_2}dir1/file2")
        out_ls2 = client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_2}dir1/")
        log.info(f"ls output in {fuse_mounting_dir_2} is {out_ls2[0]}")
        if "file2" in out_ls2[0]:
            raise CommandFailed(
                f"file2 can not be written thru {fuse_mounting_dir_2} with client2 BZ-2232446"
            )
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
