import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-10529 - Single CephFS mount on multiple clients and
    run IO's on same directory from each clients and exersize POSIX locks

        Pre-requisites:
    1. Create cephfs volume
       ceph fs create volume create <vol_name>

    Test operation:
    1. Create directories in the cephfs volume
        2 dir as kernel mount and 2 dir as fuse mount.
    2. Run IO's and fill data on kernel and fuse directories.
    3. excersize posix locks.

    Clean-up:
    1. remove directories
    2. unmount paths
    3. Remove subvolumes, subvolume groups.
    """

    try:
        tc = "10529"
        log.info("Running cephfs %s test case" % tc)
        fs_util = FsUtils(ceph_cluster)
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1, client2, client3, client4 = ([] for _ in range(4))
        client1.append(client_info["fuse_clients"][0])
        client2.append(client_info["fuse_clients"][1])
        client3.append(client_info["kernel_clients"][0])
        client4.append(client_info["kernel_clients"][1])

        rc1 = fs_util_v1.auth_list(client1)
        rc2 = fs_util_v1.auth_list(client2)
        rc3 = fs_util_v1.auth_list(client3)
        rc4 = fs_util_v1.auth_list(client4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")

        fs_util_v1.fuse_mount(client1, client_info["mounting_dir"])
        fs_util_v1.fuse_mount(client2, client_info["mounting_dir"])
        fs_util_v1.kernel_mount(
            client3, client_info["mounting_dir"], ",".join(client_info["mon_node_ip"])
        )
        fs_util_v1.kernel_mount(
            client4, client_info["mounting_dir"], ",".join(client_info["mon_node_ip"])
        )

        with parallel() as p:
            p.spawn(
                fs_util.read_write_IO,
                client1,
                client_info["mounting_dir"],
                "g",
                "write",
            )
            p.spawn(
                fs_util.read_write_IO, client2, client_info["mounting_dir"], "g", "read"
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                "",
                0,
                2,
                iotype="smallfile",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                "",
                0,
                2,
                iotype="smallfile",
            )
            p.spawn(
                fs_util.read_write_IO,
                client4,
                client_info["mounting_dir"],
                "g",
                "readwrite",
            )
            p.spawn(fs_util.read_write_IO, client3, client_info["mounting_dir"])
            for op in p:
                (return_counts, rc) = op

        print("Results:")
        result = fs_util.rc_verify(tc, return_counts)
        print(result)
        tc = "10529"
        log.info("Test for CEPH-%s will start:" % tc)
        md5sum_file_lock = []
        with parallel() as p:
            p.spawn(fs_util.file_locking, client1, client_info["mounting_dir"])
            p.spawn(fs_util.file_locking, client2, client_info["mounting_dir"])
            for output in p:
                md5sum_file_lock = output

        if 0 in md5sum_file_lock:
            log.info("file locking success")
        else:
            raise CommandFailed("file locking failed")

        if len(md5sum_file_lock) == 2:
            log.info(
                "File Locking mechanism is working,data is not corrupted,"
                "test case CEPH-%s passed" % tc
            )
        else:
            log.error(
                "File Locking mechanism is failed,data is corrupted,"
                "test case CEPH-%s failed" % (tc)
            )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Test completed for CEPH-%s" % (tc))
        log.info("Cleaning up!-----")
        if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
            rc = fs_util.client_clean_up(
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
                "umount",
            )
        else:
            rc = fs_util.client_clean_up(
                client_info["fuse_clients"], "", client_info["mounting_dir"], "umount"
            )
        if rc == 0:
            log.info("CLeaned up successfull")
