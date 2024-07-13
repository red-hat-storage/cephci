"""
This is cephfs testcase that perform MDSfailover on active-active mdss,
performing client IOs with no pinning, then IO operation with pinning
Which require 4 clients, 2 for fuse mount and rest2 for kernel mount
"""

import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    MDSfailover on active-active mdss and perform pinning.

    Arguments:
        ceph_cluster: Cluster information
        kw :
            config: cluster configurations

    Returns:
        0 on success or 1 for failures
    """
    try:
        start = timeit.default_timer()
        config = kw.get("config")
        fs_util = FsUtils(ceph_cluster)
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc != 0:
            raise CommandFailed("fetching client info failed")
        log.info("Retrieved client information successfully")
        client1, client2, client3, client4 = ([] for _ in range(4))
        client1.append(client_info["fuse_clients"][0])
        client2.append(client_info["fuse_clients"][1])
        client3.append(client_info["kernel_clients"][0])
        client4.append(client_info["kernel_clients"][1])
        installer = ceph_cluster.get_nodes("installer")
        num_of_dirs = config.get("num_of_dirs")
        tc = "11227"
        dir_name = "dir"
        log.info(f"Running cephfs {tc} test case")

        fs_util_v1.auth_list(clients)
        fs_util_v1.fuse_mount(client1, client_info["mounting_dir"])
        fs_util_v1.fuse_mount(client2, client_info["mounting_dir"])
        fs_util_v1.kernel_mount(
            client3, client_info["mounting_dir"], installer[0].ip_address
        )
        fs_util_v1.kernel_mount(
            client4, client_info["mounting_dir"], installer[0].ip_address
        )
        rc = fs_util_v1.activate_multiple_mdss(clients)
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            raise CommandFailed("Activate multiple mdss failed")
        client1[0].exec_command(
            sudo=True, cmd=f"mkdir {client_info['mounting_dir']}{dir_name}"
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
                client2,
                client_info["mounting_dir"],
                "",
                0,
                2,
                iotype="smallfile",
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
                fs_util.read_write_IO,
                client4,
                client_info["mounting_dir"],
                "g",
                "readwrite",
            )
            p.spawn(fs_util.read_write_IO, client3, client_info["mounting_dir"])
            for op in p:
                return_counts, rc = op

        result = fs_util.rc_verify("", return_counts)

        client1[0].exec_command(
            sudo=True, cmd=f"mkdir {client_info['mounting_dir']}testdir"
        )

        if result == "Data validation success":
            print("Data validation success")
            fs_util_v1.activate_multiple_mdss(clients)
            log.info(f"Execution of Test case CEPH-{tc} started:")
            num_of_dirs = int(num_of_dirs / 5)
            with parallel() as p:
                p.spawn(
                    fs_util.mkdir_bulk,
                    client1,
                    0,
                    num_of_dirs * 1,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client2,
                    num_of_dirs * 1 + 1,
                    num_of_dirs * 2,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client1,
                    num_of_dirs * 2 + 1,
                    num_of_dirs * 3,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client2,
                    num_of_dirs * 3 + 1,
                    num_of_dirs * 4,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client1,
                    num_of_dirs * 4 + 1,
                    num_of_dirs * 5,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                )
                for op in p:
                    rc = op
            if rc == 0:
                log.info("Directories created successfully")
            else:
                raise CommandFailed("Directory creation failed")

            with parallel() as p:
                p.spawn(
                    fs_util.max_dir_io,
                    client1,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    0,
                    num_of_dirs * 1,
                    10,
                )
                p.spawn(
                    fs_util.max_dir_io,
                    client2,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    num_of_dirs * 1,
                    num_of_dirs * 2,
                    10,
                )
                rc = fs_util.check_mount_exists(client1[0])
                if rc == 0:
                    fs_util.pinning(
                        client1,
                        0,
                        10,
                        client_info["mounting_dir"] + "testdir/",
                        dir_name,
                        0,
                    )

                p.spawn(
                    fs_util.max_dir_io,
                    client3,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    num_of_dirs * 3,
                    num_of_dirs * 4,
                    10,
                )
                p.spawn(
                    fs_util.max_dir_io,
                    client4,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    num_of_dirs * 4,
                    num_of_dirs * 5,
                    10,
                )

            with parallel() as p:
                p.spawn(
                    fs_util_v1.pinned_dir_io_mdsfailover,
                    client1,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    0,
                    10,
                    100,
                    fs_util_v1.mds_fail_over,
                )

            with parallel() as p:
                p.spawn(
                    fs_util.pinning,
                    client2,
                    10,
                    num_of_dirs * 1,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client3,
                    num_of_dirs * 1,
                    num_of_dirs * 2,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client4,
                    num_of_dirs * 2,
                    num_of_dirs * 3,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client1,
                    num_of_dirs * 3,
                    num_of_dirs * 4,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client3,
                    num_of_dirs * 4,
                    num_of_dirs * 5,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    1,
                )

            with parallel() as p:
                p.spawn(
                    fs_util_v1.pinned_dir_io_mdsfailover,
                    client1,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    0,
                    10,
                    100,
                    fs_util_v1.mds_fail_over,
                )
            with parallel() as p:
                p.spawn(
                    fs_util_v1.pinned_dir_io_mdsfailover,
                    client2,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    0,
                    10,
                    100,
                    fs_util_v1.mds_fail_over,
                )
            with parallel() as p:
                p.spawn(
                    fs_util_v1.pinned_dir_io_mdsfailover,
                    client3,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    0,
                    10,
                    100,
                    fs_util_v1.mds_fail_over,
                )
            with parallel() as p:
                p.spawn(
                    fs_util_v1.pinned_dir_io_mdsfailover,
                    client4,
                    client_info["mounting_dir"] + "testdir/",
                    dir_name,
                    0,
                    10,
                    100,
                    fs_util_v1.mds_fail_over,
                )

            log.info(f"Execution of Test case CEPH-{tc} ended:")
            log.info("Cleaning up!-----")
            if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
                rc_client = fs_util_v1.client_clean_up(
                    "umount",
                    client_info["fuse_clients"],
                    client_info["kernel_clients"],
                    client_info["mounting_dir"],
                )
                rc_mds = fs_util_v1.mds_cleanup(clients, None)

            else:
                rc_client = fs_util_v1.client_clean_up(
                    "umount",
                    client_info["fuse_clients"],
                    "",
                    client_info["mounting_dir"],
                )
                rc_mds = fs_util_v1.mds_cleanup(clients, None)

            if rc_client == 0 and rc_mds == 0:
                log.info("Cleaning up successfull")
            else:
                return 1
        print("Script execution time:------")
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)
        print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info("Cleaning up!-----")
        if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
            rc_client = fs_util_v1.client_clean_up(
                "umount",
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
            )
            rc_mds = fs_util_v1.mds_cleanup(clients, None)

        else:
            rc_client = fs_util_v1.client_clean_up(
                "umount", client_info["fuse_clients"], "", client_info["mounting_dir"]
            )
            rc_mds = fs_util_v1.mds_cleanup(clients, None)
        if rc_client == 0 and rc_mds == 0:
            log.info("Cleaning up successfull")
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
