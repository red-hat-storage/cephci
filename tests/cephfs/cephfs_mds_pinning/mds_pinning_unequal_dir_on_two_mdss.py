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
    CEPH-11229
    Max Pinning of subtress/directories to both MDS and perform Failover of MDS
    Active-active multi MDS having standby for each MDS with pinning
    maximum subtrees to Two MDS(Max Directory: 100k) and
    perform failover of all active MDS
    """
    try:
        start = timeit.default_timer()
        config = kw.get("config")
        num_of_dirs = config.get("num_of_dirs")
        tc = "11229"
        dir_name = "dir"
        test_dir = "testdir/"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        clients = ceph_cluster.get_ceph_objects("client")
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
        fs_details = fs_util_v1.get_fs_info(client1[0], default_fs)

        if not fs_details:
            fs_util_v1.create_fs(client1[0], default_fs)
        rc1 = fs_util_v1.auth_list(client1)
        rc2 = fs_util_v1.auth_list(client2)
        rc3 = fs_util_v1.auth_list(client3)
        rc4 = fs_util_v1.auth_list(client4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")

        fs_util_v1.fuse_mount(
            client1,
            client_info["mounting_dir"],
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util_v1.fuse_mount(
            client2,
            client_info["mounting_dir"],
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util_v1.kernel_mount(
            client3,
            client_info["mounting_dir"],
            ",".join(client_info["mon_node_ip"]),
            extra_params=f",fs={default_fs}",
        )
        fs_util_v1.kernel_mount(
            client4,
            client_info["mounting_dir"],
            ",".join(client_info["mon_node_ip"]),
            extra_params=f",fs={default_fs}",
        )

        client1[0].exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")

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
                1,
                iotype="smallfile",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                "",
                0,
                1,
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
        if result == "Data validation success":
            log.info("Data validation success")
            for client in client1:
                client.exec_command(
                    cmd="sudo mkdir %s%s" % (client_info["mounting_dir"], test_dir)
                )
            log.info("Execution of Test case CEPH-%s started:" % (tc))
            num_of_dirs = int(num_of_dirs / 5)
            with parallel() as p:
                p.spawn(
                    fs_util.mkdir_bulk,
                    client1,
                    0,
                    num_of_dirs * 1,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client2,
                    num_of_dirs * 1 + 1,
                    num_of_dirs * 2,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client1,
                    num_of_dirs * 2 + 1,
                    num_of_dirs * 3,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client2,
                    num_of_dirs * 3 + 1,
                    num_of_dirs * 4,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                )
                p.spawn(
                    fs_util.mkdir_bulk,
                    client1,
                    num_of_dirs * 4 + 1,
                    num_of_dirs * 5,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                )

            with parallel() as p:
                p.spawn(
                    fs_util.pinning,
                    client2,
                    0,
                    num_of_dirs * 1,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    0,
                )
                p.spawn(
                    fs_util.pinning,
                    client3,
                    num_of_dirs * 1,
                    num_of_dirs * 2,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client4,
                    num_of_dirs * 2,
                    num_of_dirs * 3,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client1,
                    num_of_dirs * 3,
                    num_of_dirs * 4,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    1,
                )
                p.spawn(
                    fs_util.pinning,
                    client3,
                    num_of_dirs * 4,
                    num_of_dirs * 5,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    1,
                )

            with parallel() as p:
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client3,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    0,
                    num_of_dirs / 2,
                    1,
                    fs_util.mds_fail_over,
                    client_info["clients"][0:],
                )
            with parallel() as p:
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client4,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    num_of_dirs / 2,
                    num_of_dirs,
                    2,
                    fs_util.mds_fail_over,
                    client_info["clients"][0:],
                )
            with parallel() as p:
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client2,
                    client_info["mounting_dir"] + test_dir,
                    dir_name,
                    num_of_dirs * 4,
                    num_of_dirs * 5,
                    1,
                    fs_util.mds_fail_over,
                    client_info["clients"][0:],
                )
                for op in p:
                    return_counts, rc = op
            log.info("Execution of Test case CEPH-%s ended" % (tc))
            print("Results:")
            result = fs_util.rc_verify(tc, return_counts)
            print(result)
            log.info("Cleaning up!-----")
            if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
                rc_client = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    client_info["kernel_clients"],
                    client_info["mounting_dir"],
                    "umount",
                )
                rc_mds = fs_util_v1.mds_cleanup(clients, None, fs_name=default_fs)
            else:
                rc_client = fs_util_v1.client_clean_up(
                    "umount",
                    client_info["fuse_clients"],
                    "",
                    client_info["mounting_dir"],
                )
                rc_mds = fs_util_v1.mds_cleanup(clients, None, fs_name=default_fs)

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
        log.error(e)
        log.error(traceback.format_exc())
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
            log.info("Cleaning up successfull")
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
