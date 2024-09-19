import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11224
    * Mount CephFS on multiple clients, perform read and write
        operation on same directory from different clients
    """
    try:
        start = timeit.default_timer()
        config = kw.get("config")
        tc = "11224"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_util = FsUtils(ceph_cluster)
        build = config.get("build", config.get("rhbuild"))
        num_of_osds = config.get("num_of_osds")
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
        cluster_health_beforeIO = check_ceph_healthly(
            client1[0],
            num_of_osds,
            len(client_info["mon_node"]),
            build,
            None,
            300,
        )

        rc1 = fs_util_v1.auth_list(client1)
        rc2 = fs_util_v1.auth_list(client2)
        rc3 = fs_util_v1.auth_list(client3)
        rc4 = fs_util_v1.auth_list(client4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(client1[0], default_fs)

        if not fs_details:
            fs_util_v1.create_fs(client1, default_fs)

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
        client1[0].exec_command(
            sudo=True, cmd="mkdir %s%s" % (client_info["mounting_dir"], dir_name)
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
                dir_name,
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
        if result == "Data validation success":
            print("Data validation success")
            dirs, rc = fs_util.mkdir(
                client1, 0, 3, client_info["mounting_dir"], dir_name
            )
            if rc == 0:
                log.info("Directories created")
            else:
                raise CommandFailed("Directory creation failed")
            dirs = dirs.split("\n")
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="fio",
                )
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    100,
                    iotype="touch",
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info["mounting_dir"],
                    dirs[1],
                    0,
                    1,
                    iotype="dd",
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info["mounting_dir"],
                    dirs[2],
                    0,
                    1,
                    iotype="smallfile",
                )
                for op in p:
                    return_counts, rc = op

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="smallfile_create",
                    fnum=10,
                    fsize=1024,
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[1],
                    0,
                    1,
                    iotype="smallfile_create",
                    fnum=10,
                    fsize=1024,
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[2],
                    0,
                    1,
                    iotype="smallfile_create",
                    fnum=10,
                    fsize=1024,
                )
                for op in p:
                    return_counts, rc = op
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="smallfile_delete",
                    fnum=10,
                    fsize=1024,
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[1],
                    0,
                    1,
                    iotype="smallfile_delete",
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[2],
                    0,
                    1,
                    iotype="smallfile_delete",
                    fnum=10,
                    fsize=1024,
                )
                for op in p:
                    return_counts, rc = op
            cluster_health_afterIO = check_ceph_healthly(
                client1[0],
                num_of_osds,
                len(client_info["mon_node"]),
                build,
                None,
                300,
            )

            log.info("Execution of Test case CEPH-%s ended" % (tc))
            print("Results:")
            result = fs_util.rc_verify(tc, return_counts)
            if cluster_health_beforeIO == cluster_health_afterIO:
                print(result)

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="smallfile_create",
                    fnum=1000,
                    fsize=10,
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    5,
                    iotype="fio",
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    10,
                    iotype="dd",
                )
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="smallfile",
                )

            with parallel() as p:
                p.spawn(
                    fs_util.read_write_IO,
                    client1,
                    client_info["mounting_dir"],
                    "g",
                    "read",
                    dir_name=dirs[2],
                )
                p.spawn(
                    fs_util.read_write_IO,
                    client2,
                    client_info["mounting_dir"],
                    "g",
                    "read",
                    dir_name=dirs[0],
                )

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="smallfile_create",
                    fnum=1000,
                    fsize=10,
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    5,
                    iotype="fio",
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    10,
                    iotype="dd",
                )
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info["mounting_dir"],
                    dirs[0],
                    0,
                    1,
                    iotype="smallfile",
                )

            with parallel() as p:
                p.spawn(
                    fs_util.read_write_IO,
                    client1,
                    client_info["mounting_dir"],
                    "g",
                    "read",
                    dir_name=dirs[0],
                )
                p.spawn(
                    fs_util.read_write_IO,
                    client2,
                    client_info["mounting_dir"],
                    "g",
                    "read",
                    dir_name=dirs[0],
                )

            log.info("Cleaning up!-----")
            if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
                rc = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    client_info["kernel_clients"],
                    client_info["mounting_dir"],
                    "umount",
                )
                if rc == 0:
                    log.info("Cleaning up successfull")
                else:
                    return 1
            else:
                rc = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    "",
                    client_info["mounting_dir"],
                    "umount",
                )
                if rc == 0:
                    log.info("Cleaning up successfull")
                else:
                    return 1

        log.info("Execution of Test case CEPH-%s ended" % (tc))
        print("Results:")
        result = fs_util.rc_verify(tc, return_counts)
        print(result)
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
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
