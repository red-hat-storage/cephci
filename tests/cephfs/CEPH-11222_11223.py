import random
import string
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utils import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        tc = "11222"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        num_of_osds = config.get("num_of_osds")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1 = []
        client2 = []
        client3 = []
        client4 = []
        client1.append(client_info["fuse_clients"][0])
        client2.append(client_info["fuse_clients"][1])
        client3.append(client_info["kernel_clients"][0])
        client4.append(client_info["kernel_clients"][1])
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
        print(rc1, rc2, rc3, rc4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")

        rc1 = fs_util.fuse_mount(client1, client_info["mounting_dir"])
        rc2 = fs_util.fuse_mount(client2, client_info["mounting_dir"])

        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            raise CommandFailed("Fuse mount failed")

        rc3 = fs_util.kernel_mount(
            client3, client_info["mounting_dir"], client_info["mon_node_ip"]
        )
        rc4 = fs_util.kernel_mount(
            client4, client_info["mounting_dir"], client_info["mon_node_ip"]
        )
        if rc3 == 0 and rc4 == 0:
            log.info("kernel mount passed")
        else:
            raise CommandFailed("kernel mount failed")
        rc = fs_util.activate_multiple_mdss(client_info["mds_nodes"])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            raise CommandFailed("Activate multiple mdss failed")
        cluster_health_beforeIO = check_ceph_healthly(
            client_info["mon_node"][0],
            num_of_osds,
            len(client_info["mon_node"]),
            build,
            None,
            300,
        )

        dir1 = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        for client in client_info["clients"]:
            log.info("Creating directory:")
            client.exec_command(
                cmd="sudo mkdir %s%s" % (client_info["mounting_dir"], dir1)
            )
            log.info("Creating directories with breadth and depth:")
            client.exec_command(
                sudo=True,
                cmd=f"python3 smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files 1000 "
                f"--files-per-dir 10 --dirs-per-dir 2 --top {client_info['mounting_dir']}{dir1}",
                timeout=300,
            )
            return_counts = fs_util.io_verify(client)
            result = fs_util.rc_verify("", return_counts)
            print(result)

            break

        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir1,
                0,
                5,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir1,
                0,
                100,
                iotype="touch",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir1,
                0,
                5,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir1,
                0,
                5,
                iotype="smallfile",
            )
            for op in p:
                return_counts, rc = op
        result1 = fs_util.rc_verify("", return_counts)
        print(result1)

        for client in client_info["clients"]:
            client.exec_command(
                cmd="sudo rm -rf %s%s" % (client_info["mounting_dir"], dir1)
            )
            break

        for client in client_info["clients"]:
            log.info("Creating directories with breadth and depth:")
            client.exec_command(
                sudo=True,
                cmd="python3 smallfile/smallfile_cli.py "
                "--operation create --threads 10 "
                " --file-size 4 --files 1000 "
                "--files-per-dir 10 --dirs-per-dir 2"
                " --top %s%s" % (client_info["mounting_dir"], dir1),
                timeout=300,
            )
            return_counts = fs_util.io_verify(client)
            result = fs_util.rc_verify("", return_counts)
            print(result)
            log.info("Renaming the dirs:")
            client.exec_command(
                sudo=True,
                cmd="python3 smallfile/smallfile_cli.py "
                "--operation rename --threads 10 --file-size 4"
                " --file-size 4 --files 1000 "
                "--files-per-dir 10 --dirs-per-dir 2"
                " --top %s%s" % (client_info["mounting_dir"], dir1),
                timeout=300,
            )
            return_counts = fs_util.io_verify(client)
            result = fs_util.rc_verify("", return_counts)
            print(result)

            break
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir1,
                0,
                5,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir1,
                0,
                100,
                iotype="touch",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir1,
                0,
                5,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir1,
                0,
                5,
                iotype="smallfile",
            )
            for op in p:
                return_counts, rc = op
        result2 = fs_util.rc_verify("", return_counts)
        print(result2)
        cluster_health_afterIO = check_ceph_healthly(
            client_info["mon_node"][0],
            num_of_osds,
            len(client_info["mon_node"]),
            build,
            None,
            300,
        )
        client1[0].exec_command(
            cmd="sudo mkdir %s%s" % (client_info["mounting_dir"], dir_name)
        )
        with parallel() as p:
            p.spawn(
                fs_util.read_write_IO,
                client1,
                client_info["mounting_dir"],
                "read",
                "m",
                dir_name=dir_name,
            )
            p.spawn(
                fs_util.read_write_IO,
                client3,
                client_info["mounting_dir"],
                "write",
                "m",
                dir_name=dir_name,
            )
            p.spawn(
                fs_util.read_write_IO,
                client2,
                client_info["mounting_dir"],
                "read",
                "m",
                dir_name=dir_name,
            )
            p.spawn(
                fs_util.read_write_IO,
                client4,
                client_info["mounting_dir"],
                "write",
                "m",
                dir_name=dir_name,
            )
            p.spawn(
                fs_util.read_write_IO,
                client1,
                client_info["mounting_dir"],
                dir_name=dir_name,
            )
            p.spawn(
                fs_util.read_write_IO,
                client3,
                client_info["mounting_dir"],
                dir_name=dir_name,
            )
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info["mounting_dir"],
                dir_name,
                0,
                5,
                iotype="smallfile",
            )
            for op in p:
                return_counts, rc = op
        result = fs_util.rc_verify("11223", return_counts)
        print(result)

        if cluster_health_beforeIO == cluster_health_afterIO:
            print("Testcase %s passed" % (tc))
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
                    client_info["fuse_clients"],
                    "",
                    client_info["mounting_dir"],
                    "umount",
                )
            if rc == 0:
                log.info("Cleaning up successfull")
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
            fs_util.client_clean_up(
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
                "umount",
            )
        else:
            fs_util.client_clean_up(
                client_info["fuse_clients"], "", client_info["mounting_dir"], "umount"
            )
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
