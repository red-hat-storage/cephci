import timeit
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Mount CephFS on multiple clients, perform write from one client and read the same data from other clients
    """
    try:
        start = timeit.default_timer()
        tc = "11220"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        default_fs = "cephfs" if not erasure else "cephfs-ec"

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
        fs_details = fs_util_v1.get_fs_info(client1[0], default_fs)
        if not fs_details:
            fs_util_v1.create_fs(client1[0], default_fs)
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

        log.info("Creating directory:")
        for node in client1:
            out, rc = node.exec_command(
                cmd="sudo mkdir %s%s" % (client_info["mounting_dir"], dir_name)
            )
            print(out)
            break

        return_counts1, rc1 = fs_util.stress_io(
            client1, client_info["mounting_dir"], dir_name, 0, 1, iotype="smallfile"
        )
        return_counts2, rc2 = fs_util.stress_io(
            client2, client_info["mounting_dir"], dir_name, 0, 1, iotype="fio"
        )
        return_counts3, rc3 = fs_util.read_write_IO(
            client3, client_info["mounting_dir"], dir_name=dir_name
        )
        if rc1 == 0 and rc2 == 0 and rc3 == 0:
            log.info("IOs on clients successfull")
            log.info("Testcase %s passed" % (tc))
            log.info("Cleaning up!-----")
            if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
                rc = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    client_info["kernel_clients"],
                    client_info["mounting_dir"],
                    "umount",
                )
                client1[0].exec_command(
                    sudo=True, cmd=f"ceph fs set {default_fs} max_mds 1"
                )
            else:
                rc = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    "",
                    client_info["mounting_dir"],
                    "umount",
                )
                client1[0].exec_command(
                    sudo=True, cmd=f"ceph fs set {default_fs} max_mds 1"
                )
            if rc == 0:
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
        log.error(e)
        log.error(traceback.format_exc())

        return 1
