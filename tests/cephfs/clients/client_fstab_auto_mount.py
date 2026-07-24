import time
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
    CEPH-11336 - Update fstab and reboot client to check auto mount of FS works
    Args:
        ceph_cluster:
        **kw:

    Test case Steps:
    1. Mount the volumes on ceph-fuse and kernel mounts and Add fstab entries
    2. Reboot the client nodes
    3. Run Stress_ios on all the clients after reboot
    4. Restart MDS and mon nodes and run IOs
    5. Restart Network on all the clients and run IOs
    6. Clean-up all the nodes and remove fstab entries
    Returns:
        0 --> if TC PASS
        1--> if TC FAIL
    """
    try:
        start = timeit.default_timer()
        tc = "11336-fuse client"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        config = kw.get("config")
        num_of_osds = config.get("num_of_osds")
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        default_fs = "cephfs" if not erasure else "cephfs-ec"

        fs_util = FsUtils(ceph_cluster)

        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            log.error("fetching client info failed")
            return 1
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
        print(rc1, rc2, rc3, rc4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            log.error("auth list failed")
            return 1

        fs_util_v1.fuse_mount(
            client1,
            client_info["mounting_dir"],
            fstab=True,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util_v1.fuse_mount(
            client2,
            client_info["mounting_dir"],
            fstab=True,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util_v1.kernel_mount(
            client3,
            client_info["mounting_dir"],
            ",".join(client_info["mon_node_ip"]),
            extra_params=f",fs={default_fs}",
            fstab=True,
        )
        fs_util_v1.kernel_mount(
            client4,
            client_info["mounting_dir"],
            ",".join(client_info["mon_node_ip"]),
            extra_params=f",fs={default_fs}",
            fstab=True,
        )

        cluster_health_beforeIO = check_ceph_healthly(
            client1[0],
            num_of_osds,
            len(client_info["mon_node"]),
            build,
            None,
            300,
        )
        # rc = fs_util_v1.activate_multiple_mdss(client_info["clients"][0:], default_fs)
        # if rc == 0:
        #     log.info("Activate multiple mdss successfully")
        # else:
        #     raise CommandFailed("Activate multiple mdss failed")
        client1[0].exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")
        client1[0].exec_command(
            sudo=True, cmd=f"ceph fs set {default_fs} allow_standby_replay true"
        )
        fs_name = default_fs
        for mon in fs_util_v1.mons:
            FsUtilsV1.deamon_op(mon, "mon.ceph", "restart")
        for mon in fs_util_v1.mons:
            FsUtilsV1.check_deamon_status(mon, "mon.ceph", "active")
        for mds in fs_util_v1.mdss:
            FsUtilsV1.deamon_op(mds, rf"mds\.{fs_name}\.", "restart")
        for mds in fs_util_v1.mdss:
            FsUtilsV1.check_deamon_status(mds, rf"mds\.{fs_name}\.", "active")
        client1[0].exec_command(
            cmd="sudo mkdir -p %s%s" % (client_info["mounting_dir"], dir_name)
        )
        if client1[0].node.exit_status == 0:
            log.info("Dir created")
        else:
            raise CommandFailed("Dir creation failed")
        fs_util_v1.reboot_node(client1[0])
        with parallel() as p:
            p.spawn(
                fs_util.read_write_IO,
                client1,
                client_info["mounting_dir"],
                "g",
                "write",
            )
            p.spawn(
                fs_util.read_write_IO, client3, client_info["mounting_dir"], "g", "read"
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir_name,
                0,
                50,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                dir_name,
                0,
                1,
                iotype="smallfile_create",
                fnum=1000,
                fsize=100,
            )
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info["mounting_dir"],
                dir_name,
                0,
                1,
                iotype="smallfile",
            )
        res = []
        with parallel() as p:
            for node in client_info["mds_nodes"]:
                p.spawn(fs_util.heartbeat_map, node)
                for op in p:
                    res.append(op)
        print(res)
        fs_util_v1.reboot_node(client2[0])
        with parallel() as p:
            p.spawn(
                fs_util.read_write_IO,
                client1,
                client_info["mounting_dir"],
                "g",
                "write",
            )

            p.spawn(
                fs_util.read_write_IO, client4, client_info["mounting_dir"], "g", "read"
            )

            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir_name,
                0,
                1,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info["mounting_dir"],
                dir_name,
                0,
                500,
                iotype="touch",
            ),
        cluster_health_afterIO = check_ceph_healthly(
            client1[0],
            num_of_osds,
            len(client_info["mon_node"]),
            build,
            None,
            300,
        )
        if cluster_health_afterIO == cluster_health_beforeIO:
            log.info("cluster is healthy")
        else:
            log.error("cluster is not healty")
            return 1
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                dir_name,
                0,
                500,
                iotype="touch",
            )
            for node in client_info["osd_nodes"]:
                fs_util_v1.reboot_node(node)
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="fio",
            ),
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                dir_name,
                0,
                500,
                iotype="touch",
            )
            for node in client_info["clients"]:
                fs_util.network_disconnect(node)
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                dir_name,
                0,
                10,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                dir_name,
                0,
                500,
                iotype="touch",
            )
            for node in client_info["osd_nodes"]:
                fs_util.pid_kill(node, "osd")

        time.sleep(100)
        cluster_health_afterIO = check_ceph_healthly(
            client1[0],
            num_of_osds,
            len(client_info["mon_node"]),
            build,
            None,
            300,
        )
        if cluster_health_beforeIO == cluster_health_afterIO:
            log.info("Cluster is healthy")
        else:
            return 1
        log.info("Cleaning up!-----")
        if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
            rc = fs_util.client_clean_up(
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
                "umount",
            )
            client1[0].exec_command(
                sudo=True, cmd=f"ceph fs set {default_fs} allow_standby_replay true"
            )
            for mon in fs_util_v1.mons:
                FsUtilsV1.deamon_op(mon, "mon.ceph", "restart")
            for mon in fs_util_v1.mons:
                FsUtilsV1.check_deamon_status(mon, "mon", "active")
            for mds in fs_util_v1.mdss:
                FsUtilsV1.deamon_op(mds, rf"mds\.{fs_name}\.", "restart")
            for mds in fs_util_v1.mdss:
                FsUtilsV1.check_deamon_status(mds, rf"mds\.{fs_name}\.", "active")
        else:
            rc = fs_util.client_clean_up(
                client_info["fuse_clients"], "", client_info["mounting_dir"], "umount"
            )

            client1[0].exec_command(
                sudo=True, cmd=f"ceph fs set {default_fs} allow_standby_replay true"
            )
            for mon in fs_util_v1.mons:
                FsUtilsV1.deamon_op(mon, "mon.ceph", "restart")
            for mon in fs_util_v1.mons:
                FsUtilsV1.check_deamon_status(mon, "mon", "active")
            for mds in fs_util_v1.mdss:
                FsUtilsV1.deamon_op(mds, rf"mds\.{fs_name}\.", "restart")
            for mds in fs_util_v1.mdss:
                FsUtilsV1.check_deamon_status(mds, rf"mds\.{fs_name}\.", "active")

        log.info("Execution of Test cases CEPH-%s ended:" % (tc))
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
        for client in [client1, client2, client3, client4]:
            client[0].exec_command(
                sudo=True, cmd="mv /etc/fstab.backup /etc/fstab", check_ec=False
            )

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
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info("Cleaning up!-----")
        for client in [client1, client2, client3, client4]:
            client[0].exec_command(
                sudo=True, cmd="mv /etc/fstab.backup /etc/fstab", check_ec=False
            )
