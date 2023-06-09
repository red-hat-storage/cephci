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
    CEPH-11231 - Active-active multi MDS with standby and pinning subtrees to all MDS, perform system tests like,
    OSD/MON/MDS reboot/service restart/kernal crash, etc
    Args:
        ceph_cluster:
        **kw:
    Test Steps Perofrmed:
    1. Mount four clients 2 with fuse and 2 with Kernel.
    2. create 50 directories and pin 1-25 to rank 0 and 25 -50 to rank 1
    3. run IOs and parallel perform below operations on mon and osd nodes
        reboot
        network outage
        kill Services
    Returns:
        0 --> if testcase PASS
        1 --> if Test case FAIL

    """
    try:
        start = timeit.default_timer()
        tc = "11231"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
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

        fs_util_v1.fuse_mount(client1, client_info["mounting_dir"], fstab=True)
        fs_util_v1.fuse_mount(client2, client_info["mounting_dir"], fstab=True)
        fs_util_v1.kernel_mount(
            client3,
            client_info["mounting_dir"],
            ",".join(client_info["mon_node_ip"]),
            fstab=True,
        )
        fs_util_v1.kernel_mount(
            client4,
            client_info["mounting_dir"],
            ",".join(client_info["mon_node_ip"]),
            fstab=True,
        )
        rc = fs_util_v1.activate_multiple_mdss(client_info["clients"][0:])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            raise CommandFailed("Activate multiple mdss failed")
        client1[0].exec_command(
            sudo=True, cmd="mkdir -p %s%s" % (client_info["mounting_dir"], dir_name)
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
                1,
                iotype="smallfile",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                dir_name,
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
        result = "Data validation success"
        if result == "Data validation success":
            print("Data validation success")
            fs_util_v1.activate_multiple_mdss(client_info["clients"][0:])
            log.info("Execution of Test case CEPH-%s started:" % (tc))
            for client in client1:
                client.exec_command(
                    cmd="sudo mkdir -p %s%s_{1..50}"
                    % (client_info["mounting_dir"], dir_name)
                )
                if client.node.exit_status == 0:
                    log.info("directories created succcessfully")
                else:
                    raise CommandFailed("directories creation failed")
            with parallel() as p:
                p.spawn(
                    fs_util.pinning,
                    client1,
                    1,
                    25,
                    client_info["mounting_dir"],
                    dir_name,
                    0,
                )
                p.spawn(
                    fs_util.pinning,
                    client3,
                    26,
                    50,
                    client_info["mounting_dir"],
                    dir_name,
                    1,
                )
            with parallel() as p:
                p.spawn(
                    fs_util.max_dir_io,
                    client1,
                    client_info["mounting_dir"],
                    dir_name,
                    1,
                    25,
                    1000,
                )
                p.spawn(
                    fs_util.max_dir_io,
                    client3,
                    client_info["mounting_dir"],
                    dir_name,
                    26,
                    50,
                    1000,
                )
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
                for node in client_info["mon_node"]:
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
                for node in client_info["mon_node"]:
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
                for node in client_info["mon_node"]:
                    fs_util.pid_kill(node, "mon")
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
                for node in client_info["osd_nodes"]:
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
                for node in client_info["osd_nodes"]:
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
                for mon in fs_util_v1.mons:
                    FsUtilsV1.deamon_op(mon, "mon", "restart")
            log.info("Execution of Test case CEPH-%s ended:" % (tc))
            log.info("Cleaning up!-----")
            if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
                rc_client = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    client_info["kernel_clients"],
                    client_info["mounting_dir"],
                    "umount",
                )
            else:
                rc_client = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    "",
                    client_info["mounting_dir"],
                    "umount",
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
            rc = fs_util_v1.client_clean_up(
                "umount",
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
            )
        else:
            rc = fs_util_v1.client_clean_up(
                "umount", client_info["fuse_clients"], "", client_info["mounting_dir"]
            )
        if rc == 0:
            log.info("Cleaning up successfull")
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
