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
    CEPH-11233 - Subtree Split and Subtree merging by pinning Subtrees/directories to MDS.
    Args:
        ceph_cluster:
        **kw:
    Steps Performed:
    1. Create FS and mount on all the ways (Fuse, kernel, NFS)
    2. Write Data to the mounts
    3. Create directory and pin it to rank 0
    4. Enable allow_dir_fragmentation flag and set max_mds to 2
    5. Stress IO and check if the load is shared upon 2 MDS using get subtrees

    Returns:
        0 --> if test PASS
        1 --> if test FAIL

    """
    try:
        start = timeit.default_timer()
        tc = "11233"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        clients = ceph_cluster.get_ceph_objects("client")
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
        print(rc1, rc2, rc3, rc4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            log.error("auth list failed")
            return 1
        default_fs = "cephfs"
        client1[0].exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")
        fs_util_v1.fuse_mount(client1, client_info["mounting_dir"])
        fs_util_v1.fuse_mount(client2, client_info["mounting_dir"])
        fs_util_v1.kernel_mount(
            client3, client_info["mounting_dir"], ",".join(client_info["mon_node_ip"])
        )
        fs_util_v1.kernel_mount(
            client4, client_info["mounting_dir"], ",".join(client_info["mon_node_ip"])
        )

        rc = fs_util_v1.activate_multiple_mdss(client_info["clients"][0:])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            raise CommandFailed("Activate multiple mdss failed")

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
        result = "Data validation success tested"
        if "Data validation success" in result:
            print("Data validation success")
            tc = "11233"
            log.info("Execution of Test cases %s started:" % (tc))
            fs_util.allow_dir_fragmentation(client_info["clients"][0:])
            log.info("Creating directory:")
            for node in client_info["fuse_clients"]:
                out, rc = node.exec_command(
                    cmd="sudo mkdir -p %s%s" % (client_info["mounting_dir"], dir_name)
                )
                node.exec_command(
                    sudo=True,
                    cmd=f"setfattr -n ceph.dir.pin -v 0 {client_info['mounting_dir']}{dir_name}",
                )
                print(out)
                break
            active_mds_list = fs_util_v1.get_active_mdss(client1[0])
            node1_before_io, _, rc = fs_util_v1.get_mds_info(
                *active_mds_list, info="get subtrees"
            )
            if rc == 0:
                log.info("Got mds subtree info")
            else:
                raise CommandFailed("Mds info command failed")

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info["mounting_dir"],
                    dir_name,
                    0,
                    1000,
                    iotype="touch",
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dir_name,
                    1000,
                    2000,
                    iotype="touch",
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info["mounting_dir"],
                    dir_name,
                    2000,
                    3000,
                    iotype="touch",
                )
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info["mounting_dir"],
                    dir_name,
                    3000,
                    4000,
                    iotype="touch",
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    dir_name,
                    4000,
                    5000,
                    iotype="touch",
                )

            node1_after_io, _, rc = fs_util_v1.get_mds_info(
                *active_mds_list, info="get subtrees"
            )
            if rc == 0:
                log.info("Got mds subtree info")
            else:
                raise CommandFailed("Mds info command failed")

            rc = fs_util.client_clean_up(
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
            )
            if rc == 0:
                log.info("Cleaning mount success")
            else:
                raise CommandFailed("Cleaning mount failed")

            node1_after_del, _, rc = fs_util_v1.get_mds_info(
                *active_mds_list, info="get subtrees"
            )
            if rc == 0:
                log.info("Got mds subtree info")
            else:
                raise CommandFailed("Mds info command failed")

            log.info("Execution of Test case 11233 ended:")
            print("Results:")
            if node1_before_io != node1_after_io and node1_after_io != node1_after_del:
                log.info(node1_before_io)
                log.info(node1_after_del)
                log.info(node1_after_io)
                log.info("Test case %s Passed" % (tc))
            else:
                return 1

            if client3[0].pkg_type != "deb" and client4[0].pkg_type != "deb":
                rc_client = fs_util.client_clean_up(
                    client_info["fuse_clients"],
                    client_info["kernel_clients"],
                    client_info["mounting_dir"],
                    "umount",
                )
                rc_mds = fs_util_v1.mds_cleanup(clients, None)

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
