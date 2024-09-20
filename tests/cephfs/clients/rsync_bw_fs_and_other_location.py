import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11298 - rsync files and directories to outside the filesystem and vice-versa
        Pre-requisites:
    1. Create cephfs volume
       ceph fs create volume create <vol_name>
    2. Create directories in the cephfs volume

    Test operation:

    1. Run IO's and fill data into kernel and fuse directories.
    2. rsync the data from source to target.
    3. rsync the data from target to source.

    Clean-up:
    1. remove directories
    2. unmount paths
    """
    try:
        tc = "11298"
        source_dir = "/mnt/source"
        target_dir = "target"
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

        for client in client_info["clients"]:
            client.exec_command(cmd="sudo rm -rf  %s" % source_dir)
            client.exec_command(cmd="sudo mkdir %s" % source_dir)

        for client in client_info["clients"]:
            client.exec_command(
                cmd="sudo mkdir %s%s" % (client_info["mounting_dir"], target_dir)
            )
            break

        with parallel() as p:
            p.spawn(fs_util.stress_io, client1, source_dir, "", 0, 100, iotype="touch")
            p.spawn(fs_util.read_write_IO, client1, source_dir, "g", "write")
            p.spawn(fs_util.stress_io, client2, source_dir, "", 0, 10, iotype="dd")
            p.spawn(
                fs_util.stress_io, client3, source_dir, "", 0, 10, iotype="smallfile"
            )
            p.spawn(fs_util.stress_io, client4, source_dir, "", 0, 1, iotype="fio")
            for op in p:
                return_counts1, rc = op

        with parallel() as p:
            p.spawn(
                fs_util.rsync,
                client1,
                source_dir,
                "%s%s" % (client_info["mounting_dir"], target_dir),
            )
            p.spawn(
                fs_util.rsync,
                client2,
                source_dir,
                "%s%s" % (client_info["mounting_dir"], target_dir),
            )
            p.spawn(
                fs_util.rsync,
                client3,
                source_dir,
                "%s%s" % (client_info["mounting_dir"], target_dir),
            )

            p.spawn(
                fs_util.rsync,
                client4,
                source_dir,
                "%s%s" % (client_info["mounting_dir"], target_dir),
            )
            for op in p:
                return_counts2, rc = op

        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info["mounting_dir"],
                target_dir,
                0,
                100,
                iotype="touch",
            )
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info["mounting_dir"],
                target_dir,
                0,
                11,
                iotype="dd",
            )
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info["mounting_dir"],
                target_dir,
                0,
                3,
                iotype="fio",
            )
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info["mounting_dir"],
                target_dir,
                0,
                1,
                iotype="fio",
            )
            for op in p:
                return_counts3, rc = op
        with parallel() as p:
            p.spawn(
                fs_util.rsync,
                client1,
                "%s%s/*" % (client_info["mounting_dir"], target_dir),
                source_dir,
            )
            p.spawn(
                fs_util.rsync,
                client2,
                "%s%s/*" % (client_info["mounting_dir"], target_dir),
                source_dir,
            )
            p.spawn(
                fs_util.rsync,
                client3,
                "%s%s/*" % (client_info["mounting_dir"], target_dir),
                source_dir,
            )
            p.spawn(
                fs_util.rsync,
                client4,
                "%s%s/*" % (client_info["mounting_dir"], target_dir),
                source_dir,
            )
            for op in p:
                return_counts4, rc = op

        rc = (
            list(return_counts1.values())
            + list(return_counts2.values())
            + list(return_counts3.values())
            + list(return_counts4.values())
        )
        rc_set = set(rc)
        if len(rc_set) == 1:
            print("Test case CEPH-%s passed" % (tc))
        else:
            print(("Test case CEPH-%s failed" % (tc)))
        log.info("Test completed for CEPH-%s" % (tc))
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Cleaning up!-----")
        for client in client_info["clients"]:
            client.exec_command(cmd="sudo rm -rf  %s" % source_dir)
        for client in client_info["clients"]:
            client.exec_command(
                cmd="sudo rm -rf %s%s" % (client_info["mounting_dir"], target_dir)
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
        else:
            return 1
