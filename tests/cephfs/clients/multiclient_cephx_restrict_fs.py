import timeit
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11338:
    Configure CephFS with multiple clients using cephx and restrict
    Filesystem r/w access for different clients, Path restrictions,
    OSD restrictions and layout modification Restrictions
    Args:
        ceph_cluster:
        **kw:

    Returns:
        0--> if TC PASS
        1 --> if TC FAIL

    """
    try:
        start = timeit.default_timer()
        dir_name = "dir"
        log.info("Running cephfs 11338 test case")
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        default_fs = "cephfs" if not erasure else "cephfs-ec"

        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
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
        dirs, rc = fs_util.mkdir(client1, 1, 3, client_info["mounting_dir"], dir_name)
        if rc == 0:
            log.info("Directories created")
        dirs = dirs.split("\n")
        """
        new clients with restrictions
        """
        new_client1_name = client_info["fuse_clients"][0].node.hostname + "_%s" % (
            dirs[0]
        )
        new_client2_name = client_info["fuse_clients"][1].node.hostname + "_%s" % (
            dirs[0]
        )
        new_client3_name = client_info["kernel_clients"][0].node.hostname + "_%s" % (
            dirs[1]
        )
        new_client3_mouting_dir = "/mnt/%s_%s/" % (
            client_info["kernel_clients"][0].node.hostname,
            dirs[1],
        )
        new_client2_mouting_dir = "/mnt/%s_%s/" % (
            client_info["fuse_clients"][1].node.hostname,
            dirs[0],
        )
        new_client1_mouting_dir = "/mnt/%s_%s/" % (
            client_info["fuse_clients"][0].node.hostname,
            dirs[0],
        )
        # import pdb

        # pdb.set_trace()
        rc1 = fs_util_v1.auth_list(
            client1, path=dirs[0], permission="rw", mds=True, fs_name=default_fs
        )
        # pdb.set_trace()
        rc2 = fs_util_v1.auth_list(
            client2, path=dirs[0], permission="r", mds=True, fs_name=default_fs
        )
        # pdb.set_trace()
        rc3 = fs_util_v1.auth_list(
            client3, path=dirs[1], permission="*", mds=True, fs_name=default_fs
        )
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            log.error("auth list failed")
            return 1
        fs_util_v1.fuse_mount(
            client1,
            new_client1_mouting_dir,
            new_client_hostname=new_client1_name,
            extra_params=f"-r /{dirs[0]} --client_fs {default_fs}",
        )
        fs_util_v1.fuse_mount(
            client2,
            new_client2_mouting_dir,
            new_client_hostname=new_client2_name,
            extra_params=f"-r /{dirs[0]} --client_fs {default_fs}",
        )

        fs_util_v1.kernel_mount(
            client3,
            new_client3_mouting_dir,
            ",".join(client_info["mon_node_ip"]),
            new_client_hostname=new_client3_name,
            sub_dir=f"/{dirs[1]}",
            extra_params=f",fs={default_fs}",
        )
        _, rc = fs_util.stress_io(
            client1,
            new_client1_mouting_dir,
            "",
            0,
            1,
            iotype="smallfile_create",
            fnum=100,
            fsize=10,
        )

        if rc == 0:
            log.info("Permissions set  for client %s is working " % new_client1_name)
        else:
            log.error("Permissions set  for client %s is failed" % new_client1_name)
            return 1
        _, rc = fs_util.stress_io(
            client1,
            new_client1_mouting_dir,
            "",
            0,
            1,
            iotype="smallfile_delete",
            fnum=100,
            fsize=10,
        )
        if rc == 0:
            log.info(
                "Permissions set  for client %s is working properly" % new_client1_name
            )
        else:
            log.error("Permissions set  for client %s is failed" % new_client1_name)
            return 1
        try:
            _, rc = fs_util.stress_io(
                client2, new_client2_mouting_dir, "", 0, 1, iotype="touch"
            )
        except CommandFailed:
            log.info(
                "Permissions set  for client %s is working properly" % new_client2_name
            )

        _, rc = fs_util.stress_io(
            client3,
            new_client3_mouting_dir,
            "",
            0,
            1,
            iotype="smallfile_create",
            fnum=100,
            fsize=10,
        )

        if rc == 0:
            log.info(
                "Permissions set  for client %s is working properly" % new_client3_name
            )
        else:
            log.error("Permissions set  for client %s is failed" % new_client3_name)
            return 1
        _, rc = fs_util.stress_io(
            client3,
            new_client3_mouting_dir,
            "",
            0,
            1,
            iotype="smallfile_delete",
            fnum=100,
            fsize=10,
        )
        if rc == 0:
            log.info("Permissions set  for client %s is working properly")
        else:
            log.error("Permissions set  for client %s is failed")
            return 1

        fs_util.client_clean_up(
            client1, "", new_client1_mouting_dir, "umount", client_name=new_client1_name
        )
        fs_util.client_clean_up(
            client2, "", new_client2_mouting_dir, "umount", client_name=new_client2_name
        )
        fs_util.client_clean_up(
            "", client3, new_client3_mouting_dir, "umount", client_name=new_client3_name
        )

        fs_util_v1.auth_list(
            client1, path=dirs[0], permission="rw", osd=True, fs_name=default_fs
        )
        fs_util_v1.auth_list(
            client3, path=dirs[1], permission="r", osd=True, fs_name=default_fs
        )

        fs_util_v1.fuse_mount(
            client1,
            new_client1_mouting_dir,
            new_client_hostname=new_client1_name,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util_v1.kernel_mount(
            client3,
            new_client3_mouting_dir,
            ",".join(client_info["mon_node_ip"]),
            new_client_hostname=new_client3_name,
            extra_params=f",fs={default_fs}",
        )

        fs_util.stress_io(
            client1,
            new_client1_mouting_dir,
            "",
            0,
            1,
            iotype="smallfile_delete",
            fnum=100,
            fsize=10,
        )
        try:
            if client_info["kernel_clients"][0].pkg_type == "rpm":
                client_info["kernel_clients"][0].exec_command(
                    cmd="sudo dd if=/dev/zero of=%s/file bs=10M count=10"
                    % new_client3_mouting_dir
                )

        except CommandFailed as e:
            log.info(e)
            log.info(
                "Permissions set  for client %s is working properly"
                % (client_info["kernel_clients"][0].node.hostname + "_" + (dirs[1]))
            )

        fs_util.client_clean_up(
            client1,
            "",
            new_client1_mouting_dir,
            "umount",
            client_name=client_info["fuse_clients"][0].node.hostname
            + "_%s" % (dirs[0]),
        )

        fs_util.client_clean_up(
            "", client3, new_client3_mouting_dir, "umount", client_name=new_client3_name
        )
        fs_util_v1.auth_list(
            client1, path=dirs[0], layout_quota="rwp", fs_name=default_fs
        )
        fs_util_v1.auth_list(
            client3, path=dirs[1], layout_quota="rw", fs_name=default_fs
        )

        fs_util_v1.fuse_mount(
            client1, new_client1_mouting_dir, new_client_hostname=new_client1_name
        )
        fs_util_v1.kernel_mount(
            client3,
            new_client3_mouting_dir,
            ",".join(client_info["mon_node_ip"]),
            new_client_hostname=new_client3_name,
        )
        file_name = "file1"
        client_info["fuse_clients"][0].exec_command(
            cmd="sudo touch %s/%s" % (new_client1_mouting_dir, file_name)
        )
        client_info["fuse_clients"][0].exec_command(
            cmd="sudo mkdir  %s/%s" % (new_client1_mouting_dir, dirs[0])
        )

        try:
            fs_util.setfattr(
                client3, "stripe_unit", "1048576", new_client3_mouting_dir, file_name
            )
            fs_util.setfattr(
                client3, "max_bytes", "100000000", new_client3_mouting_dir, dirs[1]
            )
        except CommandFailed:
            log.info("Permission denied for setting attrs,success")
        fs_util.setfattr(
            client1, "stripe_unit", "1048576", new_client1_mouting_dir, file_name
        )
        fs_util.setfattr(
            client1, "max_bytes", "100000000", new_client1_mouting_dir, dirs[0]
        )
        fs_util.client_clean_up(
            client1, "", new_client1_mouting_dir, "umount", client_name=new_client1_name
        )

        fs_util.client_clean_up(
            "", client3, new_client3_mouting_dir, "umount", client_name=new_client3_name
        )
        fs_util.client_clean_up(
            client_info["fuse_clients"],
            client_info["kernel_clients"],
            client_info["mounting_dir"],
            "umount",
        )
        print("Script execution time:------")
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)
        print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
