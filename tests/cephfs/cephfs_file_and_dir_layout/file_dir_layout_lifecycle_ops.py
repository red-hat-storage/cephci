import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11334
    Pre-Req :
    * Configure cluster and PG's in active + clean state.
    * Configure minimum 2 clients with Fuse client and another 1 client with kernel client.

    Test the following:
    - Layout fields
    - Reading layouts with getfattr
    - Writing layouts with setfattr
    - Clearing layouts
    - Inheritance of layouts
    - Adding a data pool to the MDS

    """
    try:
        tc = "11334"
        file_name = "file"

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

        vals, rc = fs_util.getfattr(client1, client_info["mounting_dir"], file_name)
        rc = fs_util.setfattr(
            client1, "stripe_unit", "1048576", client_info["mounting_dir"], file_name
        )
        if rc == 0:
            log.info("Setfattr stripe_unit for file %s success" % file_name)
        else:
            raise CommandFailed("Setfattr stripe_unit for file %s success" % file_name)
        rc = fs_util.setfattr(
            client1, "stripe_count", "8", client_info["mounting_dir"], file_name
        )
        if rc == 0:
            log.info("Setfattr stripe_count for file %s success" % file_name)
        else:
            raise CommandFailed("Setfattr stripe_count for file %s success" % file_name)
        rc = fs_util.setfattr(
            client1, "object_size", "10485760", client_info["mounting_dir"], file_name
        )
        if rc == 0:
            log.info("Setfattr object_size for file %s success" % file_name)
        else:
            raise CommandFailed("Setfattr object_size for file %s success" % file_name)
        fs_util.create_pool(client_info["clients"][0], "new_data_pool", 64, 64)
        rc = fs_util.add_pool_to_fs(
            client_info["clients"][0], default_fs, "new_data_pool"
        )
        if 0 in rc:
            log.info("Adding new pool to cephfs success")
        else:
            raise CommandFailed("Adding new pool to cephfs failed")
        rc = fs_util.setfattr(
            client1, "pool", "new_data_pool", client_info["mounting_dir"], file_name
        )
        if rc == 0:
            log.info("Setfattr pool for file %s success" % file_name)
        else:
            raise CommandFailed("Setfattr pool for file %s success" % file_name)

        vals, rc = fs_util.getfattr(client1, client_info["mounting_dir"], file_name)
        log.info("Read individual layout fields by using getfattr:")
        for client in client1:
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.pool %s%s"
                % (client_info["mounting_dir"], file_name)
            )
            if vals["pool"] in out:
                log.info("reading pool by getfattr successfull")
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.stripe_unit  %s%s"
                % (client_info["mounting_dir"], file_name)
            )
            if vals["stripe_unit"] in out:
                log.info("reading stripe_unit by getfattr successfull")
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.stripe_count  %s%s"
                % (client_info["mounting_dir"], file_name)
            )
            if vals["stripe_count"] in out:
                log.info("reading stripe_count by getfattr successfull")
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.object_size  %s%s"
                % (client_info["mounting_dir"], file_name)
            )
            if vals["object_size"] in out:
                log.info("reading object_size by getfattr successfull")
            break
        rc = fs_util.remove_pool_from_fs(
            client_info["clients"][0], default_fs, "new_data_pool"
        )
        if 0 in rc:
            log.info("Pool removing success")
        else:
            raise CommandFailed("Pool removing  failed")
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
                client_info["fuse_clients"], "", client_info["mounting_dir"], "umount"
            )

        if rc_client == 0:
            log.info("Cleaning up successfull")
        else:
            return 1
        return 0

    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
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
                client_info["fuse_clients"], "", client_info["mounting_dir"], "umount"
            )

        if rc_client == 0:
            log.info("Cleaning up successfull")
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
