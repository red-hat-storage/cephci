import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575762 - unlink and create operations concurrently.
    Procedure :
    1. Create 3 Subvolumes - each mounted on kernel, fuse and nfs.
    2. Run create and unlink ops for few iterations(10 * 1024files) each on all the clients.
    3. Check for Ceph Health Status to validate if there is no Health Warn or Health Error status.

    Clean-up:
    1. Remove files from mountpoint, Unmount subvolumes.
    3. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    4. ceph fs subvolumegroup rm <vol_name> <group_name>
    """

    try:
        tc = "CEPH-83575762"
        log.info(f"Running cephfs {tc} test case")
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(clients[0], default_fs)

        if not fs_details:
            fs_util_v1.create_fs(clients[0], default_fs)
        subvolume_group_name = "subvol_group1"
        subvolume_name = "subvol"

        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": subvolume_group_name,
        }
        fs_util_v1.create_subvolumegroup(clients[0], **subvolumegroup)
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": f"{subvolume_name}_1",
                "group_name": subvolume_group_name,
            },
            {
                "vol_name": default_fs,
                "subvol_name": f"{subvolume_name}_2",
                "group_name": subvolume_group_name,
            },
            {
                "vol_name": default_fs,
                "subvol_name": f"{subvolume_name}_3",
                "group_name": subvolume_group_name,
            },
        ]
        for subvolume in subvolume_list:
            fs_util_v1.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info(
            "Mount 1 subvolume on kernel , 1 subvolume on Fuse and 1 subvolume as NFS mount on  → Client1"
        )

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util_v1.get_mon_node_ips()
        subvol_path_kernel, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}_1 {subvolume_group_name}",
        )
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path_kernel.strip()}",
            extra_params=f",fs={default_fs}",
        )

        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path_fuse, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}_2 {subvolume_group_name}",
        )
        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path_fuse.strip()} --client_fs {default_fs}",
        )

        nfs_name = "cephfs-nfs"
        nfs_mountung_dir_1 = f"/mnt/cephfs-nfs{mounting_dir}_1/"
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        clients[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        clients[0].exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=clients[0], process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        subvol_path_nfs, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}_3 {subvolume_group_name}",
        )
        nfs_export_name_1 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        fs_util_v1.create_nfs_export(
            clients[0], nfs_name, nfs_export_name_1, default_fs, path=subvol_path_nfs
        )
        rc = fs_util_v1.cephfs_nfs_mount(
            clients[0], nfs_server, nfs_export_name_1, nfs_mountung_dir_1
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1

        log.info("Run create and unlink ops.")
        for j in range(10):
            for i in range(1024):
                filename = f"file-{i}"
                for directory in [
                    kernel_mounting_dir_1,
                    fuse_mounting_dir_1,
                    nfs_mountung_dir_1,
                ]:
                    clients[0].exec_command(
                        sudo=True, cmd=f"touch {directory}{filename}"
                    )
                    clients[0].exec_command(
                        sudo=True, cmd=f"rm -rf {directory}{filename}"
                    )
        log.info(
            "Check for the Ceph Health to see if there are any crash and Health Errors."
        )
        ceph_health = fs_util_v1.get_ceph_health_status(clients[0])
        log.info(ceph_health)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        fs_util_v1.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util_v1.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=nfs_mountung_dir_1
        )

        for subvolume in subvolume_list:
            fs_util_v1.remove_subvolume(clients[0], **subvolume)

        fs_util_v1.remove_subvolumegroup(
            clients[0], default_fs, subvolume_group_name, force=True
        )
