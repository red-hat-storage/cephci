import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575622 - create a file(fileA) , create a hard link (fileB),
        remove the fileA first and then fileB and run this in a loop for some time and check for deadlock
    Procedure :
    1. Create 2 Subvolumes - each mounted on kernel and fuse.
    2. Run unlink and rename to validate if any deadlock is seen.
    3. Check for Ceph Health Status to validate any warnings or errors if any deadlock is occured.

    Clean-up:
    1. Remove files from mountpoint, Unmount subvolumes.
    3. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    4. ceph fs subvolumegroup rm <vol_name> <group_name>
    """

    try:
        tc = "CEPH-83575622"
        log.info(f"Running cephfs {tc} test case")
        fs_util_v1 = FsUtilsV1(ceph_cluster)
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
        default_fs = "cephfs"
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
        ]
        for subvolume in subvolume_list:
            fs_util_v1.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount 1 subvolume on kernel and 1 subvolume on Fuse → Client1")

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

        log.info("Run the unlink and reintegrate straydn(rename) for around 30mins")
        kernel_file_path = f"{kernel_mounting_dir_1}primary_file_k"
        fuse_file_path = f"{fuse_mounting_dir_1}primary_file_f"
        run_time = 30 * 60  # 30 minutes * 60 seconds/minute
        start_time = time.time()
        # Step 1: Create a file and hard links to 5 different files
        while time.time() - start_time < run_time:
            clients[0].exec_command(sudo=True, cmd=f"touch {kernel_file_path}")
            clients[0].exec_command(sudo=True, cmd=f"touch {fuse_file_path}")
            for i in range(1, 6):
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ln {kernel_file_path} {kernel_mounting_dir_1}hardlink{i}_file_k",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ln {fuse_file_path} {fuse_mounting_dir_1}hardlink{i}_file_f",
                )

            # Step 2: Remove the file and hard links
            for i in range(6):
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"rm -rf {kernel_mounting_dir_1}{'primary_file_k' if i == 0 else f'hardlink{i}_file_k'}",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"rm -rf {fuse_mounting_dir_1}{'primary_file_f' if i == 0 else f'hardlink{i}_file_f'}",
                )
        time.sleep(1)

        log.info(
            "Check for the Ceph Health to see if there are any deadlock bw unlink and rename."
        )
        fs_util_v1.get_ceph_health_status(clients[0])

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        fs_util_v1.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )

        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )

        for subvolume in subvolume_list:
            fs_util_v1.remove_subvolume(clients[0], **subvolume)

        fs_util_v1.remove_subvolumegroup(
            clients[0], default_fs, subvolume_group_name, force=True
        )
