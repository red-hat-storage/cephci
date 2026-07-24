import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83586294 - Ensure there are no reports of oversized cache warnings from MDS

    Procedure :
    - Enable standby-replay on Filesystem
    - Create 10 Subvolumes
    - Run Crefi on all 10 subvolumes.
    - Validate if MDS is reporting any Oversized Cache Warnings

    Clean-up:
    Delete all Subvolumes created
    """

    try:
        tc = "CEPH-CEPH-83586294"
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
        subvolume_name = "subvol_"

        log.info("Change the max_mds to 1")
        cmd = f"ceph fs set {default_fs} max_mds 1"
        clients[0].exec_command(sudo=True, cmd=cmd)

        log.info(f"Enable standby replay to {default_fs}")
        cmd = f"ceph fs set {default_fs} allow_standby_replay true"
        clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Validate if Standby Replay is enabled")
        fs_util_v1.wait_for_stable_fs(clients[0], standby_replay="true")

        log.info("Setting log_to_file of mds to True")
        fs_util_v1.config_set_runtime(clients[0], "mds", "log_to_file", "true")

        log.info("Setting debug_ms of mds to 1")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_ms", 1)

        log.info("Setting debug_mds of mds to 20")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_mds", 20)

        log.info("Setting mds_cache_memory_limit of mds to 4294967296")
        fs_util_v1.config_set_runtime(
            clients[0], "mds", "mds_cache_memory_limit", 4294967296
        )

        log.info("Create a SubvolumeGroup")
        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": subvolume_group_name,
        }
        fs_util_v1.create_subvolumegroup(clients[0], **subvolumegroup)

        log.info("Create 10 Subvolumes")
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": subvolume_name,
                "group_name": subvolume_group_name,
            },
        ]
        for i in range(1, 11):
            subvolume_list[0]["subvol_name"] = subvolume_name + str(i)
            fs_util_v1.create_subvolume(clients[0], **subvolume_list[0])

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        log.info("Create 5 mount paths and mount 5 subvolumes using Kernel Client")
        mon_node_ips = fs_util_v1.get_mon_node_ips()
        kernel_subvol_mount_paths = []
        for i in range(1, 6):
            subvol_path_kernel, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}{i} {subvolume_group_name}",
            )
            kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_{i}/"
            fs_util_v1.kernel_mount(
                [clients[0]],
                kernel_mounting_dir,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path_kernel.strip()}",
                extra_params=f",fs={default_fs}",
            )
            kernel_subvol_mount_paths.append(kernel_mounting_dir.strip())

        log.info("Create 5 mount paths and mount 5 subvolumes using Fuse Client")
        fuse_subvol_mount_paths = []
        for i in range(6, 11):
            subvol_path_fuse, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}{i} {subvolume_group_name}",
            )
            fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}_{i}/"
            fs_util_v1.fuse_mount(
                [clients[0]],
                fuse_mounting_dir,
                extra_params=f" -r {subvol_path_fuse.strip()} --client_fs {default_fs}",
            )
            fuse_subvol_mount_paths.append(fuse_mounting_dir.strip())

        log.info("Clone Crefi IO Tool and run IO's on all 10 Subvolumes")
        working_dir = "/home/cephuser"
        crefi_url = "https://github.com/vijaykumar-koppad/Crefi.git"
        crefi_cli_cmd = "python3 crefi.py --fop create --multi -b 100 -d 100 -n 100 -t text --random --min=1K --max=8K"
        clients[0].exec_command(
            sudo=True, cmd=f"cd {working_dir} ; git clone {crefi_url}"
        )
        for kernel_path in kernel_subvol_mount_paths:
            log.info(kernel_path)
            clients[0].exec_command(
                sudo=True, cmd=f"cd {working_dir}/Crefi ; {crefi_cli_cmd} {kernel_path}"
            )
        for fuse_path in fuse_subvol_mount_paths:
            log.info(fuse_path)
            clients[0].exec_command(
                sudo=True, cmd=f"cd {working_dir}/Crefi ; {crefi_cli_cmd} {fuse_path}"
            )

        log.info(
            "Validate if Ceph State is Healthy without any MDS OverSized Cache Warnings Reported"
        )
        fs_util_v1.get_ceph_health_status(clients[0])
        out, rc = clients[0].exec_command(sudo=True, cmd="ceph -s")
        log.info(out)

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Unmount all 10 Subvolumes")
        for i in range(1, 6):
            kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_{i}/"
            fs_util_v1.client_clean_up(
                "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir
            )
        for i in range(6, 11):
            fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}_{i}/"
            fs_util_v1.client_clean_up(
                "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir
            )

        log.info("Delete all 10 Subvolumes and SubvolumeGroup")
        for i in range(1, 11):
            subvolume_list[0]["subvol_name"] = subvolume_name + str(i)
            fs_util_v1.remove_subvolume(clients[0], **subvolume_list[0])
        fs_util_v1.remove_subvolumegroup(
            clients[0], default_fs, subvolume_group_name, force=True
        )

        log.info("Remove Crefi Dir")
        clients[0].exec_command(sudo=True, cmd=f"rm -rf {working_dir}/Crefi")

        log.info("Reset all configs")
        cmd = f"ceph fs set {default_fs} max_mds 2"
        clients[0].exec_command(sudo=True, cmd=cmd)

        cmd = f"ceph fs set {default_fs} allow_standby_replay false"
        clients[0].exec_command(sudo=True, cmd=cmd)

        fs_util_v1.wait_for_stable_fs(clients[0], standby_replay="false")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_mds", 1)
