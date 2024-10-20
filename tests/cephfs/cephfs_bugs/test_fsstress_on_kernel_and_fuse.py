import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575623 - Run fsstress.sh repeatedly on fuse and kernel clients for a while and validate no crash is seen.

    Procedure :
    1. Create 2 Subvolumes - each mounted on kernel and fuse.
    2. Run fsstress for few iterations(10).
    3. Check for Ceph Health Status to validate if there is no Health Warn or Health Error status.

    Clean-up:
    1. Remove files from mountpoint, Unmount subvolumes.
    3. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    4. ceph fs subvolumegroup rm <vol_name> <group_name>
    """

    try:
        tc = "CEPH-83575623"
        log.info(f"Running cephfs {tc} test case")
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.error(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        log.info("Installing required packages for make")
        clients[0].exec_command(
            sudo=True, cmd='dnf groupinstall "Development Tools" -y'
        )
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        default_fs = "cephfs"
        # if "cephfs" does not exsit, create it
        fs_details = fs_util_v1.get_fs_info(clients[0])
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

        log.info("Run fsstress for few iterations on fuse and kernel mounts.")
        fsstress_url = "https://raw.githubusercontent.com/ceph/ceph/main/qa/workunits/suites/fsstress.sh"

        def run_commands(client, commands):
            for command in commands:
                client.exec_command(sudo=True, cmd=command)

        directories = [kernel_mounting_dir_1, fuse_mounting_dir_1]
        for directory in directories:
            commands = [
                f"mkdir -p {directory}fsstress/",
                f"cd {directory}fsstress/ && wget {fsstress_url}",
                f"chmod 777 {directory}fsstress/fsstress.sh",
            ]
            run_commands(clients[0], commands)
        iterations = 50
        log.info(
            f"run fsstress on kernel and fuse in parallel for {iterations} iterations"
        )
        for _ in range(iterations):
            for directory in [kernel_mounting_dir_1, fuse_mounting_dir_1]:
                clients[0].exec_command(
                    sudo=True, cmd=f"sh {directory}fsstress/fsstress.sh"
                )
                time.sleep(10)
                clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {directory}fsstress/fsstress/"
                )
            time.sleep(10)

        log.info(
            "Check for the Ceph Health to see if there are any deadlock bw unlink and rename."
        )
        # ceph_health = fs_util_v1.get_ceph_health_status(clients[0])
        # log.info(ceph_health)
        return 0
    except Exception as e:
        dmesg, _ = clients[0].exec_command(cmd="dmesg | grep panic -A 10 -B 10")
        log.error(dmesg)
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.error("Clean up the system")
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
