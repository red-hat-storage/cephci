import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83604070 - Create Filesystem recursively with the same name

    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create a file system with default value
    2. Create subvolume
    3. Mount file system using fuse mount, kernel mount and nfs
    4. Run IOs
    5. Unmount all the Subvolumes, Subvolume group and FS
    6. Delete FS
    7. Repeat the same steps with the same FS name for 5 runs
    """

    tc = "CEPH-83604070"
    log.info(f"Running CephFS tests {tc}")
    test_data = kw.get("test_data")
    fs_util = FsUtils(ceph_cluster, test_data=test_data)
    config = kw.get("config")
    clients = ceph_cluster.get_ceph_objects("client")
    build = config.get("build", config.get("rhbuild"))

    log.info("checking Pre-requisites")
    if not clients:
        log.info(
            f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
        )
        return 1

    fs_name = "cephfs_recursive"

    subvolume_group_name = "subvol_group1"
    subvolume_name = "subvol"
    subvolumegroup = {
        "vol_name": fs_name,
        "group_name": subvolume_group_name,
    }
    subvolume_list = [
        {
            "vol_name": fs_name,
            "subvol_name": f"{subvolume_name}_1",
            "group_name": subvolume_group_name,
        },
        {
            "vol_name": fs_name,
            "subvol_name": f"{subvolume_name}_2",
            "group_name": subvolume_group_name,
        },
        {
            "vol_name": fs_name,
            "subvol_name": f"{subvolume_name}_3",
            "group_name": subvolume_group_name,
        },
    ]

    # Run the FS lifecycle for 5 iteration
    for i in range(1, 6):
        log.info(f"Loop {i}: Started file system lifecycle")
        try:
            fs_util.prepare_clients(clients, build)
            fs_util.auth_list(clients)
            client1 = clients[0]

            # Creation of FS
            fs_util.create_fs(client1, fs_name)
            fs_util.wait_for_mds_process(client1, fs_name)

            # Creation of subvolume group and subvolumes
            fs_util.create_subvolumegroup(client1, **subvolumegroup)

            for subvolume in subvolume_list:
                fs_util.create_subvolume(client1, **subvolume)
                subvolume_size_subvol = fs_util.get_subvolume_info(
                    client=client1, **subvolume
                )
                log.info(f"Subvolume Info before IO: {subvolume_size_subvol}")

            # Mounting Subvolume1 on Fuse
            log.info(f"Mount subvolume {subvolume_name}_1 on Fuse Client")
            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )

            fuse_mount_dir = f"/mnt/cephfs_fuse{mounting_dir}_1/"
            subvol_path_fuse, rc = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} {subvolume_name}_1 {subvolume_group_name}",
            )
            fs_util.fuse_mount(
                [client1],
                fuse_mount_dir,
                extra_params=f" -r {subvol_path_fuse.strip()} --client_fs {fs_name}",
            )

            client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mount_dir}/fuse_ios")
            files = config.get("files", 10)
            file_size = config.get("file_size", 1024)
            threads = config.get("threads", 10)
            client1.exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create "
                f"--threads {threads} --file-size {file_size} --files {files} --response-times Y --top "
                f"{fuse_mount_dir}/fuse_ios",
                long_running=True,
            )

            # Mounting Subvolume2 on Kernel
            log.info(f"Mount subvolume {subvolume_name}_2 on Kernel Client")
            kernel_mount_dir = f"/mnt/cephfs_kernel{mounting_dir}_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            subvol_path_kernel, rc = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} {subvolume_name}_2 {subvolume_group_name}",
            )
            fs_util.kernel_mount(
                [client1],
                kernel_mount_dir,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path_kernel.strip()}",
                extra_params=f",fs={fs_name}",
            )

            client1.exec_command(
                sudo=True, cmd=f"mkdir -p {kernel_mount_dir}/kernel_ios"
            )
            client1.exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create "
                f"--threads {threads} --file-size {file_size} --files {files} --response-times Y --top "
                f"{kernel_mount_dir}/kernel_ios",
                long_running=True,
            )

            # Mounting Subvolume3 on nfs
            log.info(f"Mount subvolume {subvolume_name}_3 on NFS Client")
            nfs_servers = ceph_cluster.get_ceph_objects("nfs")
            nfs_server = nfs_servers[0].node.hostname
            nfs_name = "cephfs-nfs"

            client1.exec_command(
                sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
            )
            if not wait_for_process(
                client=client1, process_name=nfs_name, ispresent=True
            ):
                raise CommandFailed("Failed to create nfs cluster")
            log.info("ceph nfs cluster created successfully")

            nfs_export_name = "/export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )

            subvol_path_nfs, rc = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} {subvolume_name}_3 {subvolume_group_name}",
            )
            export_path = f"{subvol_path_nfs}"
            nfs_mounting_dir = "/mnt/nfs_" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )

            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )

            rc = fs_util.cephfs_nfs_mount(
                client1, nfs_server, nfs_export_name, nfs_mounting_dir
            )
            if not rc:
                log.error("cephfs nfs export mount failed")
                return 1
            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}/nfs_dir")
            client1.exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create "
                f"--threads {threads} --file-size {file_size} --files {files} --response-times Y --top "
                f"{nfs_mounting_dir}/nfs_dir",
                long_running=True,
            )

            for subvolume in subvolume_list:
                subvolume_size_subvol = fs_util.get_subvolume_info(
                    client=client1, **subvolume
                )
                log.info(f"Subvolume Info after IO: {subvolume_size_subvol}")

        except Exception as e:
            log.info(e)
            log.info(traceback.format_exc())
            return 1

        finally:
            fs_util.client_clean_up(
                "umount", fuse_clients=[client1], mounting_dir=fuse_mount_dir
            )

            fs_util.client_clean_up(
                "umount",
                kernel_clients=[client1],
                mounting_dir=kernel_mount_dir,
            )

            client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/*")
            client1.exec_command(sudo=True, cmd=f"umount {nfs_mounting_dir}")
            client1.exec_command(
                sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/", check_ec=False
            )
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
                check_ec=False,
            )

            for subvolume in subvolume_list:
                fs_util.remove_subvolume(client1, **subvolume)

            fs_util.remove_subvolumegroup(client1, **subvolumegroup)

            fs_util.remove_fs(client1, fs_name)
            log.info(f"Loop {i}: Completed file system lifecycle")

    log.info(f"Completed CephFS tests {tc}")
    return 0
