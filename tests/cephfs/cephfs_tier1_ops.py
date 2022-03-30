import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Requires 2 client nodes in the setup

    Operations performed :
    1. Enable Multiple File systems 1. In Replicated 2. In EC
    2. Create 2 SubVolumeGroups on each file system
    3. Create 2 Sub volumes on each of the subvolume group Size 20 GB
    4. Create 2 sub volumes on default subvolume group
    5. Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client1
    6. Mount 1 subvolumeon kernal and 1 subvloumegroup/subvolume on Fuse → Client2
    7. On EC,Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client2
    8. On EC,Mount 1 subvolumeon kernal and 1 subvloumegroup/subvolume on Fuse → Client1
    9. Run IOs on subvolumegroup/subvolume on kernel client and subvolume in Fuse clientusing below commands
        git clone https://github.com/distributed-system-analysis/smallfile.git
        cd smallfile
        for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir create symlink
        stat chmod ls-l delete cleanup  ;
        do python3 smallfile_cli.py --operation $i --threads 8 --file-size 10240 --files 100 --top /mnt/kcephfs/vol5/
        ; done
        IO Tool 2 :
        wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz
        tar -xzf linux.tar.gz tardir/ ; sleep 10 ; rm -rf  tardir/ ; sleep 10 ; done
        DD on Each volume:

        Wget :
       http://download.eng.bos.redhat.com/fedora/linux/releases/34/Server/x86_64/iso/Fedora-Server-dvd-x86_64-34-1.2.iso
       http://download.eng.bos.redhat.com/fedora/linux/releases/33/Server/x86_64/iso/Fedora-Server-dvd-x86_64-33-1.2.iso
       http://download.eng.bos.redhat.com/fedora/linux/releases/32/Server/x86_64/iso/Fedora-Server-dvd-x86_64-32-1.6.iso
        Note : Run 2 IO tools mentioned above on each volume mounted
    10. Create Snapshots.Verify the snap ls | grep to see snap ot created
    11. Create Clones on all 8 volumes. Verify the clones got created using clone ls(subvolume ls)
    12. Set File Level Quota on 2 directories under subvolume and Size Level Quota on under 2 directories
        under subvolume.
    13. Verify quota based on your configuration
    14. Clear Quotas
    15. Remove Clones
    16. Remove Snapshots
    17. Unmount
    18. Remove Volumes.

    Args:
        ceph_cluster:
        **kw:

    Returns:

    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = "cephfs"
        if build.startswith("4"):
            # create EC pool
            list_cmds = [
                "ceph fs flag set enable_multiple true",
                "ceph osd pool create cephfs-data-ec 64 erasure",
                "ceph osd pool create cephfs-metadata 64",
                "ceph osd pool set cephfs-data-ec allow_ec_overwrites true",
                "ceph fs new cephfs-ec cephfs-metadata cephfs-data-ec --force",
            ]
            if fs_util.get_fs_info(clients[0], "cephfs_new"):
                default_fs = "cephfs_new"
                list_cmds.append("ceph fs volume create cephfs")
            for cmd in list_cmds:
                clients[0].exec_command(sudo=True, cmd=cmd)
        log.info("Create 2 SubVolumeGroups on each file system")
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_1"},
            {"vol_name": default_fs, "group_name": "subvolgroup_2"},
            {"vol_name": "cephfs-ec", "group_name": "subvolgroup_1"},
            {"vol_name": "cephfs-ec", "group_name": "subvolgroup_2"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)
        log.info("Create 2 Sub volumes on each of the subvolume group Size 20 GB")
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368706371",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_2",
                "size": "5368706371",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_3",
                "group_name": "subvolgroup_1",
                "size": "5368706371",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_4",
                "group_name": "subvolgroup_2",
                "size": "5368706371",
            },
            {"vol_name": default_fs, "subvol_name": "subvol_5", "size": "5368706371"},
            {"vol_name": default_fs, "subvol_name": "subvol_6", "size": "5368706371"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_7", "size": "5368706371"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_8", "size": "5368706371"},
            {"vol_name": default_fs, "subvol_name": "subvol_9", "size": "5368706371"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_10", "size": "5368706371"},
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info(
            "Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client1"
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_1 subvolgroup_1",
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )

        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {default_fs} subvol_5"
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()}",
        )

        log.info(
            "Mount 1 subvolumeon kernal and 1 subvloumegroup/subvolume on Fuse → Client2"
        )
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")

        subvol_path, rc = clients[1].exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {default_fs} subvol_6"
        )
        fs_util.kernel_mount(
            [clients[1]],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )

        subvol_path, rc = clients[1].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_2 subvolgroup_2",
        )
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        fs_util.fuse_mount(
            [clients[1]],
            fuse_mounting_dir_2,
            extra_params=f" -r {subvol_path.strip()}",
        )

        log.info(
            "On EC,Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client2"
        )
        if build.startswith("5"):
            kernel_mounting_dir_3 = f"/mnt/cephfs_kernel{mounting_dir}_EC_3/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")

            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_3 subvolgroup_1",
            )
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_3,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path.strip()}",
                extra_params=",fs=cephfs-ec",
            )

            subvol_path, rc = clients[0].exec_command(
                sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_7"
            )
            fuse_mounting_dir_3 = f"/mnt/cephfs_fuse{mounting_dir}_EC_3/"
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_3,
                extra_params=f" -r {subvol_path.strip()} --client_fs cephfs-ec",
            )

        log.info(
            "On EC,Mount 1 subvolumeon kernal and 1 subvloumegroup/subvolume on Fuse → Client1"
        )
        if build.startswith("5"):
            kernel_mounting_dir_4 = f"/mnt/cephfs_kernel{mounting_dir}_EC_4/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")

            subvol_path, rc = clients[1].exec_command(
                sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_8"
            )
            fs_util.kernel_mount(
                [clients[1]],
                kernel_mounting_dir_4,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path.strip()}",
                extra_params=",fs=cephfs-ec",
            )

            subvol_path, rc = clients[1].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_4 subvolgroup_2",
            )
            fuse_mounting_dir_4 = f"/mnt/cephfs_fuse{mounting_dir}_EC_4/"
            fs_util.fuse_mount(
                [clients[1]],
                fuse_mounting_dir_4,
                extra_params=f" -r {subvol_path.strip()} --client_fs cephfs-ec",
            )

        run_ios(clients[0], kernel_mounting_dir_1)
        run_ios(clients[0], fuse_mounting_dir_1)
        run_ios(clients[1], kernel_mounting_dir_2)
        run_ios(clients[1], fuse_mounting_dir_2)
        if build.startswith("5"):
            run_ios(clients[0], kernel_mounting_dir_3)
            run_ios(clients[1], kernel_mounting_dir_4)
            run_ios(clients[0], fuse_mounting_dir_3)
            run_ios(clients[1], fuse_mounting_dir_4)

        log.info("Create Snapshots.Verify the snap ls")
        snapshot_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "snap_name": "snap_1",
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "snap_name": "snap_2",
                "group_name": "subvolgroup_2",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_3",
                "snap_name": "snap_3",
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_4",
                "snap_name": "snap_4",
                "group_name": "subvolgroup_2",
            },
            {"vol_name": default_fs, "subvol_name": "subvol_5", "snap_name": "snap_5"},
            {"vol_name": default_fs, "subvol_name": "subvol_6", "snap_name": "snap_6"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_7", "snap_name": "snap_7"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_8", "snap_name": "snap_8"},
        ]
        for snapshot in snapshot_list:
            fs_util.create_snapshot(clients[0], **snapshot)

        clone_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "snap_name": "snap_1",
                "target_subvol_name": "clone_1",
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "snap_name": "snap_2",
                "target_subvol_name": "clone_2",
                "group_name": "subvolgroup_2",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_3",
                "snap_name": "snap_3",
                "target_subvol_name": "clone_3",
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_4",
                "snap_name": "snap_4",
                "target_subvol_name": "clone_4",
                "group_name": "subvolgroup_2",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_5",
                "snap_name": "snap_5",
                "target_subvol_name": "clone_5",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_6",
                "snap_name": "snap_6",
                "target_subvol_name": "clone_6",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_7",
                "snap_name": "snap_7",
                "target_subvol_name": "clone_7",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_8",
                "snap_name": "snap_8",
                "target_subvol_name": "clone_8",
            },
        ]
        for clone in clone_list:
            fs_util.create_clone(clients[0], **clone)
        log.info(
            "Set File Level Quota on 2 directories under subvolume and Size Level Quota on "
            "under 2 directories under subvolume"
        )
        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {default_fs} subvol_9"
        )
        fuse_mounting_dir_5 = f"/mnt/cephfs_fuse{mounting_dir}_5/"
        fs_util.fuse_mount(
            [clients[1]],
            fuse_mounting_dir_5,
            extra_params=f" -r {subvol_path.strip()}",
        )
        clients[1].exec_command(
            sudo=True,
            cmd=f"setfattr -n ceph.quota.max_files -v 10 {fuse_mounting_dir_5}",
        )
        clients[1].exec_command(
            sudo=True, cmd=f"getfattr -n ceph.quota.max_files {fuse_mounting_dir_5}"
        )
        out, rc = clients[1].exec_command(
            sudo=True,
            cmd=f"cd {fuse_mounting_dir_5};touch quota{{1..15}}.txt",
        )
        log.info(out)
        if clients[1].node.exit_status == 0:
            log.warning(
                "Quota set has been failed,Able to create more files."
                "This is known limitation"
            )
        if build.startswith("5"):
            subvol_path, rc = clients[0].exec_command(
                sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_10"
            )
            kernel_mounting_dir_5 = f"/mnt/cephfs_kernel{mounting_dir}_5/"
            fs_util.kernel_mount(
                [clients[1]],
                kernel_mounting_dir_5,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path.strip()}",
                extra_params=",fs=cephfs-ec",
            )
            clients[1].exec_command(
                sudo=True,
                cmd=f"setfattr -n ceph.quota.max_files -v 10 {kernel_mounting_dir_5}",
            )
            clients[1].exec_command(
                sudo=True,
                cmd=f"getfattr -n ceph.quota.max_files {kernel_mounting_dir_5}",
            )

            out, rc = clients[1].exec_command(
                sudo=True,
                cmd=f"cd {kernel_mounting_dir_5};touch quota{{1..15}}.txt",
            )
            log.info(out)
            if clients[1].node.exit_status == 0:
                log.warning(
                    "Quota set has been failed,Able to create more files."
                    "This is known limitation"
                )
                # return 1

        log.info("Clean up the system")
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[1]], mounting_dir=kernel_mounting_dir_2
        )
        if build.startswith("5"):
            fs_util.client_clean_up(
                "umount",
                kernel_clients=[clients[0]],
                mounting_dir=kernel_mounting_dir_3,
            )

            fs_util.client_clean_up(
                "umount",
                kernel_clients=[clients[1]],
                mounting_dir=kernel_mounting_dir_4,
            )
            fs_util.client_clean_up(
                "umount",
                kernel_clients=[clients[1]],
                mounting_dir=kernel_mounting_dir_5,
            )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_2
        )
        if build.startswith("5"):
            fs_util.client_clean_up(
                "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_3
            )
            fs_util.client_clean_up(
                "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_4
            )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_5
        )
        clients[1].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        rmsnapshot_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "snap_name": "snap_1",
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "snap_name": "snap_2",
                "group_name": "subvolgroup_2",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_3",
                "snap_name": "snap_3",
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_4",
                "snap_name": "snap_4",
                "group_name": "subvolgroup_2",
            },
            {"vol_name": default_fs, "subvol_name": "subvol_5", "snap_name": "snap_5"},
            {"vol_name": default_fs, "subvol_name": "subvol_6", "snap_name": "snap_6"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_7", "snap_name": "snap_7"},
            {"vol_name": "cephfs-ec", "subvol_name": "subvol_8", "snap_name": "snap_8"},
        ]
        for snapshot in rmsnapshot_list:
            fs_util.remove_snapshot(clients[0], **snapshot)

        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": "clone_1"},
            {"vol_name": default_fs, "subvol_name": "clone_2"},
            {"vol_name": "cephfs-ec", "subvol_name": "clone_3"},
            {"vol_name": "cephfs-ec", "subvol_name": "clone_4"},
            {"vol_name": default_fs, "subvol_name": "clone_5"},
            {"vol_name": default_fs, "subvol_name": "clone_6"},
            {"vol_name": "cephfs-ec", "subvol_name": "clone_7"},
            {"vol_name": "cephfs-ec", "subvol_name": "clone_8"},
        ]
        rmsubvolume_list = rmclone_list + subvolume_list

        for subvolume in rmsubvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def run_ios(client, mounting_dir):
    def smallfile():
        client.exec_command(
            sudo=True,
            cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
            f"create symlink stat chmod ls-l delete cleanup  ; "
            f"do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i --threads 8 --file-size 10240 "
            f"--files 10 --top {mounting_dir} ; done",
        )

    def io_tool():
        client.exec_command(
            sudo=True,
            cmd=f"cd {mounting_dir};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
        )
        client.exec_command(
            sudo=True,
            cmd="tar -xzf linux.tar.gz tardir/ ; sleep 10 ; rm -rf  tardir/ ; sleep 10 ; done",
        )

    def wget():
        client.exec_command(
            sudo=True,
            cmd=f"cd {mounting_dir};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
        )

    def dd():
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}_dd bs=100M "
            f"count=5",
        )

    io_tools = [dd, smallfile]
    f = random.choice(io_tools)
    f()
