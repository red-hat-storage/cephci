import traceback
import logging
import random
import string

from ceph.parallel import parallel

from tests.cephfs.cephfs_utilsV1 import FsUtils

log = logging.getLogger(__name__)


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
        for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir create symlink stat chmod ls-l delete cleanup  ; do python3 smallfile_cli.py --operation $i --threads 8 --file-size 10240 --files 100 --top /mnt/kcephfs/vol5/ ; done
        IO Tool 2 :
        wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz
        tar -xzf linux.tar.gz tardir/ ; sleep 10 ; rm -rf  tardir/ ; sleep 10 ; done
        DD on Each volume:
         
        Wget : http://download.eng.bos.redhat.com/fedora/linux/releases/34/Server/x86_64/iso/Fedora-Server-dvd-x86_64-34-1.2.iso
        http://download.eng.bos.redhat.com/fedora/linux/releases/33/Server/x86_64/iso/Fedora-Server-dvd-x86_64-33-1.2.iso
        http://download.eng.bos.redhat.com/fedora/linux/releases/32/Server/x86_64/iso/Fedora-Server-dvd-x86_64-32-1.6.iso
        Note : Run 2 IO tools mentioned above on each volume mounted
    10. Create Snapshots.Verify the snap ls | grep to see snap ot created
    11. Create Clones on all 8 volumes. Verify the clones got created using clone ls(subvolume ls)
    12. Set File Level Quota on 2 directories under subvolume and Size Level Quota on under 2 directories under subvolume.
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
        results = []
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        log.info("Create 2 SubVolumeGroups on each file system")
        cmds = [
            "ceph fs subvolumegroup create cephfs subvolgroup_1",
            "ceph fs subvolumegroup create cephfs subvolgroup_2",
            "ceph fs subvolumegroup create cephfs-ec subvolgroup_1",
            "ceph fs subvolumegroup create cephfs-ec subvolgroup_2",
        ]
        for command in cmds:
            clients[0].exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")
        log.info("Create 2 Sub volumes on each of the subvolume group Size 20 GB")
        cmds = [
            "ceph fs subvolume create cephfs subvol_1 --size 5368706371 --group_name subvolgroup_1",
            "ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_2",
            "ceph fs subvolume create cephfs-ec subvol_3 --size 5368706371 --group_name subvolgroup_1",
            "ceph fs subvolume create cephfs-ec subvol_4 --size 5368706371 --group_name subvolgroup_2",
            "ceph fs subvolume create cephfs subvol_5 --size 5368706371",
            "ceph fs subvolume create cephfs subvol_6 --size 5368706371",
            "ceph fs subvolume create cephfs-ec subvol_7 --size 5368706371",
            "ceph fs subvolume create cephfs-ec subvol_8 --size 5368706371",
            "ceph fs subvolume create cephfs subvol_9 --size 5368706371",
            "ceph fs subvolume create cephfs-ec subvol_10 --size 5368706371",
        ]
        for command in cmds:
            clients[0].exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client1")
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs subvol_1 subvolgroup_1"
        )
        fs_util.kernel_mount([clients[0]], kernel_mounting_dir_1, ",".join(mon_node_ips),
                             sub_dir=f"{subvol_path.read().decode().strip()}")

        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs subvol_5"
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount([clients[0]], fuse_mounting_dir_1, extra_params=f" -r {subvol_path.read().decode().strip()}")

        log.info("Mount 1 subvolumeon kernal and 1 subvloumegroup/subvolume on Fuse → Client2")
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")

        subvol_path, rc = clients[1].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs subvol_6"
        )
        fs_util.kernel_mount([clients[1]], kernel_mounting_dir_2, ",".join(mon_node_ips),
                             sub_dir=f"{subvol_path.read().decode().strip()}")

        subvol_path, rc = clients[1].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs subvol_2 subvolgroup_2"
        )
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        fs_util.fuse_mount([clients[1]], fuse_mounting_dir_2, extra_params=f" -r {subvol_path.read().decode().strip()}")

        log.info("On EC,Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client2")
        kernel_mounting_dir_3 = f"/mnt/cephfs_kernel{mounting_dir}_EC_3/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_3 subvolgroup_1"
        )
        fs_util.kernel_mount([clients[0]], kernel_mounting_dir_3, ",".join(mon_node_ips),
                             sub_dir=f"{subvol_path.read().decode().strip()}", extra_params=",fs=cephfs-ec")

        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_7"
        )
        fuse_mounting_dir_3 = f"/mnt/cephfs_fuse{mounting_dir}_EC_3/"
        fs_util.fuse_mount([clients[0]], fuse_mounting_dir_3,
                           extra_params=f" -r {subvol_path.read().decode().strip()} --client_fs cephfs-ec")

        log.info("On EC,Mount 1 subvolumeon kernal and 1 subvloumegroup/subvolume on Fuse → Client1")
        kernel_mounting_dir_4 = f"/mnt/cephfs_kernel{mounting_dir}_EC_4/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")

        subvol_path, rc = clients[1].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_8"
        )
        fs_util.kernel_mount([clients[1]], kernel_mounting_dir_4, ",".join(mon_node_ips),
                             sub_dir=f"{subvol_path.read().decode().strip()}", extra_params=",fs=cephfs-ec")

        subvol_path, rc = clients[1].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_4 subvolgroup_2"
        )
        fuse_mounting_dir_4 = f"/mnt/cephfs_fuse{mounting_dir}_EC_4/"
        fs_util.fuse_mount([clients[1]], fuse_mounting_dir_4,
                           extra_params=f" -r {subvol_path.read().decode().strip()} --client_fs cephfs-ec")

        run_ios(clients[0], kernel_mounting_dir_1)
        run_ios(clients[0], fuse_mounting_dir_1)
        run_ios(clients[1], kernel_mounting_dir_2)
        run_ios(clients[1], fuse_mounting_dir_2)

        log.info("Create Snapshots.Verify the snap ls | grep to see snap ot created")
        cmds = [
            "ceph fs subvolume snapshot create cephfs subvol_1 snap_1 --group_name subvolgroup_1",
            "ceph fs subvolume snapshot create cephfs subvol_2 snap_2 --group_name subvolgroup_2",
            "ceph fs subvolume snapshot create cephfs-ec subvol_3 snap_3 --group_name subvolgroup_1",
            "ceph fs subvolume snapshot create cephfs-ec subvol_4 snap_4 --group_name subvolgroup_2",
            "ceph fs subvolume snapshot create cephfs subvol_5 snap_5",
            "ceph fs subvolume snapshot create cephfs subvol_6 snap_6",
            "ceph fs subvolume snapshot create cephfs-ec subvol_7 snap_7",
            "ceph fs subvolume snapshot create cephfs-ec subvol_8 snap_8"
        ]
        for command in cmds:
            clients[0].exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")

        cmds = [
            "ceph fs subvolume snapshot clone cephfs subvol_1 snap_1 clone_1 --group_name subvolgroup_1",
            "ceph fs subvolume snapshot clone cephfs subvol_2 snap_2 clone_2 --group_name subvolgroup_2",
            "ceph fs subvolume snapshot clone cephfs-ec subvol_3 snap_3 clone_3 --group_name subvolgroup_1",
            "ceph fs subvolume snapshot clone cephfs-ec subvol_4 snap_4 clone_4 --group_name subvolgroup_2",
            "ceph fs subvolume snapshot clone cephfs subvol_5 snap_5 clone_5",
            "ceph fs subvolume snapshot clone cephfs subvol_6 snap_6 clone_6",
            "ceph fs subvolume snapshot clone cephfs-ec subvol_7 snap_7 clone_7",
            "ceph fs subvolume snapshot clone cephfs-ec subvol_8 snap_8 clone_8"
        ]
        for command in cmds:
            clients[0].exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")

        subvolumes_list = fs_util.get_all_subvolumes(clients[0])
        log.info("Verfiy all clone volunes are created")
        clones_check = all(i in [f"clone_{i}" for i in range(1, 9)] for i in subvolumes_list)
        if clones_check:
            log.error("All the clones not created in the file system")
            return 1

        log.info(
            "Set File Level Quota on 2 directories under subvolume and Size Level Quota on under 2 directories under subvolume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs subvol_9"
        )
        fuse_mounting_dir_5 = f"/mnt/cephfs_fuse{mounting_dir}_5/"
        fs_util.fuse_mount([clients[1]], fuse_mounting_dir_5, extra_params=f" -r {subvol_path.read().decode().strip()}")
        clients[1].exec_command(sudo=True, cmd=f"setfattr -n ceph.quota.max_files -v 10 {fuse_mounting_dir_5}")
        clients[1].exec_command(sudo=True, cmd=f"getfattr -n ceph.quota.max_files {fuse_mounting_dir_5}")

        subvol_path, rc = clients[0].exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs-ec subvol_10"
        )
        kernel_mounting_dir_5 = f"/mnt/cephfs_kernel{mounting_dir}_5/"
        fs_util.kernel_mount([clients[1]], kernel_mounting_dir_5, ",".join(mon_node_ips),
                             sub_dir=f"{subvol_path.read().decode().strip()}", extra_params=",fs=cephfs-ec")
        clients[1].exec_command(sudo=True, cmd=f"setfattr -n ceph.quota.max_files -v 10 {kernel_mounting_dir_5}")
        clients[1].exec_command(sudo=True, cmd=f"getfattr -n ceph.quota.max_files {kernel_mounting_dir_5}")

        log.info("Clean up the system")
        fs_util.client_clean_up("umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1)
        fs_util.client_clean_up("umount", kernel_clients=[clients[1]], mounting_dir=kernel_mounting_dir_2)
        fs_util.client_clean_up("umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_3)
        fs_util.client_clean_up("umount", kernel_clients=[clients[1]], mounting_dir=kernel_mounting_dir_4)
        fs_util.client_clean_up("umount", kernel_clients=[clients[1]], mounting_dir=kernel_mounting_dir_5)
        fs_util.client_clean_up("umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1)
        fs_util.client_clean_up("umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_2)
        fs_util.client_clean_up("umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_3)
        fs_util.client_clean_up("umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_4)
        fs_util.client_clean_up("umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_5)
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            "ceph fs subvolume snapshot rm cephfs subvol_1 snap_1 --group_name subvolgroup_1",
            "ceph fs subvolume snapshot rm cephfs subvol_2 snap_2 --group_name subvolgroup_2",
            "ceph fs subvolume snapshot rm cephfs-ec subvol_3 snap_3 --group_name subvolgroup_1",
            "ceph fs subvolume snapshot rm cephfs-ec subvol_4 snap_4 --group_name subvolgroup_2",
            "ceph fs subvolume snapshot rm cephfs subvol_5 snap_5",
            "ceph fs subvolume snapshot rm cephfs subvol_6 snap_6",
            "ceph fs subvolume snapshot rm cephfs-ec subvol_7 snap_7",
            "ceph fs subvolume snapshot rm cephfs-ec subvol_8 snap_8",
        ]
        commands.extend(["ceph fs subvolume rm cephfs subvol_1 --group_name subvolgroup_1",
                         "ceph fs subvolume rm cephfs subvol_2 --group_name subvolgroup_2",
                         "ceph fs subvolume rm cephfs-ec subvol_3 --group_name subvolgroup_1",
                         "ceph fs subvolume rm cephfs-ec subvol_4 --group_name subvolgroup_2",
                         "ceph fs subvolume rm cephfs subvol_5",
                         "ceph fs subvolume rm cephfs subvol_6",
                         "ceph fs subvolume rm cephfs-ec subvol_7",
                         "ceph fs subvolume rm cephfs-ec subvol_8",
                         "ceph fs subvolume rm cephfs subvol_9",
                         "ceph fs subvolume rm cephfs-ec subvol_10", ])
        commands.extend(["ceph fs subvolume rm cephfs clone_1",
                         "ceph fs subvolume rm cephfs clone_2",
                         "ceph fs subvolume rm cephfs-ec clone_3",
                         "ceph fs subvolume rm cephfs-ec clone_4",
                         "ceph fs subvolume rm cephfs clone_5",
                         "ceph fs subvolume rm cephfs clone_6",
                         "ceph fs subvolume rm cephfs-ec clone_7",
                         "ceph fs subvolume rm cephfs-ec clone_8"])
        commands.extend(["ceph fs subvolumegroup rm cephfs subvolgroup_1",
                         "ceph fs subvolumegroup rm cephfs subvolgroup_2",
                         "ceph fs subvolumegroup rm cephfs-ec subvolgroup_1",
                         "ceph fs subvolumegroup rm cephfs-ec subvolgroup_2", ])
        for command in commands:
            clients[0].exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")
        log.info("Testcase Results:")
        for res in results:
            log.info(res)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def run_ios(client, mounting_dir):
    def smallfile():
        client.exec_command(sudo=True,
                            cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
                                f"create symlink stat chmod ls-l delete cleanup  ; "
                                f"do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i --threads 8 --file-size 10240 "
                                f"--files 10 --top {mounting_dir} ; done")

    def io_tool():
        client.exec_command(sudo=True,
                            cmd=f"cd {mounting_dir};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz")
        client.exec_command(sudo=True,
                            cmd=f"tar -xzf linux.tar.gz tardir/ ; sleep 10 ; rm -rf  tardir/ ; sleep 10 ; done")

    def wget():
        client.exec_command(sudo=True,
                            cmd=f"cd {mounting_dir};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz")

    io_tools = [smallfile, wget]
    f = random.choice(io_tools)
    f()
