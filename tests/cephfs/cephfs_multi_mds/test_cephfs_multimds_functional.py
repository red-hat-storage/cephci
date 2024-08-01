import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Run the test to set up Ceph filesystems, create subvolumes, mount them,
    and validate standby-replay MDS functionality.

    This function performs the following Scenarios :
    1. Prepare clients and authenticate them.
    2. Create two Ceph filesystems with specified configurations.
    3. Create subvolume groups and subvolumes within each filesystem.
    4. Mount subvolumes using kernel and fuse mounts.
    5. Create a specified number of files in the mounted subvolumes.
    Scenario 1 :
        Enable and validate standby-replay MDS for the filesystems.
    Scenario 2 :
        Verify MDS failover functionality by rebooting active MDS nodes and ensuring standby-replay nodes take over.

    Args:
        ceph_cluster (object): The Ceph cluster object.
        **kw: Additional keyword arguments, including:
            - config (dict): Configuration dictionary with the following keys:
                - build (str): Build version.
                - rhbuild (str): RH build version.
                - num_of_osds (int): Number of OSDs.
                - num_of_files (int): Number of files to create.

    Returns:
        int: 0 if the test runs successfully, 1 if there is any error.

    Raises:
        Exception: If any unexpected error occurs during the process.
    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        fs_name1 = "cephfs-func1"
        fs_name2 = "cephfs-func2"
        max_mds_fs_name1 = 2
        max_mds_fs_name2 = 1
        subvolume_group_name = "subvol_group1"
        subvolume_name = "subvol_"
        num_of_osds = config.get("num_of_osds")
        num_of_files = config.get("num_of_files")

        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)

        mds_names_hostname = []
        for mds in mds_nodes:
            mds_names_hostname.append(mds.node.hostname)
        log.info(f"MDS List : {mds_names_hostname}")

        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        mds_hosts_list1 = mds_names[:5]
        mds_hosts_1 = " ".join(mds_hosts_list1) + " "
        mds_host_list1_length = len(mds_hosts_list1)
        mds_hosts_list2 = mds_names[5:]
        mds_hosts_2 = " ".join(mds_hosts_list2) + " "
        mds_host_list2_length = len(mds_hosts_list2)

        log.info("Create Filesystems with standby-replay")
        fs_details = [
            (fs_name1, mds_host_list1_length, mds_hosts_1, max_mds_fs_name1),
            (fs_name2, mds_host_list2_length, mds_hosts_2, max_mds_fs_name2),
        ]
        for fs_name, mds_host_list_length, mds_hosts, max_mds in fs_details:
            clients[0].exec_command(
                sudo=True,
                cmd=f'ceph fs volume create {fs_name} --placement="{mds_host_list_length} {mds_hosts}"',
            )
            clients[0].exec_command(
                sudo=True, cmd=f"ceph fs set {fs_name} max_mds {max_mds}"
            )
            fs_util_v1.wait_for_mds_process(clients[0], fs_name)

        log.info("Create a SubvolumeGroup for both filesystems")
        subvolumegroup1 = {
            "vol_name": fs_name1,
            "group_name": subvolume_group_name,
        }
        subvolumegroup2 = {
            "vol_name": fs_name2,
            "group_name": subvolume_group_name,
        }
        fs_util_v1.create_subvolumegroup(clients[0], **subvolumegroup1)
        fs_util_v1.create_subvolumegroup(clients[0], **subvolumegroup2)

        log.info("Create 2 Subvolumes for each filesystem")
        subvolume_list1 = [
            {
                "vol_name": fs_name1,
                "subvol_name": subvolume_name,
                "group_name": subvolume_group_name,
            },
        ]
        subvolume_list2 = [
            {
                "vol_name": fs_name2,
                "subvol_name": subvolume_name,
                "group_name": subvolume_group_name,
            },
        ]
        for i in range(1, 3):
            subvolume_list1[0]["subvol_name"] = subvolume_name + str(i)
            fs_util_v1.create_subvolume(clients[0], **subvolume_list1[0])
            subvolume_list2[0]["subvol_name"] = subvolume_name + str(i)
            fs_util_v1.create_subvolume(clients[0], **subvolume_list2[0])

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        log.info("Create 1 kernel mount for subvolume1 of both filesystems")
        mon_node_ips = fs_util_v1.get_mon_node_ips()
        kernel_subvol_mount_paths = []

        # Kernel mount for subvolume1 of fs_name1
        subvol_path_kernel1_fs1, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name1} {subvolume_name}1 {subvolume_group_name}",
        )
        kernel_mounting_dir1_fs1 = f"/mnt/{fs_name1}_kernel{mounting_dir}_1/"
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mounting_dir1_fs1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path_kernel1_fs1.strip()}",
            extra_params=f",fs={fs_name1}",
        )
        kernel_subvol_mount_paths.append(kernel_mounting_dir1_fs1.strip())

        # Kernel mount for subvolume1 of fs_name2
        subvol_path_kernel1_fs2, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name2} {subvolume_name}1 {subvolume_group_name}",
        )
        kernel_mounting_dir1_fs2 = f"/mnt/{fs_name2}_kernel{mounting_dir}_1/"
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mounting_dir1_fs2,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path_kernel1_fs2.strip()}",
            extra_params=f",fs={fs_name2}",
        )
        kernel_subvol_mount_paths.append(kernel_mounting_dir1_fs2.strip())

        log.info("Create 1 fuse mount for subvolume2 of both filesystems")
        fuse_subvol_mount_paths = []

        # Fuse mount for subvolume2 of fs_name1
        subvol_path_fuse2_fs1, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name1} {subvolume_name}2 {subvolume_group_name}",
        )
        fuse_mounting_dir2_fs1 = f"/mnt/{fs_name1}_fuse{mounting_dir}_2/"
        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mounting_dir2_fs1,
            extra_params=f" -r {subvol_path_fuse2_fs1.strip()} --client_fs {fs_name1}",
        )
        fuse_subvol_mount_paths.append(fuse_mounting_dir2_fs1.strip())

        # Fuse mount for subvolume2 of fs_name2
        subvol_path_fuse2_fs2, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name2} {subvolume_name}2 {subvolume_group_name}",
        )
        fuse_mounting_dir2_fs2 = f"/mnt/{fs_name2}_fuse{mounting_dir}_2/"
        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mounting_dir2_fs2,
            extra_params=f" -r {subvol_path_fuse2_fs2.strip()} --client_fs {fs_name2}",
        )
        fuse_subvol_mount_paths.append(fuse_mounting_dir2_fs2.strip())

        log.info("Kernel subvolume mount paths: " + str(kernel_subvol_mount_paths))
        log.info("Fuse subvolume mount paths: " + str(fuse_subvol_mount_paths))

        log.info(f"Creating up to {num_of_files} files in the filesystem")
        all_paths = kernel_subvol_mount_paths + fuse_subvol_mount_paths
        for path in all_paths:
            fs_util_v1.create_files_in_path(
                clients[0],
                path,
                num_of_files=num_of_files,
                batch_size=100,
            )

        log.info(
            "----------Scenario 1 : Enable/Disable Standby-Replay on a "
            "File System and validate if it's assigned/unassigned correctly.----------"
        )
        for fs_name in [fs_name1, fs_name2]:
            result = fs_util_v1.set_and_validate_mds_standby_replay(
                clients[0],
                fs_name,
                1,
            )
            log.info(f"Ceph fs status after enabling standby-replay : {result}")

        log.info(
            "Check for the Ceph Health status to see if it's Healthy after enabling Standby-Replay"
        )
        fs_util_v1.get_ceph_health_status(clients[0])
        log.info("\n----------Scenario 1 Completed Successfully----------")

        log.info(
            "\n----------Scenario 2 : Verify that failover to the standby-replay daemon works correctly "
            "when an active MDS fails----------"
        )
        log.info("Capture the mds states before rebooting")
        mds_info_before_for_fs1 = fs_util_v1.get_mds_states_active_standby_replay(
            fs_name1, clients[0]
        )
        log.info(f"MDS Info for {fs_name1} Before Reboot: {mds_info_before_for_fs1}")
        mds_info_before_for_fs2 = fs_util_v1.get_mds_states_active_standby_replay(
            fs_name2, clients[0]
        )
        log.info(f"MDS Info for {fs_name2}Before Reboot: {mds_info_before_for_fs2}")

        log.info(
            f"Run IO's and reboot the Active MDS Nodes on {fs_name1} and wait for Failover to happen"
        )
        fs_util_v1.runio_reboot_active_mds_nodes(
            fs_util_v1,
            ceph_cluster,
            fs_name1,
            clients[0],
            num_of_osds,
            build,
            fuse_subvol_mount_paths,
            "fuse",
        )

        log.info(
            f"Run IO's and reboot the Active MDS Nodes on {fs_name2} and wait for Failover to happen"
        )
        fs_util_v1.runio_reboot_active_mds_nodes(
            fs_util_v1,
            ceph_cluster,
            fs_name2,
            clients[0],
            num_of_osds,
            build,
            kernel_subvol_mount_paths,
            "kernel",
        )

        log.info("Capture the mds states after rebooting")
        mds_info_after_for_fs1 = fs_util_v1.get_mds_states_active_standby_replay(
            fs_name1, clients[0]
        )
        log.info(f"MDS Info for {fs_name1} After Reboot: {mds_info_after_for_fs1}")
        mds_info_after_for_fs2 = fs_util_v1.get_mds_states_active_standby_replay(
            fs_name2, clients[0]
        )
        log.info(f"MDS Info for {fs_name2} After Reboot: {mds_info_after_for_fs2}")

        log.info("Compare MDS info before and after the reboot")
        for fs_name, before_info, after_info in [
            (fs_name1, mds_info_before_for_fs1, mds_info_after_for_fs1),
            (fs_name2, mds_info_before_for_fs2, mds_info_after_for_fs2),
        ]:
            if compare_mds_info(before_info, after_info):
                log.info(
                    f"MDS nodes for {fs_name} have been successfully replaced by "
                    f"standby-replay nodes and are back to 2 active and 2 standby-replay nodes."
                )
            else:
                log.error(
                    f"MDS nodes replacement did not occur as expected for {fs_name}."
                )
                return 1
        log.info(
            "Check for the Ceph Health status to see if it's Healthy after failover."
        )
        fs_util_v1.get_ceph_health_status(clients[0])
        log.info("----------Scenario 2 Completed Successfully----------")

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")


def compare_mds_info(pre_reboot_info, post_reboot_info):
    """
    Compare MDS info before and after the reboot.

    Args:
        pre_reboot_info (dict): MDS info before the reboot.
        post_reboot_info (dict): MDS info after the reboot.

    Returns:
        bool: True if the number of active nodes and standby-replay nodes are correct, otherwise False.
    """

    for rank in pre_reboot_info:
        if rank not in post_reboot_info:
            return False

        pre_active = pre_reboot_info[rank]["active"]
        log.info(f"Pre Active : {pre_active}")
        post_active = post_reboot_info[rank]["active"]
        log.info(f"Post Active : {post_active}")

        pre_standby_replay = set(pre_reboot_info[rank]["standby-replay"])
        log.info(f"Pre Standby {pre_standby_replay}")
        post_standby_replay = set(post_reboot_info[rank]["standby-replay"])
        log.info(f"Post Standby {post_standby_replay}")

        # Check if post-reboot active node was in pre-reboot standby-replay nodes
        if post_active not in pre_standby_replay:
            return False

        # Ensure equal number of active and standby-replay nodes
        if len(pre_standby_replay) != len(post_standby_replay):
            return False

    return True
