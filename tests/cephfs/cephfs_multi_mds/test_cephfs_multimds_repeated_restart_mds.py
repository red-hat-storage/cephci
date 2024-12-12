import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        fs_name = "cephfs-mds"
        max_mds_fs_name = 1
        subvolume_group_name = "subvol_group1"
        subvolume_name = "subvol_"
        num_of_osds = config.get("num_of_osds")

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
        mds_hosts_list = mds_names[:3]
        mds_hosts = " ".join(mds_hosts_list) + " "
        mds_host_list_length = len(mds_hosts_list)

        log.info("Create Filesystems with standby-replay")
        fs_details = [
            (fs_name, mds_host_list_length, mds_hosts, max_mds_fs_name),
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
            "vol_name": fs_name,
            "group_name": subvolume_group_name,
        }
        fs_util_v1.create_subvolumegroup(clients[0], **subvolumegroup1)

        log.info("Create 2 Subvolumes for each filesystem")
        subvolume_list1 = [
            {
                "vol_name": fs_name,
                "subvol_name": subvolume_name,
                "group_name": subvolume_group_name,
            },
        ]
        for i in range(1, 3):
            subvolume_list1[0]["subvol_name"] = subvolume_name + str(i)
            fs_util_v1.create_subvolume(clients[0], **subvolume_list1[0])

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        log.info(f"Create 1 kernel mount for subvolume1 of {fs_name} ")
        mon_node_ips = fs_util_v1.get_mon_node_ips()
        kernel_subvol_mount_paths = []

        subvol_path_kernel1_fs1, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvolume_name}1 {subvolume_group_name}",
        )
        kernel_mounting_dir1_fs1 = f"/mnt/{fs_name}_kernel{mounting_dir}_1/"
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mounting_dir1_fs1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path_kernel1_fs1.strip()}",
            extra_params=f",fs={fs_name}",
        )
        kernel_subvol_mount_paths.append(kernel_mounting_dir1_fs1.strip())

        log.info(f"Create 1 fuse mount for subvolume2 of {fs_name}")
        fuse_subvol_mount_paths = []

        subvol_path_fuse2_fs1, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvolume_name}2 {subvolume_group_name}",
        )
        fuse_mounting_dir2_fs1 = f"/mnt/{fs_name}_fuse{mounting_dir}_2/"
        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mounting_dir2_fs1,
            extra_params=f" -r {subvol_path_fuse2_fs1.strip()} --client_fs {fs_name}",
        )
        fuse_subvol_mount_paths.append(fuse_mounting_dir2_fs1.strip())

        log.info("Kernel subvolume mount paths: " + str(kernel_subvol_mount_paths))
        log.info("Fuse subvolume mount paths: " + str(fuse_subvol_mount_paths))

        for fs_name in [fs_name]:
            result = fs_util_v1.set_and_validate_mds_standby_replay(
                clients[0],
                fs_name,
                1,
            )
            log.info(f"Ceph fs status after enabling standby-replay : {result}")

        log.info(
            f"Run IOs and reboot the Active MDS Nodes on {fs_name} and wait for Failover to happen for 5 iterations"
        )

        iteration_count = 10
        for iteration in range(iteration_count):
            log.info(
                f"Starting iteration {iteration + 1}/{iteration_count} for IO and reboot testing"
            )
            try:
                fs_util_v1.runio_reboot_active_mds_nodes(
                    fs_util_v1,
                    ceph_cluster,
                    fs_name,
                    clients[0],
                    num_of_osds,
                    build,
                    fuse_subvol_mount_paths,
                )
                log.info(f"Iteration {iteration + 1} completed successfully.")

            except Exception as e:
                log.error(f"Error during iteration {iteration + 1}: {e}")
                log.error(traceback.format_exc())
                break

        log.info("Completed all iterations for IO and MDS reboot testing.")

        log.info(
            "Check for the Ceph Health status to see if it's Healthy after failover."
        )
        fs_util_v1.get_ceph_health_status(clients[0])

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        all_paths = kernel_subvol_mount_paths + fuse_subvol_mount_paths
        for path in all_paths:
            clients[0].exec_command(sudo=True, cmd=f"rm -rf {path}*")
            clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")
            clients[0].exec_command(sudo=True, cmd=f"rm -rf {path}")
        log.info("Delete Subvolumes for both filesystems")
        for i in range(1, 3):
            subvolume_list1[0]["subvol_name"] = subvolume_name + str(i)
            fs_util_v1.remove_subvolume(clients[0], **subvolume_list1[0], validate=True)

        log.info("Delete Subvolume Groups for both filesystems")
        fs_util_v1.remove_subvolumegroup(clients[0], **subvolumegroup1, validate=True)

        log.info("Delete Filesystems with standby-replay")
        fs_util_v1.remove_fs(clients[0], fs_name, validate=True)
