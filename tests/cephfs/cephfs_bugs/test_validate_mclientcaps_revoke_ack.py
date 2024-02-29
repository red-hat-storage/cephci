import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83584084 - Validate if no repeated mclientscaps(revoke) entries in MDS which
                    was causing the Ceph Status to go Unhealthy

    Args:
        ceph_cluster (CephCluster): Ceph cluster object.
        **kw: Additional keyword arguments.

    Returns:
        int: 0 if successful, 1 if failed.

    Procedure:
        1. Create 2 Subvolumes.
        2. Write a large file on both the Subvolumes.
        3. Validate the Ceph Status - should remain healthy.
        4. Validate no mclientcaps(revoke) warnings seen in all active MDS Logs.
    Clean-up:
        1. Delete Subvolumes and Subvolumegroup
    """
    try:
        tc = "CEPH-83584084"
        log.info(f"Running cephfs {tc} test case")
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
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

        log.info("Setting log_to_file of mds to True")
        fs_util_v1.config_set_runtime(clients[0], "mds", "log_to_file", "true")
        log.info("Setting debug_ms of mds to 1")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_ms", 1)
        log.info("Setting debug_mds of mds to 20")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_mds", 20)

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

        file_paths = [
            f"/mnt/cephfs_kernel{mounting_dir}_1/large_file_kernel_1.txt",
            f"/mnt/cephfs_fuse{mounting_dir}_1/large_file_fuse_1.txt",
        ]

        create_large_file_script = "create_large_file.py"
        clients[0].upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_bugs/create_large_file.py",
            dst=f"/root/{create_large_file_script}",
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"python /root/{create_large_file_script} {' '.join(file_paths)}",
            timeout=7200,
        )

        log.info("Check for the Ceph Health.")
        fs_util_v1.get_ceph_health_status(clients[0])

        fsid = fs_util_v1.get_fsid(clients[0])
        mds = fs_util_v1.get_active_mdss(clients[0], default_fs)
        active_mdss_hostnames = [i.split(".")[1] for i in mds]
        log.info(f"Active MDS Nodes are : {mds}")
        for mds_node in mds_nodes:
            if mds_node.node.hostname in active_mdss_hostnames:
                log.info("Verify if there are repeated Revoke entries in MDS log.")
                out, rc = mds_node.exec_command(
                    sudo=True,
                    cmd=f"grep -i 'mclientcaps(revoke)'  /var/log/ceph/{fsid}/ceph-mds*",
                    check_ec=False,
                )
                log.info(out)
                if "mclientcaps(revoke)" not in out:
                    log.info(
                        "Test is passed: No repeated mclientcaps Revoke entries found in MDS log."
                    )
                else:
                    log.error(
                        "Error: Repeated mclientcaps Revoke entries found in MDS log."
                    )
                    raise CommandFailed(f"Repeated mclientcaps seen in : {out}")
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

        log.info("Setting debug_ms of mds back to 0")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_ms", 0)
        log.info("Setting debug_mds of mds back to 0")
        fs_util_v1.config_set_runtime(clients[0], "mds", "debug_mds", 0)
