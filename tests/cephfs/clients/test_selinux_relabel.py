import os
import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83595737 - Selinux Relabel - Functional Tests
    Test to execute the SELinux label application and validation on CephFS subvolumes.
    This function performs the following steps:
    1. Prepares the Ceph clients and authenticates them.
    2. Creates a Ceph filesystem if it doesn't exist.
    3. Creates a subvolumegroup and multiple subvolumes within it.
    4. Mounts two subvolumes on two different clients using kernel mount.
    5. Creates directories within the mounted subvolumes and applies SELinux labels to them.
    6. Validates that the correct SELinux context is applied.
    7. Checks the health status of the Ceph cluster.
    8. Cleans up the environment by unmounting and removing subvolumes and the subvolumegroup.

    Args:
        ceph_cluster (CephCluster): The Ceph cluster object.
        **kw: Additional keyword arguments for configuration and other parameters.

    Returns:
        int: 0 if the test case executes successfully, 1 if an error occurs.
    """

    try:
        tc = "CEPH-83595737"
        log.info(f"Running cephfs {tc} test case")
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.error(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        default_fs = "cephfs"
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
        log.info("Mount 2 subvolume on kernel Client1 and client2")

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

        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        mon_node_ips = fs_util_v1.get_mon_node_ips()
        subvol_path_kernel, rc = clients[1].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}_2 {subvolume_group_name}",
        )
        fs_util_v1.kernel_mount(
            [clients[1]],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path_kernel.strip()}",
            extra_params=f",fs={default_fs}",
        )

        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 1 : Create a directory with 1M files, "
            "apply SELinux label, and measure time taken..          "
            "\n---------------***************---------------"
        )

        test_dirs = [
            {
                "client": clients[0],
                "path": os.path.join(kernel_mounting_dir_1, "test_dir_1M_files"),
            },
            {
                "client": clients[1],
                "path": os.path.join(kernel_mounting_dir_2, "test_dir_1M_files"),
            },
        ]

        num_files = 1000000
        label_to_apply = "mnt_t"

        for td in test_dirs:
            client = td["client"]
            test_dir = td["path"]

            log.info(f"Creating directory: {test_dir}")
            create_path(client, test_dir)

            log.info(f"Creating {num_files} files in {test_dir}")
            client.exec_command(
                sudo=True,
                cmd=f"bash -c 'for i in $(seq 1 {num_files}); do touch {test_dir}/file_$i; done'",
                timeout=7200,
            )

            time.sleep(60)  # Allow file creation to complete

            log.info(
                f"Applying SELinux label '{label_to_apply}' to {test_dir} and measuring time taken."
            )
            start_time = time.time()
            apply_selinux_label(client, label_to_apply, test_dir)
            end_time = time.time()

            time_taken = end_time - start_time
            log.info(
                f"Time taken to apply SELinux label to {test_dir}: {time_taken} seconds"
            )

            log.info(f"Validating SELinux context for {test_dir}")
            if validate_selinux_context(client, label_to_apply, test_dir):
                log.error(f"Failed to validate SELinux context for {test_dir}")
                return 1

        log.info(
            "Check for the Ceph Health status to see if it's Healthy after enabling Standby-Replay"
        )
        fs_util_v1.get_ceph_health_status(clients[0])
        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 1 Completed Successfully          "
            "\n---------------***************---------------"
            "\n"
        )

        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 2 : 1M file creation from Client1 and  "
            "SELinux label application from Client2..          "
            "\n---------------***************---------------"
        )

        kernel_mounting_dir = f"/mnt/cephfs_kernel_shared_{mounting_dir}/"
        mon_node_ips = fs_util_v1.get_mon_node_ips()
        subvol_path_kernel, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}_1 {subvolume_group_name}",
        )
        for client in clients[:2]:
            fs_util_v1.kernel_mount(
                [client],
                kernel_mounting_dir,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path_kernel.strip()}",
                extra_params=f",fs={default_fs}",
            )

        test_dir_s3 = os.path.join(kernel_mounting_dir, "test_dir_1M_files_s3")
        create_path(clients[0], test_dir_s3)

        log.info("Creating 1M files from Client1")
        num_files = 1000000
        clients[0].exec_command(
            sudo=True,
            cmd=f"bash -c 'for i in $(seq 1 {num_files}); do touch {test_dir_s3}/file_$i; done'",
            timeout=7200,
        )

        time.sleep(60)  # Ensure file creation is completed

        log.info("Applying SELinux label from Client2")
        label_to_apply = "mnt_t"
        start_time = time.time()
        apply_selinux_label(clients[1], label_to_apply, test_dir_s3)
        end_time = time.time()

        time_taken = end_time - start_time
        log.info(
            f"Time taken to apply SELinux label on Client2 for {test_dir_s3}: {time_taken} seconds"
        )

        time.sleep(30)  # Ensure selinux labels are applied.

        log.info("Validating SELinux context from both the clients")
        if validate_selinux_context(clients[0], label_to_apply, test_dir_s3):
            log.error(f"Failed to validate SELinux context for {test_dir}")
            return 1

        if validate_selinux_context(clients[1], label_to_apply, test_dir_s3):
            log.error(f"Failed to validate SELinux context for {test_dir}")
            return 1

        log.info(
            "Check for the Ceph Health status to see if it's Healthy after enabling Standby-Replay"
        )
        fs_util_v1.get_ceph_health_status(clients[0])
        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 2 Completed Successfully          "
            "\n---------------***************---------------"
            "\n"
        )

        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 3 : Create the directory and assign most "
            "commonly used SELinux context types...          "
            "\n---------------***************---------------"
        )

        log.info("Creating directories and applying SELinux labels")
        selinux_labels = {
            "var_t": "var",
            "home_t": "home",
            "httpd_sys_content_t": "var/www",
            "nfs_t": "nfs_share",
            "samba_share_t": "samba_share",
            "tmp_t": "tmp",
            "httpd_t": "httpd_process",
            "sshd_t": "sshd_process",
            "mysqld_t": "mysqld_process",
            "ftpd_t": "ftpd_process",
            "cron_t": "cron_process",
            "device_t": "device",
            "usb_device_t": "usb_device",
            "network_t": "network",
            "port_t": "network_port",
            "netif_t": "network_interface",
            "netnode_t": "network_node",
            "init_t": "init_process",
            "systemd_t": "systemd_process",
            "kernel_t": "kernel_process",
            "logrotate_t": "logrotate_process",
        }

        mounts = [
            {"mount_dir": kernel_mounting_dir_1, "client": clients[0]},
            {"mount_dir": kernel_mounting_dir_2, "client": clients[1]},
        ]

        for mount in mounts:
            mount_dir = mount["mount_dir"]
            client = mount["client"]

            for label, sub_path in selinux_labels.items():
                full_path = os.path.join(mount_dir, sub_path)
                create_path(client, full_path)

                # Apply SELinux label
                if apply_selinux_label(client, label, full_path):
                    log.error(
                        f"Failed to apply SELinux label '{label}' to '{full_path}'"
                    )
                    return 1

                # Validate SELinux label
                if validate_selinux_context(client, label, full_path):
                    log.error(
                        f"Failed to validate SELinux context for '{full_path}'. Known Issue ans is being "
                        f"tracked at - https://bugzilla.redhat.com/show_bug.cgi?id=2309363"
                    )
                    return 1

        log.info(
            "Check for the Ceph Health status to see if it's Healthy after enabling Standby-Replay"
        )
        fs_util_v1.get_ceph_health_status(clients[0])
        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 3 Completed Successfully          "
            "\n---------------***************---------------"
            "\n"
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.error("Clean up the system")
        kernel_mounts = [
            (clients[0], kernel_mounting_dir_1),
            (clients[1], kernel_mounting_dir_2),
            (clients[0], kernel_mounting_dir),
            (clients[1], kernel_mounting_dir),
        ]

        for client, mounting_dir in kernel_mounts:
            fs_util_v1.client_clean_up(
                "umount", kernel_clients=[client], mounting_dir=mounting_dir
            )

        for subvolume in subvolume_list:
            fs_util_v1.remove_subvolume(clients[0], **subvolume)

        fs_util_v1.remove_subvolumegroup(
            clients[0], default_fs, subvolume_group_name, force=True
        )


def apply_selinux_label(client, label, path):
    """
    This function applies a specified SELinux type label to a given path on a mount point.

    Args:
        client (CephClient): The client object where the label is to be applied.
        label (str): The SELinux type label to apply.
        path (str): The directory path where the SELinux label should be applied.

    Returns:
        None
    """
    try:
        client.exec_command(sudo=True, cmd=f"chcon -R --type={label} {path}")
        log.info(f"Applied SELinux label {label} to {path}")
    except Exception as e:
        log.error(f"Failed to apply SELinux label {label} to {path}: {e}")


def validate_selinux_context(client, expected_label, path):
    """
    This function checks whether the SELinux type label of a specified path matches the expected label.

    Args:
        client (CephClient): The client object where the validation is performed.
        expected_label (str): The expected SELinux type label.
        path (str): The directory path where the SELinux context is validated.

    Returns:
        None
    """
    cmd = f"ls -lZd {path} | awk '{{print $5}}' | cut -d: -f3"
    result, _ = client.exec_command(sudo=True, cmd=cmd)
    context_type = result.strip()

    if context_type:
        if context_type == expected_label:
            log.info(
                f"Validation successful: {path} has correct SELinux context {context_type}"
            )
        else:
            log.error(
                f"Validation failed: {path} has incorrect SELinux context {context_type}. "
                f"Expected {expected_label}"
            )
            return 1
    else:
        log.error(f"Validation failed: No SELinux context found for {path}")


def create_path(client, path):
    """
    This function creates a specified directory path on the client's filesystem.
    Args:
        client (CephClient): The client object where the path is to be created.
        path (str): The directory path to create.

    Returns:
        None
    """
    try:
        client.exec_command(sudo=True, cmd=f"mkdir -p {path}")
        log.info(f"Created path: {path}")
    except Exception as e:
        log.error(f"Failed to create path '{path}': {e}")
