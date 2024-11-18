import json
import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def validate_fs_info(expected_fs, output):
    """
    Validate fs information restriction for clients
    :param expected_fs - only fs expected to show in output
    :param output - output of 'ceph fs ls'
    """
    if len(output) != 1 and output[0]["name"] != expected_fs:
        raise CommandFailed("fs is not matching with authorized FS")
    log.info(f"File systems Information is restricted to {expected_fs} only")


def run(ceph_cluster, **kw):
    """
     Pre-requisites:
    - Create a CephFS volume with root squash enabled.
    - Ensure MDS daemons are deployed for the filesystem.
    - Set up clients and ensure they are authorized for specific CephFS mounts.

    Test Operations:
    1. Authorize `client1` for the first CephFS volume with root squash enabled.
    2. Mount CephFS via both FUSE and kernel mounts on `client1`.
    3. Run the following scenarios to validate permissions and root squash behavior:
       - Scenario 1: Attempt to create and delete directories and files as a non-root user. Expecation : should pass
       - Scenario 2: Attempt to create and delete directories and files as a root user,
                    verifying root squash restrictions. Expectation : should fail
       - Scenario 3: Write data to files as a non-root user, unmount and remount the volumes,
                and verify the contents using a different client (`client2`).

    Clean-up:
    1. Unmount all CephFS mounts.
    2. Delete the created directories and remove the client authorizations.
    3. Remove the CephFS volume if it is no longer needed.
    """
    try:
        tc = "CEPH-83602912"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1, client2 = clients[0], clients[1]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        fs1 = "cephfs-root_squash"
        ceph_client_name = "fs_1"
        fs_details = fs_util.get_fs_info(clients[0], fs1)
        if not fs_details:
            fs_util.create_fs(clients[0], fs1)
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs1}",
        )
        client1.exec_command(sudo=True, cmd=f"chmod +777 {fuse_mount_dir}")
        log.info(f"Creating client authorized to {fs1}")
        fs_util.fs_client_authorize(
            client1, fs1, f"{ceph_client_name}", "/", "rw root_squash"
        )

        log.info(f"Verifying file system information for {ceph_client_name}")
        command = f"ceph auth get client.{ceph_client_name} -o /etc/ceph/ceph.client.{ceph_client_name}.keyring"
        for client in [client1, client2]:
            client.exec_command(sudo=True, cmd=command)
        command = (
            f"sudo ceph fs ls -n client.{ceph_client_name} -k "
            f"/etc/ceph/ceph.client.{ceph_client_name}.keyring --format json"
        )
        out, rc = client1.exec_command(cmd=command)
        output = json.loads(out)
        validate_fs_info(fs1, output)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        kernel_mounting_dir_1 = f"/home/cephuser/cephfs_kernel{mounting_dir}_1/"
        fuse_mounting_dir_1 = f"/home/cephuser/cephfs_fuse{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        client1.exec_command(cmd=f"mkdir -p {fuse_mounting_dir_1}")
        client1.exec_command(
            cmd=f"sudo ceph-fuse -n client.{ceph_client_name} {fuse_mounting_dir_1} --client_fs {fs1}",
        )
        client1.exec_command(cmd=f"mkdir -p {kernel_mounting_dir_1}")
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            new_client_hostname=ceph_client_name,
            extra_params=f",fs={fs1}",
        )
        log.info(
            "Scenario 1: Validate creation of directory under the root of filesystem with non root user"
        )
        for path, file_name in zip(
            [fuse_mounting_dir_1, kernel_mounting_dir_1],
            ["fuse_non_root", "kernel_non_root"],
        ):

            client1.exec_command(cmd=f"mkdir {path}/{file_name}")
            client1.exec_command(cmd=f"touch {path}/{file_name}.txt")
            client1.exec_command(cmd=f"rm {path}/{file_name}.txt")
            client1.exec_command(cmd=f"rmdir {path}/{file_name}")
        log.info(
            "Scenario 2: Validate creation of directory under the root of filesystem with root user"
        )
        for path, file_name in zip(
            [fuse_mounting_dir_1, kernel_mounting_dir_1], ["fuse_root", "kernel_root"]
        ):
            out, rc = client1.exec_command(
                sudo=True, cmd=f"mkdir {path}/{file_name}", check_ec=False
            )
            if not rc:
                raise CommandFailed(
                    f"We are able to create folder where root_squash is enabled with {path}"
                )
            out, rc = client1.exec_command(
                sudo=True, cmd=f"touch {path}/{file_name}.txt", check_ec=False
            )
            if not rc:
                raise CommandFailed(
                    f"We are able to create file where root_squash is enabled with {path}"
                )
            out, rc = client1.exec_command(
                sudo=True, cmd=f"rm {path}/{file_name}.txt", check_ec=False
            )
            if not rc:
                raise CommandFailed(
                    f"We are able to remove file where root_squash is enabled with {path}"
                )
            out, rc = client1.exec_command(
                sudo=True, cmd=f"rmdir {path}/{file_name}", check_ec=False
            )
            if not rc:
                raise CommandFailed(
                    f"We are able to remove file where root_squash is enabled with {path}"
                )
        log.info(
            "Scenario 3: Wrtie the files from non root user and unmount and remount "
            "and verify the contents from different client "
        )
        content_to_write = "This is test data for verification."
        for path, file_name in zip(
            [fuse_mounting_dir_1, kernel_mounting_dir_1],
            ["fuse_non_root", "kernel_non_root"],
        ):
            client1.exec_command(cmd=f"mkdir {path}/{file_name}")
            client1.exec_command(cmd=f"touch {path}/{file_name}.txt")
            client1.exec_command(
                cmd=f"echo '{content_to_write}' > {path}/{file_name}.txt"
            )
            # Verify written data by reading the file content
            log.info(f"Reading data from {path}/{file_name}.txt to verify content")
            out, rc = client1.exec_command(cmd=f"cat {path}/{file_name}.txt")

            # Check if the content matches
            if out.strip() == content_to_write:
                log.info(
                    f"Content verified successfully in {path}/{file_name}.txt: '{out}'"
                )
            else:
                log.error(
                    f"Content mismatch in {path}/{file_name}.txt. Expected: '{content_to_write}', but got: '{out}'"
                )
                raise CommandFailed(
                    f"Content mismatch in {path}/{file_name}.txt. Expected: '{content_to_write}', but got: '{out}'"
                )

        for path in [fuse_mounting_dir_1, kernel_mounting_dir_1]:
            client1.exec_command(f"sudo umount {path}")

        client2.exec_command(cmd=f"mkdir -p {fuse_mounting_dir_1}")
        client2.exec_command(
            sudo=True,
            cmd=f"sudo ceph-fuse -n client.{ceph_client_name} {fuse_mounting_dir_1} --client_fs {fs1}",
        )
        client2.exec_command(cmd=f"mkdir -p {kernel_mounting_dir_1}")
        fs_util.kernel_mount(
            [client2],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            new_client_hostname=ceph_client_name,
            extra_params=f",fs={fs1}",
        )
        for path, file_name in zip(
            [fuse_mounting_dir_1, kernel_mounting_dir_1],
            ["fuse_non_root", "kernel_non_root"],
        ):
            log.info(f"Reading data from {path}/{file_name}.txt to verify content")
            out, rc = client2.exec_command(cmd=f"cat {path}/{file_name}.txt")

            # Check if the content matches
            if out.strip() == content_to_write:
                log.info(
                    f"Content verified successfully in {path}/{file_name}.txt: '{out}'"
                )
            else:
                log.error(
                    f"Content mismatch in {path}/{file_name}.txt. Expected: '{content_to_write}', but got: '{out}'"
                )
                raise CommandFailed(
                    f"Content mismatch in {path}/{file_name}.txt. Expected: '{content_to_write}', but got: '{out}'"
                )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        for client in [client1, client2]:
            log.info("Unmounting and removing the mount folders if they exist")
            for path in [fuse_mount_dir, fuse_mounting_dir_1, kernel_mounting_dir_1]:
                # Unmount the path
                client.exec_command(sudo=True, cmd=f"umount {path}", check_ec=False)
                # Remove the directory
                client.exec_command(sudo=True, cmd=f"rmdir {path}", check_ec=False)

        client1.exec_command(sudo=True, cmd=f"ceph auth del client.{ceph_client_name}")

        fs_util.remove_fs(client1, fs1)
