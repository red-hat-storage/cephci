import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import get_storage_stats

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83593150 - Validate all extended attributes that are supported in filesystem
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 1 file systems with placement arguments
    2. Setfattr and getfattr all the attributes
    3. setfacl and getfacl
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs_xattr"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1, default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_xattr_set_get"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_xattr_set_get_fuse",
                "group_name": "subvolgroup_xattr_set_get",
                "size": "5368706371",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_xattr_set_get_kernel",
                "group_name": "subvolgroup_xattr_set_get",
                "size": "5368706371",
            },
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_xattr_set_get_fuse subvolgroup_xattr_set_get",
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
            extra_params=f",fs={default_fs}",
        )

        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_xattr_set_get_fuse subvolgroup_xattr_set_get",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"

        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()} --client_fs {default_fs}",
        )
        clients[0].exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}/test_dir")
        clients[0].exec_command(sudo=True, cmd=f"touch {fuse_mounting_dir_1}/test.txt")
        log.info("Scenario 1: Setting user trusted security xattr ")
        namespaces = {
            "user": {"user.test": "test_fuse", "user.test_kernel": "test_kernel"},
            "trusted": {
                "trusted.test": "test_fuse",
                "trusted.test_kernel": "test_kernel",
            },
            "security": {
                "security.test": "test_fuse",
                "security.test_kernel": "test_kernel",
            },
            "ceph": {
                # "ceph.dir.layout.stripe_unit": "4194304",
                # "ceph.dir.layout.stripe_count": "1",
                # "ceph.dir.layout.object_size": "4194304", #BZ Raised for set failing
            },
        }
        # Loop through namespaces and apply attributes
        for namespace, attributes in namespaces.items():
            for attribute, value in attributes.items():
                for item in ["test_dir", "test.txt"]:
                    # Set the attribute
                    fs_util.set_xattrs(
                        clients[0],
                        f"{fuse_mounting_dir_1}/{item}",
                        key=attribute,
                        value=value,
                    )
                    # Get the attribute
                    out, rc = fs_util.get_xattrs(
                        clients[0], f"{kernel_mounting_dir_1}/{item}", key=attribute
                    )
                    if value not in out:
                        raise CommandFailed(
                            f"Failed to set the correct value for {attribute}. Value set is {out}"
                        )
                    fs_util.rm_xattrs(
                        clients[0], f"{kernel_mounting_dir_1}/{item}", key=attribute
                    )

        log.info("Scenario 2: Setting Ceph File and dir attributes")
        log.info("Set pool attributes on file and dir")
        log.info("Add 1 more data pool to existing file system")
        fs_util.create_osd_pool(clients[0], pool_name=f"{default_fs}-data-2")
        fs_util.create_osd_pool(clients[0], pool_name=f"{default_fs}-data-3")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool {default_fs} {default_fs}-data-2"
        )
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool {default_fs} {default_fs}-data-3"
        )

        # Define paths
        dir_path_pool = f"{fuse_mounting_dir_1}/test_dir_pool"
        dir_path_pool_name = f"{fuse_mounting_dir_1}/test_dir_pool_name"
        dir_path_pool_id = f"{fuse_mounting_dir_1}/test_dir_pool_id"
        file_path_pool = f"{dir_path_pool}/file_with_layout_pool.txt"
        file_path_pool_name = f"{dir_path_pool_name}/file_with_layout_pool_name.txt"
        file_path_pool_id = f"{dir_path_pool_id}/file_with_layout_pool_id.txt"

        dir_path_pool_kernel = f"{kernel_mounting_dir_1}/test_dir_pool"
        dir_path_pool_name_kernel = f"{kernel_mounting_dir_1}/test_dir_pool_name"
        dir_path_pool_id_kernel = f"{kernel_mounting_dir_1}/test_dir_pool_id"
        file_path_pool_kernel = f"{dir_path_pool_kernel}/file_with_layout_pool.txt"
        file_path_pool_name_kernel = (
            f"{dir_path_pool_name_kernel}/file_with_layout_pool_name.txt"
        )
        file_path_pool_id_kernel = (
            f"{dir_path_pool_id_kernel}/file_with_layout_pool_id.txt"
        )

        # Create directories and files
        client1.exec_command(
            sudo=True,
            cmd=f"mkdir -p {dir_path_pool} {dir_path_pool_name} {dir_path_pool_id}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"touch {file_path_pool} {file_path_pool_name} {file_path_pool_id}",
        )

        # Verify pool attributes for directories
        verify_pool_attributes(
            client1,
            fs_util,
            dir_path_pool,
            "ceph.dir.layout.pool",
            f"{default_fs}-data-2",
            f"{default_fs}-data-2",
            dir_path_pool_kernel,
        )
        verify_pool_attributes(
            client1,
            fs_util,
            dir_path_pool_name,
            "ceph.dir.layout.pool_name",
            f"{default_fs}-data-3",
            f"{default_fs}-data-3",
            dir_path_pool_name_kernel,
        )
        pool_num_2 = fs_util.get_pool_num(client1, f"{default_fs}-data-2")
        verify_pool_attributes(
            client1,
            fs_util,
            dir_path_pool_id,
            "ceph.dir.layout.pool_id",
            pool_num_2,
            f"{default_fs}-data-2",
            dir_path_pool_id_kernel,
        )

        # Verify pool attributes for files
        verify_pool_attributes(
            client1,
            fs_util,
            file_path_pool,
            "ceph.file.layout.pool",
            f"{default_fs}-data-2",
            f"{default_fs}-data-2",
            file_path_pool_kernel,
            is_file=True,
        )
        verify_pool_attributes(
            client1,
            fs_util,
            file_path_pool_name,
            "ceph.file.layout.pool_name",
            f"{default_fs}-data-3",
            f"{default_fs}-data-3",
            file_path_pool_name_kernel,
            is_file=True,
        )
        verify_pool_attributes(
            client1,
            fs_util,
            file_path_pool_id,
            "ceph.file.layout.pool_id",
            pool_num_2,
            f"{default_fs}-data-2",
            file_path_pool_id_kernel,
            is_file=True,
        )

        log.info("Scenario 3: Setting system attributes")
        set_and_verify_acl(
            client1, kernel_mounting_dir_1, "acl_kernel", "acl_kernel_file"
        )

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info("Clean Up in progress")
        for mount in [fuse_mounting_dir_1, kernel_mounting_dir_1]:
            log.info("Unmounting the mounts if created")
            client1.exec_command(sudo=True, cmd=f"umount {mount}", check_ec=False)
        for mount in [fuse_mounting_dir_1, kernel_mounting_dir_1]:
            log.info("Removing the mount folders created")
            client1.exec_command(sudo=True, cmd=f"rmdir {mount}", check_ec=False)
        for subvolume in subvolume_list:
            fs_util.remove_subvolume(client1, **subvolume)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
        fs_util.remove_fs(client1, default_fs)


def verify_pool_attributes(
    client, fs_util, path, attr_name, attr_value, pool_name, kernel_path, is_file=False
):
    """
    Set and validate pool attributes for directories and files.

    :param client: Ceph client node
    :param fs_util: Ceph FS utility object
    :param path: Path to the directory or file
    :param attr_name: Attribute name to set
    :param pool_name: Pool value to set
    :param other_before_objects: Object count for the other pool
    :param is_file: Boolean flag to indicate if the path is a file
    """
    before_objects = get_storage_stats(client, pool_name=pool_name)

    fs_util.set_xattrs(client, path, key=attr_name, value=f"{attr_value}")
    out, rc = fs_util.get_xattrs(client, kernel_path, key=attr_name)
    if f"{attr_value}" not in out:
        raise CommandFailed(
            f"Failed to set the correct value for ceph.dir.layout.pool_name. Value set is {out}"
        )
    if is_file:
        client.exec_command(
            sudo=True, cmd=f"dd if=/dev/zero of={kernel_path} bs=10M count=10"
        )
    else:
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_path}/bytes_1.txt bs=10M count=10",
        )

    validate_objects(client, before_objects, f"{pool_name}")


@retry(CommandFailed, tries=3, delay=20)
def validate_objects(client, before, pool_name, expectation="increase"):
    log.info(f"Pool name we are validating {pool_name}")
    objects_after = get_storage_stats(client, pool_name=f"{pool_name}")

    before_objects = int(before.get("objects"))
    after_objects = int(objects_after.get("objects"))

    if expectation == "increase":
        if after_objects <= before_objects:
            raise CommandFailed(
                f"Expected the number of objects to increase. Object stats before: {before}, after: {objects_after}"
            )
    elif expectation == "decrease":
        if after_objects >= before_objects:
            raise CommandFailed(
                f"Expected the number of objects to decrease. Object stats before: {before}, after: {objects_after}"
            )
    elif expectation == "no change":
        if after_objects != before_objects:
            raise CommandFailed(
                f"Expected the number of objects to remain the same. Object stats before: {before}, "
                f"after: {objects_after}"
            )
    else:
        raise ValueError(
            "Invalid expectation parameter. Use 'increase', 'decrease', or 'no change'."
        )

    log.info(
        f"Validation successful. Object stats before: {before}, after: {objects_after}"
    )


def set_and_verify_acl(
    client, mount_dir, dir_name, file_name, username="cephuser", acl_file=None
):
    """
    Set and verify ACLs for a directory and a file, including additional options.

    :param client: Ceph client node
    :param mount_dir: The mount directory where the test directory is located
    :param dir_name: The name of the directory to set ACLs on
    :param file_name: The name of the file to set ACLs on
    :param acl_file: Optional ACL file to set ACLs from
    """
    # Define paths
    dir_path = f"{mount_dir}/{dir_name}"
    file_path = f"{mount_dir}/{file_name}"
    client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}")
    client.exec_command(sudo=True, cmd=f"touch {file_path}")
    # Set ACLs for the directory
    client.exec_command(sudo=True, cmd=f"setfacl -m u:{username}:rwx {dir_path}")
    log.info(f"ACLs set for directory {dir_path}")

    # Verify the ACLs for the directory
    out, rc = client.exec_command(sudo=True, cmd=f"getfacl {dir_path}")
    if f"{username}:rwx" not in out:
        raise CommandFailed(f"Failed to set ACL for {dir_path}. Output: {out}")

    # Set ACLs for the file
    client.exec_command(sudo=True, cmd=f"setfacl -m u:{username}:rw {file_path}")
    log.info(f"ACLs set for file {file_path}")

    # Verify the ACLs for the file
    out, rc = client.exec_command(sudo=True, cmd=f"getfacl {file_path}")
    if f"{username}" not in out:
        raise CommandFailed(f"Failed to set ACL for {file_path}. Output: {out}")

    # Test permission updates by creating a new directory and file
    client.exec_command(cmd=f"mkdir {dir_path}/new_dir")
    client.exec_command(cmd=f"touch {dir_path}/new_file.txt")
    log.info("New directory and file created to verify ACL permissions.")

    # Set ACLs from file if provided
    if acl_file:
        client.exec_command(sudo=True, cmd=f"setfacl -M {acl_file} {dir_path}")
        log.info(f"ACLs set from file {acl_file} for directory {dir_path}")

    # Validate read permissions
    # try:

    client.exec_command(cmd=f"cat {file_path}")
    log.info(f"Read permissions validated for file {file_path}")

    # Remove ACLs
    client.exec_command(sudo=True, cmd=f"setfacl -x u:{username} {dir_path}")
    log.info(f"ACLs removed for directory {dir_path}")

    out, rc = client.exec_command(sudo=True, cmd=f"getfacl {dir_path}")
    if f"{username}" in out:
        raise CommandFailed(f"Failed to remove ACL for {dir_path}. Output: {out}")

    client.exec_command(cmd=f"cat {file_path}")
    out, rc = client.exec_command(cmd=f"touch {file_path}_cephuser", check_ec=False)

    if not rc:
        raise CommandFailed(
            f"Able to read file without permissions, file name: {file_path}_cephuser"
        )
    log.info(f"Read permissions validated for file {file_path}_cephuser")


log.info("ACL set and verified successfully.")
