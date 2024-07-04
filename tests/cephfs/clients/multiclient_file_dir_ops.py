import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573529 - Validate file locking in fs - Run 2 identical jobs on same mounts from different.

    Steps Performed:
    1. Create volume and mount using fuse it on 2 clients
    2. Create a dir in client1 and check if it refelects in client 2
    3. Create a file and write data from client1 and check if the contents are same in client2
    4. Update the file from client2 and check if the contents are updated in client1
    5. Delete dir from client1 and check if it reflects from client2
    6. Delete file from client1 and check if it refelects from client2
    Args:
        ceph_cluster:
        **kw:

    Returns:
        0 --> if test PASS
        1 --> if test FAIL

    """
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)

        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")

        version, rc = clients[0].exec_command(
            sudo=True, cmd="ceph version --format json"
        )
        fs_util.prepare_clients([clients[0]], build)
        fs_util.auth_list([clients[0], clients[1]])
        if not build.startswith(("3", "4", "5")):
            if not fs_util.validate_fs_info(clients[0], "cephfs"):
                log.error("FS info Validation failed")
                return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount([clients[0], clients[1]], fuse_mounting_dir)

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0], clients[1]], kernel_mounting_dir, ",".join(mon_node_ips)
        )
        dir_operations(
            client1=clients[0], client2=clients[1], fuse_mounting_dir=fuse_mounting_dir
        )
        dir_operations(
            client1=clients[0],
            client2=clients[1],
            fuse_mounting_dir=fuse_mounting_dir,
            kernel_mounting_dir=kernel_mounting_dir,
        )

        file_operations(
            client1=clients[0], client2=clients[1], fuse_mounting_dir=fuse_mounting_dir
        )
        file_operations(
            client1=clients[0],
            client2=clients[1],
            fuse_mounting_dir=fuse_mounting_dir,
            kernel_mounting_dir=kernel_mounting_dir,
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("---clean up---------")
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir
        )
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mounting_dir,
        )


# Function for directory operations
def dir_operations(client1, client2, fuse_mounting_dir, **kwargs):
    # Create a directory on client 1
    client1.exec_command(
        sudo=True, cmd=f"mkdir {fuse_mounting_dir}/dir_multiclient_file_dir_ops"
    )

    # Check if the directory reflects on client 2
    if kwargs.get("kernel_mounting_dir"):
        out, rc = client2.exec_command(
            sudo=True, cmd=f'ls {kwargs.get("kernel_mounting_dir")}'
        )
    else:
        out, rc = client2.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir}")
    if "dir" in out:
        log.info("Directory exists on client 2")
    else:
        log.error("Directory does not exist on client 2")
        raise CommandFailed("Directory does not exist on client 2")

    # Delete the directory on client 1
    client1.exec_command(
        sudo=True, cmd=f"rmdir {fuse_mounting_dir}/dir_multiclient_file_dir_ops"
    )

    # Check if the directory is deleted on client 2
    validate_dir_deletion(client2, fuse_mounting_dir, **kwargs)


@retry(CommandFailed, tries=5, delay=30)
def validate_dir_deletion(client2, fuse_mounting_dir, **kwargs):
    if not kwargs.get("kernel_mounting_dir"):
        out, rc = client2.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir}")
    else:
        out, rc = client2.exec_command(
            sudo=True, cmd=f'ls {kwargs.get("kernel_mounting_dir")}'
        )
    if "dir_multiclient_file_dir_ops" in out:
        log.error(f"Directory still exists on client 2 {out}")
        raise CommandFailed("Directory still exists on client 2")
    else:
        log.info("Directory is deleted on client 2")


# Function for file operations
def file_operations(client1, client2, fuse_mounting_dir, **kwargs):
    # Create a file on client 1 and write data to it
    client1.exec_command(
        sudo=True, cmd=f'echo "Hello, world!" > {fuse_mounting_dir}/file.txt'
    )

    # Check if the file contents are the same on client 2
    if not kwargs.get("kernel_mounting_dir"):
        out, rc = client2.exec_command(
            sudo=True, cmd=f"cat {fuse_mounting_dir}/file.txt"
        )
    else:
        out, rc = client2.exec_command(
            sudo=True, cmd=f'cat {kwargs.get("kernel_mounting_dir")}/file.txt'
        )
    if out.strip() == "Hello, world!":
        log.info("File contents are the same on client 2")
    else:
        log.error("File contents are different on client 2")
        raise CommandFailed("File contents are different on client 2")
    # Update the file on client 2 and check if the contents are updated on client 1
    if not kwargs.get("kernel_mounting_dir"):
        client2.exec_command(
            sudo=True, cmd=f'echo "Updated contents" > {fuse_mounting_dir}/file.txt'
        )
    else:
        client2.exec_command(
            sudo=True,
            cmd=f'echo "Updated contents" > {kwargs.get("kernel_mounting_dir")}/file.txt',
        )

    # Check if the updated file contents are reflected on client 1
    out, rc = client1.exec_command(sudo=True, cmd=f"cat {fuse_mounting_dir}/file.txt")

    if out.strip() == "Updated contents":
        print("File contents are updated on client 1")
    else:
        log.error("File contents are not updated on client 1")
        raise CommandFailed("File contents are not updated on client 1")

    # Delete the file on client 1
    client1.exec_command(sudo=True, cmd=f"rm {fuse_mounting_dir}/file.txt")

    # Check if the file is deleted on client 2
    if not kwargs.get("kernel_mounting_dir"):
        out, rc = client2.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir}")
    else:
        out, rc = client2.exec_command(
            sudo=True, cmd=f'ls {kwargs.get("kernel_mounting_dir")}'
        )
    if "file.txt" in out:
        log.error("File still exists on client 2")
        raise CommandFailed("File still exists on client 2")
    else:
        log.info("File is deleted on client 2")
