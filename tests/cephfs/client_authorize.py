import logging
import secrets
import string
import traceback

from ceph.ceph import CommandFailed

logger = logging.getLogger(__name__)
log = logger


def verify_write_failure(client, kernel_mount_dir, fuse_mount_dir, client_name):
    """
    This function is used to verify write failure on directory in cephfs.
    Verification will be done on kernel_mount & fuse_mount for ceph client.
    :param client:
    :param kernel_mount_dir:
    :param fuse_mount_dir:
    :param client_name:
    """
    try:
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mount_dir}/file bs=10M count=10",
        )
    except CommandFailed as e:
        log.info(e)
        log.info(
            f"Permissions set for client {client_name} is working for kernel mount"
        )
    else:
        log.error(
            f"Permissions set for client {client_name} is not working for kernel mount"
        )
        return 1
    try:
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mount_dir}/file bs=10M count=10",
        )
    except CommandFailed as e:
        log.info(e)
        log.info(f"Permissions set for client {client_name} is working for fuse mount")
    else:
        log.error(
            f"Permissions set for client {client_name} is not working for fuse mount"
        )
        return 1
    return 0


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83574483"
        log.info("Running cephfs %s test case" % (tc))

        config = kw.get("config")
        rhbuild = config.get("rhbuild")
        from tests.cephfs.cephfs_utilsV1 import FsUtils

        fs_util = FsUtils(ceph_cluster)
        client = ceph_cluster.get_ceph_objects("client")
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)

        # Create client with read-write permission on "/" directory
        mount_points = []
        client_name = "Client_1"
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        fs_name = "cephfs"
        if "4." in rhbuild:
            fs_name = "cephfs_new"
        log.info(f"Testing {client_name} with read-write permission on root directory")
        fs_util.fs_client_authorize(client[0], fs_name, client_name, "/", "rw")
        # Mount cephfs on kernel & fuse client
        fs_util.kernel_mount(
            client,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname=client_name,
        )
        fs_util.fuse_mount(client, fuse_mount_dir, new_client_hostname=client_name)
        # Create directories & files inside them for this & next test scenarios
        for num in range(1, 4):
            log.info("Creating Directories")
            out, rc = client[0].exec_command(
                sudo=True, cmd="mkdir %s/%s_%d" % (kernel_mount_dir, "dir", num)
            )
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={kernel_mount_dir}/dir_{num}/file_{num} bs=10M count=10",
            )
        # Test read & write opearions on "/" directory on both kernel & fuse mount
        commands = [
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top {kernel_mount_dir}",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top {kernel_mount_dir}",
            f"dd if=/dev/zero of={fuse_mount_dir}/file bs=10M count=10",
            f"dd if={fuse_mount_dir}/file of={fuse_mount_dir}/dir_1/file2 bs=10M count=10",
        ]
        for command in commands:
            _, err = client[0].exec_command(sudo=True, cmd=command, long_running=True)
            if err:
                log.error(f"Permissions set for client {client_name} is not working")
                return 1
        log.info(f"Permissions set for client {client_name} is working")
        # Create client with read permission on "/" directory & read-write permission on "dir1" directory
        client_name = "Client_2"
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        log.info(
            f"Testing {client_name} with read permission on root & read-write permission on /dir_1"
        )
        fs_util.fs_client_authorize(
            client[0], fs_name, client_name, "/", "r", extra_params=" /dir_1 rw"
        )
        # Mount cephfs on kernel & fuse client
        fs_util.kernel_mount(
            client, kernel_mount_dir, mon_node_ip, new_client_hostname=client_name
        )
        fs_util.fuse_mount(client, fuse_mount_dir, new_client_hostname=client_name)
        # Verify write operation on "/" directory fails
        rc = verify_write_failure(
            client[0], kernel_mount_dir, fuse_mount_dir, client_name
        )
        if rc == 1:
            return 1
        # Test read operation "/" directory & read-write operation on "dir1" directory
        commands = [
            f"dd if={fuse_mount_dir}/file of={fuse_mount_dir}/dir_1/file_copy_2 bs=10M count=10",
            f"dd if={kernel_mount_dir}/file of={kernel_mount_dir}/dir_1/file_copy_3 bs=10M count=10",
        ]
        for command in commands:
            _, err = client[0].exec_command(sudo=True, cmd=command, long_running=True)
            if err:
                log.error(f"Permissions set for client {client_name} is not working")
                return 1
        log.info(f"Permissions set for client {client_name} is working")
        # Create client with read permission on "dir_2" directory
        client_name = "Client_3"
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        log.info(
            f"Testing {client_name} with read-write permission on /dir_2 directory"
        )
        fs_util.fs_client_authorize(client[0], fs_name, client_name, "/dir_2", "rw")
        # Mount cephfs on kernel & fuse client on sub_directory "dir_2"
        fs_util.kernel_mount(
            client,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname=client_name,
            sub_dir="dir_2",
        )
        fs_util.fuse_mount(
            client,
            fuse_mount_dir,
            new_client_hostname=client_name,
            extra_params=" -r /dir_2",
        )
        # Verify mount on root directory fails
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        try:
            fs_util.kernel_mount(
                client, kernel_mount_dir, mon_node_ip, new_client_hostname=client_name
            )
        except AssertionError as e:
            log.info(e)
            log.info(
                f"Permissions set for client {client_name} is working for kernel mount"
            )
        except CommandFailed as e:
            log.info(e)
            err = str(e)
            err = err.split()
            if "mount" in err:
                log.info(
                    f"Permissions set for client {client_name} is working for kernel mount"
                )
            else:
                log.info(traceback.format_exc())
                return 1
        except Exception as e:
            log.info(e)
            log.info(traceback.format_exc())
            return 1
        else:
            log.error(
                f"Permissions set for client {client_name} is not working for kernel mount"
            )
            return 1
        try:
            fs_util.fuse_mount(client, fuse_mount_dir, new_client_hostname=client_name)
        except AssertionError as e:
            log.info(e)
            log.info(
                f"Permissions set for client {client_name} is working for fuse mount"
            )
        except CommandFailed as e:
            log.info(e)
            err = str(e)
            err = err.split()
            if "mount" in err:
                log.info(
                    f"Permissions set for client {client_name} is working for fuse mount"
                )
            else:
                log.info(traceback.format_exc())
                return 1
        except Exception as e:
            log.info(e)
            log.info(traceback.format_exc())
            return 1
        else:
            log.error(
                f"Permissions set for client {client_name} is not working for fuse mount"
            )
            return 1
        # Test read & write opearions on kernel & fuse mount
        commands = [
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top {kernel_mount_dir}",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top {kernel_mount_dir}",
            f"dd if=/dev/zero of={fuse_mount_dir}/file bs=10M count=10",
            f"dd if={fuse_mount_dir}/file of={fuse_mount_dir}/file bs=10M count=10",
        ]
        for command in commands:
            _, err = client[0].exec_command(sudo=True, cmd=command, long_running=True)
            if err:
                log.error(f"Permissions set for client {client_name} is not working")
                return 1
        log.info(f"Permissions set for client {client_name} is working")
        # Create client with read permission on "dir_3" directory
        client_name = "Client_4"
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        log.info(f"Testing {client_name} with read permission on /dir_3 directory")
        fs_util.fs_client_authorize(client[0], fs_name, client_name, "/dir_3", "r")
        # Verify mount on root directory fails
        try:
            fs_util.kernel_mount(
                client, kernel_mount_dir, mon_node_ip, new_client_hostname=client_name
            )
        except AssertionError as e:
            log.info(e)
            log.info(
                f"Permissions set for client {client_name} is working for kernel mount"
            )
        except CommandFailed as e:
            log.info(e)
            err = str(e)
            err = err.split()
            if "mount" in err:
                log.info(
                    f"Permissions set for client {client_name} is working for kernel mount"
                )
            else:
                log.info(traceback.format_exc())
                return 1
        except Exception as e:
            log.info(e)
            log.info(traceback.format_exc())
            return 1
        else:
            log.error(f"Permissions set for client {client_name} is not working")
            return 1
        try:
            fs_util.fuse_mount(client, fuse_mount_dir, new_client_hostname=client_name)
        except AssertionError as e:
            log.info(e)
            log.info(
                f"Permissions set for client {client_name} is working for fuse mount"
            )
        except CommandFailed as e:
            log.info(e)
            err = str(e)
            err = err.split()
            if "mount" in err:
                log.info(
                    f"Permissions set for client {client_name} is working for fuse mount"
                )
            else:
                log.info(traceback.format_exc())
                return 1
        except Exception as e:
            log.info(e)
            log.info(traceback.format_exc())
            return 1
        else:
            log.error(f"Permissions set for client {client_name} is not working")
            return 1
        # Mount cephfs on kernel & fuse client on sub_directory "dir_3"
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        fs_util.kernel_mount(
            client,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname=client_name,
            sub_dir="dir_3",
        )
        fs_util.fuse_mount(
            client,
            fuse_mount_dir,
            new_client_hostname=client_name,
            extra_params=" -r /dir_3",
        )
        # Verify write opearions on kernel & fuse mount fails
        rc = verify_write_failure(
            client[0], kernel_mount_dir, fuse_mount_dir, client_name
        )
        if rc == 1:
            return 1
        # Verify read opearions on kernel & fuse mount
        commands = [
            f"dd if={fuse_mount_dir}/file_3 of=~/file_3 bs=10M count=10",
            f"dd if={kernel_mount_dir}/file_3 of=~/file_33 bs=10M count=10",
        ]
        for command in commands:
            _, err = client[0].exec_command(sudo=True, cmd=command, long_running=True)
            if err:
                log.error(f"Permissions set for client {client_name} is not working")
                return 1
        log.info(f"Permissions set for client {client_name} is working")
        log.info("Clean up the system")
        out, rc = client[0].exec_command(sudo=True, cmd=f"rm -rf {mount_points[1]}/*")
        for mount_point in mount_points:
            out, rc = client[0].exec_command(sudo=True, cmd=f"umount {mount_point}")
            if "5." in rhbuild:
                out, err = client[1].exec_command(
                    sudo=True, cmd=f"umount {mount_point}"
                )
        for mount_point in mount_points:
            out, rc = client[0].exec_command(sudo=True, cmd=f"rm -rf {mount_point}/")
            if "5." in rhbuild:
                out, err = client[1].exec_command(
                    sudo=True, cmd=f"rm -rf {mount_point}/"
                )
        for num in range(1, 5):
            out, err = client[0].exec_command(
                sudo=True, cmd=f"ceph auth rm client.client_{num}"
            )
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
