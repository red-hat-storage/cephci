import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.client_authorize import test_read_write_op, verify_write_failure
from utility.log import Log

log = Log(__name__)


def verify_mount_failure_on_root(
    fs_util,
    client,
    kernel_mount_dir,
    fuse_mount_dir,
    client_name,
    mon_node_ip,
    **kwargs,
):
    """
    Verify kernel & fuse mount on root directory fails for client.
    :param client:
    :param kernel_mount_dir:
    :param kernel_mount_dir:
    :param fuse_mount_dir:
    :param client_name:
    :param mon_node_ip:
    **kwargs:
        extra_params : we can include extra parameters for mount options such as fs_name
    """
    kernel_fs_para = f",fs={kwargs.get('fs_name', '')}"
    fuse_fs_para = f" --client_fs {kwargs.get('fs_name', '')}"
    try:
        fs_util.kernel_mount(
            client,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname=client_name,
            extra_params=kernel_fs_para,
        )
    except AssertionError as e:
        log.info(e)
        log.info(f"Permissions set for {client_name} is working for kernel mount")
    except CommandFailed as e:
        log.info(e)
        err = str(e)
        err = err.split()
        if "mount" in err:
            log.info(f"Permissions set for {client_name} is working for kernel mount")
        else:
            log.info(traceback.format_exc())
            return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    else:
        log.error(f"Permissions set for {client_name} is not working for kernel mount")
        return 1
    try:
        fs_util.fuse_mount(
            client,
            fuse_mount_dir,
            new_client_hostname=client_name,
            extra_params=fuse_fs_para,
        )
    except AssertionError as e:
        log.info(e)
        log.info(f"Permissions set for {client_name} is working for fuse mount")
    except CommandFailed as e:
        log.info(e)
        err = str(e)
        err = err.split()
        if "mount" in err:
            log.info(f"Permissions set for {client_name} is working for fuse mount")
        else:
            log.info(traceback.format_exc())
            return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    else:
        log.error(f"Permissions set for {client_name} is not working for fuse mount")
        return 1


def verifiy_client_eviction(client, kernel_mount_dir, fuse_mount_dir, client_name):
    """
    Verify kernel & fuse mount of client which is evicted
    :param client:
    :param kernel_mount_dir:
    :param fuse_mount_dir:
    :param client_name:
    """
    try:
        client.exec_command(sudo=True, cmd=f"ls {kernel_mount_dir}")
    except CommandFailed as e:
        log.info(e)
        log.info(f"kernel mount is evicted successfully for {client_name}")
    else:
        log.error(f"kernel mount of {client_name} has not been evicted")
        return 1
    try:
        client.exec_command(sudo=True, cmd=f"ls {fuse_mount_dir}")
    except CommandFailed as e:
        log.info(e)
        log.info(f"fuse mount is evicted successfully for {client_name}")
    else:
        log.error(f"fuse mount of {client_name} has not been evicted")
        return 1
    return 0


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Create cephfs volume
       creats fs volume create <vol_name>

    Subvolume authorize operations
    1. Create cephfs subvolume
       ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>]
    2. Create client (say client.1) with read-write permission on subvolume created in step 1
       ceph fs subvolume authorize <vol_name> <sub_name> <auth_id> [--access_level=rw]
    3. Verify client is created in authorized list of clients in subvolume
       ceph fs subvolume authorized_list <vol_name> <sub_name> [--group_name=<group_name>]
    4. Get path of subvolume
       ceph fs subvolume getpath <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    5. Mount kcephfs on path of subvolume we got in step 3 with “client.1” created in step 2
       mount -t ceph MONITOR-1_NAME:6789:/SUBVOLUME_PATH MOUNT_POINT -o name=CLIENT_ID,fs=FILE_SYSTEM_NAME
    6. Verify read-write operations
    7. Mount ceph-fuse  on path of subvolume we got in step 3 with “client.1”
       ceph-fuse -n client.CLIENT_ID MOUNT_POINT -r SUBVOLUME_PATH
    8. Verify read-write operation
    9. Verify kcephfs mount on “/” directory  fails with “client.1”
       mount -t ceph MONITOR-1_NAME:6789,MONITOR-2_NAME:6789,MONITOR-3_NAME:6789:/ -o name=CLIENT_ID,fs=FILE_SYSTEM_NAME
    10. Verify ceph-fuse mount on “/” directory fails with “client.1”
        ceph-fuse -n client.CLIENT_ID MOUNT_POINT
    11. Evict “client1”
        ceph fs subvolume evict <vol_name> <sub_name> <auth_id> [--group_name=<group_name>]
    12. Verify kernel & fuse mounts of “client1” are not accessible
    13. Deauthorize “client1”
        ceph fs subvolume deauthorize <vol_name> <sub_name> <auth_id> [--group_name=<group_name>]
    14. Verify client is removed in authorized list of clients in subvolume
        ceph fs subvolume authorized_list <vol_name> <sub_name> [--group_name=<group_name>]
    15. Create client (say client.2) with read-only permission on subvolume created in step 1
        ceph fs subvolume authorize <vol_name> <sub_name> <auth_id> [--access_level=r]
    16. Mount kcephfs on path of subvolume we got in step 16 with “client.2” created in step 15
    17. Verify read operation on subvolume
    18. Verify write operation fails on subvolume
    19. Mount ceph-fuse  on path of subvolume we got in step 16 with “client.2”
    20. Repeat steps 17-18
    21. Verify kcephfs mount on “/” directory  fails with “client.2”
    22. Verify ceph-fuse mount on “/” directory fails with “client.2”
    23. Evict “client2”
    24. Verify kernel & fuse mounts of “client2” are not accessible
    25. Deauthorize “client2”
    26. Verify client is removed in authorized list of clients in subvolume
    27. Create cephfs subvolumegroup
        ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
    28. Create cephfs subvolume in subvolumegroup
        ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
    29. Reapeat steps 2-27 on cephfs subvolume created in above step (step 28)

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    3. ceph fs subvolumegroup rm <vol_name> <group_name>
    4. Remove all the cephfs mounts
    """
    try:
        tc = "CEPH-83574596"
        log.info("Running cephfs %s test case" % (tc))

        config = kw.get("config")
        rhbuild = config.get("rhbuild")

        fs_util = FsUtils(ceph_cluster)
        client = ceph_cluster.get_ceph_objects("client")
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)

        fs_name = "cephfs"
        subvolume_name = "sub1"
        subvolgroup_name = ""
        if "4." in rhbuild:
            fs_name = "cephfs_new"
        client_no = 1
        for i in range(1, 3):
            if subvolgroup_name == "":
                log.info("Creating Cephfs subvolume")
                fs_util.create_subvolume(client[0], fs_name, subvolume_name)
            else:
                log.info("Creating Cephfs subvolumegroup")
                fs_util.create_subvolumegroup(client[0], fs_name, subvolgroup_name)
                log.info("Creating Cephfs subvolume in subvolumegroup")
                fs_util.create_subvolume(
                    client[0], fs_name, subvolume_name, group_name=subvolgroup_name
                )

            mount_points = []
            client_name = "Client" + str(client_no)
            kernel_mount_dir = "/mnt/" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            fuse_mount_dir = "/mnt/" + "".join(
                secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
            )
            mount_points.extend([kernel_mount_dir, fuse_mount_dir])
            log.info("Testing client with read-write permission on subvolume")
            fs_util.subvolume_authorize(
                client[0],
                fs_name,
                subvolume_name,
                client_name,
                extra_params=f" {subvolgroup_name} --access_level=rw",
            )
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume authorized_list {fs_name} {subvolume_name} {subvolgroup_name}",
            )
            if client_name in out:
                log.info("Client creation successful")
            else:
                log.error("Client creation failed")
                return 1
            log.info("Getting the path of Cephfs subvolume")
            subvol_path, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} {subvolume_name} {subvolgroup_name}",
            )
            subvol_path = subvol_path.strip()
            log.info(f"Testing kernel & fuse mount for {client_name}")
            fs_util.kernel_mount(
                client,
                kernel_mount_dir,
                mon_node_ip,
                new_client_hostname=client_name,
                sub_dir=f"{subvol_path}",
            )
            fs_util.fuse_mount(
                client,
                fuse_mount_dir,
                new_client_hostname=client_name,
                extra_params=f" -r {subvol_path}",
            )
            log.info(f"Testing read-write operation on {client_name}")
            rc = test_read_write_op(
                client[0], kernel_mount_dir, fuse_mount_dir, client_name
            )
            if rc == 1:
                return 1
            kernel_mount_dir2 = "/mnt/" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            fuse_mount_dir2 = "/mnt/" + "".join(
                secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
            )
            log.info(f"Verifying mount on root directory fails for {client_name}")
            rc = verify_mount_failure_on_root(
                fs_util,
                client,
                kernel_mount_dir2,
                fuse_mount_dir2,
                client_name,
                mon_node_ip,
            )
            if rc == 1:
                return 1
            log.info(f"Testing client eviction for {client_name}")
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume evict {fs_name} {subvolume_name} {client_name} {subvolgroup_name}",
            )
            rc = verifiy_client_eviction(
                client[0], kernel_mount_dir, fuse_mount_dir, client_name
            )
            if rc == 1:
                return 1
            log.info(f"Testing deauthorization for {client_name}")
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume deauthorize {fs_name} {subvolume_name} {client_name} {subvolgroup_name}",
            )
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume authorized_list {fs_name} {subvolume_name} {subvolgroup_name}",
            )
            if subvolume_name not in out:
                log.info(f"{client_name} is deauthorized successfully")
            else:
                log.error(f"{client_name} deauthorization failed")
                return 1
            client_no += 1
            client_name = "Client" + str(client_no)
            kernel_mount_dir = "/mnt/" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            fuse_mount_dir = "/mnt/" + "".join(
                secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
            )
            mount_points.extend([kernel_mount_dir, fuse_mount_dir])
            log.info("Testing client with read-only permission on subvolume")
            fs_util.subvolume_authorize(
                client[0],
                fs_name,
                subvolume_name,
                client_name,
                extra_params=f" {subvolgroup_name} --access_level=r",
            )
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume authorized_list {fs_name} {subvolume_name} {subvolgroup_name}",
            )
            if client_name in out:
                log.info("Client creation successful")
            else:
                log.error("Client creation failed")
                return 1
            log.info(f"Testing kernel & fuse mount for {client_name}")
            fs_util.kernel_mount(
                client,
                kernel_mount_dir,
                mon_node_ip,
                new_client_hostname=client_name,
                sub_dir=f"{subvol_path}",
            )
            fs_util.fuse_mount(
                client,
                fuse_mount_dir,
                new_client_hostname=client_name,
                extra_params=f" -r {subvol_path}",
            )
            log.info(f"Testing read operation for {client_name}")
            commands = [
                f"dd if={kernel_mount_dir}/file of=~/file1 bs=10M count=10",
                f"dd if={fuse_mount_dir}/file  of=~/file2 bs=10M count=10",
            ]
            for command in commands:
                err = client[0].exec_command(sudo=True, cmd=command, long_running=True)
                if err:
                    log.error(f"Permissions set for {client_name} is not working")
                    return 1
            log.info(f"Verifying write operation fails for {client_name}")
            rc = verify_write_failure(
                client[0], kernel_mount_dir, fuse_mount_dir, client_name
            )
            if rc == 1:
                return 1
            kernel_mount_dir2 = "/mnt/" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            fuse_mount_dir2 = "/mnt/" + "".join(
                secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
            )
            log.info(f"Verifying mount on root directory fails for {client_name}")
            rc = verify_mount_failure_on_root(
                fs_util,
                client,
                kernel_mount_dir2,
                fuse_mount_dir2,
                client_name,
                mon_node_ip,
            )
            if rc == 1:
                return 1
            log.info(f"Testing client eviction for {client_name}")
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume evict {fs_name} {subvolume_name} {client_name} {subvolgroup_name}",
            )
            rc = verifiy_client_eviction(
                client[0], kernel_mount_dir, fuse_mount_dir, client_name
            )
            if rc == 1:
                return 1
            log.info(f"Testing deauthorization for {client_name}")
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume deauthorize {fs_name} {subvolume_name} {client_name} {subvolgroup_name}",
            )
            out, rc = client[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume authorized_list {fs_name} {subvolume_name} {subvolgroup_name}",
            )
            if subvolume_name not in out:
                log.info(f"{client_name} is deauthorized successfully")
            else:
                log.error(f"{client_name} deauthorization failed")
                return 1
            log.info("Cleaning up the system")
            kernel_mount_dir = "/mnt/" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            mount_points.extend([kernel_mount_dir])
            fs_util.kernel_mount(
                client, kernel_mount_dir, mon_node_ip, new_client_hostname="admin"
            )
            out, rc = client[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mount_dir}/*"
            )
            for mount_point in mount_points:
                out, rc = client[0].exec_command(sudo=True, cmd=f"umount {mount_point}")
                if "5." in rhbuild:
                    out, err = client[1].exec_command(
                        sudo=True, cmd=f"umount {mount_point}"
                    )
            for mount_point in mount_points:
                out, rc = client[0].exec_command(
                    sudo=True, cmd=f"rm -rf {mount_point}/"
                )
                if "5." in rhbuild:
                    out, err = client[1].exec_command(
                        sudo=True, cmd=f"rm -rf {mount_point}/"
                    )
            client_no += 1
            subvolgroup_name = "subvolgroup1"
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
