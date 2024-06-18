import secrets
import string
import traceback
from datetime import datetime, timedelta

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
global stop_flag
stop_flag = False


def start_io_time(fs_util, client1, mounting_dir, timeout=300):
    global stop_flag
    iter = 0
    if timeout:
        stop = datetime.now() + timedelta(seconds=timeout)
    else:
        stop = 0
    while True:
        if stop and datetime.now() > stop:
            log.info("Timed out *************************")
            break
        client1.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}/run_ios_{iter}")
        fs_util.run_ios(
            client1,
            f"{mounting_dir}/run_ios_{iter}",
            io_tools=["smallfile", "file_extract"],
        )
        iter = iter + 1
        if stop_flag:
            log.info("Exited as stop flag is set to True")
            break


def run(ceph_cluster, **kw):
    """
    CEPH-83591419 - Validate Root Sqaush operations on Cephfs
    Test Steps:
    1. Create 2 file systems
        1. Cephfs_1 → 1 Active MDS and 1 Standby
        2. Cephfs_2 → 2 Active MDS and 1 Standby
    2. Create subolumegroups
       1. Subvol_group_1
       2. Subvol_group_2
    3. Create Subvolumes in above filesystems
       1. Subvol_1
       2. Subvol_2
       3. Subvol_group_1 → Subvol_3
       4. Subvol_group_2 → Subvol_4
    4. Create multiple ceph clients
       1. Client 1: with out RootSquash
       2. Client 2: with roosquash with out any path in cephfs_1
       3. Client 3: with rootsquash with a particluar path in cephfs_1
       4. Client 4: With rootsquash with particular path in both filesystems(Cephfs_1, Cephfs_2)
    5. Mount using kernel and fuse with above clients on 2 Virtual Machines
       1. Mount 1: Kernel client_1
       2. Mount 2: Fuse Client_1
       3. Mount 3: kernel Client_2
       4. Mount 4: Fuse Client_2
       5. Mount 5: kernel Client_3
       6. Mount 6: Fuse Client_3
       7. Mount 7: kernel Client_4
       8. Mount 8: Fuse Client_4
    6. Perform below operations on Above mounts on both Virtual Machines
    """
    try:
        tc = "CEPH-83591419"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client, client1 = clients[0], clients[1]
        if len(clients) < 2:
            log.info(f"This requires atleast 2 clients total clients : {len(clients)}")
            log.error(f"This requires atleast 2 clients total clients : {len(clients)}")
            return -1
        log.info("checking Pre-requisites")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        log.info(
            "Create Test configuration - Additional CephFS and one subvolume in each CephFS"
        )

        fs_list = ["cephfs_1", "cephfs_2"]
        subvolume_group_list = ["subvol_group_1", "subvol_group_2"]
        subvolume_list = [
            {"subvol_name": "subvol_1"},
            {"subvol_name": "subvol_2"},
            {"subvol_name": "subvol_3", "group_name": "subvol_group_1"},
            {"subvol_name": "subvol_4", "group_name": "subvol_group_2"},
        ]
        [fs_util.create_fs(client, fs_name) for fs_name in fs_list]
        for fs in fs_list:
            for subvolume_group in subvolume_group_list:
                subvolumegroup = {"vol_name": fs, "group_name": subvolume_group}
                fs_util.create_subvolumegroup(client, **subvolumegroup)
            for subvolume in subvolume_list:
                fs_util.create_subvolume(client, vol_name=fs, **subvolume)
        fs_util.create_ceph_client(
            client,
            "client_x",
            mon_caps="allow *",
            mds_caps="allow *",
            osd_caps="allow *",
        )
        fs_util.create_ceph_client(
            client,
            "client_1",
            mon_caps="allow *",
            mds_caps="allow * root_squash",
            osd_caps="allow *",
        )
        fs_util.create_ceph_client(
            client,
            "client_2",
            mon_caps="allow *",
            mds_caps=f"allow * fsname={fs_list[0]} root_squash",
            osd_caps="allow *",
        )
        fs_util.create_ceph_client(
            client,
            "client_3",
            mon_caps="allow *",
            # Commenting out these lines because client creation is failing based on feedback from the development team.
            # This can be updated later for the multifs case.
            # mds_caps=f"allow rw fsname={fs_list[0]} root_squash,allow * path=/,allow *
            # fsname={fs_list[1]} root_squash path=/",
            mds_caps=f"allow rw fsname={fs_list[0]} root_squash, allow rw fsname={fs_list[0]} path=/volumes",
            # f", allow rw fsname={fs_list[1]}",
            osd_caps=f"allow rw tag {fs_list[0]} data={fs_list[0]}, allow rw tag {fs_list[1]} data={fs_list[1]}",
        )
        fs_util.create_ceph_client(
            client,
            "client_4",
            mon_caps="allow *",
            # Commenting out these lines because client creation is failing based on feedback from the development team.
            # This can be updated later for the multifs case.
            # mds_caps=f"allow rw fsname={fs_list[0]} root_squash,allow * path=/,allow *
            # fsname={fs_list[1]} root_squash path=/",
            mds_caps=f"allow rw fsname={fs_list[0]} root_squash, allow rw fsname={fs_list[0]} path=/volumes"
            f", allow rw fsname={fs_list[1]}",
            osd_caps=f"allow rw tag {fs_list[0]} data={fs_list[0]}, allow rw tag {fs_list[1]} data={fs_list[1]}",
        )
        fs_util.create_ceph_client(
            client,
            "client_5",
            mon_caps="allow *",
            mds_caps=f"allow * fsname={fs_list[0]} root_squash,allow * path=/volumes/subvol_group_1/subvol_3,"
            f"allow rw fsname={fs_list[1]} root_squash,allow * path=/volumes/subvol_group_2/subvol_4",
            osd_caps="allow *",
        )
        fs_util.create_ceph_client(
            client,
            "client_7",
            mon_caps="allow *",
            mds_caps=f"allow * fsname={fs_list[0]} root_squash,allow * path=/volumes/subvol_group_1/subvol_3,"
            f"allow rw fsname={fs_list[1]} root_squash,allow * path=/volumes/subvol_group_2/subvol_4",
            osd_caps="allow *",
        )
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )

        fs_util.fuse_mount(
            [clients[0]],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_list[0]}",
        )
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        kernel_mount_dir = "/mnt/kernel_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs_list[0]}",
        )
        with parallel() as p:
            global stop_flag
            p.spawn(
                start_io_time,
                fs_util,
                clients[0],
                fuse_mount_dir,
                timeout=0,
            )
            p.spawn(
                start_io_time,
                fs_util,
                clients[0],
                kernel_mount_dir,
            )

            log.info(
                "Test 1: Attempting to create a client with a non-existent path. "
                "Expected behavior: The operation should succeed, but the user should be unable to create any folders "
                "until the non-existent path is created. Once the path is created, "
                "the user should be able to create folders inside it."
            )
            fs_util.create_ceph_client(
                client,
                "client_6",
                mon_caps="allow *",
                mds_caps=f"allow * fsname={fs_list[0]} root_squash,allow * path=/not_defined",
                osd_caps="allow *",
            )
            mon_node_ips = fs_util.get_mon_node_ips()
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_scenario_1",
                new_client_hostname="client_6",
                extra_params=f" --client_fs {fs_list[0]}",
            )

            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_scenario_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_6",
                extra_params=f",fs={fs_list[0]}",
            )
            validate_file_dir(
                client,
                path="/mnt/client_root_squash_scenario_1",
                file_name="scenario_1",
                allowed=False,
            )
            validate_file_dir(
                client1,
                path="/mnt/client_root_squash_scenario_1",
                file_name="scenario_1",
                allowed=False,
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_x",
                new_client_hostname="client_x",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            client.exec_command(
                sudo=True, cmd="mkdir -p /mnt/client_root_squash_x/not_defined"
            )
            validate_file_dir(
                client,
                path="/mnt/client_root_squash_scenario_1/not_defined",
                file_name="scenario_1_1",
            )
            validate_file_dir(
                client1,
                path="/mnt/client_root_squash_scenario_1/not_defined",
                file_name="scenario_1_1",
            )

            log.info(
                "Test 2: Client should not be able to create or delete folders under "
                "any path of any filesystem present if no path is provided"
            )

            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_sceario_2",
                new_client_hostname="client_1",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_sceario_2_1",
                new_client_hostname="client_1",
                extra_params=f" --client_fs {fs_list[1]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_sceario_2",
                ",".join(mon_node_ips),
                new_client_hostname="client_1",
                extra_params=f",fs={fs_list[0]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_sceario_2_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_1",
                extra_params=f",fs={fs_list[1]}",
            )
            for cl in [client, client1]:
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_sceario_2",
                    file_name="test_scenario_2",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_sceario_2/volumes",
                    file_name="test_scenario_2_1",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_sceario_2_1/volumes",
                    file_name="test_scenario_2_1",
                    allowed=False,
                )

            log.info(
                "Test 3: Client should not be able to create or delete folders in any path "
                "in any filesystem even if specific Fs is provided"
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_3",
                new_client_hostname="client_2",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_3_1",
                new_client_hostname="client_2",
                extra_params=f" --client_fs {fs_list[1]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_3",
                ",".join(mon_node_ips),
                new_client_hostname="client_2",
                extra_params=f",fs={fs_list[0]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_3_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_2",
                extra_params=f",fs={fs_list[1]}",
            )
            for cl in [client, client1]:
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_3",
                    file_name="test_scenario_3",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_3_1",
                    file_name="test_scenario_3_1",
                    allowed=False,
                )
            log.info(
                "NOTE: *******Change the allowed flag based on BZ : 2293943********"
            )

            log.info(
                "Test 4: Create a client with 2 file system with 1 enabling root_squash and other without root_squash"
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_4",
                new_client_hostname="client_3",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_4_1",
                new_client_hostname="client_4",
                extra_params=f" --client_fs {fs_list[1]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_4",
                ",".join(mon_node_ips),
                new_client_hostname="client_3",
                extra_params=f",fs={fs_list[0]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_4_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_4",
                extra_params=f",fs={fs_list[1]}",
            )
            for cl in [client, client1]:
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_4",
                    file_name="test_scenario_4",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_4_1/volumes/",
                    file_name="test_scenario_4_1",
                )

            log.info(
                "Test 5: Creation of folders should not be allowed in any other paths than specified in caps"
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_5",
                new_client_hostname="client_5",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_5_1",
                new_client_hostname="client_5",
                extra_params=f" --client_fs {fs_list[1]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_5",
                ",".join(mon_node_ips),
                new_client_hostname="client_5",
                extra_params=f",fs={fs_list[0]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_5_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_5",
                extra_params=f",fs={fs_list[1]}",
            )
            for cl in [client, client1]:
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_5",
                    file_name="test_scenario_5",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_5/volumes",
                    file_name="test_scenario_5",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_5/volumes/subvol_group_1/subvol_3/",
                    file_name="test_scenario_5",
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_5_1/",
                    file_name="test_scenario_5_1",
                    allowed=False,
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_5_1/volumes/subvol_group_2/subvol_4/",
                    file_name="test_scenario_5_1",
                )
            log.info(
                "Test 6: Update client caps and validate it reflects on the mounts"
            )
            fs_util.update_ceph_client(
                client,
                "client_5",
                mon_caps="allow *",
                mds_caps=f"allow * fsname={fs_list[0]} root_squash,allow * path=/,"
                f"allow rw fsname={fs_list[1]} root_squash,allow * path=/",
                osd_caps="allow *",
            )
            for cl in [client, client1]:
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_5/volumes/",
                    file_name="test_scenario_6",
                    allowed=False,
                )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_6",
                new_client_hostname="client_5",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_6_1",
                new_client_hostname="client_5",
                extra_params=f" --client_fs {fs_list[1]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_6",
                ",".join(mon_node_ips),
                new_client_hostname="client_5",
                extra_params=f",fs={fs_list[0]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_6_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_5",
                extra_params=f",fs={fs_list[1]}",
            )
            for cl in [client, client1]:
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_6/volumes/",
                    file_name="test_scenario_6",
                )
                validate_file_dir(
                    cl,
                    path="/mnt/client_root_squash_Scenario_6_1/volumes/",
                    file_name="test_scenario_6",
                )
            log.info(
                "Test 7: Chmod on the root squash directory and it should not be allowed"
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_7",
                new_client_hostname="client_7",
                extra_params=f" --client_fs {fs_list[0]}",
            )
            fs_util.fuse_mount(
                [client],
                "/mnt/client_root_squash_Scenario_7_1",
                new_client_hostname="client_7",
                extra_params=f" --client_fs {fs_list[1]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_7",
                ",".join(mon_node_ips),
                new_client_hostname="client_7",
                extra_params=f",fs={fs_list[0]}",
            )
            fs_util.kernel_mount(
                [client1],
                "/mnt/client_root_squash_Scenario_7_1",
                ",".join(mon_node_ips),
                new_client_hostname="client_7",
                extra_params=f",fs={fs_list[1]}",
            )
            for cl in [client, client1]:
                validate_chmod(
                    cl,
                    path="/mnt/client_root_squash_Scenario_7",
                    allowed=False,
                )
                validate_chmod(
                    cl,
                    path="/mnt/client_root_squash_Scenario_7/volumes",
                    allowed=False,
                )
                validate_chmod(
                    cl,
                    path="/mnt/client_root_squash_Scenario_7/volumes/subvol_group_1/subvol_3/",
                )
                validate_chmod(
                    cl,
                    path="/mnt/client_root_squash_Scenario_7_1/",
                    allowed=False,
                )
                validate_chmod(
                    cl,
                    path="/mnt/client_root_squash_Scenario_7_1/volumes/subvol_group_2/subvol_4/",
                )

            stop_flag = True
            return 0
    except Exception as e:
        stop_flag = True
        log.error(e)
        log.error(traceback.format_exc())
    finally:
        log.info("Cleanup In Progress")
        stop_flag = True
        for cl in [client, client1]:
            log.info("Unmounting the mounts if created")
            cl.exec_command(
                sudo=True, cmd="umount /mnt/client_root_squash*", check_ec=False
            )
        for cl in [client, client1]:
            log.info("Removing the mount folders created")
            cl.exec_command(
                sudo=True, cmd="rmdir /mnt/client_root_squash*", check_ec=False
            )
        client_list = [f"client_{i}" for i in range(1, 8)]
        for ceph_client in client_list:
            client.exec_command(sudo=True, cmd=f"ceph auth del client.{ceph_client}")
        client.exec_command(sudo=True, cmd="ceph auth del client.client_x")
        for fs in fs_list:
            for subvolume in subvolume_list:
                fs_util.remove_subvolume(client, vol_name=fs, **subvolume)
            for subvolume_group in subvolume_group_list:
                subvolumegroup = {"vol_name": fs, "group_name": subvolume_group}
                fs_util.create_subvolumegroup(client, **subvolumegroup)
            fs_util.remove_fs(client, fs)


def validate_file_dir(client, path, file_name, allowed=True):
    if allowed:
        client.exec_command(sudo=True, cmd=f"mkdir {path}/{file_name}")
        client.exec_command(sudo=True, cmd=f"touch {path}/{file_name}.txt")
        client.exec_command(sudo=True, cmd=f"rm {path}/{file_name}.txt")
        client.exec_command(sudo=True, cmd=f"rmdir {path}/{file_name}")
    else:
        out, rc = client.exec_command(
            sudo=True, cmd=f"mkdir {path}/{file_name}", check_ec=False
        )
        if not rc:
            raise CommandFailed(
                f"We are able to create folder where root_squash is enabled with {path}"
            )
        out, rc = client.exec_command(
            sudo=True, cmd=f"touch {path}/{file_name}.txt", check_ec=False
        )
        if not rc:
            raise CommandFailed(
                f"We are able to create file where root_squash is enabled with {path}"
            )
        out, rc = client.exec_command(
            sudo=True, cmd=f"rm {path}/{file_name}.txt", check_ec=False
        )
        if not rc:
            raise CommandFailed(
                f"We are able to remove file where root_squash is enabled with {path}"
            )
        out, rc = client.exec_command(
            sudo=True, cmd=f"rmdir {path}/{file_name}", check_ec=False
        )
        if not rc:
            raise CommandFailed(
                f"We are able to remove file where root_squash is enabled with {path}"
            )


def validate_chmod(client, path, allowed=True):
    if allowed:
        client.exec_command(sudo=True, cmd=f"chmod +777 {path}")
    else:
        out, rc = client.exec_command(
            sudo=True, cmd=f"chmod +777 {path}", check_ec=False
        )
        if not rc:
            raise CommandFailed(
                f"We are able to execute chmod on folder where root_squash is enabled with {path}"
            )
