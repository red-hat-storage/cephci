import datetime
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.lib.fscrypt_utils import FscryptUtils
from utility.log import Log

log = Log(__name__)
global cg_test_io_status


def test_setup(fs_util, ceph_cluster, default_fs, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if the volume is not there")

    fs_details = fs_util.get_fs_info(client, default_fs)
    fs_vol_created = 0
    if not fs_details:
        fs_util.create_fs(client, default_fs)
        fs_vol_created = 1

    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    nfs_server = nfs_servers[0].node.hostname
    nfs_name = "cephfs-nfs"

    client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
    )
    if wait_for_process(client=client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")

    log.info(
        "Create subvolumegroup, Create subvolume in subvolumegroup and default group"
    )
    subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
    fs_util.create_subvolumegroup(client, **subvolumegroup)
    sv_list = []
    for i in range(1, 3):
        sv_def = {
            "vol_name": default_fs,
            "subvol_name": f"sv_def_{i}",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client, **sv_def)
        sv_list.append(sv_def)
    sv_non_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_non_def_1",
        "group_name": "subvolgroup_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_non_def)
    sv_list.append(sv_non_def)
    setup_params = {
        "fs_name": default_fs,
        "subvolumegroup": subvolumegroup,
        "sv_list": sv_list,
        "nfs_name": nfs_name,
        "nfs_server": nfs_server,
        "fs_vol_created": fs_vol_created,
    }
    return setup_params


def fscrypt_mount(fs_util, fs_name, client, client1, sv_list, nfs_params):
    """
    This method is to run mount on test subvolumes
    """
    try:
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(4))
        )

        mnt_type_list = ["kernel", "fuse", "nfs"]
        mount_details = {}

        for sv in sv_list:
            sv_name = sv["subvol_name"]
            mount_details.update({sv_name: {}})
            cmd = f"ceph fs subvolume getpath {fs_name} {sv_name}"
            if sv.get("group_name"):
                cmd += f" {sv['group_name']}"

            subvol_path, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )

            mnt_path = subvol_path.strip()
            mnt_path = subvol_path
            nfs_export_name = f"/export_{sv_name}_" + "".join(
                random.choice(string.digits) for i in range(3)
            )
            mount_params = {
                "fs_util": fs_util,
                "client": client1,
                "mnt_path": mnt_path,
                "fs_name": fs_name,
                "export_created": 0,
                "nfs_export_name": nfs_export_name,
                "nfs_server": nfs_params["nfs_server"],
                "nfs_name": nfs_params["nfs_name"],
            }

            for mnt_type in mnt_type_list:
                mounting_dir, _ = fs_util.mount_ceph(mnt_type, mount_params)
                mount_details[sv_name].update({mnt_type: {"mountpoint": mounting_dir}})
                if mnt_type == "nfs":
                    mount_details[sv_name][mnt_type].update(
                        {"nfs_export": nfs_export_name}
                    )

        return mount_details
    except Exception as ex:
        log.error(ex)
        return 1


def run(ceph_cluster, **kw):
    """
    FScrypt functional tests - Polarion TC CEPH-83607378
    ------------------------   -------------------------
    Type - Sanity / Lifecycle

    1.Create FS volume and subvolumes across default and non-default groups, 2 in default and 1 in non-default
    2.Mount subvolumes with kernel client
    3.Create 2 test directories in each subvolumes
    4.Perform fscrypt setup on mount point of one subvolume from each group
    5.Perform fscrypt encrypt on 2 test directories of subvolumes where fscrypt setup done
    6.In test directories across all subvolumes, add directories with depth as 10 and breadth as 5
    and 2 files in each dir
    7.Run FIO on each file in continuous mode until execution run time in background
    8.On subvolume mount points where fscrypt is setup, perform below ops,
        a.fscrypt encrypt ( this internally creates protector and policy and attaches to encryption path)
        b.Validate file and directories(store file and directory names before lock) - name and content
        c.fscrypt lock and validate file and dir names are encrypted, file contents are encrypted
        d.fscrypt unlock and validate file and dir names are readable, file contents are readable.
        e.fscrypt purge - To remove encryption on test directories

    Type - Functional

    Clean Up:
        Delete data in test directories
        Unmount subvolumes
        Remove subvolumes and subvolumegroup
        Remove FS volume if created
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsv1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fscrypt_util = FscryptUtils(ceph_cluster)

        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1

        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = config.get("fs_name", "cephfs")
        default_fs = default_fs if not erasure else "cephfs-ec"
        cleanup = config.get("cleanup", 1)
        client = clients[0]
        client1 = clients[1]
        log.info("checking Pre-requisites")
        log.info("Verify Cluster is healthy before test")
        if wait_for_healthy_ceph(client, fs_util, 300) == 0:
            client1.exec_command(
                sudo=True,
                cmd="ceph fs status;ceph status -s;ceph health detail",
            )
            assert False, "Cluster health is not OK even after waiting for 300secs"

        log.info("Setup test configuration")
        setup_params = test_setup(fs_util, ceph_cluster, default_fs, client)
        fs_name = setup_params["fs_name"]
        log.info("Mount subvolumes")
        mount_details = fscrypt_mount(
            fs_util, fs_name, client, client1, setup_params["sv_list"]
        )
        test_case_name = config.get("test_name", "all_tests")
        test_functional = ["fscrypt_lifecycle_test"]

        if test_case_name in test_functional:
            test_list = [test_case_name]
        else:
            test_list = test_functional

        fscrypt_test_params = {
            "ceph_cluster": ceph_cluster,
            "fs_name": fs_name,
            "fs_util": fs_util,
            "fscrypt_util": fscrypt_util,
            "clients": clients,
            "setup_params": setup_params,
            "mount_details": mount_details,
        }
        for test_name in test_list:
            log.info(
                f"\n\n                                   ============ {test_name} ============ \n"
            )

            fscrypt_test_params.update({"test_case": test_name})
            test_status = fscrypt_test_run(fscrypt_test_params)

            if test_status == 1:
                result_str = f"Test {test_name} failed"
                assert False, result_str
            else:
                result_str = f"Test {test_name} passed"
                log.info(result_str)
            time.sleep(30)  # Wait before next test start
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        wait_time_secs = 300
        if wait_for_healthy_ceph(client, fs_util, wait_time_secs) == 0:
            client.exec_command(
                sudo=True,
                cmd="ceph fs status;ceph status -s;ceph health detail",
            )
            assert (
                False
            ), f"Cluster health is not OK even after waiting for {wait_time_secs}secs"

        if cleanup:
            for sv_name in mount_details:
                for mnt_type in mount_details[sv_name]:
                    mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
                    cmd = f"umount -l {mountpoint}"
                    client1.exec_command(
                        sudo=True,
                        cmd=cmd,
                    )
                    if mnt_type == "nfs":
                        nfs_export = mount_details[sv_name][mnt_type]["nfs_export"]
                        cmd = f"ceph nfs export rm {setup_params['nfs_name']} {nfs_export}"
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster delete {setup_params['nfs_name']}",
                check_ec=False,
            )
            sv_list = setup_params["sv_list"]
            for i in range(len(sv_list)):
                subvol_name = sv_list[i]["subvol_name"]
                fs_name = sv_list[i]["vol_name"]
                fs_util.remove_subvolume(
                    client,
                    fs_name,
                    subvol_name,
                    validate=True,
                    group_name=sv_list[i].get("group_name", None),
                )
                if sv_list[i].get("group_name"):
                    group_name = sv_list[i]["group_name"]
            fs_util.remove_subvolumegroup(client, default_fs, group_name, validate=True)


def fscrypt_test_run(fscrypt_test_params):
    if fscrypt_test_params["test_case"] == "fscrypt_lifecycle_test":
        test_status = fscrypt_lifecycle(fscrypt_test_params)
        return test_status


def fscrypt_lifecycle(fscrypt_test_params):
    log.info("FScrypt lifecycle test across Kernel, Ceph-fuse and NFS mountpoints")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]

    clients = fscrypt_test_params["clients"]

    mount_details = fscrypt_test_params["mount_details"]
    test_status = 0

    client1 = clients[1]

    test_status = 0
    mnt_type = "kernel"
    log.info("Create 2 test directories in each subvolume")
    for sv_name in mount_details:
        mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
        for i in range(2):
            cmd = f"mkdir {mountpoint}/fscrypt_testdir_{i}"
            client1.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
    log.info("fscrypt setup on mountpoint of one subvolume from each group")
    sv_from_def_grp = 0
    fscrypt_sv = {}
    for sv_name in mount_details:
        if "sv_def" in sv_name:
            sv_from_def_grp += 1
        mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
        if sv_from_def_grp <= 1 or "sv_non_def" in sv_name:
            test_status = fscrypt_util.setup(client1, mountpoint)
            fscrypt_sv.update({sv_name: {}})
            if test_status == 1:
                log.error(f"FScrypt setup on {mountpoint} failed for {sv_name}")
                return 1

    log.info(
        "fscrypt encrypt on 2 test directories of subvolumes where fscrypt setup done"
    )
    for sv_name in fscrypt_sv:
        mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
        encrypt_path_list = []
        for i in range(2):
            encrypt_path = f"{mountpoint}/fscrypt_testdir_{i}"

            encrypt_params = fscrypt_util.encrypt(client1, encrypt_path, mountpoint)
            encrypt_path_dict = {
                "encrypt_path_info": {
                    "encrypt_params": encrypt_params,
                    "encrypt_path": encrypt_path,
                }
            }
            encrypt_path_list.append(encrypt_path_dict)
            if encrypt_params == 1:
                log.error(f"Encrypt on {mountpoint} failed for {sv_name}")
                return 1
        fscrypt_sv[sv_name].update({"encrypt_path_list": encrypt_path_list})

    log.info(
        "In encrypt path,add directories with depth as 10 and breadth as 5 and 2 files in each dir"
    )
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        encrypt_path_list_new = []
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            file_list = add_dataset(client1, encrypt_path)
            fscrypt_sv[sv_name].update({"file_list": file_list})
            encrypt_dict["encrypt_path_info"].update({"file_list": file_list})
            encrypt_path_list_new.append(encrypt_dict)
        fscrypt_sv[sv_name].update({"encrypt_path_list": encrypt_path_list_new})

    log.info("Validate file and directory names,file contents and ops before encrypt")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            fscrypt_util.validate_fscrypt(client1, "unlock", encrypt_path)

    log.info("fscrypt lock")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            file_list = encrypt_dict["encrypt_path_info"]["file_list"]
            test_status += fscrypt_util.lock(client1, encrypt_path)
            cmd = f"find {encrypt_path}"
            out, _ = client1.exec_command(sudo=True, cmd=cmd)
            log.info(out)
            for file_path in file_list:
                if file_path in out:
                    log.error(
                        f"fscrypt lock has not suceeded for file and dir names:{out}"
                    )
                    test_status += 1

    log.info("Validate file and directory names,file contents and ops in locked state")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            fscrypt_util.validate_fscrypt(client1, "lock", encrypt_path)

    log.info("fscrypt unlock")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            encrypt_params = encrypt_dict["encrypt_path_info"]["encrypt_params"]
            protector_id = encrypt_params["protector_id"]
            mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
            test_status += fscrypt_util.unlock(
                client1, encrypt_path, mnt_pt, protector_id
            )
            cmd = f"find {encrypt_path}"
            out, _ = client1.exec_command(sudo=True, cmd=cmd)
            for file_path in file_list:
                if encrypt_path in file_path:
                    if file_path not in out:
                        log.error(
                            "fscrypt unlock has not suceeded for file and dir names:%s not in %s",
                            file_path,
                            out,
                        )
                        test_status += 1

    # Read of encrypt path after unlock issue : https://issues.redhat.com/browse/RHEL-79046
    """
    log.info("Validate file and directory names,file contents and ops in unlocked state")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]['encrypt_path_list']
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict['encrypt_path_info']['encrypt_path']
            fscrypt_util.validate_fscrypt(client1,'unlock',encrypt_path)
    """

    log.info("fscrypt purge")
    for sv_name in fscrypt_sv:
        mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
        test_status += fscrypt_util.purge(client1, mnt_pt)

    log.info("Validate file and directory names,file contents and ops in locked state")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            fscrypt_util.validate_fscrypt(client1, "lock", encrypt_path)

    log.info("fscrypt unlock after purge")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            encrypt_params = encrypt_dict["encrypt_path_info"]["encrypt_params"]
            protector_id = encrypt_params["protector_id"]
            mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
            test_status += fscrypt_util.unlock(
                client1, encrypt_path, mnt_pt, protector_id
            )
            cmd = f"find {encrypt_path}"
            out, _ = client1.exec_command(sudo=True, cmd=cmd)
            for file_path in file_list:
                if encrypt_path in file_path:
                    if file_path not in out:
                        log.error(
                            "fscrypt unlock has not suceeded for file and dir names:%s not in %s",
                            file_path,
                            out,
                        )
                        test_status += 1

    # Read of encrypt path after unlock issue : https://issues.redhat.com/browse/RHEL-79046
    """
    log.info("Validate file and directory names,file contents and ops in unlocked state")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]['encrypt_path_list']
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict['encrypt_path_info']['encrypt_path']
            fscrypt_util.validate_fscrypt(client1,'unlock',encrypt_path)
    """

    log.info("fscrypt metadata destroy for policy and protector")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_params = encrypt_dict["encrypt_path_info"]["encrypt_params"]
            protector_id = encrypt_params["protector_id"]
            mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
            protector_params = {"id": protector_id}
            protector_id = fscrypt_util.metadata_ops(
                client1, "destroy", "protector", mnt_pt, **protector_params
            )
            policy_id = encrypt_params["policy_id"]
            policy_params = {"id": policy_id}
            policy_id = fscrypt_util.metadata_ops(
                client1, "destroy", "policy", mnt_pt, **policy_params
            )
    if test_status:
        return 1

    return 0


def wait_for_healthy_ceph(client1, fs_util, wait_time_secs):
    # Returns 1 if healthy, 0 if unhealthy
    ceph_healthy = 0
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_time_secs)
    while ceph_healthy == 0 and (datetime.datetime.now() < end_time):
        try:
            fs_util.get_ceph_health_status(client1)
            ceph_healthy = 1
        except Exception as ex:
            log.info(ex)
            log.info(
                f"Wait for sometime to check if Cluster health can be OK, current state : {ex}"
            )
            time.sleep(5)

    if ceph_healthy == 0:
        return 0
    return 1


def add_dataset(client, encrypt_path):
    """
    This method is to add add files using dd to directory path created as - mix(depth-10,breadth-5)
    Also to add some test files used for validation in lock/unlock states
    """
    file_list = []

    log.info("Add directory with multi-level breadth and depth")
    dir_path = f"{encrypt_path}/"

    # multi-depth
    for i in range(1, 11):
        dir_path += f"dir_{i}/"
    cmd = f"mkdir -p {dir_path}"
    client.exec_command(sudo=True, cmd=cmd)
    for i in range(2):
        file_path = f"{dir_path}dd_file_2m_{i}"
        file_list.append(file_path)
    # multi-breadth
    dir_path = f"{encrypt_path}"
    for i in range(2, 6):
        cmd = f"mkdir {dir_path}/dir_{i}/"
        client.exec_command(sudo=True, cmd=cmd)
        for j in range(2):
            file_path = f"{dir_path}/dir_{i}/dd_file_2m_{j}"
            file_list.append(file_path)

    for file_path in file_list:
        client.exec_command(
            sudo=True,
            cmd=f"dd bs=1M count=2 if=/dev/urandom of={file_path}",
        )

    log.info("Add directory with files used for lock/unlock tests")
    linux_files = ["/var/log/messages", "/var/log/cloud-init.log", "/var/log/dnf.log"]
    for i in range(1, 25):
        linux_file = random.choice(linux_files)
        file_path = f"{encrypt_path}/testfile_{i}"
        client.exec_command(
            sudo=True,
            cmd=f"dd bs=1M count=2 if={linux_file} of={file_path}",
        )

    return file_list


def fscrypt_io(client, file_list, run_time):

    def fscrypt_fio(client, file_path):
        client.exec_command(
            sudo=True,
            cmd=f"fio --name={file_path} --ioengine=libaio --size 2M --rw=write --bs=1M --direct=1 "
            f"--numjobs=1 --iodepth=5 --runtime=10",
            timeout=20,
            long_running=True,
        )

    end_time = datetime.datetime.now() + datetime.timedelta(seconds=run_time)
    while datetime.datetime.now() < end_time:
        with parallel() as p:
            for file_path in file_list:
                p.spawn(fscrypt_fio, client, file_path, validate=False)
