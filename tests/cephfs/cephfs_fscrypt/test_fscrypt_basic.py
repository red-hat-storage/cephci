import datetime
import random
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.fscrypt_utils import FscryptUtils
from utility.log import Log

log = Log(__name__)
global cg_test_io_status
global fscrypt_util


def run(ceph_cluster, **kw):
    """
    FScrypt functional tests - Polarion TC CEPH-83607378
    ------------------------   -------------------------
    Type - Sanity / Lifecycle
    Workflow1:
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

    Workflow2:
    Verify fscrypt encrypt not supported for non-empty directory path

    Workflow3:
    Verify fscrypt encrypt lock does not encrypt metadata of files including, size, timestamp,permissions and
    extended sttributes

    Clean Up:
        Unmount subvolumes
        Remove subvolumes and subvolumegroup
        Remove FS volume if created
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
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
                "This test requires minimum 2 client nodes.This has only %s clients",
                len(clients),
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
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        log.info("Setup test configuration")
        setup_params = cephfs_common_utils.test_setup(default_fs, client)
        fs_name = setup_params["fs_name"]
        log.info("Mount subvolumes")
        mount_details = cephfs_common_utils.test_mount(clients, setup_params)
        clients = [client, client1]
        test_case_name = config.get("test_name", "all_tests")
        test_functional = [
            "fscrypt_lifecycle",
            "fscrypt_non_empty_dir",
            "fscrypt_metadata_not_encrypted",
        ]

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
                "\n\n                                   ============ %s ============ \n",
                test_name,
            )

            fscrypt_test_params.update({"test_case": test_name})
            test_status = fscrypt_test_run(fscrypt_test_params)

            if test_status == 1:
                result_str = f"Test {test_name} failed"
                assert False, result_str
            else:
                result_str = f"Test {test_name} passed"
                log.info(result_str)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        wait_time_secs = 300
        if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
            assert False, "Cluster health is not OK even after waiting for sometime"

        if cleanup:
            cephfs_common_utils.test_cleanup(client, setup_params, mount_details)


def fscrypt_test_run(fscrypt_test_params):
    fscrypt_tests = {
        "fscrypt_lifecycle": fscrypt_lifecycle,
        "fscrypt_non_empty_dir": fscrypt_non_empty_dir,
        "fscrypt_metadata_not_encrypted": fscrypt_metadata_not_encrypted,
    }
    test_case = fscrypt_test_params["test_case"]
    test_status = fscrypt_tests[test_case](fscrypt_test_params)
    return test_status


def fscrypt_lifecycle(fscrypt_test_params):
    """
    This is a testcase module to verify encrypt lifecycle with all command options -
    setup,encrypt,lock,unlock,metadata create|destroy|purge
    Args:
    Required:fscrypt_test_params in below format,
    fscrypt_test_params = {
            "fscrypt_util": fscrypt_util,
            "clients": clients,
            "mount_details": mount_details,
        }
    """
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
            encrypt_args = {
                "protector_source": random.choice(["custom_passphrase", "raw_key"])
            }
            encrypt_params = fscrypt_util.encrypt(
                client1, encrypt_path, mountpoint, **encrypt_args
            )
            encrypt_path_dict = {
                "encrypt_path_info": {
                    "encrypt_params": encrypt_params,
                    "encrypt_path": encrypt_path,
                }
            }
            encrypt_path_list.append(encrypt_path_dict)
            if encrypt_params == 1:
                log.error("Encrypt on %s failed for %s", mountpoint, sv_name)
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
            file_list = fscrypt_util.add_dataset(client1, encrypt_path)
            fscrypt_sv[sv_name].update({"file_list": file_list})
            encrypt_dict["encrypt_path_info"].update({"file_list": file_list})
            encrypt_path_list_new.append(encrypt_dict)
        fscrypt_sv[sv_name].update({"encrypt_path_list": encrypt_path_list_new})

    log.info("Validate file and directory names,file contents and ops before lock")
    if test_validate(client1, "unlock", fscrypt_sv, fscrypt_util):
        test_status += 1

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
                        "fscrypt lock has not suceeded for file and dir names:%s", out
                    )
                    test_status += 1

    log.info("Validate file and directory names,file contents and ops in locked state")
    if test_validate(client1, "lock", fscrypt_sv, fscrypt_util):
        test_status += 1

    log.info("fscrypt unlock")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            encrypt_params = encrypt_dict["encrypt_path_info"]["encrypt_params"]
            protector_id = encrypt_params["protector_id"]
            mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
            unlock_args = {"key": encrypt_params["key"]}
            test_status += fscrypt_util.unlock(
                client1, encrypt_path, mnt_pt, protector_id, **unlock_args
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
    if test_validate(client1,"unlock",fscrypt_sv, fscrypt_util):
        test_status += 1
    """

    log.info("fscrypt purge")
    for sv_name in fscrypt_sv:
        mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
        test_status += fscrypt_util.purge(client1, mnt_pt)

    log.info("Validate file and directory names,file contents and ops in locked state")
    if test_validate(client1, "lock", fscrypt_sv, fscrypt_util):
        test_status += 1

    log.info("fscrypt unlock after purge")
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            encrypt_params = encrypt_dict["encrypt_path_info"]["encrypt_params"]
            protector_id = encrypt_params["protector_id"]
            mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
            unlock_args = {"key": encrypt_params["key"]}
            test_status += fscrypt_util.unlock(
                client1, encrypt_path, mnt_pt, protector_id, **unlock_args
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
    if test_validate(client1,"unlock",fscrypt_sv, fscrypt_util):
        test_status += 1
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


def fscrypt_non_empty_dir(fscrypt_test_params):
    """
    This is a testcase module to verify encrypt not allowed on non-empty directory path
    Args:
    Required:fscrypt_test_params in below format,
    fscrypt_test_params = {
            "fscrypt_util": fscrypt_util,
            "clients": clients,
            "mount_details": mount_details,
        }
    """
    log.info("FScrypt test to verify encrypt not allowed on non-empty directory path")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    clients = fscrypt_test_params["clients"]
    mount_details = fscrypt_test_params["mount_details"]
    client1 = clients[1]
    log.info("Create test directory in one of subvolumes and add some data")
    sv_name = random.choice(list(mount_details.keys()))
    mnt_type = random.choice(list(mount_details[sv_name].keys()))
    mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
    encrypt_path = f"{mountpoint}/fscrypt_non_empty_dir"
    cmd = f"mkdir {encrypt_path}"
    client1.exec_command(
        sudo=True,
        cmd=cmd,
        check_ec=False,
    )
    log.info("Add file and dir to encrypt dir path")
    cmd = f"cd {encrypt_path}/;cp /var/log/messages .;"
    cmd += "mkdir dir1;cp messages dir1/"
    client1.exec_command(
        sudo=True,
        cmd=cmd,
        check_ec=False,
    )
    log.info("fscrypt setup on mountpoint")
    if fscrypt_util.setup(client1, mountpoint):
        log.error(f"FScrypt setup on {mountpoint} failed for {sv_name}")
        return 1

    log.info("fscrypt encrypt on non-empty test directory")
    try:
        fscrypt_util.encrypt(client1, encrypt_path, mountpoint)
        log.error(
            "FAIL:FScrypt suceeded on non_empty dir %s, it's not expected", encrypt_path
        )
        return 1
    except CommandFailed as ex:
        exp_msg = "cannot be encrypted because it is non-empty"
        if exp_msg in str(ex):
            log.info("PASS:FScrypt didn't suceed on non_empty encrypt path")
            return 0
        else:
            log.error(
                "FAIL: Incorrect error message for FScrypt on non_empty encrypt path"
            )
            return 1


def fscrypt_metadata_not_encrypted(fscrypt_test_params):
    """
    This is a testcase module to verify File metadata like size,timestamp,extended attrs are not encrypted
    Args:
    Required:fscrypt_test_params in below format,
    fscrypt_test_params = {
            "fscrypt_util": fscrypt_util,
            "clients": clients,
            "mount_details": mount_details,
        }
    """
    log.info(
        "FScrypt test to verify File metadata like size,timestamp,extended attrs are not encrypted"
    )
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    clients = fscrypt_test_params["clients"]
    mount_details = fscrypt_test_params["mount_details"]
    client1 = clients[1]

    log.info("Create test directory in one of subvolumes and add some data")
    sv_name = random.choice(list(mount_details.keys()))
    mnt_type = random.choice(list(mount_details[sv_name].keys()))
    mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
    encrypt_path = f"{mountpoint}/test_encrypt_metadata"
    cmd = f"mkdir {encrypt_path}"
    client1.exec_command(
        sudo=True,
        cmd=cmd,
        check_ec=False,
    )
    test_status = fscrypt_util.setup(client1, mountpoint)
    if test_status == 1:
        log.error(f"FScrypt setup on {mountpoint} failed for {sv_name}")
        return 1
    log.info("fscrypt encrypt on %s", encrypt_path)
    encrypt_args = {"protector_source": random.choice(["custom_passphrase", "raw_key"])}
    encrypt_params = fscrypt_util.encrypt(
        client1, encrypt_path, mountpoint, **encrypt_args
    )
    encrypt_dict = {
        "encrypt_path_info": {
            "encrypt_params": encrypt_params,
            "encrypt_path": encrypt_path,
        }
    }
    if encrypt_params == 1:
        log.error("Encrypt on %s failed", encrypt_path)
        return 1

    log.info(
        "In encrypt path,add directories with depth as 10 and breadth as 5 and 2 files in each dir"
    )

    fscrypt_util.add_dataset(client1, encrypt_path)
    log.info("Validate file and directory metadata before lock")
    if fscrypt_util.validate_fscrypt_metadata(client1, encrypt_path):
        test_status += 1

    log.info("fscrypt lock")
    test_status += fscrypt_util.lock(client1, encrypt_path)

    log.info("Validate file and directory metadata after lock")
    if fscrypt_util.validate_fscrypt_metadata(client1, encrypt_path):
        test_status += 1

    log.info("fscrypt unlock")

    encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
    encrypt_params = encrypt_dict["encrypt_path_info"]["encrypt_params"]
    protector_id = encrypt_params["protector_id"]
    mnt_pt = mount_details[sv_name][mnt_type]["mountpoint"]
    unlock_args = {"key": encrypt_params["key"]}
    test_status += fscrypt_util.unlock(
        client1, encrypt_path, mnt_pt, protector_id, **unlock_args
    )

    log.info("Validate file and directory metadata after unlock")
    if fscrypt_util.validate_fscrypt_metadata(client1, encrypt_path):
        test_status += 1

    if test_status:
        return 1

    return 0


# HELPER ROUTINES


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


def test_validate(client, mode, fscrypt_sv, fscrypt_util):
    """
    This method will validate data in encrypt path as per given mode - lock/unlock
    """
    for sv_name in fscrypt_sv:
        encrypt_path_list = fscrypt_sv[sv_name]["encrypt_path_list"]
        for encrypt_dict in encrypt_path_list:
            encrypt_path = encrypt_dict["encrypt_path_info"]["encrypt_path"]
            if fscrypt_util.validate_fscrypt(client, mode, encrypt_path):
                return 1
    return 0
