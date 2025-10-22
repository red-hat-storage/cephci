import os
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.fscrypt_utils import FscryptUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    FScrypt Negative tests - Polarion TC CEPH-83607406
    ------------------------   -------------------------
    Common setup routine:
    1.Create FS volume and subvolumes across default and non-default groups, 1 in default and 1 in non-default
      Use enctag field in subvolume create cli for one of the subvolumes
    2.Mount subvolumes with Fuse client
    3.Create empty directory and enable encryption.

    Workflow1: Filename length validation for max and min characters
    1.Create files with name consisting of 1,250-256 chars and validate error message for >255 char length.

    Workflow2: Remove encryption key when in locked and unlocked mode and validate outcome
    1. In unlocked mode, files become locked after key removal
    2. In locked mode, files remain locked with key removal
    3. Validate warning message in both cases.

    Workflow3: Create encryption with key from user1 and try removing key through user2
    1. Add files to encrypt path through user1 or client1
    2. Mount the same subvolume in client2 with user2 and try accessing files created.Verify the need for key.
    3. Unlock files in client2 for same path, validate access
    4. Remove key from client22 and validate warning message
    5. Verify files not accessible from both clients after key removal from client2

    Workflow4: Validate error message when incorrect key is provided for unlock

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
            log.error(
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
        log.info("checking Pre-requisites")
        for client_tmp in clients:
            if cephfs_common_utils.client_mount_cleanup(client_tmp):
                log.error("Client mount cleanup didn't suceed")
                fs_util.reboot_node(client_tmp)
        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        log.info("Setup test configuration")
        setup_params = cephfs_common_utils.test_setup(default_fs, client)
        fs_name = setup_params["fs_name"]
        log.info("Mount subvolumes")
        mount_details = cephfs_common_utils.test_mount(clients, setup_params)
        test_case_name = config.get("test_name", "all_tests")
        test_negative = [
            "fscrypt_filename_length",
            "fscrypt_key_remove",
            "fscrypt_key_remove_diff_clients",
            "fscrypt_key_msg_validation",
        ]

        if test_case_name in test_negative:
            test_list = [test_case_name]
        else:
            test_list = test_negative

        fscrypt_test_params = {
            "ceph_cluster": ceph_cluster,
            "fs_name": fs_name,
            "cephfs_common_utils": cephfs_common_utils,
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
                log.error(result_str)
                return 1
            else:
                result_str = f"Test {test_name} passed"
                log.info(result_str)
            time.sleep(10)  # Wait before next test start
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        wait_time_secs = 300
        if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
            assert (
                False
            ), f"Cluster health is not OK even after waiting for {wait_time_secs}secs"

        if cleanup:
            cephfs_common_utils.test_cleanup(client, setup_params, mount_details)
            fs_util.remove_fs(client, default_fs)


def fscrypt_test_run(fscrypt_test_params):
    fscrypt_tests = {
        "fscrypt_filename_length": fscrypt_filename_length,
        "fscrypt_key_remove": fscrypt_key_remove,
        "fscrypt_key_remove_diff_clients": fscrypt_key_remove_diff_clients,
        "fscrypt_key_msg_validation": fscrypt_key_msg_validation,
    }
    test_case = fscrypt_test_params["test_case"]
    test_status = fscrypt_tests[test_case](fscrypt_test_params)
    return test_status


def fscrypt_filename_length(fscrypt_test_params):
    """
    Test workflow: Encryption validation with Filename length as max and min characters
    """
    log.info("Encryption validation with Filename length as max and min characters")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
    sv_name = random.choice(list(mount_details.keys()))
    mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
    encrypt_path = encrypt_info[sv_name]["path"]
    minchar_file = get_filename(1)
    avgchar_file = get_filename(245)
    maxchar_file = get_filename(255)
    long_file = get_filename(256)
    file_list = {
        "minchar": minchar_file,
        "avgchar": avgchar_file,
        "maxchar": maxchar_file,
        "long": long_file,
    }

    for file_type in file_list:
        cmd = f"echo test > {encrypt_path}/{file_list[file_type]}"
        try:
            mnt_client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        except CommandFailed as ex:
            if "File name too long" in str(ex) and file_type == "long":
                log.info("File create fails for 256 chars as expected")
            else:
                log.error("File create failed, cmd - %s", cmd)
    file_list = fscrypt_util.get_file_list(mnt_client, encrypt_path)
    if fscrypt_util.lock(mnt_client, encrypt_path):
        return 1
    if fscrypt_util.verify_encryption(mnt_client, encrypt_path, file_list):
        return 1
    return 0


def fscrypt_key_remove(fscrypt_test_params):
    """
    Test Workflow : Remove encryption key when in locked and unlocked mode and validate outcome
    """
    log.info(
        "Remove encryption key when in locked and unlocked mode and validate outcome"
    )
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    sv_name = random.choice(list(mount_details.keys()))
    mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
    mnt_pt = mount_details[sv_name]["fuse"]["mountpoint"]

    for encrypt_mode in ["unlock", "lock"]:
        encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
        encrypt_path = encrypt_info[sv_name]["path"]
        fscrypt_util.add_dataset(mnt_client, encrypt_path)
        file_list = fscrypt_util.get_file_list(mnt_client, encrypt_path)
        if encrypt_mode == "lock":
            fscrypt_util.lock(mnt_client, encrypt_path)
        cmd = f"echo y | fscrypt purge {mnt_pt}"
        out, _ = mnt_client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        log.info(out)
        exp_str = "WARNING: Encrypted data on this filesystem will be inaccessible"
        if exp_str not in str(out):
            log.error("Expected warning message during metadata purge is NOT seen")
            return 1
        if fscrypt_util.verify_encryption(mnt_client, encrypt_path, file_list):
            log.error(
                "Files are NOT encrypted, but it was expected after metadata purge"
            )
            return 1

    return 0


def fscrypt_key_remove_diff_clients(fscrypt_test_params):
    """
    Test workflow : Create encryption with key from user1 and try removing key through user2
    """
    log.info("Create encryption with key from user1 and try removing key through user2")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    clients = fscrypt_test_params["clients"]
    cephfs_common_utils = fscrypt_test_params["cephfs_common_utils"]
    mount_details = fscrypt_test_params["mount_details"]
    setup_params = fscrypt_test_params["setup_params"].copy()
    fs_name = fscrypt_test_params["fs_name"]
    client = clients[0]
    sv_name = random.choice(list(mount_details.keys()))
    mnt_client_1 = mount_details[sv_name]["fuse"]["mnt_client"]
    for client in clients:
        if client.node.hostname != mnt_client_1.node.hostname:
            mnt_client_2 = client
            break
    sv_list = setup_params["sv_list"]
    fs_name = setup_params["fs_name"]
    for sv in sv_list:
        if sv_name == sv["subvol_name"]:
            cmd = f"ceph fs subvolume getpath {fs_name} {sv_name}"
            if sv.get("group_name"):
                cmd += f" {sv['group_name']}"
            subvol_path, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
    mnt_path = subvol_path.strip()
    mount_params = {
        "client": mnt_client_2,
        "mnt_path": mnt_path,
        "fs_name": fs_name,
        "export_created": 0,
    }
    log.info("Mount the same subvolume %s in client2", sv_name)
    mnt_pt_2, _ = cephfs_common_utils.mount_ceph("fuse", mount_params)
    if fscrypt_util.setup(mnt_client_2, mnt_pt_2):
        return 1
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
    encrypt_path_1 = encrypt_info[sv_name]["path"]
    encrypt_params = encrypt_info[sv_name]["encrypt_params"]
    protector_id = encrypt_params["protector_id"]
    log.info("Add files to encrypt path through client1")
    fscrypt_util.add_dataset(mnt_client_1, encrypt_path_1)
    _, dir_name = os.path.split(encrypt_path_1)
    encrypt_path_2 = f"{mnt_pt_2}/{dir_name}"
    file_list_1 = fscrypt_util.get_file_list(mnt_client_1, encrypt_path_1)

    log.info("Try accessing files created in client1 through client2")
    if fscrypt_util.verify_encryption(mnt_client_2, encrypt_path_2, file_list_1):
        log.error(
            "Files are NOT encrypted, but it was expected for access through client2"
        )
        return 1
    log.info(
        "Files encrypted by default from Client %s mountpoint %s",
        mnt_client_2.node.hostname,
        mnt_pt_2,
    )
    files_list = fscrypt_util.get_file_list(mnt_client_2, encrypt_path_2)
    file_path = random.choice(files_list)
    cmd = f"echo test >> {file_path}"
    log.info("Verify the need for key for file access in client2")
    try:
        out, _ = mnt_client_2.exec_command(sudo=True, cmd=cmd)
        log.error("File write op should not suceed")
        return 1
    except CommandFailed as ex:
        if "Required key not available" not in str(ex):
            log.error("File write op failed with unexpected error")
            return 1
    key_path = encrypt_params["key"]
    if encrypt_params.get("key"):
        _, key_file = os.path.split(encrypt_params["key"])
        key_path = f"{mnt_pt_2}{key_file}"
    unlock_args = {"key": key_path}
    log.info("Unlock files in client2 for same path")
    fscrypt_util.unlock(
        mnt_client_2, encrypt_path_2, mnt_pt_2, protector_id, **unlock_args
    )
    file_list_2 = fscrypt_util.get_file_list(mnt_client_2, encrypt_path_2)
    if not fscrypt_util.verify_encryption(mnt_client_2, encrypt_path_2, file_list_2):
        log.error("Files are encrypted even after unlock")
        return 1
    log.info("Files are not encrypted after unlock in Client2,Validating access")
    file_path = random.choice(file_list_2)
    cmd = f"echo test >> {file_path}"
    try:
        out, _ = mnt_client_2.exec_command(sudo=True, cmd=cmd)
    except CommandFailed as ex:
        if "Required key not available" not in str(ex):
            log.error("File write op failed, remains encrypted after unlock")
            return 1
        log.error(ex)
        return 1

    log.info("Remove key from client2 and validate warning message")
    cmd = f"echo y | fscrypt purge {mnt_pt_2}"
    out, _ = client.exec_command(
        sudo=True,
        cmd=cmd,
    )
    exp_str = "WARNING: Encrypted data on this filesystem will be inaccessible"
    if exp_str not in str(out):
        log.error("Expected warning message during purge is NOT seen")
        return 1
    if fscrypt_util.verify_encryption(mnt_client_2, encrypt_path_2, file_list_2):
        log.error("Files are not locked in client2 after key removal")
        return 1
    time.sleep(5)
    if not fscrypt_util.verify_encryption(mnt_client_1, encrypt_path_1, file_list_1):
        log.error("Files locked in client1 when key removed from client2")
        return 1
    log.info(
        "Verified that files are locked in client2 after key removal,unlocked in client1"
    )
    log.info("Client2 mountpoint cleanup")
    mnt_client_2.exec_command(sudo=True, cmd=f"umount -l {mnt_pt_2};rm -rf {mnt_pt_2}")
    return 0


def fscrypt_key_msg_validation(fscrypt_test_params):
    """
    Test workflow : Validate error message when incorrect key is provided for unlock
    """
    log.info("Validate error message when incorrect key is provided for unlock")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    sv_name = random.choice(list(mount_details.keys()))
    mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
    mnt_pt = mount_details[sv_name]["fuse"]["mountpoint"]
    log.info("FScrypt encrypt on %s", mnt_pt)
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
    encrypt_path = encrypt_info[sv_name]["path"]
    encrypt_params = encrypt_info[sv_name]["encrypt_params"]
    protector_id = encrypt_params["protector_id"]
    log.info("FScrypt lock on %s", encrypt_path)
    fscrypt_util.lock(mnt_client, encrypt_path)
    wrng_protector_id = "1234"
    log.info("Unlock with incorrect protector_id %s", wrng_protector_id)
    try:
        fscrypt_util.unlock(mnt_client, encrypt_path, mnt_pt, wrng_protector_id)
    except CommandFailed as ex:
        log.info(ex)
        exp_str = f"protector metadata for {wrng_protector_id} not found"
        if exp_str not in str(ex):
            log.error("Expected error message not seen with incorrect protector_id ")
            return 1
    unlock_args = {"key": encrypt_params["key"]}
    log.info("Unlock with correct protector_id %s", protector_id)
    fscrypt_util.unlock(mnt_client, encrypt_path, mnt_pt, protector_id, **unlock_args)
    return 0


# HELPER ROUTINES
def get_filename(char_cnt):
    """
    This helper method generates filename with given number of characters
    """
    rand_char = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(1))
    )
    filename = rand_char * char_cnt
    return filename
