import random
import re
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
    FScrypt Functional tests - Polarion TC CEPH-83619943
    ------------------------   -------------------------
    Workflow1: Enctag Validation
    1.Create FS volume and subvolumes across default and non-default groups, 1 in default and 1 in non-default
      Use enctag field in subvolume create cli for one of the subvolumes
    2.Mount subvolumes with Fuse client
    4.Run CRUD ops on enctag using cli 'fs subvolume enctag...'
    5.Set enctag again on subvolumes.Perform fscrypt setup on mount point of subvolumes
    5.fscrypt encrypt on test directory.Repeat step3
    6.fscrypt lock testdir, Repeat step3

    Workflow2: Data Path testing on FScrypt encrypted directory
    Test writing data with small, half write on previous block and trailing on new block
    Test writing data with huge hole, half write on previous block and trailing on new block
    Test writing data past many holes on offset 0 of block
    Test simple rmw
    Test copy smaller file -> larger file gets new file size
    Test overwrite/cp displays effective_size and not real size
    Test lchown to ensure target is set
    Test 900m hole 100m data write
    Test 200M overwrite of 1G file
    Test truncate down from 1GB
    Test invalidate cache on truncate

    Workflow3: New fscrypt xttrs validation
    Verify new xattrs ceph.fscrypt.file and ceph.fscrypt.encname

    Workflow4: Encryption on diff file types - Sparse files, symlinks,regular data files,image,video
    Add all file stypes as dataset into encrypt directory and validation encryption in locked and unlocked mode

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
                log.error("Client old mountpoints cleanup didn't suceed")
                fs_util.reboot_node(client_tmp)
        log.info("Setup test configuration")
        setup_params = cephfs_common_utils.test_setup(default_fs, client)
        fs_name = setup_params["fs_name"]
        log.info("Mount subvolumes")
        mount_details = cephfs_common_utils.test_mount(
            clients, setup_params, mnt_type_list=["kernel", "fuse"]
        )
        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        test_case_name = config.get("test_name", "all_tests")
        test_functional = [
            "fscrypt_enctag",
            "fscrypt_datapath",
            "fscrypt_xttrs",
            "fscrypt_file_types",
        ]

        if test_case_name in test_functional:
            test_list = [test_case_name]
        else:
            test_list = test_functional

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
            fs_util.remove_fs(client, fs_name)


def fscrypt_test_run(fscrypt_test_params):
    """
    Test runner utility
    Required params:
    testcase name, test params in below format,
    fscrypt_test_params = {
            "ceph_cluster": ceph_cluster,
            "fs_name": fs_name,
            "cephfs_common_utils": cephfs_common_utils,
            "fscrypt_util": fscrypt_util,
            "clients": clients,
            "setup_params": setup_params,
            "mount_details": mount_details,
        }
    """
    fscrypt_tests = {
        "fscrypt_enctag": fscrypt_enctag,
        "fscrypt_datapath": fscrypt_datapath,
        "fscrypt_xttrs": fscrypt_xttrs,
        "fscrypt_file_types": fscrypt_file_types,
    }
    test_case = fscrypt_test_params["test_case"]
    test_status = fscrypt_tests[test_case](fscrypt_test_params)
    return test_status


def fscrypt_enctag(fscrypt_test_params):
    """
    Test workflow - CRUD ops for Enctag
    """
    log.info("CRUD ops for Enctag")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    clients = fscrypt_test_params["clients"]
    cephfs_common_utils = fscrypt_test_params["cephfs_common_utils"]
    mount_details = fscrypt_test_params["mount_details"]
    setup_params = fscrypt_test_params["setup_params"].copy()
    fs_name = fscrypt_test_params["fs_name"]
    client = clients[0]
    log.info("Use enctag field in subvolume create cli")
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(4))
    )
    sv_enc_tag = {
        "vol_name": fs_name,
        "subvol_name": "sv_enc_tag",
        "enctag": f"enc_tag_{rand_str}",
    }
    cephfs_common_utils.create_subvolume(client, **sv_enc_tag)
    sv_non_def_enc_tag = {
        "vol_name": fs_name,
        "subvol_name": "sv_non_def_enc_tag",
        "enctag": f"enc_tag_{rand_str}",
        "group_name": setup_params["subvolumegroup"]["group_name"],
    }
    cephfs_common_utils.create_subvolume(client, **sv_non_def_enc_tag)
    sv_list = [sv_enc_tag, sv_non_def_enc_tag]
    sv_list_tmp = setup_params["sv_list"]
    sv_list_tmp.extend(sv_list)
    fscrypt_test_params["setup_params"].update({"sv_list": sv_list_tmp})
    setup_params["sv_list"] = sv_list
    mnt_details = cephfs_common_utils.test_mount(
        clients, setup_params, mnt_type_list=["kernel", "fuse"]
    )
    mount_details.update(mnt_details)
    log.info("Run CRUD ops on enctag")
    if enc_tag_crud_ops(client, cephfs_common_utils, sv_list):
        return 1
    log.info(
        "Set enctag again on subvolumes.Perform fscrypt setup on mount point of subvolumes"
    )
    enc_tag_crud_ops(client, cephfs_common_utils, sv_list, ["set"])
    for sv in sv_list:
        sv_name = sv["subvol_name"]
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(4))
        )
        mnt_client = mnt_details[sv_name]["fuse"]["mnt_client"]
        mnt_pt = mnt_details[sv_name]["fuse"]["mountpoint"]
        if fscrypt_util.setup(mnt_client, mnt_pt):
            return 1
        encrypt_path = f"{mnt_pt}/testdir_{rand_str}"
        cmd = f"mkdir {encrypt_path}"
        mnt_client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        encrypt_args = {
            "protector_source": random.choice(["custom_passphrase", "raw_key"])
        }
        log.info("fscrypt encrypt on test directory")
        encrypt_params = fscrypt_util.encrypt(
            mnt_client, encrypt_path, mnt_pt, **encrypt_args
        )
        log.info("Run CRUD ops on enctag")
        if enc_tag_crud_ops(client, cephfs_common_utils, [sv]):
            return 1
        log.info("fscrypt lock testdir")
        if fscrypt_util.lock(mnt_client, encrypt_path):
            return 1
        log.info("Run CRUD ops on enctag")
        if enc_tag_crud_ops(client, cephfs_common_utils, [sv]):
            return 1
        protector_id = encrypt_params["protector_id"]
        unlock_args = {"key": encrypt_params["key"]}
        if fscrypt_util.unlock(
            mnt_client, encrypt_path, mnt_pt, protector_id, **unlock_args
        ):
            return 1
    return 0


def fscrypt_datapath(fscrypt_test_params):
    """
    Test workflow - Data Path testing on FScrypt encrypted directory
    """
    log.info("FScrypt data path testing")
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]

    Test1 = "Test writing data with small, half write on previous block and trailing on new block"
    Test2 = "Test writing data with huge hole, half write on previous block and trailing on new block"
    Test3 = "Test writing data past many holes on offset 0 of block"
    Test4 = "Test simple rmw"
    Test5 = "Test copy smaller file -> larger file gets new file size"
    Test6 = "Test overwrite/cp displays effective_size and not real size"
    Test7 = "Test lchown to ensure target is set"
    Test8 = "Test 900m hole 100m data write"
    Test9 = "Test 200M overwrite of 1G file"
    Test10 = "Test truncate down from 1GB"
    Test11 = "Test invalidate cache on truncate"

    def data_path_test1(client, test_path):
        src_file = f"{test_path}/../testfile"
        dst_file = f"{test_path}/dp_test1"
        cmd = f"python -c 'print('s' * 5529)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=3379;"
        cmd += f"ls -l {dst_file}"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"python -c 'print('t' * 4033)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=4127;"
        cmd += f"ls -l {dst_file}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        return 0

    def data_path_test2(client, test_path):
        src_file = f"{test_path}/../testfile"
        dst_file = f"{test_path}/dp_test2"
        cmd = f"python -c 'print('s' * 4096)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=2147477504;"
        cmd += f"ls -l {dst_file}"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"python -c 'print('t' * 8)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=12;"
        cmd += f"ls -l {dst_file}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        return 0

    def data_path_test3(client, test_path):
        src_file = f"{test_path}/../testfile"
        dst_file = f"{test_path}/dp_test3"
        cmd = f"python -c 'print('s' * 3192)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=60653568;"
        cmd += f"ls -l {dst_file}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        return 0

    def data_path_test4(client, test_path):
        src_file = f"{test_path}/../testfile"
        dst_file = f"{test_path}/dp_test4"
        cmd = f"python -c 'print('s' * 32)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=0;"
        cmd += f"ls -l {dst_file}"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"python -c 'print('t' * 8)' > {src_file};"
        cmd += f"ls -l {src_file};"
        cmd += f"dd of={dst_file} bs=1 if={src_file} seek=8;"
        cmd += f"ls -l {dst_file}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        return 0

    def data_path_test5(client, test_path):
        f1 = f"{test_path}/dp_test5_f1"
        f2 = f"{test_path}/dp_test5_f2"
        cmd = f"touch {f1};truncate -s 1048576 {f1};"
        cmd += f"touch {f2};truncate -s 1024 {f2};"
        cmd += f"cp -f {f2} {f1};"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"ls -sh {f1}"
        cmd += "| awk '{ print $1 }'"
        f1_size, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"ls -sh {f2}"
        cmd += "| awk '{ print $1 }'"
        f2_size, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info("f1_size - %s,f2_size - %s", f1_size, f2_size)
        if str(f1_size) != str(f2_size):
            raise Exception("File size not correct after small_file to large_file copy")
        return 0

    def data_path_test6(client, test_path):
        f1 = f"{test_path}/dp_test6_f1"
        cmd = f"touch {f1};truncate -s 68686 {f1};"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"ls -sh {f1}"
        cmd += "| awk '{ print $1 }'"
        f1_size, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(f1_size)
        if "68K" not in str(f1_size):
            raise Exception("File size not correct after truncate")
        return 0

    def data_path_test7(client, test_path):
        f1 = f"{test_path}/dp_test7_symlink"
        cmd = f"cp /var/log/messages {test_path}/../;ln -s {test_path}/../messages {f1};ls -l {f1};"
        cmd += f"chown cephuser:cephuser {f1}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        cmd = f"ls -lh {test_path}/../messages"
        cmd += "| awk '{ print $3 }'"
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ls -l {test_path}/../messages",
            check_ec=False,
        )
        if "cephuser" not in str(out):
            raise Exception("chown on symlink file failed")
        return 0

    def data_path_test8(client, test_path):
        f1 = f"{test_path}/../dp_test8_f1"
        f2 = f"{test_path}/dp_test8_f2"
        client.exec_command(
            sudo=True,
            cmd=f"truncate -s 1g {f1}",
            check_ec=False,
        )
        cmd = f"dd of={f2} bs=1M if={f1} seek=900"
        client.exec_command(
            sudo=True, cmd=cmd, check_ec=False, long_running=True, timeout=600
        )
        return 0

    def data_path_test9(client, test_path):
        f1 = f"{test_path}/dp_test9_f1"
        client.exec_command(
            sudo=True,
            cmd=f"truncate -s 1g {f1}",
            check_ec=False,
        )
        cmd = f"dd if=/dev/urandom of={f1} seek=400 count=200 bs=1M"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        return 0

    def data_path_test10(client, test_path):
        f1 = f"{test_path}/dp_test10_f1"
        cmd = f"truncate -s 1024M {f1};ls -l {f1};truncate -s 900M {f1};ls -l {f1};"
        cmd += f"truncate -s 400M {f1};ls -l {f1};truncate -s 1M {f1};ls -l {f1};"
        client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        cmd = f"ls -sh {f1}"
        cmd += "| awk '{ print $1 }'"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        if "1.0M" not in str(out):
            raise Exception("truncate down to 1M from 1G didn't suceed")
        return 0

    def data_path_test11(client, test_path):
        f1 = f"{test_path}/dp_test11_f1"
        cmd = f"echo ab > {f1};truncate -s 1 {f1};cat {f1}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        log.info(out)
        if str(out) != "a":
            raise Exception("Cache invalidate by truncate didn't suceed")
        return 0

    test_cmds = {
        Test1: data_path_test1,
        Test2: data_path_test2,
        Test3: data_path_test3,
        Test4: data_path_test4,
        Test5: data_path_test5,
        Test6: data_path_test6,
        Test7: data_path_test7,
        Test8: data_path_test8,
        Test9: data_path_test9,
        Test10: data_path_test10,
        Test11: data_path_test11,
    }
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
    test_status = 0
    for test_case in test_cmds:
        sv_name = random.choice(list(mount_details.keys()))
        mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
        encrypt_path = encrypt_info[sv_name]["path"]
        log.info("\n\nStarting Data Path Test - %s\n", test_case)
        try:
            test_cmds[test_case](mnt_client, encrypt_path)
        except CommandFailed as ex:
            log.error("Data path test %s failed with error - %s", test_case, ex)
            test_status = 1
    return test_status


def fscrypt_xttrs(fscrypt_test_params):
    """
    Test workflow - New fscrypt xttrs validation
    Verify new xattrs ceph.fscrypt.file and ceph.fscrypt.encname
    """
    log.info(
        "Verify new xattrs ceph.fscrypt.auth, ceph.fscrypt.file and ceph.fscrypt.encname"
    )
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
    for sv_name in mount_details:
        encrypt_path = encrypt_info[sv_name]["path"]
        mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
        cmd = f"cp /var/log/messages {encrypt_path}/;"
        mnt_client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        file_path = f"{encrypt_path}/messages"
        # Add code when File xttr ceph.fscrypt.encname is available
        patterns = {
            "ceph.fscrypt.file": r"ceph.fscrypt.file=0\S+AAAAAAA=",
            "ceph.fscrypt.auth": r"ceph.fscrypt.auth=\S+",
        }
        for xttr in ["ceph.fscrypt.auth", "ceph.fscrypt.file"]:
            cmd = f"getfattr -n {xttr} {file_path}"
            out, _ = mnt_client.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
            log.info(out)
            out_list = out.split("\n")
            log.info(out_list)
            search_str = [line for line in out_list if xttr in line][0]
            exp_str = patterns[xttr]
            match = re.fullmatch(exp_str, search_str)
            if not match:
                log.error(
                    "FScrypt File xttr %s format is incorrect for file %s",
                    xttr,
                    file_path,
                )
                return 1
            log.info(
                "FScrypt File xttr %s format is correct for file %s", xttr, file_path
            )
    return 0


def fscrypt_file_types(fscrypt_test_params):
    """
    Test workflow - Encryption on diff file types - Sparse files, symlinks,regular data files,image,video
    Add all file types as dataset into encrypt directory and validation encryption in locked and unlocked mode
    """
    log.info(
        "Encryption on diff file types - Sparse files, symlinks,regular data files,image,video"
    )
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details)
    sv_name = random.choice(list(mount_details.keys()))
    mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
    mnt_pt = mount_details[sv_name]["fuse"]["mountpoint"]
    encrypt_params = encrypt_info[sv_name]["encrypt_params"]
    protector_id = encrypt_params["protector_id"]
    encrypt_path = encrypt_info[sv_name]["path"]
    add_file_types(mnt_client, encrypt_path)
    log.info("Validate files in unlocked mode")
    if fscrypt_util.validate_fscrypt(mnt_client, "unlock", encrypt_path):
        return 1
    log.info("Lock the encrypt_path")
    fscrypt_util.lock(mnt_client, encrypt_path)
    log.info("Validate lock status on each file type")
    if fscrypt_util.validate_fscrypt(mnt_client, "lock", encrypt_path):
        return 1
    log.info("Unlock the encrypt_path")
    unlock_args = {"key": encrypt_params["key"]}
    fscrypt_util.unlock(mnt_client, encrypt_path, mnt_pt, protector_id, **unlock_args)
    log.info("Validate files in unlocked mode")
    if fscrypt_util.validate_fscrypt(mnt_client, "unlock", encrypt_path):
        return 1
    return 0


# HELPER ROUTINES
def enc_tag_crud_ops(client, cephfs_common_utils, sv_list, ops=["set", "get", "rm"]):
    """
    This helper routine invokes enc_tag testlib with prerequisites and op_type and returns
    0 - success , 1 - failure
    """
    for sv in sv_list:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(4))
        )
        enc_args = {
            "enc_tag": f"enc_tag_{rand_str}",
            "group_name": sv.get("group_name", None),
            "fs_name": sv["vol_name"],
        }
        for op in ops:
            op_status = cephfs_common_utils.enc_tag(
                client, op, sv["subvol_name"], **enc_args
            )
            return (
                1 if op_status == 1 else log.info("enc_tag %s status:%s", op, op_status)
            )
    return 0


def add_file_types(mnt_client, encrypt_path):
    """
    This helper routine adds different file types to given encrypt path including
    sparse,hard and soft links,image,video and audio files
    """
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(4))
    )
    subdir1 = f"{encrypt_path}/data_dir1_{rand_str}"
    subdir2 = f"{encrypt_path}/data_dir2_{rand_str}"
    subdir3 = f"{encrypt_path}/data_dir3_{rand_str}"
    image_path = (
        "https://github.com/yavuzceliker/sample-images/blob/main/images/image-1.jpg"
    )
    video_path = "http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerJoyrides.mp4"
    audio_path = "https://voiceage.com/wbsamples/in_mono/Chorus.wav"
    cmd = f"mkdir {subdir1};mkdir {subdir2};yum install -y --nogpgcheck wget"
    mnt_client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    sl_cmds = f"ln -s /var/log/messages {encrypt_path}/softlink_messages1;"
    sl_cmds += f"ln -s /var/log/messages {encrypt_path}/softlink_messages2;"
    hl_cmds = f"ln /var/log/messages {encrypt_path}/hardlink_messages1;"
    hl_cmds += f"ln /var/log/messages {encrypt_path}/hardlink_messages2"
    sp_cmds = f"truncate -s 1m {encrypt_path}/sparse_file_1m;truncate -s 10k {encrypt_path}/sparse_file_10k;"
    sp_cmds += f"truncate -s 1g {encrypt_path}/sparse_file_1g;truncate -s 3k {encrypt_path}/sparse_file_3k"

    file_type_cmds = {
        "soft_link": sl_cmds,
        "hard_link": hl_cmds,
        "image": f"cd {subdir1};wget {image_path};cd {subdir2};wget {image_path};cd {subdir3};wget {image_path}",
        "video": f"cd {subdir1};wget {video_path};cd {subdir2};wget {video_path};cd {subdir3};wget {video_path}",
        "audio": f"cd {subdir1};wget {audio_path};cd {subdir2};wget {audio_path};cd {subdir3};wget {audio_path}",
        "sparse": sp_cmds,
    }
    for file_type in file_type_cmds:
        mnt_client.exec_command(
            sudo=True, cmd=f"{file_type_cmds[file_type]}", check_ec=False
        )
    return 0
