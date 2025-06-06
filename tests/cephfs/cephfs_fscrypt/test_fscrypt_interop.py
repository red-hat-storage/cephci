import datetime
import random
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.fscrypt_utils import FscryptUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    FScrypt Interop tests - Polarion TC CEPH-83607408
    ------------------------   -------------------------
    Common setup routine:
    1.Create FS volume and subvolumes across default and non-default groups, 2 in default and 1 in non-default
    2.Mount subvolumes with Fuse client
    3.Create empty directory and enable encryption.

    Workflow1: Snapshot restore of encrypted data to same and non-encrypted subvolume
    1.Add dataset to encrypted directory in subvolume,Create snapshot s1 and modify data and create another snapshot s2
    2.Perform restore from s1 to same subvolume and to other non-encrypted subvolume
    3.Repeat step2 with s2 restore

    Workflow2: MDS failover to standby-replay while IO in progress from encrypted to non-encrypted and vice-versa
    1. Modify MDS config to standby_replay mode
    2. Perform RW ops between encrypted subvolume fuse mountpoint and non-encrypted subvolume mount
    3. Perform MDS failover and wait for Active status.
    4. Verify IO can continue and suceed after MDS failover.

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
        log.info("Setup test configuration")
        setup_params = cephfs_common_utils.test_setup(default_fs, client)
        fs_name = setup_params["fs_name"]
        log.info("Mount subvolumes")
        mount_details = cephfs_common_utils.test_mount([clients[1]], setup_params)
        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        test_case_name = config.get("test_name", "all_tests")
        test_interop = [
            "fscrypt_snap_restore",
            "fscrypt_mds_failover",
        ]

        if test_case_name in test_interop:
            test_list = [test_case_name]
        else:
            test_list = test_interop

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
                assert False, result_str
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
        log.info("Clean Up in progress")
        wait_time_secs = 300
        if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
            log.error("Cluster health is not OK even after waiting for 300secs")
        if cleanup:
            cephfs_common_utils.test_cleanup(client, setup_params, mount_details)
            fs_util.remove_fs(client, default_fs)


def fscrypt_test_run(fscrypt_test_params):
    fscrypt_tests = {
        "fscrypt_snap_restore": fscrypt_snap_restore,
        "fscrypt_mds_failover": fscrypt_mds_failover,
    }
    test_case = fscrypt_test_params["test_case"]
    test_status = fscrypt_tests[test_case](fscrypt_test_params)
    return test_status


def fscrypt_snap_restore(fscrypt_test_params):
    """
    Test workflow: Snapshot restore of encrypted data to same and non-encrypted subvolume
    """
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    cephfs_common_utils = fscrypt_test_params["cephfs_common_utils"]
    sv_list = fscrypt_test_params["setup_params"]["sv_list"]
    sv_name = random.choice(list(mount_details.keys()))
    test_status = 0
    mount_details_1 = {}
    mount_details_1.update({sv_name: mount_details[sv_name]})
    mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
    mnt_pt = mount_details[sv_name]["fuse"]["mountpoint"]
    log.info("Fscrypt encrypt on %s", mnt_pt)
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details_1)
    encrypt_path = encrypt_info[sv_name]["path"]
    encrypt_params = encrypt_info[sv_name]["encrypt_params"]
    mnt_pt_base = get_non_encrypted_sv(sv_name, mount_details)
    fscrypt_util.add_dataset(mnt_client, encrypt_path)
    test_status += fscrypt_util.validate_fscrypt_with_lock_unlock(
        mnt_client, mnt_pt, encrypt_path, encrypt_params
    )
    group_name = None
    for sv in sv_list:
        if sv["subvol_name"] == sv_name and sv.get("group_name"):
            group_name = sv["group_name"]
    snap1 = f"{sv_name}_fscrypt_snap1"
    snapshot = {
        "vol_name": fscrypt_test_params["fs_name"],
        "snap_name": snap1,
        "group_name": group_name,
        "subvol_name": sv_name,
    }
    log.info("Create Snapshot %s on %s", snap1, sv_name)
    cephfs_common_utils.create_snapshot(mnt_client, **snapshot)
    time.sleep(5)
    file_list = fscrypt_util.get_file_list(mnt_client, encrypt_path)
    file_list_1 = [random.choice(file_list) for _ in range(0, 5)]
    fscrypt_io(mnt_client, file_list_1, 10)
    snap2 = f"{sv_name}_fscrypt_snap2"
    snapshot = {
        "vol_name": fscrypt_test_params["fs_name"],
        "snap_name": snap2,
        "group_name": group_name,
        "subvol_name": sv_name,
    }
    log.info("Create Snapshot %s on %s", snap2, sv_name)
    cephfs_common_utils.create_snapshot(mnt_client, **snapshot)
    for snap_name in [snap1, snap2]:
        check_snap_at_client(mnt_client, encrypt_path, 2)
        log.info(
            "Perform Snap Restore from %s to %s and %s", snap_name, mnt_pt, mnt_pt_base
        )
        for restore_dst in [mnt_pt, mnt_pt_base]:
            if snap_restore_op(mnt_client, encrypt_path, restore_dst, snap_name):
                log.error("Snap restore from %s to %s failed", mnt_pt, restore_dst)
                test_status += 1
            if restore_dst == mnt_pt_base:
                file_list = fscrypt_util.get_file_list(mnt_client, restore_dst)
                file_path = random.choice(file_list)
                cmd = f"echo test >> {file_path}"
                try:
                    mnt_client.exec_command(sudo=True, cmd=cmd)
                except CommandFailed as ex:
                    log.error(
                        "%s write error on restore file from encrypted volume - %s",
                        file_path,
                        str(ex),
                    )
                    test_status += 1
    if test_status > 0:
        return 1
    return 0


def fscrypt_mds_failover(fscrypt_test_params):
    """
    Test Workflow : MDS failover to standby-replay while IO in progress from encrypted to non-encrypted and vice-versa
    """
    fscrypt_util = fscrypt_test_params["fscrypt_util"]
    mount_details = fscrypt_test_params["mount_details"]
    cephfs_common_utils = fscrypt_test_params["cephfs_common_utils"]
    client = fscrypt_test_params["clients"][0]
    fs_name = fscrypt_test_params["fs_name"]
    test_status = 0
    log.info("Modify MDS config to standby_replay mode")
    cmd = f"ceph orch apply mds {fs_name} --placement='label:mds';sleep 10;ceph fs set {fs_name} max_mds 2;"
    cmd += f"sleep 5;ceph fs set {fs_name} allow_standby_replay true"
    client.exec_command(
        sudo=True,
        cmd=cmd,
    )
    if not cephfs_common_utils.check_active_mds_count(client, fs_name, 2):
        log.error("Two Active MDS not found")
        return 1
    try:
        cephfs_common_utils.wait_for_standby_replay_mds(client, fs_name)
    except CommandFailed as ex:
        log.error("Standby Replay MDS daemons seems not running:%s", str(ex))
        return 1
    log.info(
        "Perform RW ops between encrypted subvolume fuse mountpoint and non-encrypted subvolume mount"
    )
    sv_name = random.choice(list(mount_details.keys()))
    mount_details_1 = {}
    mount_details_1.update({sv_name: mount_details[sv_name]})
    mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
    mnt_pt = mount_details[sv_name]["fuse"]["mountpoint"]
    log.info("FScrypt encrypt on %s", mnt_pt)
    encrypt_info = fscrypt_util.encrypt_dir_setup(mount_details_1)
    encrypt_path = encrypt_info[sv_name]["path"]
    encrypt_params = encrypt_info[sv_name]["encrypt_params"]
    mnt_pt_base = get_non_encrypted_sv(sv_name, mount_details)
    log.info("Verify IO between encrypted and non-encrypted can suceed")
    if rw_ops_bw_enc_and_non_enc_sv(mnt_client, encrypt_path, mnt_pt_base):
        return 1
    log.info("Perform MDS failover and wait for Active status")
    if cephfs_common_utils.rolling_mds_failover(mnt_client, fs_name):
        return 1
    log.info("Verify lock and unlock on encrypt_path after MDS failover")
    test_status += fscrypt_util.validate_fscrypt_with_lock_unlock(
        mnt_client, mnt_pt, encrypt_path, encrypt_params
    )
    log.info("Verify IO can continue and suceed after unlock")
    test_status += rw_ops_bw_enc_and_non_enc_sv(mnt_client, encrypt_path, mnt_pt_base)
    if test_status > 0:
        return 1
    return 0


# HELPER ROUTINES
def get_non_encrypted_sv(sv_name, mount_details):
    """
    This helper method generates filename with given number of characters
    """
    for sv_name_tmp in mount_details:
        # Kernel supported client rhel10 is not ready with ceph pkgs, add kernel when ready
        mnt_type = random.choice(["fuse", "nfs"])
        if sv_name != sv_name_tmp:
            mnt_pt_base = mount_details[sv_name_tmp][mnt_type]["mountpoint"]
    return mnt_pt_base


def snap_restore_op(client, restore_src, restore_dst, snap1):
    """
    This method is to restore snapshot data from source to dst for given snapshot
    Required params:
    restore_src : Source mountpath
    restore_dst : Dest mountpath
    snap_name: A snapshot directory in .snap dir to restore from
    """
    cmd = f"cp -rf {restore_src}/.snap/_{snap1}*/* {restore_dst}/"
    try:
        client.exec_command(sudo=True, cmd=cmd)
        return 0
    except CommandFailed as ex:
        log.error("Restore op failed:%s", str(ex))
        return 1


def rw_ops_bw_enc_and_non_enc_sv(client, enc_path, non_enc_path):
    """
    This method will generate Read-Write IO Patterns between two given mountpaths
    Required:
    mnt_client - Client object to run IO
    mnt_pt - Encrypted mountpath
    mnt_pt_base - Non-encrypted mountpath
    """
    cmd_list = [
        f"cp -f /var/log/messages {enc_path}/",
        f"cp -f {enc_path}/messages {non_enc_path}/",
        f"cp -rf {non_enc_path}/* {enc_path}/",
        f"ls -l {non_enc_path}/",
        f"ls -l {enc_path}/",
    ]
    try:
        for cmd in cmd_list:
            out, _ = client.exec_command(sudo=True, cmd=cmd)
            log.info(out)
        return 0
    except CommandFailed as ex:
        log.error("RW ops between encrypted and non-encrypted path failed", str(ex))
        return 1
    except BaseException as ex1:
        log.error(ex1)
        return 1


def fscrypt_io(client, file_list, run_time):
    def fscrypt_fio(client, file_path):
        client.exec_command(
            sudo=True,
            cmd=f"fio --name={file_path} --ioengine=libaio --size 2M --rw=write --bs=1M --direct=1 "
            f"--numjobs=1 --iodepth=5 --runtime=10",
            timeout=20,
        )

    end_time = datetime.datetime.now() + datetime.timedelta(seconds=run_time)
    while datetime.datetime.now() < end_time:
        with parallel() as p:
            for file_path in file_list:
                p.spawn(fscrypt_fio, client, file_path)


@retry(CommandFailed, tries=10, delay=5)
def check_snap_at_client(client, encrypt_path, snap_cnt):
    """
    This method waits for snaps to be listed at client mount path
    """
    out, _ = client.exec_command(sudo=True, cmd=f"ls {encrypt_path}/.snap/")
    log.info(out)
    snap_list = out.split()
    if len(snap_list) != snap_cnt:
        raise CommandFailed(
            "Expected snapshots not listed in .snap in %s", encrypt_path
        )
    return 0
