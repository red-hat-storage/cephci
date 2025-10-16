import json
import random
import string
import time
import traceback
from threading import Thread

from tests.cephfs.cephfs_IO_lib import FSIO
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # ===============================================================================
    Toggle Snapshot Visibility - Polarion TC CEPH - 83621385
    ---------------------------------------   -------------------------
    1.Create subvolume across default and non-default group.
    2.Verify default values for subvolume snapshot_visibility, ceph config mgr and client
    client_respect_subvolume_snapshot_visibility
    3.Modify each and validate corresponding behavior for Snapshot visibility in fuse and nfs and
      Snapshot ops is allowed or not. ops - snapshot create/ls,clone,snap-schedule
    Clean Up:
        Unmount subvolumes
        Remove snap-schedules,subvolumes and subvolumegroup
        Remove FS volume if created
    """
    try:
        global fs_system_utils, snap_util, cephfs_common_utils, fs_io, nfs_name, nfs_server, sv_test_params, clients

        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        fs_io = FSIO(ceph_cluster)
        erasure = (
            FsUtilsv1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )

        setup_params = None
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
        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        nfs_name = "cephfs-nfs"
        setup_params = cephfs_common_utils.test_setup(
            default_fs, client, nfs_name=nfs_name
        )
        mount_details = cephfs_common_utils.test_mount(clients, setup_params)
        nfs_server = setup_params["nfs_server"]
        snap_util.allow_minutely_schedule(client, allow=True)
        snap_util.enable_snap_schedule(client)
        # sv - snapshot visibility
        sv_test_params = {
            "clients": clients,
            "setup_params": setup_params,
            "mount_details": mount_details,
        }
        test_name = "test_snapshot_visibility_basic"
        test_status = snapshot_visibility_test_run()
        if test_status == 1:
            result_str = f"Test {test_name} failed"
            log.error(result_str)
            return 1
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
            log.error(
                "Cluster health is not OK even after waiting for %s secs",
                wait_time_secs,
            )
        if io_complete != 1:
            for p in write_procs:
                fs_system_utils.wait_for_proc(p, 300)

        log.info("Reset snapshot_visibility to Default setting")
        kwargs = {"client_respect_snapshot_visibility": "false"}
        snap_util.snapshot_visibility_client_mgr(client, "client", "set", **kwargs)
        snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **kwargs)
        if cleanup:
            cephfs_common_utils.test_cleanup(client, setup_params, mount_details)
            fs_util.remove_fs(client, default_fs)
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster delete {nfs_name}",
                check_ec=False,
            )


def snapshot_visibility_test_run():
    """
    Test workflow - CephFS Snapshot Visibility Basic test
    """
    log.info("CephFS Snapshot Visibility Basic test")
    setup_params = sv_test_params["setup_params"]
    mount_details = sv_test_params["mount_details"]
    clients = sv_test_params["clients"]
    client = clients[0]
    test_status = 0
    sv_list = setup_params["sv_list"]
    fs_name = setup_params["fs_name"]
    sv_list = setup_params["sv_list"]
    global io_complete, write_procs
    io_complete = None
    log.info(
        "Verify default values for subvolume snapshot_visibility, ceph config mgr and client"
    )
    exp_snap_visibility = "1"
    exp_snap_visibility_client = "false"
    exp_snap_visibility_mgr = "false"

    for sv in sv_list:
        kwargs = {
            "vol_name": fs_name,
            "sub_name": sv["subvol_name"],
            "group_name": sv.get("group_name", None),
        }
        actual_snap_visibility = snap_util.snapshot_visibility(client, "get", **kwargs)
        if actual_snap_visibility != exp_snap_visibility:
            log.error(
                "Default value for subvolume snapshot visibility is not as Expected"
            )
            log.info(
                "Actual: %s, Expected: %s", actual_snap_visibility, exp_snap_visibility
            )
            test_status += 1
    actual_snap_visibility_client = snap_util.snapshot_visibility_client_mgr(
        client, "client", "get", **kwargs
    )
    actual_snap_visibility_mgr = snap_util.snapshot_visibility_client_mgr(
        client, "mgr", "get", **kwargs
    )
    if actual_snap_visibility_client != exp_snap_visibility_client:
        log.error(
            "Default value for client config client_respect_subvolume_snapshot_visibility not as Expected"
        )
        log.info(
            "Actual: %s, Expected: %s",
            actual_snap_visibility_client,
            exp_snap_visibility_client,
        )
        test_status += 1
    if actual_snap_visibility_mgr != exp_snap_visibility_mgr:
        log.error(
            "Default value for mgr config client_respect_subvolume_snapshot_visibility is not as Expected"
        )
        log.info(
            "Actual: %s, Expected: %s",
            actual_snap_visibility_mgr,
            exp_snap_visibility_mgr,
        )
        test_status += 1
    if test_status > 0:
        return 1

    log.info("Run CephFS IO tools - df,smallfile")
    io_args = {"run_time": 1}
    io_tools = ["dd", "smallfile", "file_extract"]
    write_procs = []
    for sv in sv_list:
        mnt_client = mount_details[sv["subvol_name"]]["nfs"]["mnt_client"]
        mnt_pt = mount_details[sv["subvol_name"]]["nfs"]["mountpoint"]
        p = Thread(
            target=fs_io.run_ios_V1,
            args=(mnt_client, mnt_pt, io_tools),
            kwargs=io_args,
        )
        p.start()
        if "file_extract" in io_tools:
            io_tools.remove("file_extract")
        write_procs.append(p)
    io_complete = 0

    rand_str = "".join(
        [random.choice(string.ascii_lowercase + string.digits) for _ in range(3)]
    )
    log.info("Create Snapshots as pre-requisites for next step")
    for sv in sv_list:
        snap_name = f"{sv['subvol_name']}_snap_{rand_str}"
        snap_clone_args = {
            "vol_name": sv["vol_name"],
            "subvol_name": sv["subvol_name"],
            "snap_name": snap_name,
            "group_name": sv.get("group_name", None),
        }
        cephfs_common_utils.create_snapshot(client, **snap_clone_args, validate=False)
    log.info(
        "Modify each and validate corresponding behavior for Snapshot visibility and Ops Allowability"
    )
    str_tmp = "client_respect_subvolume_snapshot_visibility"
    log.info(
        "Config to be tested : subvolume snapshot_visibility,mgr and client ceph config for %s",
        str_tmp,
    )
    log.info(
        "SubTest1-snapshot_visibility:true,ceph client config %s:false, ceph mgr config %s:false",
        str_tmp,
        str_tmp,
    )
    sv_obj = random.choice(sv_list)
    if validate_snapshot_visibility_wrapper(client, "true", "false", sv_obj):
        return 1
    log.info(
        "SubTest2-snapshot_visibility:true,ceph client config %s:true, ceph mgr config %s:true",
        str_tmp,
        str_tmp,
    )
    sv_obj = random.choice(sv_list)
    if validate_snapshot_visibility_wrapper(client, "true", "true", sv_obj):
        return 1
    log.info(
        "SubTest3-snapshot_visibility:false,ceph client config %s:true, ceph mgr config %s:true",
        str_tmp,
        str_tmp,
    )
    sv_obj = random.choice(sv_list)
    if validate_snapshot_visibility_wrapper(client, "false", "true", sv_obj):
        return 1
    log.info(
        "SubTest4-snapshot_visibility:false,ceph client config %s:false, ceph mgr config %s:false",
        str_tmp,
        str_tmp,
    )
    sv_obj = random.choice(sv_list)
    if validate_snapshot_visibility_wrapper(client, "false", "false", sv_obj):
        return 1

    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)
    io_complete = 1

    return test_status


# HELPER ROUTINES


def validate_snapshot_visibility_wrapper(client, subvol_sv_val, client_mgr_val, sv):
    """
    This method invokes testlib snap_util.validate_snapshot_visibility with required params and does
    validations for visibility and ops - snapshot,clone. Also validates snap-schedule ops for given snapshot_visibility
    config
    Required params:
    client: for ceph cmds
    subvol_sv_val : subvolume snapshot_visibility value
    client_mgr_val : ceph config client/mgr client_respect_subvolume_snapshot_visibility value
    sv_list : list of subvolume objects
    Return: 0 - Success, 1 - failure
    """
    mnt_client = random.choice(clients)
    sv_name = sv["subvol_name"]
    test_args = {
        "sub_name": sv_name,
        "group_name": sv.get("group_name", None),
        "vol_name": sv["vol_name"],
        "value": subvol_sv_val,
        "mnt_type": random.choice(["fuse"]),
        "client_respect_snapshot_visibility": client_mgr_val,
        "nfs_server": nfs_server,
        "nfs_name": nfs_name,
    }
    snap_util.snapshot_visibility(client, "set", **test_args)
    snap_util.snapshot_visibility_client_mgr(client, "client", "set", **test_args)
    snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **test_args)
    if snap_util.validate_snapshot_visibility(client, mnt_client, **test_args):
        return 1
    log.info("Snap-Schedule test validation")
    snap_ops = {
        "true": {"true": "1", "false": "1"},  # allowed
        "false": {"true": "1", "false": "1"},
    }
    expected_snap_ops = snap_ops[subvol_sv_val][client_mgr_val]
    snap_params = {
        "client": client,
        "path": "/",
        "sched": "1m",
        "subvol_name": sv_name,
        "group_name": sv.get("group_name", None),
        "fs_name": sv["vol_name"],
    }

    actual_snap_ops = "0"
    snap_util.create_snap_schedule(snap_params)
    cmd = f"ceph fs snap-schedule status {snap_params['path']}  --fs {sv['vol_name']} --subvol {sv_name} --f json"
    if sv.get("group_name"):
        cmd += f" --group {sv['group_name']}"
    time.sleep(120)  # Wait for minutely snapshots to be created
    out, rc = client.exec_command(sudo=True, cmd=cmd)
    sched_status = json.loads(out)
    for sched_item in sched_status:
        if sched_item["schedule"] == "1m" and sched_item["created_count"] > 0:
            actual_snap_ops = "1"

    snap_util.remove_snap_schedule(
        client,
        "/",
        fs_name=snap_params["fs_name"],
        subvol_name=snap_params["subvol_name"],
        group_name=snap_params["group_name"],
    )
    if expected_snap_ops != actual_snap_ops:
        str_tmp = f"Ops Allowed=1,Ops Not allowed=0,Expected:{expected_snap_ops},Actual:{actual_snap_ops},\n"
        log.error(
            "Snapshot Schedule test for given Snapshot_visibility config failed - %s",
            str_tmp,
        )
        return 1
    return 0
