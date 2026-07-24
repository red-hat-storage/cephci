import json
import random
import string
import time
import traceback
from threading import Thread

from ceph.parallel import parallel
from tests.cephfs.cephfs_IO_lib import FSIO
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)
global ceph_cluster


def run(ceph_cluster, **kw):
    """
    # ===============================================================================
    Toggle Snapshot Visibility - Polarion TC CEPH-83621388
    ---------------------------------------   -------------------------
    Test1: Verify if cli option is inherited to clone volume.
    Verify cli option on during and after clone creation on both parent and clone volume
    Validate snapshot_visibility on both parent and clone volume.

    Clean Up:
        Unmount subvolumes, remove nfs exports
        Remove subvolumes and subvolumegroup
        Remove NFS server, FS volume if created
    """
    try:
        global fs_system_utils, snap_util, cephfs_common_utils, fs_io, nfs_name, nfs_server, sv_test_params, clients
        global nfs_servers
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
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
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
        test_name = "Snapshot visibility Clone Interop"
        log.info("Starting test - %s", test_name)
        test_status = test_snapshot_visibility_clone()
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


def test_snapshot_visibility_clone():
    """
    Test workflow - CephFS Snapshot Visibility test with Clone
    """
    log.info("TEST : CephFS Snapshot Visibility test with Clone")
    setup_params = sv_test_params["setup_params"]
    mount_details = sv_test_params["mount_details"]
    clients = sv_test_params["clients"]
    client = clients[0]
    test_status = 0
    sv_list = setup_params["sv_list"]
    global io_complete, write_procs
    io_complete = None
    write_procs = start_io(sv_list, mount_details)
    io_complete = 0
    create_snapshots(client, sv_list)
    sv_obj = random.choice(sv_list)
    vol_name = sv_obj["vol_name"]
    sv_name = sv_obj["subvol_name"]
    group_name = sv_obj.get("group_name", None)
    clone_name = f"{sv_obj['subvol_name']}_clone"
    snap_name = snap_util.get_snapshot(clients[0], sv_obj)
    test_args = {
        "sub_name": sv_name,
        "group_name": group_name,
        "vol_name": vol_name,
        "value": "false",
        "mnt_type": random.choice(["fuse", "nfs"]),
        "client_respect_snapshot_visibility": "true",
        "nfs_server": nfs_server,
        "nfs_name": nfs_name,
    }
    snap_util.snapshot_visibility(client, "set", **test_args)
    snap_util.snapshot_visibility_client_mgr(client, "client", "set", **test_args)
    snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **test_args)
    clone_create = {
        "vol_name": vol_name,
        "subvol_name": sv_name,
        "snap_name": snap_name,
        "target_subvol_name": clone_name,
        "group_name": group_name,
        "target_group_name": group_name,
    }
    clone_obj = {
        "vol_name": vol_name,
        "subvol_name": clone_name,
        "group_name": group_name,
    }
    with parallel() as p:
        p.spawn(
            cephfs_common_utils.create_clone, client, **clone_create, validate=False
        )
    sv_list.append(clone_obj)
    setup_params.update({"sv_list": sv_list})
    kwargs = {
        "vol_name": vol_name,
        "sub_name": clone_name,
        "group_name": group_name,
    }
    log.info("Try getting snapshot_visibility for clone while cloning in-progress")
    retry_cnt = 6
    clone_state_out, _ = cephfs_common_utils.get_clone_status(
        client, vol_name, clone_name, group_name=group_name
    )
    clone_state = json.loads(clone_state_out)["status"]["state"]
    log.info("Current Clone status : %s", clone_state)
    actual_snap_visibility = None
    if clone_state == "complete":
        actual_snap_visibility = snap_util.snapshot_visibility(client, "get", **kwargs)
    while "complete" not in clone_state and retry_cnt > 0:
        try:
            clone_state_out, _ = cephfs_common_utils.get_clone_status(
                client, vol_name, clone_name, group_name=group_name
            )
            clone_state = json.loads(clone_state_out)["status"]["state"]
            log.info("Current Clone status : %s", clone_state)
            actual_snap_visibility = snap_util.snapshot_visibility(
                client, "get", **kwargs
            )
            log.info(
                "Default Snapshot Visibility on Clone volume : %s",
                actual_snap_visibility,
            )
        except Exception as ex:
            if "" in str(ex):
                log.info(ex)
        time.sleep(5)
        retry_cnt -= 1

    if actual_snap_visibility != "1":
        str1 = f"Expected : 1,Actual:{actual_snap_visibility}"
        log.error(
            "Snapshot Visibility on clone subvolume is not as Expected.%s",
            str1,
        )
        return 1
    if "complete" not in clone_state:
        log.error(
            "snapshot visibility get should not be allowed when clone in state - %s",
            clone_state,
        )
        return 1

    for i in range(5):
        create_snapshots(client, [clone_obj])
    kwargs.update(
        {
            "value": "false",
            "mnt_type": random.choice(["fuse", "nfs"]),
            "client_respect_snapshot_visibility": "true",
            "nfs_server": nfs_server,
            "nfs_name": nfs_name,
            "retry": True,
        }
    )
    mnt_client = mount_details[sv_obj["subvol_name"]]["nfs"]["mnt_client"]
    snap_util.snapshot_visibility(client, "set", **kwargs)
    snap_util.snapshot_visibility_client_mgr(client, "client", "set", **kwargs)
    snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **kwargs)
    log.info("Validate snapshot visibility on mountpoint")
    if snap_util.validate_snapshot_visibility(client, mnt_client, **kwargs):
        return 1

    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)
    io_complete = 1

    return test_status


# HELPER ROUTINES
def start_io(sv_list, mount_details):
    """
    This method starts IO and returns proc id
    """
    log.info("Run CephFS IO tools - df,smallfile")
    io_args = {"run_time": 1}
    io_tools = ["dd", "smallfile", "file_extract"]
    write_procs = []
    for sv in sv_list:
        if mount_details.get(sv["subvol_name"]):
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
    return write_procs


def create_snapshots(client, sv_list):
    """
    This method creates snapshots on subvolumes
    """
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
