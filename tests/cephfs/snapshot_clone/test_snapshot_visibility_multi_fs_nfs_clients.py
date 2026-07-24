import random
import string
import traceback
from threading import Thread

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_IO_lib import FSIO
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)
global ceph_cluster


def run(ceph_cluster, **kw):
    """
    # ===============================================================================
    Toggle Snapshot Visibility - Polarion TC CEPH - 83621386
    ---------------------------------------   -------------------------
    Test1: Verify CLI option and Client config across multi-FS and multi nfs servers
    Test2: Validate CLI option and client config across multiple clients

    Clean Up:
        Unmount subvolumes, remove nfs exports
        Remove subvolumes and subvolumegroup
        Remove NFS server, FS volume if created
    """
    try:
        global fs_system_utils, snap_util, cephfs_common_utils, fs_io, nfs_name, nfs_server, sv_test_params, clients
        global nfs_servers, fs_util
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
        test_info = {
            "test_snapshot_visibility_multi_client": test_snapshot_visibility_multi_client,
            "test_snapshot_visibility_multi_fs": test_snapshot_visibility_multi_fs,
            "test_snapshot_visibility_multi_nfs": test_snapshot_visibility_multi_nfs,
        }
        for test_name in test_info:
            log.info("Starting test %s", test_name)
            test_status = test_info[test_name]()
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
            cephfs_common_utils.test_cleanup(client, setup_params2, mount_details2)
            fs_util.remove_fs(client, default_fs)
            fs_util.remove_fs(client, "cephfs1")


def test_snapshot_visibility_multi_client():
    """
    Test workflow - CephFS Snapshot Visibility test with multi clients
    """
    log.info("TEST : CephFS Snapshot Visibility test with multi clients")
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
    for sv_obj in sv_list:
        mnt_client = mount_details[sv_obj["subvol_name"]]["nfs"]["mnt_client"]
        mnt_client1 = mount_details[sv_obj["subvol_name"]]["fuse"]["mnt_client"]
        if mnt_client.node.hostname == mnt_client1.node.hostname:
            next
        for mnt_tmp in [mnt_client, mnt_client1]:
            if validate_snapshot_visibility_wrapper(
                client, "false", random.choice(["true", "false"]), mnt_tmp, sv_obj
            ):
                return 1
        break
    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)
    io_complete = 1

    return test_status


def test_snapshot_visibility_multi_fs():
    """
    Test workflow - CephFS Snapshot Visibility test with multi FS
    """
    log.info("TEST : CephFS Snapshot Visibility test with multi FS")
    setup_params = sv_test_params["setup_params"]
    mount_details = sv_test_params["mount_details"]
    clients = sv_test_params["clients"]
    client = clients[0]
    test_status = 0
    sv_list = setup_params["sv_list"]
    global io_complete, write_procs
    io_complete = None
    # Create 1 more FS, create subvolume and mount on nfs
    global setup_params1, mount_details1
    setup_params1, mount_details1 = setup_test_obj("cephfs1", "cephfs")
    sv_list1 = setup_params1["sv_list"]
    # Validate snapshot visibility on one subvolume from each FS
    write_procs = start_io(sv_list, mount_details)
    write_procs1 = start_io(sv_list1, mount_details1)
    io_complete = 0
    create_snapshots(client, sv_list)
    create_snapshots(client, sv_list1)
    sv_obj = random.choice(sv_list)
    sv_obj1 = random.choice(sv_list1)
    mnt_client = mount_details[sv_obj["subvol_name"]]["nfs"]["mnt_client"]
    mnt_client1 = mount_details1[sv_obj1["subvol_name"]]["nfs"]["mnt_client"]
    for mnt_tmp, sv_tmp in zip([mnt_client, mnt_client1], [sv_obj, sv_obj1]):
        if validate_snapshot_visibility_wrapper(
            client, "false", random.choice(["true", "false"]), mnt_tmp, sv_tmp
        ):
            return 1
    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)
    for p in write_procs1:
        fs_system_utils.wait_for_proc(p, 300)
    io_complete = 1
    return test_status


def test_snapshot_visibility_multi_nfs():
    """
    Test workflow - CephFS Snapshot Visibility test with multi nfs servers
    """
    log.info("TEST : CephFS Snapshot Visibility test with multi NFS")
    setup_params = sv_test_params["setup_params"]
    mount_details = sv_test_params["mount_details"]
    clients = sv_test_params["clients"]
    client = clients[0]
    test_status = 0
    sv_list = setup_params["sv_list"]
    global io_complete, write_procs
    io_complete = None
    # Create 1 more NFS server, nfs mount existing subvolume with new nfs server
    kwargs = {
        "setup_params": setup_params,
        "nfs_servers": nfs_servers,
        "nfs_server": nfs_server,
    }
    global setup_params2, mount_details2
    setup_params2, mount_details2 = setup_test_obj("cephfs-nfs1", "nfs", **kwargs)

    # Validate snapshot visibility on one subvolume from each FS
    write_procs = start_io(sv_list, mount_details)
    write_procs1 = start_io(sv_list, mount_details2)
    io_complete = 0
    create_snapshots(client, sv_list)
    for sv_obj in sv_list:
        mnt_client1 = None
        mnt_client = mount_details[sv_obj["subvol_name"]]["nfs"]["mnt_client"]
        if mount_details2.get(sv_obj["subvol_name"]):
            mnt_client1 = mount_details2[sv_obj["subvol_name"]]["nfs"]["mnt_client"]
            for mnt_tmp in [mnt_client, mnt_client1]:
                if validate_snapshot_visibility_wrapper(
                    client, "false", random.choice(["true", "false"]), mnt_tmp, sv_obj
                ):
                    return 1
            break
    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)
    for p in write_procs1:
        fs_system_utils.wait_for_proc(p, 300)
    io_complete = 1
    return test_status


# HELPER ROUTINES


def setup_test_obj(obj_name, obj_type, **kwargs):
    """
    This method creates FS/NFS server, subvolume, and does nfs mount
    """
    if obj_type == "cephfs":
        setup_params1 = cephfs_common_utils.test_setup(
            obj_name, clients[0], nfs_name=nfs_name
        )
        mount_details1 = cephfs_common_utils.test_mount(clients, setup_params1)
        return setup_params1, mount_details1
    elif obj_type == "nfs":
        nfs_servers = kwargs["nfs_servers"]
        for nfs_server_tmp in nfs_servers:
            log.info(nfs_server_tmp.node.hostname)
            log.info(nfs_server)
            if nfs_server_tmp.node.hostname != kwargs["nfs_server"]:
                nfs_server1 = nfs_server_tmp.node.hostname
                out, _ = clients[0].exec_command(
                    sudo=True, cmd="ceph nfs cluster ls;ceph orch ls"
                )
                log.info(out)
                fs_util.create_nfs(
                    clients[0],
                    nfs_cluster_name=obj_name,
                    nfs_server_name=nfs_server1,
                )
                if wait_for_process(
                    client=clients[0], process_name=obj_name, ispresent=True
                ):
                    log.info("ceph nfs cluster created successfully")
                else:
                    raise CommandFailed("Failed to create nfs cluster")
                sv_obj = random.choice(kwargs["setup_params"]["sv_list"])
                subvol_path = cephfs_common_utils.subvolume_get_path(
                    clients[0],
                    sv_obj["vol_name"],
                    subvolume_name=sv_obj["subvol_name"],
                    subvolume_group=sv_obj.get("group_name", None),
                )
                nfs_export_name = f"/export_{sv_obj['subvol_name']}_" + "".join(
                    random.choice(string.digits) for i in range(3)
                )
                mount_params = {
                    "client": clients[1],
                    "mnt_path": subvol_path,
                    "fs_name": sv_obj["vol_name"],
                    "export_created": 0,
                    "nfs_export_name": nfs_export_name,
                    "nfs_server": nfs_server1,
                    "nfs_name": obj_name,
                }
                mounting_dir, _ = cephfs_common_utils.mount_ceph("nfs", mount_params)
                mount_details2 = {}
                mount_details2.update({sv_obj["subvol_name"]: {}})
                mount_details2[sv_obj["subvol_name"]].update(
                    {
                        "nfs": {
                            "mountpoint": mounting_dir,
                            "mnt_client": clients[1],
                            "nfs_export": nfs_export_name,
                        }
                    }
                )
                setup_params2 = {
                    "sv_list": [],
                    "nfs_name": obj_name,
                    "fs_name": sv_obj["vol_name"],
                }
                return setup_params2, mount_details2
        return None, None


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


def validate_snapshot_visibility_wrapper(
    client, subvol_sv_val, client_mgr_val, mnt_client, sv
):
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
        "client_id": mnt_client.node.hostname,
    }
    snap_util.snapshot_visibility(client, "set", **test_args)
    snap_util.snapshot_visibility_client_mgr(client, "client", "set", **test_args)
    snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **test_args)
    if snap_util.validate_snapshot_visibility(client, mnt_client, **test_args):
        return 1
    return 0
