import datetime
import json
import random
import re
import secrets
import string
import time
import traceback
from multiprocessing import Process, Value
from threading import Thread

from pip._internal.exceptions import CommandError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.snapshot_clone.cephfs_cg_io import CG_snap_IO
from tests.cephfs.snapshot_clone.cg_snap_utils import CG_Snap_Utils
from utility.log import Log

log = Log(__name__)
global cg_test_io_status


def run(ceph_cluster, **kw):
    """
    Workflows Covered :

    Type - Functional

    Workflow1 - Verify QS snap create with using quiesce,snapshot create, quiesce release subcommands and,
    with and without –await option.Run test for subvolumes across same and different groups.
    Steps:
    1. Create 2 subvolumes in non-default group, and 2 subvolumes in default group
    2. Create QS sets with mix of default and non-default subvolumegroups
       - (sv1_def,sv1_sv2_def), (sv1_def,sv1_non_def),(sv1_non_def,sv2_non_def)
    3. Run next steps sequentially on each setRun QS IO validation tool on
    4. On each set, serially, Run 'ceph fs subvolume quiesce --quiesce-set-id ... <members> ....--await' cmd
    5. Wait for status as quiesced, perform snapshot creation on all members.
    6. Run 'ceph fs subvolume quiesce --release' to release quiesce set.
    7. Repeat without --await option, verify cmd response has status as quiescing, wait for quiesced status,
      run step 5 and 6.

    Workflow2 - Verify quiesce release with if-version, repeat with exclude and include prior to release.
    Steps :
    1.Run QS IO Validation tool.
    2.Run subvolume quiesce, note quiesce set id and version. Perform quiesce release with noted version value
    in -if-version field.
    3.Run subvolume quiesce, note quiesce set id and version, perform quiesce-set changes include, exclude,
    note version updates for each change. Verify quiesce release with if-version, verify for each versions.

    Workflow3 - Verify CG quiesce on pre-provisioned quiesce set i.e., quiesce set having subvolumes already
    part of other quiesce set
    Steps:
    1. Run QS IO validation tool on selected quiesce set.
    2. Run Quiesce on subset of subvolumes in quiesce set with timeout and expiration set to 300secs
    3. Run quiesce on original quiesce set with all subvolumes including ones referred in step2.
    4. Create snapshots and perform quiesce release.
    5. Release the quiesce on set created in step2.

    Workflow4 - Verify snapshot restore of QS snap on subvolume can succeed.
    Steps:
    1. Run QS IO validation tool on selected quiesce set
    2. Perform quiesce, create snapshot snap1
    3. Release quiesce. Verify restore of snapshot snap1 contents to new dir

    Workflow5 - Validate quiesce release response when quiesce-timeout and quiesce-expire time is reached.
    Steps:
    1. Run QS IO validation tool on selected quiesce set
    2. Perform quiesce with shorter timeout as 5secs and normal expiration value,
    3. Induce delay of 5secs for quiesce by appending string "?delay_ms=5000" to each subvolume in quiesce cmd.
    4. Validate quiesce cmd response includes string ETIMEDOUT.
    5. Perform quiesce on same set again with normal timeout but shorter expiration value say 5secs.
    6. Create snapshot,wait for 6secs.
    3. Perform quiesce release when set has expired and validate release cmd response to include EPERM error.

    Workflow6 - Perform all state transitions and validate response
    Steps:
    Run QS IO validation tool on selected quiesce set
    1.State - Quiescing : Create QS without --await, when in quiescing performing below
      Include : Add new subvolumes, verify quiescing continues on new subvolumes
      Exclude : Exclude subvolume in quiescing state, verify status of QS .
    2. State - Quiesced : Create QS with quiesce command, when in quiesced state, perform below,
        Include : Add new subvolumes, verify quiescing continues on new subvolumes
        Exclude : Exclude subvolume in quiesed state, verify status of QS, attempt a release.
    3. State - Releasing : Create QS with quiesce command without --await, perform below,
        quiesce-expire : Verify releasing state goes to expired, when quiesce-expire is exhausted.
        Exclude : Exclude subvolume, verify status of QS.

    # Interop tests
    Workflow1 - Verify MDS failover during Quiescing, Quiesced and Releasing states
    Steps:
    Run QS IO validation tool on selected quiesce set
    1.In State - Quiescing, Perform MDS failover. Verify quiesce lifecycle can suceed.
    2.In State - Quiesced, Perform MDS failover. Verify quiesce lifecycle can suceed.
    3.In State - Releasing, Perform MDS failover. Verify quiesce lifecycle can suceed.

    Workflow 2 - Verify quiesce ops suceed when IOs are in parallel through Fuse,NFS and Kernel mountpoints
    Steps:
    Run IO on selected quiesce set with subvolumes mounted on one of Fuse,NFS and Kernel mountpoints
    Verify quiesce lifecycle and other quiesce commands - include,exclude,cancel,query and reset

    # Negative tests
    Negative Workflow 1 - Verify parallel quiesce calls to same set
    Steps:
    Run QS IO validation tool on selected quiesce set
    1.Run multiple parallel quiesce calls to same set
    2.Create snapshots when quiesced, wait for sometime and release quiesce
    Clean Up:

    """
    try:
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsv1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsv1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        cg_snap_util = CG_Snap_Utils(ceph_cluster)
        cg_snap_io = CG_snap_IO(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        qs_clients = [clients[0], clients[1]]
        mgr_node = ceph_cluster.get_ceph_objects("mgr")[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util_v1.prepare_clients(qs_clients, build)
        default_fs = config.get("fs_name", "cephfs")
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        qs_cnt_def = random.randrange(5, 11)
        qs_cnt = config.get("qs_cnt", qs_cnt_def)
        nfs_exists = 0
        fs_util_v1.auth_list(qs_clients)

        client1 = qs_clients[0]
        log.info("checking Pre-requisites")

        log.info("Setup Ephemeral Random pinning")
        cmd = "ceph config set mds mds_export_ephemeral_random true;"
        cmd += "ceph config set mds mds_export_ephemeral_random_max 0.75"
        client1.exec_command(sudo=True, cmd=cmd)

        log.info("Get default fragmentation size on cluster")
        cmd = "ceph config get mds mds_bal_split_size"
        out, rc = client1.exec_command(sudo=True, cmd=cmd)
        default_frag_size = out.strip()
        log.info(f"Default FS fragmentation size:{default_frag_size}")
        log.info("Change fragmentation size for FS to 100")
        cmd = "ceph config set mds mds_bal_split_size 100"
        out, rc = client1.exec_command(sudo=True, cmd=cmd)
        log.info("Verify fragmentation size on cluster")
        cmd = "ceph config get mds mds_bal_split_size"
        out, rc = client1.exec_command(sudo=True, cmd=cmd)
        new_frag_size = out.strip()
        log.info(f"FS fragmentation size:{new_frag_size}")

        fs_details = fs_util_v1.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util_v1.create_fs(client1, default_fs)

        out, rc = client1.exec_command(
            sudo=True, cmd="ceph config set mds debug_mds_quiesce 20"
        )

        test_case_name = config.get("test_name", "all_tests")
        test_functional = [
            "cg_snap_func_workflow_1",
            "cg_snap_func_workflow_2",
            "cg_snap_func_workflow_3",
            "cg_snap_func_workflow_4",
            "cg_snap_func_workflow_5",
            "cg_snap_func_workflow_6",
            "cg_snap_interop_workflow_1",
            "cg_snap_interop_workflow_2",
            "cg_snap_neg_workflow_1",
        ]

        if test_case_name in test_functional:
            test_list = [test_case_name]
        else:
            test_list = test_functional

        # Setup
        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": "subvolgroup_cg",
        }
        fs_util_v1.create_subvolumegroup(client1, **subvolumegroup)
        qs_cnt += 1
        sv_def_list = []
        sv_non_def_list = []
        for i in range(1, qs_cnt):
            sv_name = f"sv_def_{i}"
            sv_def_list.append(sv_name)
            subvolume = {
                "vol_name": default_fs,
                "subvol_name": sv_name,
                "size": "6442450944",
            }
            fs_util_v1.create_subvolume(client1, **subvolume)
        for i in range(1, qs_cnt):
            sv_name = f"sv_non_def_{i}"
            subvolume = {
                "vol_name": default_fs,
                "subvol_name": sv_name,
                "group_name": "subvolgroup_cg",
                "size": "6442450944",
            }
            fs_util_v1.create_subvolume(client1, **subvolume)
            sv_name = f"subvolgroup_cg/sv_non_def_{i}"
            sv_non_def_list.append(sv_name)

        sv_mixed_list = []
        qs_cnt -= 1
        for i in range(0, qs_cnt):
            sv_name1 = random.choice(sv_def_list)
            sv_name2 = random.choice(sv_non_def_list)
            if (sv_name2 not in sv_mixed_list) and (len(sv_mixed_list) < qs_cnt):
                sv_mixed_list.append(sv_name2)
            if (sv_name1 not in sv_mixed_list) and (len(sv_mixed_list) < qs_cnt):
                sv_mixed_list.append(sv_name1)
            if len(sv_mixed_list) == qs_cnt:
                break

        qs_sets = [
            sv_def_list,
            sv_non_def_list,
            sv_mixed_list,
        ]
        log.info(f"Test config attributes : qs_cnt - {qs_cnt}, qs_sets - {qs_sets}")
        crash_status_before = fs_util_v1.get_crash_ls_new(client1)

        log.info(f"Crash status before Test: {crash_status_before}")
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        ceph_healthy = 0
        while (datetime.datetime.now() < end_time) and (ceph_healthy == 0):
            try:
                fs_util_v1.get_ceph_health_status(client1)
                ceph_healthy = 1
            except Exception as ex:
                log.info(ex)
                time.sleep(5)
        if ceph_healthy == 0:
            assert False, "Ceph Cluster remains unhealthy even after 5mins"
        cg_test_params = {
            "ceph_cluster": ceph_cluster,
            "fs_name": default_fs,
            "fs_util": fs_util_v1,
            "cg_snap_util": cg_snap_util,
            "cg_snap_io": cg_snap_io,
            "clients": qs_clients,
            "mgr_node": mgr_node,
            "mds_nodes": mds_nodes,
            "qs_sets": qs_sets,
        }
        for test_name in test_list:
            log.info(
                f"\n\n                                   ============ {test_name} ============ \n"
            )

            if test_name == "cg_snap_interop_workflow_2":
                nfs_servers = ceph_cluster.get_ceph_objects("nfs")
                nfs_server = nfs_servers[0].node.hostname
                nfs_name = "cephfs-nfs"
                client1 = clients[0]
                client1.exec_command(
                    sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
                )
                if wait_for_process(
                    client=client1, process_name=nfs_name, ispresent=True
                ):
                    log.info("ceph nfs cluster created successfully")
                    nfs_exists = 1
                else:
                    raise CommandFailed("Failed to create nfs cluster")
                nfs_export_name = "/export_" + "".join(
                    secrets.choice(string.digits) for i in range(3)
                )
                cg_test_params.update(
                    {
                        "nfs_export_name": nfs_export_name,
                        "nfs_server": nfs_server,
                        "nfs_name": nfs_name,
                    }
                )
            cg_test_params.update({"test_case": test_name})
            test_status = cg_snap_test_func(cg_test_params)

            if test_status == 1:
                assert False, f"Test {test_name} failed"
            else:
                log.info(f"Test {test_name} passed \n")
            time.sleep(30)  # Wait before next test start
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        wait_time_secs = 300
        if wait_for_healthy_ceph(client1, fs_util_v1, wait_time_secs) == 0:
            client1.exec_command(
                sudo=True,
                cmd="ceph fs status;ceph status -s;ceph health detail",
            )
            assert (
                False
            ), f"Cluster health is not OK even after waiting for {wait_time_secs}secs"

        crash_status_after = fs_util_v1.get_crash_ls_new(client1)
        log.info(f"Crash status after Test: {crash_status_after}")

        if len(crash_status_after) > len(crash_status_before):
            assert False, "Post test validation failed, please check crash report above"

        cmd = "ceph config set mds mds_cache_quiesce_delay 0"
        out, rc = client1.exec_command(
            sudo=True,
            cmd=cmd,
        )
        log.info("Change fragmentation size for FS to default")
        cmd = f"ceph config set mds mds_bal_split_size {default_frag_size}"
        out, rc = client1.exec_command(sudo=True, cmd=cmd)
        log.info("Verify fragmentation size on cluster")
        cmd = "ceph config get mds mds_bal_split_size"
        out, rc = client1.exec_command(sudo=True, cmd=cmd)
        current_frag_size = out.strip()
        if current_frag_size != default_frag_size:
            log.error("Failed to set fragmentation size to default")

        log.info(f"FS fragmentation size:{new_frag_size}")

        if nfs_exists == 1:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster delete {nfs_name}",
                check_ec=False,
            )
        qs_cnt += 1

        for i in range(1, qs_cnt):
            subvol_name = f"sv_def_{i}"
            subvol_info = fs_util_v1.get_subvolume_info(
                client1, default_fs, subvol_name
            )
            log.info(subvol_info)
            fs_util_v1.remove_subvolume(client1, default_fs, subvol_name, validate=True)
        for i in range(1, qs_cnt):
            subvol_name = f"sv_non_def_{i}"
            subvol_info = fs_util_v1.get_subvolume_info(
                client1, default_fs, subvol_name, group_name="subvolgroup_cg"
            )
            log.info(subvol_info)
            fs_util_v1.remove_subvolume(
                client1,
                default_fs,
                subvol_name,
                validate=True,
                group_name="subvolgroup_cg",
            )
        fs_util_v1.remove_subvolumegroup(
            client1, default_fs, "subvolgroup_cg", validate=True
        )


def cg_snap_test_func(cg_test_params):
    if cg_test_params["test_case"] == "cg_snap_func_workflow_1":
        test_status = cg_snap_func_1(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_func_workflow_2":
        test_status = cg_snap_func_2(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_func_workflow_3":
        test_status = cg_snap_func_3(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_func_workflow_4":
        test_status = cg_snap_func_4(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_func_workflow_5":
        test_status = cg_snap_func_5(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_func_workflow_6":
        test_status = cg_snap_func_6(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_interop_workflow_1":
        test_status = cg_snap_interop_1(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_interop_workflow_2":
        test_status = cg_snap_interop_2(cg_test_params)
        return test_status
    elif cg_test_params["test_case"] == "cg_snap_neg_workflow_1":
        test_status = cg_snap_neg_1(cg_test_params)
        return test_status


def cg_snap_func_1(cg_test_params):
    log.info("Workflow 1a - Test quiesce lifecycle with await option")
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    clients = cg_test_params["clients"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]

    qs_clients = [client, client1]
    log.info(f"client:{client.node.hostname}")
    for client_tmp in clients:
        log.info(f"client:{client_tmp.node.hostname}")
    for qs_client in qs_clients:
        log.info(f"qs_client:{qs_client.node.hostname}")
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    total_fail = 0
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"client:{client.node.hostname}")
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 1
    p = Process(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )

    p.start()
    time.sleep(30)
    repeat_cnt = 3
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})

    i = 0
    while i < repeat_cnt:
        if p.is_alive():
            log.info(f"Quiesce Lifecycle : Iteration {i}")
            for qs_member in qs_set:
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, subvol_name, group_name=group_name
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
                else:
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, qs_member
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            log.info(f"Quiesce the set {qs_set}")
            log.info(f"client:{client.node.hostname}")
            cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            time.sleep(30)
            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            snap_name = f"cg_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                snap_create = 0
                retry_cnt = 0
                while (retry_cnt < 10) and (snap_create == 0):
                    try:
                        fs_util.create_snapshot(client, **snapshot)
                        snap_create = 1
                    except BaseException as ex:
                        log.info(ex)
                        time.sleep(3)
                        retry_cnt += 1
                if snap_create == 0:
                    test_fail += 1
                else:
                    log.info(f"Created snapshot {snap_name} on {subvol_name}")
                    snap_list.append(snap_name)
                    snap_qs_dict.update({subvol_name: snap_list})
            log.info(f"Release quiesce set {qs_id_val}")
            cg_snap_util.cg_quiesce_release(
                client, qs_id_val, if_await=True, fs_name=fs_name
            )
            i += 1
            time.sleep(30)
        else:
            i = repeat_cnt

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    wait_for_cg_io(p, qs_id_val)
    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce lifecycle with await on qs_set-{qs_id_val}"
        )
        test_fail = 1

    if test_fail == 1:
        log.error("FAIL: Workflow 1a - quiesce lifecycle with await option")
        test_fail = 0
        total_fail += 1
    else:
        log.info("PASS: Workflow 1a - quiesce lifecycle with await option")

    log.info("Workflow 1b - Test quiesce without --await option")
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 1
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    repeat_cnt = 1
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0
    while i < repeat_cnt:
        if p.is_alive():
            log.info(f"Quiesce Lifecycle : Iteration {i}")
            for qs_member in qs_set:
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, subvol_name, group_name=group_name
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
                else:
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, qs_member
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            log.info(f"Quiesce the set {qs_set}")
            qs_output = cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                if_await=False,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            log.info("Verify quiesce cmd response has status as quiescing")

            if qs_output["sets"][qs_id_val]["state"]["name"] == "QUIESCING":
                log.info("Verified quiesce state is QUIESCING without await option")
            else:
                raise Exception(f"quiesce state of set_id {qs_id_val} is not QUIESCING")
            log.info("Wait for quiesced status")
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
            qs_state_verified = 0
            while (datetime.datetime.now() < end_time) and (qs_state_verified == 0):
                qs_query = cg_snap_util.get_qs_query(
                    client, qs_id=qs_id_val, fs_name=fs_name
                )
                log.info(f"qs_query output : {qs_query}")
                if qs_query["sets"][qs_id_val]["state"]["name"] == "QUIESCED":
                    log.info("Verified quiesce state is QUIESCED without await option")
                    qs_state_verified = 1
                else:
                    time.sleep(5)
            if qs_state_verified == 0:
                raise Exception(
                    f"quiesce state of set_id {qs_id_val} is still not QUIESCED after 5mins"
                )
            time.sleep(30)
            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            snap_name = f"cg_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                snap_create = 0
                retry_cnt = 0
                while (retry_cnt < 10) and (snap_create == 0):
                    try:
                        fs_util.create_snapshot(client, **snapshot)
                        snap_create = 1
                    except BaseException as ex:
                        log.info(ex)
                        time.sleep(3)
                        retry_cnt += 1
                if snap_create == 0:
                    test_fail += 1
                else:
                    log.info(f"Created snapshot cg_snap_{rand_str} on {subvol_name}")
                    snap_list.append(snap_name)
                    snap_qs_dict.update({subvol_name: snap_list})

            log.info(f"Release quiesce set {qs_id_val}")
            qs_output = cg_snap_util.cg_quiesce_release(
                client, qs_id_val, if_await=False, fs_name=fs_name
            )
            log.info("Verify quiesce release cmd response has status as releasing")
            if qs_output["sets"][qs_id_val]["state"]["name"] == "RELEASING":
                log.info("Verified quiesce state is RELEASING without await option")
            else:
                raise Exception(f"quiesce state of set_id {qs_id_val} is not RELEASING")
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
            qs_state_verified = 0
            while (datetime.datetime.now() < end_time) and (qs_state_verified == 0):
                qs_query = cg_snap_util.get_qs_query(
                    client, qs_id=qs_id_val, fs_name=fs_name
                )
                if qs_query["sets"][qs_id_val]["state"]["name"] == "RELEASED":
                    log.info("Verified quiesce state is RELEASED without await option")
                    qs_state_verified = 1
            if qs_state_verified == 0:
                log.error(
                    f"quiesce state of set_id {qs_id_val} is still not RELEASED after 5mins"
                )
                test_fail = 1
                i = repeat_cnt
            i += 1
            time.sleep(30)
        else:
            i = repeat_cnt
            log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    wait_for_cg_io(p, qs_id_val)

    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")

    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    mnt_pt_list = []
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)

    mnt_pt_list.clear()

    snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce lifecycle with await on qs_set-{qs_id_val}"
        )
        test_fail = 1

    if test_fail == 1:
        log.error("FAIL: Workflow 1b - quiesce lifecycle without await option")
        total_fail += 1
    else:
        log.info("PASS: Workflow 1b - quiesce lifecycle without await option")
    if total_fail > 0:
        return 1
    return 0


def cg_snap_func_2(cg_test_params):
    log.info(
        "Workflow 2 - Verify quiesce release with if-version and with exclude and include prior to release."
    )
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    clients = cg_test_params["clients"]
    mds_nodes = cg_test_params["mds_nodes"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    log.info(f"client:{client.node.hostname}")
    for client_tmp in clients:
        log.info(f"client:{client_tmp.node.hostname}")
    for qs_client in qs_clients:
        log.info(f"qs_client:{qs_client.node.hostname}")
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 100
    ephemeral_pin = 1
    p = Process(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    repeat_cnt = 1
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0
    while i < repeat_cnt:
        if p.is_alive():
            log.info(f"Workflow 2 : Iteration {i}")
            for qs_member in qs_set:
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, subvol_name, group_name=group_name
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
                else:
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, qs_member
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            log.info(f"Quiesce the set {qs_set}")
            log.info(f"client:{client.node.hostname}")
            qs_op_out = cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            db_version = qs_op_out["sets"][qs_id_val]["version"]
            time.sleep(30)
            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            snap_name = f"cg_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                fs_util.create_snapshot(client, **snapshot)
                log.info(f"Created snapshot {snap_name} on {subvol_name}")
                snap_list.append(snap_name)
                snap_qs_dict.update({subvol_name: snap_list})

            log.info(f"Verify Release quiesce set {qs_id_val} with if-version")
            out = cg_snap_util.cg_quiesce_release(
                client, qs_id_val, if_await=True, if_version=db_version, fs_name=fs_name
            )
            time.sleep(10)
            if out == 1:
                test_fail += 1
                log.error(
                    f"FAIL : Quiesce set release with if-version failed on {qs_id_val}"
                )

            log.info(f"Reset the quiesce set {qs_id_val} {qs_set}")
            if (
                cg_snap_util.cg_quiesce_reset(
                    client, qs_id_val, qs_set, fs_name=fs_name
                )
                == 1
            ):
                log.error("Reset failed")
                test_fail = 1
                rand_str = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(3))
                )
                qs_id_val = f"cg_test1_{rand_str}"
                log.info("fCreate new quiesce set {qs_id_val} to continue test")
                qs_op_out = cg_snap_util.cg_quiesce(
                    client,
                    qs_set,
                    qs_id=qs_id_val,
                    timeout=300,
                    expiration=300,
                    fs_name=fs_name,
                )

            log.info(
                f"Verify release with if-version with exclude before release on quiesce set {qs_id_val}"
            )

            log.info(f"Exclude a subvolume from quiesce set {qs_id_val}")
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            ver_before_exclude = qs_query_out["sets"][qs_id_val]["version"]
            exclude_sv_name = random.choice(qs_set)
            qs_exclude_status = cg_snap_util.cg_quiesce_exclude(
                client, qs_id_val, [exclude_sv_name], if_await=True, fs_name=fs_name
            )
            if qs_exclude_status == 1:
                test_fail += 1
                log.error(f"Exclude of {exclude_sv_name} in qs set {qs_id_val} failed")
            log.info(f"Verify quiesce set {qs_id_val} state after exclude")
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            ver_after_exclude = qs_query_out["sets"][qs_id_val]["version"]
            if state == "QUIESCED":
                log.info(f"State of qs set {qs_id_val} after exclude is QUIESCED")
            else:
                log.error(
                    f"State of qs set {qs_id_val} after exclude is not as expected - {state}"
                )
                test_fail += 1

            log.info(
                f"Verify quiesce set {qs_id_val} release after exclude with if-version"
            )
            log.info(f"db_version before exclude - {ver_before_exclude}")
            log.info(f"db_version after exclude : {ver_after_exclude}")
            if ver_before_exclude == ver_after_exclude:
                log.error("db_version has NOT changed after exclude")
                test_fail += 1
            else:
                out = cg_snap_util.cg_quiesce_release(
                    client, qs_id_val, if_version=ver_after_exclude, fs_name=fs_name
                )
                if out == 1:
                    test_fail += 1
                    log.error(
                        f"FAIL : Release with if-version with exclude before release on {qs_id_val}"
                    )
            time.sleep(30)
            qs_set_new = qs_set.copy()
            qs_set_new.remove(exclude_sv_name)
            qs_id_val_new = qs_id_val + "_new"
            log.info(f"quiesce set {qs_set_new} with set-id {qs_id_val_new}")
            if (
                cg_snap_util.cg_quiesce(
                    client,
                    qs_set_new,
                    qs_id=qs_id_val_new,
                    timeout=300,
                    expiration=300,
                    fs_name=fs_name,
                )
                == 1
            ):
                log.error("quiesce failed")
                test_fail += 1
                rand_str = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(4))
                )
                qs_id_val_new = f"cg_test1_{rand_str}"
                qs_op_out = cg_snap_util.cg_quiesce(
                    client,
                    qs_set_new,
                    qs_id=qs_id_val_new,
                    timeout=300,
                    expiration=300,
                    fs_name=fs_name,
                )

            log.info(
                f"Verify release with if-version with include before release on quiesce set {qs_id_val_new}"
            )
            include_sv_name = exclude_sv_name
            log.info(
                f"Include a subvolume {include_sv_name} in quiesce set {qs_id_val_new}"
            )
            qs_query_out = cg_snap_util.get_qs_query(
                client, qs_id_val_new, fs_name=fs_name
            )
            ver_before_include = qs_query_out["sets"][qs_id_val_new]["version"]
            for qs_member in qs_query_out["sets"][qs_id_val_new]["members"]:
                if exclude_sv_name in qs_member:
                    exclude_state = qs_query_out["sets"][qs_id_val_new]["members"][
                        qs_member
                    ]["excluded"]
                    log.info(
                        f"excluded value of {exclude_sv_name} before include : {exclude_state}"
                    )
            qs_include_status = cg_snap_util.cg_quiesce_include(
                client, qs_id_val_new, [include_sv_name], if_await=True, fs_name=fs_name
            )
            if qs_include_status == 1:
                test_fail += 1
                log.error(
                    f"Include of {include_sv_name} in qs set {qs_id_val_new} failed"
                )
            log.info(f"Verify quiesce set {qs_id_val_new} state after include")
            qs_query_out = cg_snap_util.get_qs_query(
                client, qs_id_val_new, fs_name=fs_name
            )
            state = qs_query_out["sets"][qs_id_val_new]["state"]["name"]
            ver_after_include = qs_query_out["sets"][qs_id_val_new]["version"]
            if state == "QUIESCED":
                log.info(f"State of qs set {qs_id_val_new} after include is QUIESCED")
            else:
                log.error(
                    f"State of qs set {qs_id_val_new} after include is not as expected - {state}"
                )
                test_fail += 1
            if test_fail == 0:
                log.info(
                    f"Verify quiesce set {qs_id_val_new} release after include with if-version"
                )
                log.info(f"db_version before include - {ver_before_include}")
                log.info(f"db_version after include : {ver_after_include}")
                if ver_before_include == ver_after_include:
                    log.error("db_version has NOT changed after include")
                    test_fail += 1
                else:
                    out = cg_snap_util.cg_quiesce_release(
                        client,
                        qs_id_val_new,
                        if_version=ver_after_include,
                        fs_name=fs_name,
                    )
                    if out == 1:
                        test_fail += 1
                        log.error(
                            f"FAIL : Release with if-version with include before release on {qs_id_val_new}"
                        )

            if test_fail >= 1:
                i = repeat_cnt
            else:
                i += 1
                time.sleep(30)
        else:
            i = repeat_cnt

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    wait_for_cg_io(p, qs_id_val)

    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce lifecycle with await on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL: Workflow 2 - Verify quiesce release with if-version,with exclude,include prior to release"
        )
        return 1
    return 0


def cg_snap_func_3(cg_test_params):
    log.info("Workflow 3 - Verify CG quiesce on pre-provisioned quiesce set")
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    clients = cg_test_params["clients"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    log.info(f"client:{client.node.hostname}")
    for client_tmp in clients:
        log.info(f"client:{client_tmp.node.hostname}")
    for qs_client in qs_clients:
        log.info(f"qs_client:{qs_client.node.hostname}")
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 0
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    repeat_cnt = 1
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0
    while i < repeat_cnt:
        if p.is_alive():
            log.info(f"Workflow 3 : Iteration {i}")
            for qs_member in qs_set:
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, subvol_name, group_name=group_name
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
                else:
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, qs_member
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val_sub = f"cg_test1_subset_{rand_str}"
            qs_subset = random.sample(qs_set, 3)
            log.info(f"Run Quiesce on subset of subvolumes in quiesce set {qs_set}")
            log.info(f"Quiesce the subset {qs_subset}")

            qs_op_out = cg_snap_util.cg_quiesce(
                client,
                qs_subset,
                qs_id=qs_id_val_sub,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            log.info(f"quiesce cmd response : {qs_op_out}")
            log.info(f"Run quiesce on original quiesce set {qs_set}")
            qs_id_val = f"cg_test1_{rand_str}"
            qs_op_out = cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            log.info(f"quiesce cmd response : {qs_op_out}")
            time.sleep(30)
            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            snap_name = f"cg_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                snap_create = 0
                retry_cnt = 0
                while (retry_cnt < 10) and (snap_create == 0):
                    try:
                        fs_util.create_snapshot(client, **snapshot)
                        snap_create = 1
                    except BaseException as ex:
                        log.info(ex)
                        time.sleep(3)
                        retry_cnt += 1
                if snap_create == 0:
                    test_fail += 1
                else:
                    log.info(f"Created snapshot {snap_name} on {subvol_name}")
                    snap_list.append(snap_name)
                    snap_qs_dict.update({subvol_name: snap_list})

            log.info(f"Release the subset {qs_subset}")
            out = cg_snap_util.cg_quiesce_release(
                client, qs_id_val_sub, if_await=True, fs_name=fs_name
            )
            if out == 1:
                test_fail += 1
                log.error(f"FAIL : Quiesce subset release failed on {qs_id_val_sub}")
            log.info(f"Release quiesce set with id {qs_id_val}")
            out = cg_snap_util.cg_quiesce_release(
                client, qs_id_val, if_await=True, fs_name=fs_name
            )
            if out == 1:
                test_fail += 1
                log.error(f"FAIL : Quiesce set release failed on {qs_id_val}")

            if test_fail >= 1:
                i = repeat_cnt
            else:
                i += 1
                time.sleep(30)
        else:
            i = repeat_cnt

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")
    wait_for_cg_io(p, qs_id_val)

    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")

    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during workflow3 on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error("FAIL: Workflow 3 - Verify quiesce on pre-provisioned quiesce set")
        return 1
    return 0


def cg_snap_func_4(cg_test_params):
    log.info("Workflow 4 - Verify Restore suceeds from snapshot created during quiesce")
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    clients = cg_test_params["clients"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    log.info(f"client:{client.node.hostname}")
    for client_tmp in clients:
        log.info(f"client:{client_tmp.node.hostname}")
    for qs_client in qs_clients:
        log.info(f"qs_client:{qs_client.node.hostname}")
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 0
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(4))
    )
    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")

    log.info(f"Run quiesce on quiesce set {qs_set}")
    qs_id_val = f"cg_test1_{rand_str}"
    qs_op_out = cg_snap_util.cg_quiesce(
        client,
        qs_set,
        qs_id=qs_id_val,
        timeout=300,
        expiration=300,
        fs_name=fs_name,
    )
    log.info(f"quiesce cmd response : {qs_op_out}")
    time.sleep(30)
    log.info("Perform snapshot creation on all members")
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(3))
    )
    snap_name = f"cg_snap_{rand_str}"
    for qs_member in qs_set:
        snap_list = snap_qs_dict[qs_member]
        snapshot = {
            "vol_name": fs_name,
            "snap_name": snap_name,
        }
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            snapshot.update(
                {
                    "subvol_name": subvol_name,
                    "group_name": group_name,
                }
            )
        else:
            subvol_name = qs_member
            snapshot.update(
                {
                    "subvol_name": subvol_name,
                }
            )
        fs_util.create_snapshot(client, **snapshot)
        log.info(f"Created snapshot {snap_name} on {subvol_name}")
        snap_list.append(snap_name)
        snap_qs_dict.update({subvol_name: snap_list})

    log.info(f"Release quiesce set with id {qs_id_val}")
    out = cg_snap_util.cg_quiesce_release(
        client, qs_id_val, if_await=True, fs_name=fs_name
    )
    if out == 1:
        test_fail += 1
        log.error(f"FAIL : Quiesce set release failed on {qs_id_val}")
    log.info("Verify snapshot restore on each member")
    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            qs_member = subvol_name
        mnt_pt = qs_member_dict2[qs_member]["mount_point"]
        snap_list = snap_qs_dict[qs_member]
        snap_name = random.choice(snap_list)
        snap_path = f"{mnt_pt}/.snap/_{snap_name}*/"
        restore_dst = f"{mnt_pt}/restore_dst_dir/"
        cmd = f"mkdir {restore_dst};ls {snap_path}/cg_io/dd_dir/*;cp {snap_path}/cg_io/dd_dir/*file* {restore_dst}"
        try:
            out, rc = client1.exec_command(sudo=True, cmd=cmd)
            log.info(f"Restore suceeded for {qs_member} : {out}")
        except Exception as ex:
            log.error(f"Restore failed with error {ex} on {qs_member}")
            test_fail += 1

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")
    wait_for_cg_io(p, qs_id_val)
    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during workflow4 on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL: Workflow 4 - Verify Restore suceeds from snapshot created during quiesce"
        )
        return 1
    return 0


def cg_snap_func_5(cg_test_params):
    log.info(
        "Workflow 5 - Validate quiesce release response when quiesce-timeout and quiesce-expire time is reached"
    )
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    clients = cg_test_params["clients"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    log.info(f"client:{client.node.hostname}")
    for client_tmp in clients:
        log.info(f"client:{client_tmp.node.hostname}")
    for qs_client in qs_clients:
        log.info(f"qs_client:{qs_client.node.hostname}")
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 0
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(4))
    )

    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")

    qs_member_str = ""
    log.info(f"Run quiesce on quiesce set {qs_set} with shorter timeout")
    qs_id_val = f"cg_test1_{rand_str}"
    qs_set_copy = qs_set.copy()

    for qs_member in qs_set_copy:
        qs_member_str += f' "{qs_member}" '
    cmd = f"ceph fs quiesce {fs_name} {qs_member_str} --set-id  {qs_id_val} --expiration 5 --await"
    try:
        out, rc = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        log.info(f"Quiesce with shorter timeout : {out}")
        test_fail += 1
        log.error("Quiesce succeeds when timedout")
    except Exception as ex:
        log.info(ex)
        if "ETIMEDOUT" in str(ex):
            log.info("Quiesce fails as expected when timedout")
    log.info("Perform quiesce with shorter expiration")
    qs_id_val = f"cg_test2_{rand_str}"
    qs_op_out = cg_snap_util.cg_quiesce(
        client,
        qs_set,
        qs_id=qs_id_val,
        timeout=300,
        expiration=5,
        fs_name=fs_name,
    )
    log.info(f"quiesce cmd response : {qs_op_out}")
    log.info("Wait for quiesce set to expire")
    time.sleep(6)
    cmd = f"ceph fs quiesce {fs_name} --set-id  {qs_id_val} --release --await"
    try:
        out, rc = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        test_fail += 1
        log.error("Quiesce release suceeds on expired set")
    except Exception as ex:
        log.info(ex)
        if "EPERM" in str(ex):
            log.info("Quiesce release fails as expected when expired")
    log.info("Verify Quiesce state is EXPIRED")
    qs_query = cg_snap_util.get_qs_query(client, qs_id=qs_id_val, fs_name=fs_name)
    log.info(f"qs_query:{qs_query}")
    state = qs_query["sets"][qs_id_val]["state"]["name"]
    if state != "EXPIRED":
        log.error(f"Quiesce state is not EXPIRED, it is {state}")
        test_fail += 1
    log.info("Quiesce state is EXPIRED as expected")

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")
    wait_for_cg_io(p, qs_id_val)

    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")

    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during workflow5 on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL:Workflow 5-Validate quiesce release response when quiesce-timeout and quiesce-expire time reached"
        )
        return 1
    return 0


def cg_snap_func_6(cg_test_params):
    log.info("Workflow 6 - Perform all state transitions and validate response")
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 0
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    repeat_cnt = 1
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0
    while i < repeat_cnt:
        if p.is_alive():
            log.info(f"Workflow 6 : Iteration {i}")
            for qs_member in qs_set:
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, subvol_name, group_name=group_name
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
                else:
                    subvol_info = fs_util.get_subvolume_info(
                        client1, fs_name, qs_member
                    )
                    log.info(f"SUBVOL_INFO BEFORE TEST:{subvol_info}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            log.info(
                " 1.State - Quiescing:Quiesce without --await, when in quiescing perform include,exclude"
            )

            qs_set_copy = qs_set.copy()
            log.info("Induce delay in quiescing")
            cmd = "ceph config set mds mds_cache_quiesce_delay 3000"
            client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            include_sv_name = qs_set_copy.pop()
            log.info(f"Quiesce the set {qs_set_copy} without --await")
            cg_snap_util.cg_quiesce(
                client,
                qs_set_copy,
                qs_id=qs_id_val,
                if_await=False,
                timeout=300,
                expiration=100,
                fs_name=fs_name,
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} before include:{state}")
            if state != "QUIESCING":
                log.info(
                    f"State of set-id {qs_id_val} before include is not as Expected"
                )
            log.info(f"Include {include_sv_name} to set-id {qs_id_val}")
            qs_include_status = cg_snap_util.cg_quiesce_include(
                client, qs_id_val, [include_sv_name], if_await=False, fs_name=fs_name
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} after include:{state}")
            log.info(f"State of set-id {qs_id_val} before exclude:{state}")
            if state != "QUIESCING":
                log.info(
                    f"State of set-id {qs_id_val} before exclude is not as Expected"
                )

            exclude_sv_name = random.choice(qs_set_copy)
            log.info(f"Exclude {exclude_sv_name} from set-id {qs_id_val}")
            qs_exclude_status = cg_snap_util.cg_quiesce_exclude(
                client, qs_id_val, [exclude_sv_name], if_await=False, fs_name=fs_name
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            log.info(f"qs_query output:{qs_query_out}")
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} after exclude:{state}")
            log.info(f"qs_exclude_status:{qs_exclude_status}")
            log.info(f"qs_include_status:{qs_include_status}")
            if qs_exclude_status == 1:
                test_fail += 1
                log.error(f"Exclude of {exclude_sv_name} in qs set {qs_id_val} failed")
            if qs_include_status == 1:
                test_fail += 1
                log.error(f"Include of {include_sv_name} in qs set {qs_id_val} failed")
            log.info(f"Wait for QUIESCED state in set-id {qs_id_val}")
            wait_status = wait_for_cg_state(
                client, cg_snap_util, qs_id_val, "QUIESCED", fs_name
            )
            log.info(f"wait_status:{wait_status}")
            if wait_status:
                test_fail += 1
                log.error(f"qs set {qs_id_val} not reached QUIESCED state")

            cg_snap_util.cg_quiesce_release(client, qs_id_val, fs_name=fs_name)
            log.info("Reset delay in quiescing")
            cmd = "ceph config set mds mds_cache_quiesce_delay 0"
            client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            log.info(
                " 2.State - Quiesced:Quiesce with --await, when in quiesced perform include,exclude"
            )
            qs_set_copy = qs_set.copy()
            include_sv_name = qs_set_copy.pop()
            log.info(f"Quiesce the set {qs_set_copy} with --await")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            cg_snap_util.cg_quiesce(
                client,
                qs_set_copy,
                qs_id=qs_id_val,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} before include:{state}")
            if state != "QUIESCED":
                log.info(
                    f"State of set-id {qs_id_val} before include is not as Expected"
                )
            log.info(f"Include {include_sv_name} to set-id {qs_id_val}")
            qs_include_status = cg_snap_util.cg_quiesce_include(
                client, qs_id_val, [include_sv_name], if_await=False, fs_name=fs_name
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} after include:{state}")
            if state != "QUIESCED":
                log.info(
                    f"State of set-id {qs_id_val} after include is not as Expected"
                )
            log.info(f"State of set-id {qs_id_val} before exclude:{state}")
            if state != "QUIESCED":
                log.info(
                    f"State of set-id {qs_id_val} before exclude is not as Expected"
                )
            log.info(f"Exclude {exclude_sv_name} from set-id {qs_id_val}")
            exclude_sv_name = random.choice(qs_set_copy)
            qs_exclude_status = cg_snap_util.cg_quiesce_exclude(
                client, qs_id_val, [exclude_sv_name], if_await=False, fs_name=fs_name
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} after exclude:{state}")
            if state != "QUIESCED":
                log.info(
                    f"State of set-id {qs_id_val} after exclude is not as Expected"
                )
            if qs_exclude_status == 1:
                test_fail += 1
                log.error(f"Exclude of {exclude_sv_name} in qs set {qs_id_val} failed")
            if qs_include_status == 1:
                test_fail += 1
                log.error(f"Include of {include_sv_name} in qs set {qs_id_val} failed")
            log.info(f"Wait for QUIESCED state in set-id {qs_id_val}")
            if wait_for_cg_state(client, cg_snap_util, qs_id_val, "QUIESCED", fs_name):
                test_fail += 1
                log.error(f"qs set {qs_id_val} not reached QUIESCED state")
            cg_snap_util.cg_quiesce_release(client, qs_id_val, fs_name=fs_name)

            log.info(
                " 3.State - Releasing:Release without --await, when in Releasing perform exclude"
            )
            qs_set_copy = qs_set.copy()
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            exclude_sv_name = random.choice(qs_set_copy)
            log.info(f"Quiesce the set {qs_set_copy} with --await")
            cg_snap_util.cg_quiesce(
                client,
                qs_set_copy,
                qs_id=qs_id_val,
                timeout=300,
                expiration=100,
                fs_name=fs_name,
            )
            time.sleep(10)
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} before exclude:{state}")
            if state != "QUIESCED":
                log.info(
                    f"State of set-id {qs_id_val} before exclude is not as Expected"
                )
                test_fail += 1
            else:
                log.info(f"Verify exclude while Releasing quiesce set {qs_id_val}")
                out = cg_snap_util.cg_quiesce_release(
                    client, qs_id_val, if_await=False, fs_name=fs_name
                )
                if out == 1:
                    test_fail += 1
                    log.error(f"FAIL : Quiesce set release failed on {qs_id_val}")
                else:
                    try:
                        qs_exclude_status = cg_snap_util.cg_quiesce_exclude(
                            client,
                            qs_id_val,
                            [exclude_sv_name],
                            if_await=False,
                            fs_name=fs_name,
                        )
                    except Exception as ex:
                        log.info(ex)
                        if "EPERM" in str(ex):
                            log.info(
                                f"Exclude failed as expected during Releasing state on qs set {qs_id_val}"
                            )
                        else:
                            test_fail += 1
                            log.error(
                                f"Exclude passed during Releasing state on qs set {qs_id_val}"
                            )
                    qs_query_out = cg_snap_util.get_qs_query(
                        client, qs_id_val, fs_name=fs_name
                    )
                    log.info(f"qs_query_out:{qs_query_out}")
                    state = qs_query_out["sets"][qs_id_val]["state"]["name"]
                    log.info(f"State of set-id {qs_id_val} after exclude:{state}")

            if test_fail >= 1:
                i = repeat_cnt
            else:
                i += 1
                time.sleep(10)
        else:
            i = repeat_cnt

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    wait_for_cg_io(p, qs_id_val)
    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            subvol_info = fs_util.get_subvolume_info(
                client1, fs_name, subvol_name, group_name=group_name
            )
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
        else:
            subvol_info = fs_util.get_subvolume_info(client1, fs_name, qs_member)
            log.info(f"SUBVOL_INFO AFTER TEST:{subvol_info}")
    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce lifecycle with await on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL: Workflow 6 - Perform all state transitions and validate response"
        )
        return 1
    return 0


def cg_snap_interop_1(cg_test_params):
    log.info(
        "Interop Workflow 1 - Verify MDS failover during quiescing, quiesced and releasing states"
    )
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]
    fs_util = cg_test_params["fs_util"]

    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name}")
    out_list = out.split("\n")
    for line in out_list:
        if "max_mds" in line:
            str, max_mds_val = line.split()
            log.info(f"max_mds before test {max_mds_val}")
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {fs_name} --format json"
    )
    output = json.loads(out)
    mds_cnt = len(output["mdsmap"])
    if mds_cnt < 7:
        cmd = f'ceph orch apply mds {fs_name} --placement="7'
        for mds_nodes_iter in mds_nodes:
            cmd += f" {mds_nodes_iter.node.hostname}"
        cmd += '"'
        log.info("Adding 7 MDS to cluster")
        out, rc = client.exec_command(sudo=True, cmd=cmd)
    if wait_for_healthy_ceph(client1, fs_util, 300) == 0:
        return 1
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {fs_name} max_mds 4",
    )
    if wait_for_healthy_ceph(client1, fs_util, 300) == 0:
        return 1
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 120
    ephemeral_pin = 1
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(30)
    repeat_cnt = 5
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0
    while i < repeat_cnt:
        if p.is_alive():
            log.info(f"Interop Workflow 1 : Iteration {i}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_int1_{rand_str}"
            log.info(
                " 1.State - Quiescing:Quiesce without await, when in quiescing perform MDS failover"
            )
            log.info("Induce delay in quiescing")
            cmd = "ceph config set mds mds_cache_quiesce_delay 3000"
            client.exec_command(
                sudo=True,
                cmd=cmd,
            )

            log.info(f"Quiesce the set {qs_set} without --await")
            cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                if_await=False,
                timeout=300,
                expiration=100,
                fs_name=fs_name,
            )

            log.info("Perform MDS failover")
            if cg_mds_failover(fs_util, client, fs_name):
                test_fail += 1

            log.info(f"Wait for CANCELED state in set-id {qs_id_val}")
            wait_status = wait_for_cg_state(
                client, cg_snap_util, qs_id_val, "CANCELED", fs_name
            )
            log.info(f"wait_status:{wait_status}")
            if wait_status:
                test_fail += 1
                log.error(f"qs set {qs_id_val} not reached CANCELED state")

            log.info("MDS failover during quiescing: quiesce state is CANCELED")
            log.info("Reset quiesce delay to 0")
            cmd = "ceph config set mds mds_cache_quiesce_delay 0"
            out, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            time.sleep(10)

            if wait_for_healthy_ceph(client1, fs_util, 300) == 0:
                log.error("Ceph cluster is not healthy after MDS failover")
                return 1
            log.info("Verify quiesce lifecycle can suceed after mds failover")
            if cg_quiesce_lifecycle(client, cg_snap_util, qs_set, fs_name):
                test_fail += 1

            log.info(
                " 2.State - Quiesced:Quiesce with --await, when in quiesced perform MDS failover"
            )

            log.info(f"Quiesce the set {qs_set} with --await")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_int1_{rand_str}"
            cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                timeout=300,
                expiration=300,
                fs_name=fs_name,
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            log.info(f"State of set-id {qs_id_val} before MDS failover:{state}")
            if state != "QUIESCED":
                log.info(
                    f"State of set-id {qs_id_val} before mds failover is not as Expected"
                )
            log.info("Perform MDS failover when in QUIESCED state")
            if cg_mds_failover(fs_util, client, fs_name):
                test_fail += 1
            log.info(f"Wait for CANCELED state in set-id {qs_id_val}")
            wait_status = wait_for_cg_state(
                client, cg_snap_util, qs_id_val, "CANCELED", fs_name
            )
            log.info(f"wait_status:{wait_status}")
            if wait_status:
                test_fail += 1
                log.error(f"qs set {qs_id_val} not reached CANCELED state")

            log.info("MDS failover when quiesced: quiesce state is CANCELED")
            time.sleep(10)
            if wait_for_healthy_ceph(client1, fs_util, 300) == 0:
                log.error("Ceph cluster is not healthy after MDS failover")
                return 1
            log.info("Verify quiesce lifecycle can suceed after mds failover")
            if cg_quiesce_lifecycle(client, cg_snap_util, qs_set, fs_name):
                test_fail += 1

            log.info(
                " 3.State - Releasing:Release without --await, when in Releasing perform mds failover"
            )

            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            qs_id_val = f"cg_int1_{rand_str}"
            log.info(f"Quiesce the set {qs_set} with --await")
            cg_snap_util.cg_quiesce(
                client,
                qs_set,
                qs_id=qs_id_val,
                timeout=300,
                expiration=100,
                fs_name=fs_name,
            )
            time.sleep(10)
            cg_snap_util.cg_quiesce_release(
                client, qs_id_val, if_await=False, fs_name=fs_name
            )

            log.info(f"Verify mds failover while Releasing quiesce set {qs_id_val}")
            if cg_mds_failover(fs_util, client, fs_name):
                test_fail += 1
            log.info(f"Wait for RELEASED state in set-id {qs_id_val}")
            wait_status = wait_for_cg_state(
                client, cg_snap_util, qs_id_val, "RELEASED", fs_name
            )
            log.info(f"wait_status:{wait_status}")
            if wait_status:
                test_fail += 1
                log.error(f"qs set {qs_id_val} not reached RELEASED state")

            time.sleep(10)
            if wait_for_healthy_ceph(client1, fs_util, 300) == 0:
                log.error("Ceph cluster is not healthy after MDS failover")
                return 1
            log.info("Verify quiesce lifecycle can suceed after mds failover")
            if cg_quiesce_lifecycle(client, cg_snap_util, qs_set, fs_name):
                test_fail += 1

            log.info("MDS failover during Releasing: Quiesce state is RELEASED")
            if test_fail >= 1:
                i = repeat_cnt
            else:
                i += 1
                time.sleep(10)
        else:
            i = repeat_cnt

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    wait_for_cg_io(p, qs_id_val)

    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {fs_name} max_mds 2",
    )
    if wait_for_healthy_ceph(client1, fs_util, 300) == 0:
        log.error("Ceph cluster is not healthy after max_mds set to 2")
        test_fail += 1
        client.exec_command(
            sudo=True,
            cmd=f"ceph fs status {fs_name};ceph status -s;ceph health detail",
        )
    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce test on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL: Interop Workflow 1 - Verify MDS failover during quiescing, quiesced and releasing states"
        )
        return 1
    return 0


def cg_snap_interop_2(cg_test_params):
    log.info(
        "Interop Workflow 2 - Verify quiesce suceeds with IO from kernel,fuse and nfs mountpoints in parallel"
    )
    cg_test_io_status = {}
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    client = cg_test_params["clients"][0]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client1, client1]
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    cg_snap_io = cg_test_params["cg_snap_io"]

    test_fail = 0
    nfs_export_list = []
    mnt_type_list = ["fuse", "nfs", "kernel"]
    qs_set = random.choice(qs_sets)
    subvol_dict = {}
    for qs_member in qs_set:
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            cmd = f"ceph fs subvolume getpath {fs_name} {subvol_name} {group_name}"
            subvol_dict.update(
                {subvol_name: {"group_name": group_name, "mount_point": ""}}
            )
        else:
            subvol_name = qs_member
            cmd = f"ceph fs subvolume getpath {fs_name} {subvol_name}"
            subvol_dict.update({subvol_name: {"mount_point": ""}})

        subvol_path, rc = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        mnt_path = subvol_path.strip()

        mount_params = {
            "fs_util": fs_util,
            "client": client1,
            "mnt_path": mnt_path,
            "fs_name": fs_name,
            "export_created": 0,
        }
        mnt_type = random.choice(mnt_type_list)
        if mnt_type == "nfs":
            nfs_export_name = f"{cg_test_params['nfs_export_name']}_{subvol_name}"
            nfs_export_list.append(nfs_export_name)
            mount_params.update(
                {
                    "nfs_name": cg_test_params["nfs_name"],
                    "nfs_export_name": nfs_export_name,
                    "nfs_server": cg_test_params["nfs_server"],
                }
            )
        log.info(f"Perform {mnt_type} mount of {subvol_name}")
        mounting_dir, _ = fs_util.mount_ceph(mnt_type, mount_params)
        subvol_dict[subvol_name].update({"mount_point": mounting_dir})
        subvol_dict[subvol_name].update({"client": client1.node.hostname})

    client_mnt_dict = {}

    client_mnt_dict.update({client1.node.hostname: subvol_dict})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 60
    ephemeral_pin = 0
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()

    time.sleep(30)
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})

    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(4))
    )
    qs_id_val = f"cg_int_2_{rand_str}"

    log.info(f"Quiesce the set {qs_set} with id {qs_id_val}")
    cg_snap_util.cg_quiesce(
        client,
        qs_set,
        qs_id=qs_id_val,
        timeout=600,
        expiration=600,
        fs_name=fs_name,
    )

    log.info(f"Query quiesce set {qs_id_val}")
    out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
    log.info(out)
    time.sleep(5)
    log.info("Perform snapshot creation on all members")
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(3))
    )
    snap_name = f"cg_int2_snap_{rand_str}"
    for qs_member in qs_set:
        snap_list = snap_qs_dict[qs_member]
        snapshot = {
            "vol_name": fs_name,
            "snap_name": snap_name,
        }
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            snapshot.update(
                {
                    "subvol_name": subvol_name,
                    "group_name": group_name,
                }
            )
        else:
            subvol_name = qs_member
            snapshot.update(
                {
                    "subvol_name": subvol_name,
                }
            )
        try:
            fs_util.create_snapshot(client, **snapshot)
        except Exception as ex:
            log.info(ex)
        log.info(f"Created snapshot {snap_name} on {subvol_name}")
        snap_list.append(snap_name)
        snap_qs_dict.update({subvol_name: snap_list})

    log.info(f"Release quiesce set {qs_id_val}")
    cg_snap_util.cg_quiesce_release(client, qs_id_val, if_await=True, fs_name=fs_name)

    log.info(f"Reset the quiesce set - {qs_set}")
    cg_snap_util.cg_quiesce_reset(client, qs_id_val, qs_set, fs_name=fs_name)
    time.sleep(5)
    log.info("Perform snapshot creation on all members")
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(4))
    )
    snap_name = f"cg_int2_snap_{rand_str}"
    for qs_member in qs_set:
        snap_list = snap_qs_dict[qs_member]
        snapshot = {
            "vol_name": fs_name,
            "snap_name": snap_name,
        }
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            snapshot.update(
                {
                    "subvol_name": subvol_name,
                    "group_name": group_name,
                }
            )
        else:
            subvol_name = qs_member
            snapshot.update(
                {
                    "subvol_name": subvol_name,
                }
            )
        try:
            fs_util.create_snapshot(client, **snapshot)
        except Exception as ex:
            log.info(ex)
        log.info(f"Created snapshot {snap_name} on {subvol_name}")
        snap_list.append(snap_name)
        snap_qs_dict.update({subvol_name: snap_list})

    log.info(f"Cancel quiesce set {qs_id_val}")
    try:
        cg_snap_util.cg_quiesce_cancel(client, qs_id_val)
    except Exception as ex:
        log.info(ex)
    log.info(f"Query quiesce set {qs_id_val}")
    out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
    log.info(out)

    log.info(f"Reset the quiesce set - {qs_set} with id {qs_id_val}")
    cg_snap_util.cg_quiesce_reset(client, qs_id_val, qs_set, fs_name=fs_name)

    log.info(f"Exclude a subvolume from quiesce set {qs_id_val}")

    exclude_sv_name = random.choice(qs_set)
    qs_exclude_status = cg_snap_util.cg_quiesce_exclude(
        client, qs_id_val, [exclude_sv_name], if_await=True, fs_name=fs_name
    )
    if qs_exclude_status == 1:
        test_fail = 1
        log.error(f"Exclude of {exclude_sv_name} in qs set {qs_id_val} failed")
    log.info(f"Verify quiesce set {qs_id_val} state after exclude")
    qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
    log.info(qs_query_out)
    state = qs_query_out["sets"][qs_id_val]["state"]["name"]
    if state == "QUIESCED":
        log.info(f"State of qs set {qs_id_val} after exclude is QUIESCED")
    else:
        log.error(
            f"State of qs set {qs_id_val} after exclude is not as expected - {state}"
        )
        test_fail = 1
    include_sv_name = exclude_sv_name
    log.info(f"Include a subvolume {include_sv_name} in quiesce set {qs_id_val}")
    qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
    for qs_member in qs_query_out["sets"][qs_id_val]["members"]:
        if exclude_sv_name in qs_member:
            exclude_state = qs_query_out["sets"][qs_id_val]["members"][qs_member][
                "excluded"
            ]
            log.info(
                f"excluded value of {exclude_sv_name} before include : {exclude_state}"
            )
    qs_include_status = cg_snap_util.cg_quiesce_include(
        client, qs_id_val, [include_sv_name], if_await=True, fs_name=fs_name
    )
    if qs_include_status == 1:
        test_fail = 1
        log.error(f"Include of {include_sv_name} in qs set {qs_id_val} failed")

    log.info(f"Release quiesce set {qs_id_val}")
    cg_snap_util.cg_quiesce_release(client, qs_id_val, if_await=True, fs_name=fs_name)

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    wait_for_cg_io(p, qs_id_val)

    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in subvol_dict:
        mnt_pt_list.append(subvol_dict[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    log.info("Remove CG snapshots")
    for qs_member in subvol_dict:
        snap_list = snap_qs_dict[qs_member]
        if subvol_dict[qs_member].get("group_name"):
            group_name = subvol_dict[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )
    for nfs_export_name in nfs_export_list:
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {cg_test_params['nfs_name']} {nfs_export_name}",
            check_ec=False,
        )
    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce test on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL: Interop Workflow 2 - Verify quiesce with IO from kernel,fuse and nfs mountpoints"
        )
        return 1
    return 0


def cg_snap_neg_1(cg_test_params):
    log.info("Negative Workflow 1 - Verify parallel quiesce calls to same set")

    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    mds_nodes = cg_test_params["mds_nodes"]
    client = cg_test_params["clients"][0]

    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]
    client1 = cg_test_params["clients"][1]
    qs_clients = [client, client1]
    log.info(f"client:{client.node.hostname}")

    cg_snap_io = cg_test_params["cg_snap_io"]
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(client, qs_set, fs_name)
    qs_member_dict2 = cg_snap_util.mount_qs_members(client1, qs_set, fs_name)
    client_mnt_dict.update({client.node.hostname: qs_member_dict1})
    client_mnt_dict.update({client1.node.hostname: qs_member_dict2})
    time.sleep(5)
    log.info(f"Start the IO on quiesce set members - {qs_set}")

    cg_test_io_status = Value("i", 0)
    io_run_time = 20
    ephemeral_pin = 0
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            qs_clients,
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
        kwargs={"fs_name": fs_name},
    )
    p.start()
    time.sleep(10)
    repeat_cnt = 1
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0

    while i < repeat_cnt:
        parallel_cnt = 5
        qs_id_list = []
        if p.is_alive():
            log.info(f"Negative Workflow 1 : Iteration {i}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            quiesce_procs = []
            for k in range(parallel_cnt):
                qs_id_val = f"cg_neg_{k}_{rand_str}"
                log.info(
                    f"Iter{k}: Run quiesce on quiesce set {qs_set} with id {qs_id_val}"
                )
                cg_args = {
                    "qs_id": qs_id_val,
                    "timeout": 300,
                    "expiration": 300,
                    "if_await": "False",
                }
                try:
                    quiesce_proc = Thread(
                        target=cg_snap_util.cg_quiesce,
                        args=(client, qs_set),
                        kwargs=cg_args,
                    )
                    quiesce_proc.start()
                    quiesce_procs.append(quiesce_proc)
                    qs_id_list.append(qs_id_val)
                except Exception as ex:
                    log.info(ex)

            for quiesce_proc in quiesce_procs:
                quiesce_proc.join()
            time.sleep(30)

            for qs_id in qs_id_list:
                qs_query_out = cg_snap_util.get_qs_query(client, qs_id, fs_name=fs_name)
                state = qs_query_out["sets"][qs_id]["state"]["name"]
                if "QUIESCED" not in state:
                    test_fail += 1
                    log.error(f"Quiesce failed on Quiesce set id {qs_id}")

            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            snap_name = f"cg_neg1_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                snap_create = 0
                retry_cnt = 0
                while (retry_cnt < 10) and (snap_create == 0):
                    try:
                        fs_util.create_snapshot(client, **snapshot)
                        snap_create = 1
                    except BaseException as ex:
                        log.info(ex)
                        time.sleep(3)
                        retry_cnt += 1
                if snap_create == 0:
                    test_fail += 1
                else:
                    log.info(f"Created snapshot {snap_name} on {subvol_name}")
                    snap_list.append(snap_name)
                    snap_qs_dict.update({subvol_name: snap_list})

            for qs_id in qs_id_list:
                log.info(f"Release the qs_set with id {qs_id}")
                try:
                    cg_snap_util.cg_quiesce_release(
                        client, qs_id, if_await=True, fs_name=fs_name
                    )
                except Exception as ex:
                    log.info(ex)
                    qs_query_out = cg_snap_util.get_qs_query(
                        client, qs_id, fs_name=fs_name
                    )
                    state = qs_query_out["sets"][qs_id]["state"]["name"]
                    if "RELEASED" not in state:
                        test_fail += 1
                        log.error(f"Release failed on Quiesce set id {qs_id}")

            if test_fail >= 1:
                i = repeat_cnt
            else:
                i += 1
                time.sleep(30)
        else:
            i = repeat_cnt

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")
    wait_for_cg_io(p, qs_id_val)

    mnt_pt_list = []
    if ephemeral_pin == 1:
        if (
            cg_snap_util.validate_pin_stats(client, fs_util, mds_nodes, fs_name=fs_name)
            == 0
        ):
            log.error(
                "Ephemeral random pinning feature was NOT exercised during the test"
            )
        log.info("Ephemeral random pinning feature was exercised during the test")
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(client1, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    snap_name = f"cg_snap_neg_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    client, fs_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during negative workflow1 on qs_set-{qs_id_val}"
        )
        test_fail += 1

    if test_fail >= 1:
        log.error(
            "FAIL: Negative Workflow 1 - Verify parallel quiesce calls to same set"
        )
        return 1
    return 0


# HELPER ROUTINES


def wait_for_cg_io(p, qs_id_val):
    if p.is_alive():
        proc_stop = 0
        log.info("CG IO is running after quiesce lifecycle")
        wait_time = 1800
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_time)
        while (datetime.datetime.now() < end_time) and (proc_stop == 0):
            if p.is_alive():
                time.sleep(10)
            else:
                proc_stop = 1
        if proc_stop == 1:
            log.info("CG IO completed")
        elif proc_stop == 0:
            log.error("CG IO has NOT completed")

    else:
        log.info(
            f"WARN:CG IO test completed early during quiesce test on qs_set id {qs_id_val}"
        )


def wait_for_cg_state(client, cg_snap_util, qs_id_val, exp_state, fs_name="cephfs"):
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
    qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
    actual_state = qs_query_out["sets"][qs_id_val]["state"]["name"]
    while (datetime.datetime.now() < end_time) and (actual_state != exp_state):
        qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
        actual_state = qs_query_out["sets"][qs_id_val]["state"]["name"]
        if actual_state == exp_state:
            log.info(f"State of qs set {qs_id_val} is {exp_state}")
        else:
            log.error(
                f"State of qs set {qs_id_val} is not as expected - {exp_state},current state - {actual_state}"
            )
        time.sleep(2)
    if actual_state == exp_state:
        return 0
    else:
        return 1


def cg_quiesce_lifecycle(client, cg_snap_util, qs_set, fs_name="cephfs"):
    try:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(4))
        )
        qs_id_val = f"cg_test1_{rand_str}"
        log.info(f"Quiesce the set {qs_set} with --await")
        cg_snap_util.cg_quiesce(
            client,
            qs_set,
            qs_id=qs_id_val,
            timeout=300,
            expiration=100,
            fs_name=fs_name,
        )
        time.sleep(10)
        cg_snap_util.cg_quiesce_release(client, qs_id_val, fs_name=fs_name)
    except Exception as ex:
        log.info(ex)
        return 1
    return 0


def cg_mds_failover(fs_util, client, fs_name, repeat_cnt=1):
    while repeat_cnt > 0:
        mds_ls = fs_util.get_active_mdss(client, fs_name=fs_name)
        log.info("Rolling failures of MDS's")
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        log.info(out)
        output = json.loads(out)
        standby_mds = [
            mds["name"] for mds in output["mdsmap"] if mds["state"] == "standby"
        ]
        mds_to_fail = random.sample(mds_ls, len(standby_mds))
        for mds in mds_to_fail:
            out, rc = client.exec_command(cmd=f"ceph mds fail {mds}", client_exec=True)
            log.info(out)
            time.sleep(1)

        log.info(
            f"Waiting for atleast 2 active MDS after failing {len(mds_to_fail)} MDS"
        )
        if not wait_for_two_active_mds(client, fs_name):
            raise CommandError("Wait for 2 active MDS failed")
        time.sleep(10)
        out, rc = client.exec_command(cmd=f"ceph fs status {fs_name}", client_exec=True)
        log.info(f"Status of {fs_name}:\n {out}")
        out, rc = client.exec_command(cmd="ceph -s -f json", client_exec=True)
        ceph_status = json.loads(out)
        log.info(f"Ceph status: {json.dumps(ceph_status, indent=4)}")
        if ceph_status["health"]["status"] == "HEALTH_ERR":
            log.error(f"Ceph Health is NOT OK after mds{mds} failover")
            return 1
        repeat_cnt -= 1
    return 0


def wait_for_two_active_mds(client1, fs_name, max_wait_time=180, retry_interval=10):
    """
    Wait until two active MDS (Metadata Servers) are found or the maximum wait time is reached.

    Args:
        data (str): JSON data containing MDS information.
        max_wait_time (int): Maximum wait time in seconds (default: 180 seconds).
        retry_interval (int): Interval between retry attempts in seconds (default: 5 seconds).

    Returns:
        bool: True if two active MDS are found within the specified time, False if not.

    Example usage:
    ```
    data = '...'  # JSON data
    if wait_for_two_active_mds(data):
        print("Two active MDS found.")
    else:
        print("Timeout: Two active MDS not found within the specified time.")
    ```
    """

    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        out, rc = client1.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        active_mds = [
            mds
            for mds in parsed_data.get("mdsmap", [])
            if mds.get("rank", -1) in [0, 1] and mds.get("state") == "active"
        ]
        if len(active_mds) == 2:
            return True  # Two active MDS found
        else:
            time.sleep(retry_interval)  # Retry after the specified interval

    return False


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
