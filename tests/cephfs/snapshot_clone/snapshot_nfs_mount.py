import datetime
import random
import secrets
import string
import time
import traceback
from threading import Thread

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Workflows Covered : Validate Snapshot mount through NFS suceeds and snapshot data is accessible.

    Type - Functional
    Workflow1 - snap_nfs_mount_func: Verify snapshot dir mount through nfs suceeds and read of snapshot data
    Steps:
    1. On subvolume run IO, wait for some tome and creat snapshot snap1
    2. Create NFS export with snapshot snap1 path and mount the export through NFS
    3. Perform read of snapshot and verify read across dirs and files suceed
    4. Create standy subvolume and mount using kernel, fuse or nfs.
    5. Perform data copy from snapshot snap1 mount to standby volume mount path, verify it suceeds.
       Verify data copy to snapshot mount fails with appropriate error.
    6. Create another snapshot on test volume while IO in-progress say snap2 and repeat step2-3
    7. Create snapshot schedule and try performing nfs mount of scheduled snapshot path, validate read from mount path.

    Clean Up:
    1. unmount all
    2. Deactivate and remove Snap_Schedule
    3. Del Subvolumes
    3. Del SubvolumeGroups
    """
    try:
        fs_util_v1 = FsUtilsv1(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")

        build = config.get("build", config.get("rhbuild"))
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)

        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs"
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        client1 = clients[0]
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        snap_util.enable_snap_schedule(client1)
        snap_util.allow_minutely_schedule(client1, allow=True)
        fs_details = fs_util_v1.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util_v1.create_fs(client1, default_fs)

        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": "svg_snap_nfs",
        }
        fs_util_v1.create_subvolumegroup(client1, **subvolumegroup)

        dir_suffix = "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(3)]
        )
        subvolume1 = {
            "vol_name": default_fs,
            "subvol_name": f"sv_{dir_suffix}",
            "group_name": "svg_snap_nfs",
            "size": "6442450944",
        }
        subvolume2 = {
            "vol_name": default_fs,
            "subvol_name": f"sv_def_{dir_suffix}",
            "size": "6442450944",
        }
        subvolume3 = {
            "vol_name": default_fs,
            "subvol_name": f"sv_test_{dir_suffix}",
            "size": "6442450944",
        }
        for sv in [subvolume1, subvolume2, subvolume3]:
            fs_util_v1.create_subvolume(client1, **sv)
        sv_list = [subvolume1, subvolume2]
        test_str = "snap_nfs_mount_func: Verify snapshot dir mount through nfs suceeds and read of snapshot data"
        log.info(
            f"\n\n                                   ============ {test_str} ============ \n"
        )
        mnt_paths = {}
        dd_params = {
            "file_name": "dd_test_file",
            "input_type": "random",
            "bs": "1M",
            "count": 10,
        }
        smallfile_params = {
            "testdir_prefix": "smallfile_io_dir",
            "threads": 2,
            "file-size": 10240,
            "files": 10,
        }
        crefi_params = {
            "testdir_prefix": "crefi_io_dir",
            "files": 10,
            "max": "100k",
            "min": "10k",
            "type": "tar",
            "breadth": 3,
            "depth": 3,
            "threads": 2,
        }
        io_run_time_mins = 1

        io_args = {
            "run_time": io_run_time_mins,
            "dd_params": dd_params,
            "smallfile_params": smallfile_params,
            "crefi_params": crefi_params,
        }
        mnt_list = ["kernel", "fuse", "nfs"]
        io_tools = ["dd", "smallfile", "crefi"]
        mnt_paths = {}
        write_procs = []
        for sv in sv_list:
            cmd = f"ceph fs subvolume getpath {default_fs} {sv['subvol_name']} "
            if sv.get("group_name"):
                cmd += f"{sv['group_name']}"
            subvol_path, rc = client1.exec_command(
                sudo=True,
                cmd=cmd,
            )
            sv_path = subvol_path.strip()
            sv.update({"sv_path": sv_path})

            mnt_type = random.choice(mnt_list)
            mnt_params = {
                "client": client1,
                "mnt_path": sv_path,
                "fs_util": fs_util_v1,
                "fs_name": default_fs,
                "export_created": 0,
            }
            if mnt_type == "nfs":
                nfs_export_name = f"/{sv['subvol_name']}_export_" + "".join(
                    secrets.choice(string.digits) for i in range(3)
                )
                mnt_params.update(
                    {
                        "nfs_name": nfs_name,
                        "nfs_server": nfs_server,
                        "nfs_export_name": nfs_export_name,
                    }
                )
            log.info(f"Perform {mnt_type} mount of {sv['subvol_name']}")
            mnt_path, _ = fs_util_v1.mount_ceph(mnt_type, mnt_params)
            sv.update({"mnt_path": mnt_path})
            mnt_paths.update({sv["subvol_name"]: {mnt_type: mnt_path}})
            log.info(f"Start IO on {sv['subvol_name']}")
            p = Thread(
                target=fs_util_v1.run_ios_V1,
                args=(client1, mnt_path, io_tools),
                kwargs=io_args,
            )
            p.start()
            write_procs.append(p)

        log.info("Wait for some IO progress")
        time.sleep(10)

        for sv in sv_list:
            sv.update({"mnt_info": {}})
            snap_name = f"{sv['subvol_name']}_snap_1"
            nfs_export_name = f"/{sv['subvol_name']}_export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            nfs_args = {
                "nfs_server": nfs_server,
                "nfs_name": nfs_name,
                "nfs_export": nfs_export_name,
            }
            log.info(f"Create Snapshot {snap_name} on {sv['subvol_name']}")
            snapshot = {
                "vol_name": default_fs,
                "subvol_name": sv["subvol_name"],
                "snap_name": snap_name,
            }
            if sv.get("group_name"):
                snapshot.update(
                    {
                        "group_name": sv["group_name"],
                    }
                )
            fs_util_v1.create_snapshot(client1, **snapshot)

            sv.update({"snap_list": [snap_name]})
            nfs_path = snap_nfs_mount_lifecycle(
                client1, default_fs, snap_name, fs_util_v1, sv, nfs_args
            )
            if nfs_path == "1":
                return 1
            sv["mnt_info"].update({nfs_path: nfs_export_name})

        cmd = f"ceph fs subvolume getpath {default_fs} {subvolume3['subvol_name']} "

        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=cmd,
        )
        sv_path = subvol_path.strip()
        subvolume3.update({"sv_path": sv_path})
        mnt_type = random.choice(mnt_list)
        mnt_params = {
            "client": client1,
            "mnt_path": sv_path,
            "fs_util": fs_util_v1,
            "fs_name": default_fs,
            "export_created": 0,
        }
        if mnt_type == "nfs":
            nfs_export_name = f"/{subvolume3['subvol_name']}_export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            mnt_params.update(
                {
                    "nfs_name": nfs_name,
                    "nfs_server": nfs_server,
                    "nfs_export_name": nfs_export_name,
                }
            )
        log.info(f"Perform {mnt_type} mount of {subvolume3['subvol_name']}")
        mnt_path, _ = fs_util_v1.mount_ceph(mnt_type, mnt_params)
        subvolume3.update({"mnt_path": mnt_path})
        nfs_paths = []
        for nfs_path in sv["mnt_info"].keys():
            nfs_paths.append(nfs_path)
        for sv in sv_list:
            snap_name = sv["snap_list"][0]
            src_path = nfs_paths[0]
            dst_path = subvolume3["mnt_path"]
            log.info(
                f"Verify copy of data from snapshot {snap_name} nfs mount path to {dst_path}"
            )
            cmd = f"cp -r {src_path}/*/* {dst_path}/"
            out, rc = client1.exec_command(
                sudo=True,
                cmd=cmd,
            )
            log.info(f"Copy suceeded from snapshot {snap_name} nfs mount path")
            log.info(
                f"Verify data copy to snapshot {snap_name} nfs mount path doesn't suceed"
            )
            dst_path = nfs_paths[0]
            src_path = subvolume3["mnt_path"]
            cmd = f'touch {src_path}/testfile;echo "snapshot nfs mount testing" > {src_path}/testfile'
            out, rc = client1.exec_command(
                sudo=True,
                cmd=cmd,
            )
            cmd = f"cp {src_path}/testfile {dst_path}/"
            try:
                out, rc = client1.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                log.error("Write to snapshot nfs mount path suceeded, its NOT Expected")
                return 1
            except Exception as ex:
                log.info(ex)
                log.info("Write to snapshot nfs mount path fails as expected")

        for sv in sv_list:
            snap_name = f"{sv['subvol_name']}_snap_2"
            nfs_export_name = f"/{sv['subvol_name']}_export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            nfs_args = {
                "nfs_server": nfs_server,
                "nfs_name": nfs_name,
                "nfs_export": nfs_export_name,
            }
            log.info(f"Create Snapshot {snap_name} on {sv['subvol_name']}")
            snapshot = {
                "vol_name": default_fs,
                "subvol_name": sv["subvol_name"],
                "snap_name": snap_name,
            }
            if sv.get("group_name"):
                snapshot.update(
                    {
                        "group_name": sv["group_name"],
                    }
                )
            fs_util_v1.create_snapshot(client1, **snapshot)
            snap_list = sv["snap_list"]
            snap_list.append(snap_name)

            sv.update({"snap_list": snap_list})
            nfs_path = snap_nfs_mount_lifecycle(
                client1, default_fs, snap_name, fs_util_v1, sv, nfs_args
            )
            if nfs_path == "1":
                return 1
            sv["mnt_info"].update({nfs_path: nfs_export_name})

        sched_path = f"{sv_list[0]['sv_path']}/.."
        snap_nfs_params = {
            "sched": "1m",
            "path": sched_path,
            "validate": True,
            "client": client1,
            "fs_name": default_fs,
        }

        if snap_util.create_snap_schedule(snap_nfs_params) == 1:
            log.info("Snapshot schedule creation/verification failed")
            return 1
        for i in sv_list[0]:
            if i in mnt_list:
                mnt_path = sv_list[0][i]
                time.sleep(60)
                snap_list = []
                retry_cnt = 0
                while retry_cnt < 5:
                    try:
                        snap_list = snap_util.get_scheduled_snapshots(client1, mnt_path)
                        retry_cnt = 5
                    except Exception as ex:
                        log.info(ex)
                        retry_cnt += 1
                if len(snap_list) == 0:
                    log.error("No scheduled snapshot was created, exiting test")
                    return 1
                nfs_export_name = f"/{sv_list[0]['subvol_name']}_export_" + "".join(
                    secrets.choice(string.digits) for i in range(3)
                )
                nfs_args = {
                    "nfs_server": nfs_server,
                    "nfs_name": nfs_name,
                    "nfs_export": nfs_export_name,
                }
                nfs_path = snap_nfs_mount_lifecycle(
                    client1, default_fs, snap_list[0], fs_util_v1, sv_list[0], nfs_args
                )
                if nfs_path == "1":
                    return 1
                sv["mnt_info"].update({nfs_path: nfs_export_name})
        snap_util.remove_snap_schedule(client1, sched_path, fs_name=default_fs)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        snap_util.allow_minutely_schedule(client1, allow=False)

        log.info("Wait for IO to complete")
        for p in write_procs:
            if p.is_alive():
                proc_stop = 0
                io_run_time_secs = io_run_time_mins * 4 * 60
                log.info("IO is running after snap nfs mount test")
                end_time = datetime.datetime.now() + datetime.timedelta(
                    seconds=io_run_time_secs
                )
                while (datetime.datetime.now() < end_time) and (proc_stop == 0):
                    if p.is_alive():
                        time.sleep(10)
                    else:
                        proc_stop = 1
                if proc_stop == 1:
                    log.info("IO completed")
                elif proc_stop == 0:
                    log.error("IO has NOT completed")

        log.info("Unmounting...")

        for sv in sv_list:
            for mnt_path in sv["mnt_info"]:
                client1.exec_command(
                    sudo=True,
                    cmd=f"umount -l {mnt_path}",
                    check_ec=False,
                )
            for nfs_path in sv["mnt_info"]:
                log.info("NFS export delete")
                nfs_export_name = sv["mnt_info"][nfs_path]
                client1.exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
                    check_ec=False,
                )

        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {nfs_name}",
            check_ec=False,
        )
        for sv in [subvolume1, subvolume2, subvolume3]:
            subvol = sv["subvol_name"]
            for i in sv:
                if i in mnt_list:
                    mnt_path = sv[i]

                    if sv.get("group_name"):
                        snap_util.sched_snap_cleanup(
                            client1, mnt_path, subvol, sv["group_name"]
                        )
                    else:
                        snap_util.sched_snap_cleanup(client1, mnt_path, subvol)
                    client1.exec_command(
                        sudo=True,
                        cmd=f"umount -l {mnt_path}",
                        check_ec=False,
                    )
            if sv.get("snap_list"):
                for snap_name in sv["snap_list"]:
                    snapshot = {
                        "vol_name": default_fs,
                        "subvol_name": sv["subvol_name"],
                        "snap_name": snap_name,
                    }
                    if sv.get("group_name"):
                        snapshot.update(
                            {
                                "group_name": sv["group_name"],
                            }
                        )
                    fs_util_v1.remove_snapshot(client1, **snapshot)
            cmd = f"ceph fs subvolume rm {default_fs} {subvol} "
            if sv.get("group_name"):
                cmd += f"{sv['group_name']}"
            client1.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {default_fs} {subvolumegroup['group_name']}",
            check_ec=False,
        )


def snap_nfs_mount_lifecycle(client, fs_name, snap_name, fs_util, sv, nfs_args):
    """
    This method perform nfs mount of snapshot path, validates read access and returns,
    nfs_path is sucess, 1 if failure
    """
    nfs_name = nfs_args["nfs_name"]
    nfs_server = nfs_args["nfs_server"]
    nfs_export_name = nfs_args["nfs_export"]
    nfs_export_name = f"/{sv['subvol_name']}_export_{snap_name}"
    snap_path = "/volumes/"
    if sv.get("group_name"):
        snap_path = f"/volumes/{sv['group_name']}/{sv['subvol_name']}/.snap/{snap_name}"
    else:
        snap_path = f"/volumes/_nogroup/{sv['subvol_name']}/.snap/{snap_name}"
    mnt_params = {
        "client": client,
        "mnt_path": snap_path,
        "fs_util": fs_util,
        "fs_name": fs_name,
        "nfs_name": nfs_name,
        "nfs_server": nfs_server,
        "nfs_export_name": nfs_export_name,
        "export_created": 0,
    }
    log.info(f"Perform nfs mount of {sv['subvol_name']} snapshot {snap_name}")
    mnt_path, _ = fs_util.mount_ceph("nfs", mnt_params)
    log.info(
        f"Verify recursive read of snapshot mount path {mnt_path} of {sv['subvol_name']}"
    )
    cmd = f"ls -R {mnt_path} | wc -l"
    read_out, rc = client.exec_command(
        sudo=True,
        cmd=cmd,
    )
    log.info(f"read_out:{read_out}")
    if len(read_out) > 0:
        log.info(
            f"Read suceeds on snapshot mount path {mnt_path} of {sv['subvol_name']}"
        )
    else:
        log.error(
            f"Read on snapshot mount path {mnt_path} of {sv['subvol_name']} didn't suceed"
        )
        return 1
    return mnt_path
