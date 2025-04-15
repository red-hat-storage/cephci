import datetime
import json
import random
import re
import string
import time
import traceback
from multiprocessing import Value
from threading import Thread

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.snapshot_clone.cephfs_cg_io import CG_snap_IO
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from tests.cephfs.snapshot_clone.cg_snap_utils import CG_Snap_Utils
from utility.log import Log

log = Log(__name__)


def nfs_test(nfs_req_params):
    """
    NFS post upgrade validation

    Args:
        nfs_req_params as dict_type: mandatory below params,
        nfs_req_params = {
            "existing_nfs_mount" : "/mnt/nfs",
            "nfs_client" : nfs_client[0],
            "nfs_server" : nfs_server,
            "nfs_name" : default_nfs_name,
            "fs_name" : default_fs,
            "fs_util" : fs_util
        }
        param data types:
        existing_nfs_mount,nfs_client,nfs_server,nfs_name,fs_name - str
        fsutil - fsutil testlib object
    Returns:
        None
    Raises:
        AssertionError
    """
    config = nfs_req_params["config"]
    clients = nfs_req_params["clients"]
    nfs_config = config["NFS"]
    for i in nfs_config:
        nfs_name = i
        break
    nfs_config = config["NFS"][nfs_name]
    for i in nfs_config:
        nfs_export_name = i
        break
    nfs_client_name = nfs_config[nfs_export_name]["nfs_mnt_client"]
    nfs_client = [i for i in clients if i.node.hostname == nfs_client_name][0]
    existing_nfs_mount = nfs_config[nfs_export_name]["nfs_mnt_pt"]

    nfs_server_name = nfs_config[nfs_export_name]["nfs_server"]

    nfs_server_name_1 = nfs_req_params["nfs_servers"][1].node.hostname
    fs_name = nfs_req_params["fs_name"]
    fs_util = nfs_req_params["fs_util"]

    log.info("Get pre-upgrade nfs server and exports")
    out, rc = nfs_client.exec_command(sudo=True, cmd="ceph nfs cluster ls")
    if nfs_name not in out:
        assert False, f"nfs cluster {nfs_name} doesn't exist after upgrade"
    out, rc = nfs_client.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")
    output = json.loads(out)
    if len(output) == 0:
        assert False, f"nfs cluster {nfs_name} exports doesn't exist after upgrade"

    log.info("Verify existing mounts are accessible")
    out, rc = nfs_client.exec_command(sudo=True, cmd=f"ls {existing_nfs_mount}")
    dir_path = f"{existing_nfs_mount}/smallfile_nfs_dir"
    nfs_client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}", timeout=15)
    nfs_client.exec_command(
        sudo=True,
        cmd="python3 /home/cephuser/smallfile/smallfile_cli.py "
        f"--operation create --threads 1 --file-size 1024 "
        f"--files 10 --top {dir_path}",
        long_running=True,
        timeout=20,
    )
    log.info("Verify exports are active by mounting and running IO")
    nfs_mounting_dir = f"{existing_nfs_mount}_new"
    fs_util.nfs_mount_and_io(
        nfs_client, nfs_server_name, nfs_export_name, nfs_mounting_dir
    )

    log.info("Create new exports on existing nfs server")
    new_export_name = "/export2"
    path = "/"
    nfs_client.exec_command(
        sudo=True,
        cmd=f"ceph nfs export create cephfs {nfs_name} "
        f"{new_export_name} {fs_name} path={path}",
    )

    log.info("Verify ceph nfs new export is created")
    out, rc = nfs_client.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")
    if new_export_name in out:
        log.info("ceph nfs new export created successfully")
    else:
        raise CommandFailed("Failed to create nfs export")

    time.sleep(2)
    log.info("Run IO on new exports in existing nfs server")
    nfs_mounting_dir = "/mnt/nfs_new1"
    fs_util.nfs_mount_and_io(
        nfs_client, nfs_server_name, new_export_name, nfs_mounting_dir
    )

    log.info("Create new nfs cluster, new exports and run IO")

    new_nfs_name = f"{nfs_name}_new"
    out, rc = nfs_client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster create {new_nfs_name} {nfs_server_name_1}"
    )
    log.info("Verify ceph nfs cluster is created")
    if wait_for_process(client=nfs_client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")

    log.info("Create cephfs nfs export")
    new_export_name = "/export3"
    nfs_client.exec_command(
        sudo=True,
        cmd=f"ceph nfs export create cephfs {new_nfs_name} "
        f"{new_export_name} {fs_name} path={path}",
    )
    log.info("Verify ceph nfs export is created")
    out, rc = nfs_client.exec_command(
        sudo=True, cmd=f"ceph nfs export ls {new_nfs_name}"
    )
    if new_export_name in out:
        log.info("ceph nfs export created successfully")
    else:
        raise CommandFailed("Failed to create nfs export")

    time.sleep(2)
    log.info("Mount ceph nfs new exports and run IO")
    nfs_mounting_dir = "/mnt/nfs_new2"
    fs_util.nfs_mount_and_io(
        nfs_client, nfs_server_name_1, new_export_name, nfs_mounting_dir
    )


def snap_sched_test(snap_req_params):
    """
    Test Steps:
    a) Verify existing snapshots are accessible, perform read IO.
    b) Verify snapshot schedules are active.Validate snapshot schedules.
    c) Run IO, Create new manual snapshots on subvolumes and volumes.
    d) Delete old snapshot.

    Args:
        snap_req_params data type is dict with below params,
        snap_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - list type, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'

    Returns:
        None
    Raises:
        BaseException
    """
    config = snap_req_params["config"]
    vol_name = snap_req_params.get("vol_name", "cephfs")
    fs_util = snap_req_params["fs_util"]
    snap_util = snap_req_params["snap_util"]
    sv_snap = {}
    sv_sched = {}

    # Get pre-upgrade snap values
    log.info("Get snapshot configuration from pre-upgrade config")
    for svg in config["CephFS"][vol_name]:
        if "svg" in svg:
            for sv in config["CephFS"][vol_name][svg]:
                sv_data = config["CephFS"][vol_name][svg][sv]
                if sv_data.get("snap_list"):
                    sv_snap.update(
                        {
                            sv: {
                                "snap_list": sv_data["snap_list"],
                                "svg": svg,
                                "mnt_pt": sv_data["mnt_pt"],
                                "mnt_client": sv_data["mnt_client"],
                            }
                        }
                    )
                if sv_data.get("sched_path"):
                    sv_sched.update(
                        {
                            sv: {
                                "sched_path": sv_data["sched_path"],
                                "sched_list": sv_data["sched_list"],
                                "retention": sv_data["retention"],
                                "svg": svg,
                            }
                        }
                    )

    clients = snap_req_params["clients"]
    client = clients[0]
    # Mount sv
    mon_node_ips = fs_util.get_mon_node_ips()
    sv_snap_tmp = sv_snap.copy()
    sv_snap_tmp.update(sv_sched)
    log.info("Fuse-Mount cephfs volume")
    fuse_mounting_dir = f"/mnt/{vol_name}_post_upgrade_fuse"
    cephfs_client = random.choice(clients)
    fs_util.fuse_mount(
        [cephfs_client],
        fuse_mounting_dir,
        extra_params=f"-r / --client_fs {vol_name}",
    )

    log.info(
        "Using existing mountpoint,verify existing snapshots are accessible, perform readwrite IO."
    )
    for sv in sv_snap:
        mnt_pt = sv_snap[sv]["mnt_pt"]
        mnt_client_name = sv_snap[sv]["mnt_client"]
        mnt_client = [i for i in clients if i.node.hostname == mnt_client_name][0]
        cmd = f"ls {mnt_pt}/.snap/*{sv_snap[sv]['snap_list'][0]}*/*"
        out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)
        file_path = out.strip()
        cmd = f"dd if={file_path} count=10 bs=1M > read_dd"
        out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)
        dir_path = f"{mnt_pt}/smallfile_snap_dir"
        mnt_client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}", timeout=15)
        mnt_client.exec_command(
            sudo=True,
            cmd="python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 1024 "
            f"--files 10 --top {dir_path}",
            long_running=True,
            timeout=20,
        )

    log.info(
        "Perform Kernel and Fuse mount of subvolumes having pre-upgrade snapshots and schedules"
    )
    for sv in sv_snap_tmp:
        snap_client = random.choice(clients)
        fuse_mounting_dir_1 = f"/mnt/{sv_snap_tmp[sv]['svg']}_{sv}_post_upgrade_fuse"
        kernel_mounting_dir_1 = (
            f"/mnt/{sv_snap_tmp[sv]['svg']}_{sv}_post_upgrade_kernel"
        )
        subvol_path, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {vol_name} {sv} {sv_snap_tmp[sv]['svg']}",
        )
        subvol_path = subvol_path.strip()
        fs_util.fuse_mount(
            [snap_client],
            fuse_mounting_dir_1,
            extra_params=f"-r {subvol_path} --client_fs {vol_name}",
        )
        fs_util.kernel_mount(
            [snap_client],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path}",
            extra_params=f",fs={vol_name}",
        )
        if sv in sv_snap:
            sv_snap[sv]["mnt_kernel"] = kernel_mounting_dir_1
            sv_snap[sv]["mnt_fuse"] = fuse_mounting_dir_1
            sv_snap[sv]["mnt_client_new"] = snap_client
        else:
            sv_sched[sv]["mnt_kernel"] = kernel_mounting_dir_1
            sv_sched[sv]["mnt_fuse"] = fuse_mounting_dir_1
            sv_sched[sv]["mnt_client_new"] = snap_client

    log.info(
        "Using new mountpoint, verify existing snapshots are accessible, perform read IO."
    )
    for sv in sv_snap:
        snap_client = sv_snap[sv]["mnt_client_new"]
        for mnt_pt in [sv_snap[sv]["mnt_kernel"], sv_snap[sv]["mnt_fuse"]]:
            cmd = f"ls {mnt_pt}/.snap/*{sv_snap[sv]['snap_list'][0]}*/*"
            out, rc = snap_client.exec_command(sudo=True, cmd=cmd)
            file_path = out.strip()
            cmd = f"dd if={file_path} count=10 bs=1M > read_dd"
            out, rc = snap_client.exec_command(sudo=True, cmd=cmd)

    log.info("Verified that existing snapshots are accessible and read op suceeds")

    log.info("Verify snapshot schedules are active.Validate snapshot schedules.")
    for sv in sv_sched:
        sv_data = sv_sched[sv]
        cmd = f"ceph fs snap-schedule status {sv_data['sched_path']} --fs {vol_name} -f json"
        out, rc = client.exec_command(sudo=True, cmd=cmd)
        sched_status = json.loads(out)

        for sched_val in sv_data["sched_list"]:
            for sched_item in sched_status:
                if (
                    sched_item["path"] == sv_data["sched_path"]
                    and sched_item["active"] is True
                    and sched_item["schedule"] == sched_val
                ):
                    log.info(
                        f"Snap schedule {sched_val} is verified for path {sv_data['sched_path']}."
                    )
            if "M" in sched_val:
                snap_path = f"{fuse_mounting_dir}{sv_data['sched_path']}"
                snap_util.validate_snap_schedule(cephfs_client, snap_path, sched_val)
                snap_util.validate_snap_retention(
                    cephfs_client, snap_path, sv_data["sched_path"]
                )

    log.info(
        "Verified that snapshot schedule for existing subvolumes works as expected"
    )

    log.info("Run IO, Create new manual snapshots on subvolumes")
    for sv in sv_snap:
        sv_data = sv_snap[sv]
        mnt_pt = random.choice([sv_data["mnt_kernel"], sv_data["mnt_fuse"]])
        mnt_client = sv_data["mnt_client_new"]
        fs_util.run_ios_V1(mnt_client, mnt_pt)
        snapshot = {
            "vol_name": vol_name,
            "subvol_name": sv,
            "snap_name": f"snap_{sv}_post_upgrade",
            "group_name": sv_data["svg"],
        }
        fs_util.create_snapshot(client, **snapshot)

    log.info("Delete existing snapshot")
    for sv in sv_snap:
        fs_util.remove_snapshot(
            client,
            vol_name,
            sv,
            sv_snap[sv]["snap_list"][0],
            group_name=sv_snap[sv]["svg"],
        )

    log.info("Cleanup")
    sv_snap.update(sv_sched)
    for sv in sv_snap:
        mnt_client = sv_snap[sv]["mnt_client_new"]
        for mnt_pt in [sv_snap[sv]["mnt_kernel"], sv_snap[sv]["mnt_fuse"]]:
            cmd = f"umount -l {mnt_pt};rm -rf {mnt_pt}"
            out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)
    cmd = f"umount -l {fuse_mounting_dir};rm -rf {fuse_mounting_dir}"
    out, rc = cephfs_client.exec_command(sudo=True, cmd=cmd)
    return 0


def clone_test(clone_req_params):
    """
    Test Steps:
    a) Mount Clone volumes, read existing files, overwrite an existing file.Create new file.
    b) Create snapshot of clone volume
    c) Create Clone of existing clone volume.
    d) Create clone using existing snapshot
    e) Delete existing clone volume.

    Args:
        clone_req_params as dict_type with below params,
        clone_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - list type, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'
    Returns:
        None
    Raises:
        BaseException
    """
    config = clone_req_params["config"]
    vol_name = clone_req_params.get("vol_name", "cephfs")
    fs_util = clone_req_params["fs_util"]
    clone_data = {}
    new_clone_data = {}
    # Get pre-upgrade snap values
    log.info("Get Clone data from pre-upgrade config")
    for svg in config["CephFS"][vol_name]:
        for sv in config["CephFS"][vol_name][svg]:
            if "Clone" in sv:
                sv1 = config["CephFS"][vol_name][svg][sv]
                clone_data.update(
                    {
                        sv: {
                            "svg": svg,
                            "mnt_pt": sv1["mnt_pt"],
                            "mnt_client": sv1["mnt_client"],
                        }
                    }
                )
            if "svg" in svg:
                sv1 = config["CephFS"][vol_name][svg][sv]
                if sv1.get("snap_list"):
                    new_clone_data.update(
                        {
                            sv: {
                                "snap_name": sv1["snap_list"][1],
                                "group_name": svg,
                            }
                        }
                    )

    clients = clone_req_params["clients"]
    client = clients[0]

    log.info("Create new Clone using pre-upgrade snapshot")
    for sv in new_clone_data:
        clone_name = f"Clone_after_{sv}"
        clone_config = {
            "vol_name": vol_name,
            "subvol_name": sv,
            "snap_name": new_clone_data[sv]["snap_name"],
            "target_subvol_name": clone_name,
            "group_name": new_clone_data[sv]["group_name"],
            "target_group_name": new_clone_data[sv]["group_name"],
        }

        fs_util.create_clone(client, **clone_config)
        fs_util.validate_clone_state(client, clone_config)

    mon_node_ips = fs_util.get_mon_node_ips()

    log.info(
        "Using existing mountpoint,verify clones are accessible, perform readwrite IO."
    )
    for sv in clone_data:
        mnt_pt = clone_data[sv]["mnt_pt"]
        mnt_client_name = clone_data[sv]["mnt_client"]
        mnt_client = [i for i in clients if i.node.hostname == mnt_client_name][0]
        cmd = f"ls {mnt_pt}/*"
        out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)
        file_path = out.strip()
        cmd = f"dd if={file_path} count=10 bs=1M > read_dd"
        out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)
        dir_path = f"{mnt_pt}/smallfile_clone_dir"
        mnt_client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}", timeout=15)
        mnt_client.exec_command(
            sudo=True,
            cmd="python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 1024 "
            f"--files 10 --top {dir_path}",
            long_running=True,
            timeout=20,
        )

    log.info("Mount existing clones, verify IO suceeds")
    for clone in clone_data:
        fuse_mounting_dir_1 = f"/mnt/{clone}_post_upgrade_fuse"
        kernel_mounting_dir_1 = f"/mnt/{clone}_post_upgrade_kernel"
        mnt_client = random.choice(clients)
        subvol_path, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {vol_name} {clone} {clone_data[clone]['svg']}",
        )
        subvol_path = subvol_path.strip()
        fs_util.fuse_mount(
            [mnt_client],
            fuse_mounting_dir_1,
            extra_params=f"-r {subvol_path} --client_fs {vol_name}",
        )
        fs_util.kernel_mount(
            [mnt_client],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path}",
            extra_params=f",fs={vol_name}",
        )
        clone_data[clone]["mnt_kernel"] = kernel_mounting_dir_1
        clone_data[clone]["mnt_fuse"] = fuse_mounting_dir_1
        clone_data[clone]["mnt_client_new"] = mnt_client
        log.info(f"Perform existing file read on clone {clone}")
        for mnt_pt in [kernel_mounting_dir_1, fuse_mounting_dir_1]:
            cmd = f"dd if={mnt_pt}/dd_test_file count=10 bs=1M > read_dd"
            out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)

        log.info(f"Perform overwrite on existing clone {clone}")
        cmd = f"dd if=/dev/urandom of={kernel_mounting_dir_1}/dd_test_file count=10 bs=1M seek=20"
        out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)

        log.info("Create snapshot on existing clone")
        fs_util.run_ios_V1(mnt_client, fuse_mounting_dir_1)
        snapshot = {
            "vol_name": vol_name,
            "subvol_name": clone,
            "snap_name": f"snap_{clone}_post_upgrade",
            "group_name": clone_data[clone]["svg"],
        }
        fs_util.create_snapshot(client, **snapshot)

        log.info(f"Create Clone of Clone volume {clone}")
        clone_name = f"Clone_{clone}"
        clone_config = {
            "vol_name": vol_name,
            "subvol_name": clone,
            "snap_name": f"snap_{clone}_post_upgrade",
            "target_subvol_name": clone_name,
            "group_name": clone_data[clone]["svg"],
            "target_group_name": clone_data[clone]["svg"],
        }

        fs_util.create_clone(client, **clone_config)
        fs_util.validate_clone_state(client, clone_config)

    for clone in clone_data:
        mnt_client = clone_data[clone]["mnt_client_new"]
        for mnt_pt in [clone_data[clone]["mnt_kernel"], clone_data[clone]["mnt_fuse"]]:
            cmd = f"umount -l {mnt_pt};rm -rf {mnt_pt}"
            out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)

    for clone in clone_data:
        log.info("Delete existing snapshot")
        snap_name = f"snap_{clone}_post_upgrade"
        fs_util.remove_snapshot(
            client,
            vol_name,
            clone,
            snap_name,
            group_name=clone_data[clone]["svg"],
        )
        log.info("Verify existing clone volume delete")
        clone_vol = {
            "vol_name": vol_name,
            "subvol_name": clone,
            "group_name": clone_data[clone]["svg"],
        }
        fs_util.remove_subvolume(client, **clone_vol)
        break
    return 0


def dir_pin_test(dir_pin_req_params):
    """
    Test Steps:
    Get existing dirs pinned, capture ceph fs status IO stats before IO, run IO only on pinned dirs,
    capture IO stats from ceph fs status, verify increment only at MDS rank 0, the pinned MDS.

    Args:
       dir_pin_req_params as dict_type with below params,
        dir_pin_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - list type, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'
    Returns:
        None
    Raises:
        BaseException
    """
    config = dir_pin_req_params["config"]
    vol_name = dir_pin_req_params.get("vol_name", "cephfs")
    fs_util = dir_pin_req_params["fs_util"]
    client = dir_pin_req_params["clients"][0]
    pin_vol = {}
    test_status = 0
    log.info("Capture MDS dir pin data from pre-upgrade config")
    for svg in config["CephFS"][vol_name]:
        for sv in config["CephFS"][vol_name][svg]:
            if "svg" in svg:
                if config["CephFS"][vol_name][svg][sv].get("pinned_dir_list"):
                    pin_vol.update(
                        {
                            "sv": sv,
                            "svg": svg,
                            "pinned_dirs": config["CephFS"][vol_name][svg][sv][
                                "pinned_dir_list"
                            ],
                            "pin_rank": config["CephFS"][vol_name][svg][sv]["pin_rank"],
                        }
                    )

    mon_node_ips = fs_util.get_mon_node_ips()
    subvol_path, rc = client.exec_command(
        sudo=True,
        cmd=f"ceph fs subvolume getpath {vol_name} {pin_vol['sv']} {pin_vol['svg']}",
    )
    subvol_path = subvol_path.strip()
    fuse_mounting_dir_1 = f"/mnt/{pin_vol['sv']}_post_upgrade_fuse"
    kernel_mounting_dir_1 = f"/mnt/{pin_vol['sv']}_post_upgrade_kernel"
    mnt_client = random.choice(dir_pin_req_params["clients"])
    log.info(f"Mount subvolume having pinned dirs {pin_vol['sv']}")
    fs_util.fuse_mount(
        [mnt_client],
        fuse_mounting_dir_1,
        extra_params=f"-r {subvol_path} --client_fs {vol_name}",
    )
    fs_util.kernel_mount(
        [mnt_client],
        kernel_mounting_dir_1,
        ",".join(mon_node_ips),
        sub_dir=f"{subvol_path}",
        extra_params=f",fs={vol_name}",
    )

    pin_dir = random.choice(pin_vol["pinned_dirs"])
    log.info(f"Pinned dir selected for IO with kernel-mount - {pin_dir}")
    io_path_kernel = f"{kernel_mounting_dir_1}/{pin_dir}"
    pin_dir = random.choice(pin_vol["pinned_dirs"])
    log.info(f"Pinned dir selected for IO with fuse-mount - {pin_dir}")
    io_path_fuse = f"{fuse_mounting_dir_1}/{pin_dir}"
    for io_path in [io_path_kernel, io_path_fuse]:
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph fs status {vol_name} --format json",
        )
        output_before = json.loads(out)
        for mds in output_before["mdsmap"]:
            log.info(mds)
            if int(mds["rank"]) == int(pin_vol["pin_rank"]):
                mds_dirs_before = mds["dirs"]
                break
        log.info("Run IO on pinned dirs")
        fs_util.run_ios_V1(mnt_client, io_path)
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph fs status {vol_name} --format json",
        )
        output_after = json.loads(out)
        for mds in output_after["mdsmap"]:
            if int(mds["rank"]) == int(pin_vol["pin_rank"]):
                mds_dirs_after = mds["dirs"]
                break
        if int(mds_dirs_after) != int(mds_dirs_before):
            log.info(
                f"IO from pinned dirs goes through MDS rank {pin_vol['pin_rank']} as expected"
            )
            log.info(
                f"Before IO fs status : {output_before['mdsmap']},\nAfter IO fs status:{output_after['mdsmap']}"
            )
        else:
            log.info(
                f"Before IO fs status : {output_before['mdsmap']},\nAfter IO fs status:{output_after['mdsmap']}"
            )
            log.error(
                f"IO from pinned dirs doesn't go through MDS rank {pin_vol['pin_rank']}"
            )
            test_status = 1

    for mnt_pt in [fuse_mounting_dir_1, kernel_mounting_dir_1]:
        cmd = f"umount -l {mnt_pt};rm -rf {mnt_pt}"
        out, rc = mnt_client.exec_command(sudo=True, cmd=cmd)
    return test_status


def auth_test(auth_req_params):
    """
    Test Steps : Verify existing auth rule for a client user works on new mount.

    Args:
        auth_req_params as dict_type with below params,
        auth_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - list type, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'
    Returns:
        None
    Raises:
        BaseException
    """
    config = auth_req_params["config"]
    vol_name = auth_req_params.get("vol_name", "cephfs")
    fs_util = auth_req_params["fs_util"]
    client_auth = config["CephFS"][vol_name]["client_auth"]
    client_name = client_auth["client_name"]
    auth_client = auth_req_params["clients"][0]
    mon_node_ips = fs_util.get_mon_node_ips()
    test_status = 0

    log.info("Get the pre-upgrade auth config")
    for key in client_auth:
        if "/volumes" in key:
            subvol_path = key
            subvol_path_perm = client_auth[key]
    vol_path_perm = client_auth["/"]
    cmd = f"ceph auth get client.{client_name} > /etc/ceph/ceph.client.{client_name}.keyring"
    out, rc = auth_client.exec_command(sudo=True, cmd=cmd)

    log.info(f"Perform Kernel and Fuse mount for Client user {client_name}")
    fuse_mounting_dir_1 = f"/mnt/{vol_name}_post_upgrade_fuse"
    kernel_mounting_dir_1 = f"/mnt/{vol_name}_post_upgrade_kernel"
    fs_util.fuse_mount(
        [auth_client],
        fuse_mounting_dir_1,
        new_client_hostname=client_name,
        extra_params=f"-r / --client_fs {vol_name}",
    )
    fs_util.kernel_mount(
        [auth_client],
        kernel_mounting_dir_1,
        ",".join(mon_node_ips),
        sub_dir="/",
        new_client_hostname=client_name,
        extra_params=f",fs={vol_name}",
    )

    log.info(f"Verify auth rules defined for {client_name} prior to upgrade works")
    mnt_pt_list = [fuse_mounting_dir_1, kernel_mounting_dir_1]
    for mnt_pt in mnt_pt_list:
        cmd = f"cd {mnt_pt};ls -l *;touch auth_test_file"
        if vol_path_perm == "r":
            read_cmd = f"cd {mnt_pt};ls -l *;"
            write_cmd = f"cd {mnt_pt};touch auth_test_file"
            for cmd in [read_cmd, write_cmd]:
                try:
                    out, rc = auth_client.exec_command(sudo=True, cmd=cmd)
                    if "touch" in cmd:
                        log.error(
                            f"Client user {client_name} has read-only permission for /, but write works"
                        )
                        test_status = 1
                except Exception as e:
                    log.info(e)
                    if "ls" in cmd:
                        log.error(
                            f"Client user {client_name} has read-only permission for /, but read fails"
                        )
                        test_status = 1
                    else:
                        log.info(
                            f"Verified that Client user {client_name} has read-only permission for /"
                        )
        if subvol_path_perm == "rw":
            cmd = f"cd {mnt_pt}{subvol_path};ls -l *;touch auth_test_file"
            try:
                out, rc = auth_client.exec_command(sudo=True, cmd=cmd)
            except Exception as e:
                log.info(e)
                log.error(
                    f"User {client_name} has {subvol_path_perm} permission for {subvol_path}, but write fails"
                )
                test_status = 1

    for mnt_pt in mnt_pt_list:
        cmd = f"umount -l {mnt_pt};rm -rf {mnt_pt}"
        out, rc = auth_client.exec_command(sudo=True, cmd=cmd)
    return test_status


def mds_config_test(mds_req_params):
    """
    Test Steps:Verify MDS configuration remains unchanged after upgrade.
               Active and standby count remains same.

    Args:
        auth_req_params as dict_type with below params,
        auth_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - a list type data, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'
    Returns:
        None
    Raises:
        BaseException
    """
    config = mds_req_params["config"]
    vol_name = mds_req_params.get("vol_name", "cephfs")
    fs_util = mds_req_params["fs_util"]
    active_mds_before = config["CephFS"][vol_name]["active_mds"]
    standby_mds_before = config["CephFS"][vol_name]["standby_mds"]
    mds_client = mds_req_params["clients"][0]

    log.info("Get active MDS config after upgrade")

    active_mds_after = fs_util.get_active_mdss(mds_client, vol_name)
    out, rc = mds_client.exec_command(
        sudo=True, cmd=f"ceph fs status {vol_name} --format json"
    )
    output = json.loads(out)
    standby_mds_after = [
        mds["name"] for mds in output["mdsmap"] if mds["state"] == "standby"
    ]
    log.info(f"Active MDS : Before - {active_mds_before},\nAfter - {active_mds_after}")
    log.info(
        f"Standby MDS : Before - {standby_mds_before},\nAfter - {standby_mds_after}"
    )
    if len(active_mds_before) != len(active_mds_after):
        log.info(
            f"Active MDS : Before - {len(active_mds_before)},\nAfter - {len(active_mds_after)}"
        )
        log.error("Active MDS count before and after upgrade doesn't match")
        return 1
    if len(standby_mds_before) != len(standby_mds_after):
        log.info(
            f"Standby MDS : Before - {len(standby_mds_before)},\nAfter - {len(standby_mds_after)}"
        )
        log.error("Standby MDS count before and after upgrade doesn't match")
        return 1
    return 0


def cg_quiesce_test(cg_quiesce_params):
    """
    Test Steps:
    Load existing config for subvolumes and snapshots info.
    Create new subvolumes in 7.1.Run QS IO Validation tool.
    Run QS quiesce on pre-upgrade volumes and new 7.1 subvolumes. Verify snapshot create, quiesce-release works.
    Copy the contents from pre-upgrade snapshots to AFS on existing subvolumes, verify it suceeds.

    Args:
        auth_req_params as dict_type with below params,
        auth_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - a list type data, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'
    Returns:
        None
    Raises:
        BaseException
    """
    config = cg_quiesce_params["config"]
    vol_name = cg_quiesce_params.get("vol_name", "cephfs")
    fs_util = cg_quiesce_params["fs_util"]
    cg_snap_util = cg_quiesce_params["cg_snap_util"]
    cg_snap_io = cg_quiesce_params["cg_snap_io"]
    cg_client = cg_quiesce_params["clients"][0]
    cg_io_client = cg_quiesce_params["clients"][1]
    test_fail = 0
    log.info("Get current ceph version after upgrade and verify if 18.2.1 or above")
    out, rc = cg_client.exec_command(sudo=True, cmd="ceph version")
    out_item = out.split()[2]
    version = out_item.split("-")[0]
    version1 = version.replace(".", "")
    if int(version1) < 1821:
        log.warn(
            f"CG quiesce test can be perfomed only on ceph version 18.2.1 or above. Current version is {out_item}"
        )
        return 0
    log.info(f"Current ceph version : {out_item}")
    qs_info = {}
    for svg in config["CephFS"][vol_name]:
        if "svg" in svg:
            for sv in config["CephFS"][vol_name][svg]:
                sv_data = config["CephFS"][vol_name][svg][sv]
                if sv_data.get("snap_list"):
                    qs_info[sv] = {}
                    qs_info[sv].update(
                        {
                            "snap_list": sv_data["snap_list"],
                            "svg": svg,
                            "mnt_pt": sv_data["mnt_pt"],
                            "mnt_client": sv_data["mnt_client"],
                        }
                    )
    log.info(f"Existing subvolumes are : {qs_info.keys()}")
    log.info(f"qs_info : {qs_info}")
    qs_set = []
    for sv in qs_info.keys():
        qs_set.append(f"{qs_info[sv]['svg']}/{sv}")
    log.info(f"Existing qs_members: {qs_set}")
    log.info("Create new qs_members")
    qs_cnt = 6
    new_sv_cnt = qs_cnt - int(len(qs_info.keys()))
    new_sv_cnt += 1
    for i in range(1, new_sv_cnt):
        sv_name = f"sv_def_{i}"
        qs_set.append(sv_name)
        subvolume = {
            "vol_name": vol_name,
            "subvol_name": sv_name,
            "size": "6442450944",
        }
        fs_util.create_subvolume(cg_client, **subvolume)
    client_mnt_dict = {}
    qs_member_dict1 = cg_snap_util.mount_qs_members(cg_client, qs_set, vol_name)
    client_mnt_dict.update({cg_client.node.hostname: qs_member_dict1})
    qs_member_dict2 = cg_snap_util.mount_qs_members(cg_io_client, qs_set, vol_name)
    client_mnt_dict.update({cg_io_client.node.hostname: qs_member_dict2})
    log.info(f"Start the IO on quiesce set members - {qs_set}")
    cg_test_io_status = Value("i", 0)
    io_run_time = 40
    ephemeral_pin = 1
    p = Thread(
        target=cg_snap_io.start_cg_io,
        args=(
            [cg_client, cg_io_client],
            qs_set,
            client_mnt_dict,
            cg_test_io_status,
            io_run_time,
            ephemeral_pin,
        ),
    )
    p.start()
    time.sleep(10)
    snap_qs_dict = {}
    qs_id_val = "Not Started"
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    if p.is_alive():
        # time taken for 1 lifecycle : ~5secs
        log.info("Run QS quiesce on pre-upgrade volumes and new 7.1 subvolumes.")
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        qs_id_val = f"cg_test1_{rand_str}"
        log.info(f"Quiesce the set {qs_set}")
        log.info(f"client:{cg_client.node.hostname}")
        cg_snap_util.cg_quiesce(
            cg_client, qs_set, qs_id=qs_id_val, timeout=300, expiration=300
        )
        time.sleep(10)
        log.info("Perform snapshot creation on all members")
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        snap_name = f"cg_snap_{rand_str}"
        for qs_member in qs_set:
            snap_list = snap_qs_dict[qs_member]
            snapshot = {
                "vol_name": vol_name,
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
            fs_util.create_snapshot(cg_client, **snapshot)
            log.info(f"Created snapshot {snap_name} on {subvol_name}")
            snap_list.append(snap_name)
            snap_qs_dict.update({subvol_name: snap_list})
        log.info(f"Release quiesce set {qs_id_val}")
        cg_snap_util.cg_quiesce_release(cg_client, qs_id_val, if_await=True)
        log.info(
            "Copy the contents from pre-upgrade snapshots to AFS on existing subvolumes"
        )
        for qs_member in qs_member_dict2:
            mnt_pt = qs_member_dict2[qs_member]["mount_point"]
            if qs_info.get(qs_member):
                if qs_info[qs_member].get("snap_list"):
                    retry_cnt = 0
                    rand_str = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(3))
                    )
                    cmd = f"mkdir {mnt_pt}/new_dir_{rand_str};"
                    out, rc = cg_io_client.exec_command(sudo=True, cmd=cmd)
                    while retry_cnt < 5:
                        snap_list = qs_info[qs_member]["snap_list"]
                        snap_name = random.choice(snap_list)
                        cmd = f"cp -r {mnt_pt}/.snap/*{snap_name}*/* {mnt_pt}/new_dir_{rand_str}/"
                        try:
                            out, rc = cg_io_client.exec_command(sudo=True, cmd=cmd)
                            retry_cnt = 5
                        except BaseException as ex:
                            if "No such file or directory" in str(ex):
                                retry_cnt += 1
                    cmd = (
                        f"diff {mnt_pt}/.snap/*{snap_name}* {mnt_pt}/new_dir_{rand_str}"
                    )
                    out, rc = cg_io_client.exec_command(sudo=True, cmd=cmd)
                    log.info(out)
                    log.info(
                        f"Restore from old snapshot {snap_name} to AFS suceeds after CG quiesce test on {qs_member}"
                    )

    log.info(f"cg_test_io_status : {cg_test_io_status.value}")

    if p.is_alive():
        proc_stop = 0
        log.info("CG IO is running after quiesce lifecycle")
        io_run_time *= 5
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=io_run_time)
        while (datetime.datetime.now() < end_time) and (proc_stop == 0):
            if p.is_alive():
                time.sleep(10)
            else:
                proc_stop = 1
        if proc_stop == 1:
            log.info("CG IO completed")
        elif proc_stop == 0:
            raise Exception("CG IO has NOT completed")
    else:
        log.info(
            f"WARN:CG IO test completed early during quiesce lifecycle with await on qs_set {qs_id_val}"
        )

    mnt_pt_list = []
    log.info(f"Perform cleanup for {qs_set}")
    for qs_member in qs_member_dict2:
        mnt_pt_list.append(qs_member_dict2[qs_member]["mount_point"])
    log.info("Remove CG IO files and unmount")
    cg_snap_util.cleanup_cg_io(cg_io_client, mnt_pt_list)
    mnt_pt_list.clear()
    for qs_member in qs_member_dict1:
        mnt_pt_list.append(qs_member_dict1[qs_member]["mount_point"])
    cg_snap_util.cleanup_cg_io(cg_client, mnt_pt_list, del_data=0)
    mnt_pt_list.clear()

    # snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_member_dict1:
        snap_list = snap_qs_dict[qs_member]
        if qs_member_dict1[qs_member].get("group_name"):
            group_name = qs_member_dict1[qs_member]["group_name"]
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    cg_client,
                    vol_name,
                    qs_member,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
        else:
            for snap_name in snap_list:
                fs_util.remove_snapshot(
                    cg_client, vol_name, qs_member, snap_name, validate=True
                )

    if cg_test_io_status.value == 1:
        log.error(
            f"CG IO test exits with failure during quiesce lifecycle with await on qs_set-{qs_id_val}"
        )
        test_fail = 1

    if test_fail == 1:
        log.error("FAIL: CG quiesce lifecycle on set of existing and new subvolumes")
        return 1
    else:
        log.info("PASS: CG quiesce lifecycle on set of existing and new subvolumes")
        return 0


def run(ceph_cluster, **kw):
    """
    Test Details:
    1. NFS validation, CEPH-83575098:
          a)Verify existing nfs cluster is active and exports exists. Run IO on existing exports,
          create new exports with existing nfs-cluster and verify.
          b)Verify NFS workflow on new nfs cluster, add new exports, run IO on new nfs.
    2. Snapshot & Schedule :
          a) Verify existing snapshots are accessible, perform read IO.
          b) Verify snapshot schedules are active.Validate snapshot schedules.
          c) Run IO, Create new manual snapshots on subvolumes and volumes.
          d) Restore to existing snapshot(cp snap dir contents to AFS), verify files and Delete old snapshot.
    3. Clones :
          a) Mount Clone volumes, read existing files, overwrite an existing file.Create new file.
          b) Create snapshot of clone volume
          c) Create Clone of existing clone volume.
          d) Delete existing clone volume.
    4. Pinning : Get existing dirs pinned, capture ceph fs status IO stats before IO, run IO only on pinned dirs,
    capture IO stats from ceph fs status, verify increment only at MDS rank 0, the pinned MDS.
    5. Auth rules : Verify existing auth rule for a client user works on new mount.
    6. MDS configuration : Verify MDS configuration remains unchanged after upgrade.
       Active and standby count remains same.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        default_fs = "cephfs"
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        cg_snap_util = CG_Snap_Utils(ceph_cluster)
        cg_snap_io = CG_snap_IO(ceph_cluster)
        test_status = 0
        if fs_util.get_fs_info(clients[0], "cephfs_new"):
            default_fs = "cephfs_new"
            clients[0].exec_command(sudo=True, cmd="ceph fs volume create cephfs")

        log.info("Get the Ceph pre-upgrade config data from cephfs_upgrade_config.json")
        f = clients[0].remote_file(
            sudo=True,
            file_name="/home/cephuser/cephfs_upgrade_config.json",
            file_mode="r",
        )
        pre_upgrade_config = json.load(f)
        test_reqs = {
            "config": pre_upgrade_config,
            "fs_util": fs_util,
            "snap_util": snap_util,
            "clients": clients,
            "nfs_servers": nfs_servers,
            "fs_name": default_fs,
        }
        f.close()
        space_str = "\t\t\t\t\t\t\t\t\t"
        log.info(
            f"\n\n {space_str} Test1 CEPH-83575098 : Post-upgrade NFS Validation\n"
        )
        nfs_test(test_reqs)
        log.info("NFS post upgrade validation succeeded \n")
        log.info(
            f"\n\n {space_str}Test2 : Post-upgrade Snapshot and Schedule Validation\n"
        )
        snap_sched_test(test_reqs)
        log.info(" Snapshot post upgrade validation succeeded \n")

        log.info(f"\n\n {space_str} Test3 : Post-upgrade Clone Validation \n")
        clone_test(test_reqs)
        log.info(" Clone post upgrade validation succeeded \n")

        log.info(f"\n\n {space_str} Test4 : Post-upgrade Pinning Validation \n")
        test_status = dir_pin_test(test_reqs)
        if test_status == 1:
            assert False, "MDS pinning post upgrade validation failed"
        log.info(" MDS pinning post upgrade validation succeeded \n ")

        log.info(f"\n\n {space_str} Test5 : Post-upgrade Auth rules Validation \n")
        test_status = auth_test(test_reqs)
        if test_status == 1:
            assert False, "Auth rules post upgrade validation failed"
        log.info(" Auth rules post upgrade validation succeeded \n")

        log.info(f"\n\n {space_str} Test6 : Post-upgrade MDS config validation \n")
        test_status = mds_config_test(test_reqs)
        if test_status == 1:
            assert False, "MDS config post upgrade validation failed"
        log.info(" MDS config post upgrade validation succeeded \n")

        log.info(
            f"\n\n {space_str} Test7 : Post-upgrade CG quiesce feature validation \n"
        )
        test_reqs.update(
            {
                "cg_snap_util": cg_snap_util,
                "cg_snap_io": cg_snap_io,
            }
        )
        test_status = cg_quiesce_test(test_reqs)
        if test_status == 1:
            assert False, "CG quiesce post upgrade validation failed"
        log.info(" CG quiesce post upgrade validation succeeded \n")
        # Upgrade all clients
        for client in clients:
            cmd = "yum install -y --nogpgcheck ceph-common ceph-fuse"
            client.exec_command(sudo=True, cmd=cmd)
        return 0

    except Exception as e:
        log.info(traceback.format_exc())
        log.error(e)
        return 1
