import json
import random
import string
import time
import traceback
from datetime import datetime, timedelta
from threading import Thread

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def test_setup(fs_util, ceph_cluster, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if doesn't exist")
    default_fs = "cephfs-top"

    mds_nodes = ceph_cluster.get_ceph_objects("mds")
    fs_util.create_fs(client, default_fs)

    log.info("Configure/Verify 5 MDS for FS Volume, 2 Active, 3 Standyby")
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {default_fs}")
    out_list = out.split("\n")
    for line in out_list:
        if "max_mds" in line:
            str, max_mds_val = line.split()
            log.info(f"max_mds before test {max_mds_val}")
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {default_fs} --format json"
    )
    output = json.loads(out)
    mds_cnt = len(output["mdsmap"])
    if mds_cnt < 5:
        cmd = f'ceph orch apply mds {default_fs} --placement="5'
        for mds_nodes_iter in mds_nodes:
            cmd += f" {mds_nodes_iter.node.hostname}"
        cmd += '"'
        log.info("Adding 5 MDS to cluster")
        out, rc = client.exec_command(sudo=True, cmd=cmd)
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {default_fs} max_mds 2",
    )

    if wait_for_healthy_ceph(client, fs_util, 300) == 0:
        return 1

    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    nfs_server = nfs_servers[0].node.hostname
    nfs_name = "cephfs-nfs"

    client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
    )
    if wait_for_process(client=client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")
    nfs_export_name = "/export_" + "".join(
        random.choice(string.digits) for i in range(3)
    )
    log.info(
        "Create subvolumegroup, Create subvolume in subvolumegroup and default group"
    )
    subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
    fs_util.create_subvolumegroup(client, **subvolumegroup)
    sv_def_1 = {
        "vol_name": default_fs,
        "subvol_name": "sv_def_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_def_1)
    sv_def_2 = {
        "vol_name": default_fs,
        "subvol_name": "sv_def_2",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_def_2)
    sv_non_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_non_def_1",
        "group_name": "subvolgroup_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_non_def)
    sv_list = [sv_def_1, sv_def_2, sv_non_def]
    setup_params = {
        "default_fs": default_fs,
        "subvolumegroup": subvolumegroup,
        "sv_list": sv_list,
        "nfs_name": nfs_name,
        "nfs_export_name": nfs_export_name,
        "nfs_server": nfs_server,
        "max_mds_def": max_mds_val,
    }
    return setup_params


def cephfs_top_stats_validate(client, fs_name, mnt_info):
    """
    This method validates cephfs-top --dump output for given mount info and mountpoints
    Required params:
    client - for ceph cmds
    fs_name - filesystem name
    mnt_info - a dict input data, in below format,
    mnt_info = {'sv1':{'client':client_obj,'path':mnt_path,'mnt_type':'fuse'}
    }
    """
    retry_cnt = 10
    while retry_cnt > 0:
        out, _ = client.exec_command(
            sudo=True,
            cmd="cephfs-top --dump",
        )
        parsed_data = json.loads(out)
        log.info(f"cephfs-top --dump output:{parsed_data}")
        client_info = parsed_data.get("client_count")
        if client_info["total_clients"] == 0:
            retry_cnt -= 1
            time.sleep(1)
        else:
            retry_cnt = 0
    fs_info = parsed_data.get("filesystems")[fs_name]
    test_fail = 0
    mnt_pass = 0
    mnt_pnt_pass = 0
    if len(mnt_info.keys()) == client_info["total_clients"]:
        log.info(
            f"Verified client_info['total_clients']={client_info['total_clients']}"
        )
    else:
        test_fail += 1
    for mnt_iter in mnt_info:
        mnt_type = mnt_info[mnt_iter]["mnt_type"]
        if mnt_type == "libcephfs":
            nfs_server = mnt_info[mnt_iter]["nfs_server"]
            for id in fs_info:
                if nfs_server in fs_info[id]["mount_point@host/addr"]:
                    mnt_pass += 1
                    log.info(
                        f"Validated {mnt_type} mount info in cephfs-top dump output"
                    )
        elif (
            mnt_info[mnt_iter]["mnt_type"] in client_info and client_info[mnt_type] == 1
        ):
            log.info(f"Validated {mnt_type} mount info in cephfs-top dump output")
            mnt_pass += 1
    if mnt_pass != 3:
        test_fail += 1
        log.error("Validation of Mountinfo in cephfs-top dump output failed")
    for id in fs_info:
        log.info(
            f"fs_info[id]['mount_point@host/addr']:{fs_info[id]['mount_point@host/addr']}"
        )
        for mnt_iter in mnt_info:
            log.info(f"path:{mnt_info[mnt_iter]['path']}")
            path = replace_nth("/", "", mnt_info[mnt_iter]["path"], 3)
            if path in fs_info[id]["mount_point@host/addr"]:
                log.info(
                    f"Validated mountpoint {mnt_info[mnt_iter]['path']} in cephfs-top dump output"
                )
                mnt_pnt_pass += 1
    if mnt_pnt_pass < 3:
        test_fail += 1
        log.error(
            "BZ 2307251 : Validation of Mountpoint in cephfs-top dump output failed"
        )
    if test_fail > 0:
        return 1
    return 0


def mds_fail_cephfs_top_check(client, fs_name):
    """
    This method performs all active mds failure and validates cephfs-top --dump output
    """
    retry_cnt = 3
    while retry_cnt > 0:
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        for mds in parsed_data.get("mdsmap"):
            if mds.get("rank") == 0 and mds.get("state") == "active":
                active_mds_0 = mds["name"]
            elif mds.get("rank") == 1 and mds.get("state") == "active":
                active_mds_1 = mds["name"]

        out, rc = client.exec_command(
            cmd=f"ceph mds fail {active_mds_0}", client_exec=True
        )
        out, rc = client.exec_command(
            cmd=f"ceph mds fail {active_mds_1}", client_exec=True
        )
        retry_cnt1 = 5
        while retry_cnt1 > 0:
            out, _ = client.exec_command(
                sudo=True,
                cmd="cephfs-top --dump",
            )
            parsed_data = json.loads(out)
            log.info(f"cephfs-top --dump output:{parsed_data}")
            fs_info = parsed_data.get("filesystems")[fs_name]
            if len(fs_info) != 0:
                retry_cnt1 -= 1
                time.sleep(1)
            else:
                retry_cnt1 = 0
        if len(fs_info) != 0:
            retry_cnt -= 1
            time.sleep(1)
        else:
            retry_cnt = 0

    if len(fs_info) == 0:
        log.info("Validated that cephfs-top shows no entries if MDS is down")
        return 0
    log.error("cephfs-top validation when MDS is down,failed")
    return 1


def remove_fs_client_cephfs_top_check(client, fs_name, mnt_info):
    """
    This method umounts mountpoint at respective client and validates mount info removed
    in cephfs-top --dump output
    Required params:
    client - for ceph cmds
    fs_name - filesystem name
    mnt_info - a dict input data, in below format,
    mnt_info = {'sv1':{'client':client_obj,'path':mnt_path,'mnt_type':'fuse'}
    }
    """
    retry_cnt = 6
    while retry_cnt > 0:
        out, _ = client.exec_command(
            sudo=True,
            cmd="cephfs-top --dump",
        )
        parsed_data = json.loads(out)
        log.info(f"cephfs-top --dump output:{parsed_data}")
        client_info = parsed_data.get("client_count")
        if client_info["total_clients"] == 0:
            retry_cnt -= 1
            time.sleep(1)
        else:
            retry_cnt = 0
    for mnt_iter in mnt_info:
        client_tmp = mnt_info[mnt_iter]["client"]
        mnt_path = mnt_info[mnt_iter]["path"]
        mnt_type = mnt_info[mnt_iter]["mnt_type"]

        out, _ = client_tmp.exec_command(
            sudo=True,
            cmd=f"umount {mnt_path}",
        )
        if mnt_type == "libcephfs":
            nfs_name = mnt_info[mnt_iter]["nfs_name"]
            export_name = mnt_info[mnt_iter]["export_name"]
            nfs_server = mnt_info[mnt_iter]["nfs_server"]
            out, _ = client.exec_command(
                sudo=True,
                cmd=f"ceph nfs export rm {nfs_name} {export_name}",
            )
            time.sleep(2)
        retry_cnt = 6
        while retry_cnt > 0:
            umount_fail = 0
            out, _ = client.exec_command(
                sudo=True,
                cmd="cephfs-top --dump",
            )
            parsed_data = json.loads(out)
            log.info(f"cephfs-top --dump output:{parsed_data}")
            client_info = parsed_data.get("client_count")

            if mnt_type != "libcephfs" and client_info[mnt_type] > 0:
                log.error(
                    f"Client count doesn't reflect umount done for type {mnt_type}"
                )
                umount_fail += 1
            else:
                log.info(f"Client count reflects umount done for type {mnt_type}")
            fs_info = parsed_data.get("filesystems")[fs_name]
            for id in fs_info:
                search_str = nfs_server if mnt_type == "libcephfs" else mnt_path
                if search_str in fs_info[id]["mount_point@host/addr"]:
                    log.error(
                        f"{search_str} mount info is listed while it's removed,Unexpected"
                    )
                    umount_fail += 1
            if umount_fail == 0:
                log.info(
                    f"mount info is removed in cephfs-top dump after umount for {mnt_type}"
                )
            retry_cnt = retry_cnt - 1 if umount_fail > 0 else 0
            time.sleep(5)

    if umount_fail > 0:
        return 1
    return 0


def remove_fs_cephfs_top_check(client, fs_util, fs_name, mnt_info):
    """
    This methods removes subvolumes,subvolumegroup and filesystem, valdiates if corresponding filesystem entries
    removed from cephfs-top --dump output
    Required params:
    client - for ceph cmds
    fs_name - filesystem name
    mnt_info - a dict input data, in below format,
    mnt_info = {'sv1':{'client':client_obj,'path':mnt_path,'mnt_type':'fuse'}
    }
    """
    for sv in mnt_info:
        fs_util.remove_subvolume(
            client,
            fs_name,
            sv,
            validate=True,
            group_name=mnt_info[sv].get("group_name", None),
        )
        group_name = mnt_info[sv].get("group_name", None)
    fs_util.remove_subvolumegroup(client, fs_name, group_name, validate=True)
    fs_util.remove_fs(client, fs_name)
    out, _ = client.exec_command(
        sudo=True,
        cmd="cephfs-top --dump",
    )

    exp_str = "Verified cephfs-top dump out after filesystem removal"
    if "No filesystem available" in out.strip():
        log.info(exp_str)
        return 0
    parsed_data = json.loads(out)
    fs_info = parsed_data.get("filesystems")
    if fs_name not in fs_info.keys():
        log.info(exp_str)
        return 0
    else:
        log.error(
            f"cephfs-top dump output is unexpected after filesystem removal:{out}"
        )
        return 1


def wait_for_healthy_ceph(client1, fs_util, wait_time_secs):
    # Returns 1 if healthy, 0 if unhealthy
    ceph_healthy = 0
    end_time = datetime.now() + timedelta(seconds=wait_time_secs)
    while ceph_healthy == 0 and (datetime.now() < end_time):
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


def replace_nth(sub, repl, txt, nth):
    arr = txt.split(sub)
    part1 = sub.join(arr[:nth])
    part2 = sub.join(arr[nth:])
    return part1 + repl + part2


def run(ceph_cluster, **kw):
    """
    CEPH-83575020 - Verify Cephfs-top works as expected with negative test scenarios.
    Test Steps:
    install "cephfs-top" by "dnf install cephfs-top”,Installation should be successful
    Enable stats in ceph mgr module
    Create cephfs-top client user, run "ceph auth ls" | grep fstop, fstop client should be created
    Mount and run some IOs on test volumes, run "cephfs-top --dump", verify IO stats in cephfs-top
    Stop all the MDS,Stopping MDS should be successful,observe IO stats of cephfs-top,IOs stats stops changing while
    standbys come up
    While cephfs-top is running, Remove the clients mount point and validate if it reflects in cephfs-top stats, mounted
    point should be removed in the cephfs-top
    Remove the file system and validate if it reflects, volume should be removed in the cephfs
    """
    try:
        tc = "CEPH-83575020"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util_v1 = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client = clients[0]
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        cleanup_done = 0

        crash_status_before = fs_util_v1.get_crash_ls_new(client)
        log.info(f"Crash status before Test: {crash_status_before}")

        log.info("Setup test configuration")
        setup_params = test_setup(fs_util_v1, ceph_cluster, client)

        log.info("Install cephfs-top by dnf install cephfs-top")
        client.exec_command(
            sudo=True,
            cmd="dnf install cephfs-top -y",
        )
        out, rc = client.exec_command(
            sudo=True,
            cmd="dnf list cephfs-top",
        )
        if "cephfs-top" not in out:
            log.error("cephfs-top package could not be installed")
            return 1

        log.info("Enable stats in mgr module")
        client.exec_command(
            sudo=True,
            cmd="ceph mgr module enable stats",
        )
        log.info("Create cephfs-top client user and verify")
        client_user = "client.fstop"
        cmd = f"ceph auth get-or-create {client_user} mon 'allow r' mds 'allow r' osd 'allow r' mgr 'allow r'"
        cmd += " > /etc/ceph/ceph.client.fstop.keyring"
        client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        out, _ = client.exec_command(
            sudo=True,
            cmd="ceph auth ls | grep fstop",
        )
        if client_user not in out:
            log.error(f"{client_user} user not created")
            return 1

        log.info("Mount and run some IOs on test volumes")
        fs_name = setup_params["default_fs"]
        sv_list = setup_params["sv_list"]
        mnt_params = {
            "client": clients[1],
            "fs_util": fs_util_v1,
            "fs_name": fs_name,
            "export_created": 0,
        }
        nfs_params = {
            "nfs_name": setup_params["nfs_name"],
            "nfs_export_name": setup_params["nfs_export_name"],
            "nfs_server": setup_params["nfs_server"],
        }
        mnt_info = {}
        mnt_list = ["fuse", "kernel", "nfs"]
        fstop_mnt = {"fuse": "fuse", "kernel": "kclient", "nfs": "libcephfs"}
        for sv in sv_list:
            mnt_type = mnt_list.pop()
            if mnt_type == "nfs":
                mnt_params.update(nfs_params)
            cmd = f"ceph fs subvolume getpath {fs_name} {sv['subvol_name']}"
            if sv.get("group_name"):
                cmd += f" {sv['group_name']}"
            out, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            sv_path = out.strip()
            mnt_params.update({"mnt_path": sv_path})
            path, _ = fs_util_v1.mount_ceph(mnt_type, mnt_params)
            top_mnt_type = fstop_mnt[mnt_type]
            mnt_info.update(
                {
                    sv["subvol_name"]: {
                        "path": path,
                        "client": mnt_params["client"],
                        "mnt_type": top_mnt_type,
                    }
                }
            )
            if mnt_type == "nfs":
                export_name = setup_params["nfs_export_name"]
                mnt_info[sv["subvol_name"]].update(
                    {"nfs_name": setup_params["nfs_name"], "export_name": export_name}
                )
                mnt_info[sv["subvol_name"]].update(
                    {"nfs_server": setup_params["nfs_server"]}
                )
            if sv.get("group_name"):
                mnt_info[sv["subvol_name"]].update({"group_name": sv["group_name"]})
        test_fail = 0
        write_procs = []
        io_args = {"run_time": 1}
        for mnt_iter in mnt_info:
            client_tmp = mnt_info[mnt_iter]["client"]
            mnt_path = mnt_info[mnt_iter]["path"]
            p = Thread(
                target=fs_util_v1.run_ios_V1,
                args=(client_tmp, mnt_path, ["smallfile"]),
                kwargs=io_args,
            )
            p.start()
            write_procs.append(p)
        if cephfs_top_stats_validate(client, fs_name, mnt_info) == 1:
            test_fail += 1
            log.error("Test failed during cephfs_top_stats_validate")

        if mds_fail_cephfs_top_check(client, fs_name) == 1:
            test_fail += 1
            log.error("Test failed during mds_fail_cephfs_top_check")
        for p in write_procs:
            p.join()
            if p.is_alive():
                proc_stop = 0
                log.info("IO is still running")
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=100)
                while (datetime.datetime.now() < end_time) and (proc_stop == 0):
                    if p.is_alive():
                        time.sleep(10)
                    else:
                        proc_stop = 1
                if proc_stop == 1:
                    log.info("IO completed")
                elif proc_stop == 0:
                    log.error("IO has NOT completed")

        if wait_for_healthy_ceph(client, fs_util_v1, 300) == 0:
            return 1
        if remove_fs_client_cephfs_top_check(client, fs_name, mnt_info) == 1:
            test_fail += 1
            log.error("Test failed during remove_fs_client_cephfs_top_check")
        if remove_fs_cephfs_top_check(client, fs_util_v1, fs_name, mnt_info) == 1:
            test_fail += 1
            log.error("Test failed during remove_fs_cephfs_top_check")
        else:
            cleanup_done = 1

        if test_fail > 0:
            log.error("Cephfs-top negative testing failed")
            return 1
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        crash_status_after = fs_util_v1.get_crash_ls_new(client)
        log.info(f"Crash status after Test: {crash_status_before}")
        if crash_status_before != crash_status_after:
            assert (
                False
            ), f"Unexpected info in crash status:Before-{crash_status_before},After-{crash_status_after}"
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export rm {setup_params['nfs_name']} {setup_params['nfs_export_name']}",
            check_ec=False,
        )
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {setup_params['nfs_name']}",
            check_ec=False,
        )
        if cleanup_done == 0:
            log.info("Cleaning up the system")
            for sv in mnt_info:
                fs_util_v1.remove_subvolume(
                    client,
                    fs_name,
                    sv,
                    validate=True,
                    group_name=mnt_info[sv].get("group_name", None),
                )
                group_name = mnt_info[sv].get("group_name", None)
            fs_util_v1.remove_subvolumegroup(client, fs_name, group_name, validate=True)
            fs_util_v1.remove_fs(client, fs_name)
