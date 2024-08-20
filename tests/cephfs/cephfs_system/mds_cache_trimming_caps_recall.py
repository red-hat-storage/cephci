import json
import math
import random
import re
import string
import time
import traceback
from datetime import datetime, timedelta

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def test_setup(fs_util, ceph_cluster, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if doesn't exist")
    default_fs = "cephfs-mds"

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


def set_verify_mds_config(client, mem_limit, set_config=True):
    """
    This method applies mds cache mem limit config and verifies it
    """
    if set_config:
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph config set mds mds_cache_memory_limit {mem_limit}",
        )
    log.info(
        f"Verify value for ceph config mds mds mds_cache_memory_limit is {mem_limit}"
    )
    out, rc = client.exec_command(
        sudo=True,
        cmd="ceph config get mds mds_cache_memory_limit",
    )
    if mem_limit not in out:
        log.error(
            f"Value for ceph config mds mds mds_cache_memory_limit is not {mem_limit}"
        )
        return 1
    return 0


def mds_cache_io(path, client):
    dir_suffix = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(3))
    )
    io_path = f"{path}/smallfile_dir{dir_suffix}"
    cmd = f"mkdir {io_path}"
    out, rc = client.exec_command(
        sudo=True,
        cmd=cmd,
    )

    cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 1"
    cmd += f" --files 1000 --top {io_path} &"
    client.exec_command(
        sudo=True,
        cmd=cmd,
        long_running=True,
        timeout=3600,
    )


def check_mds_dbg_log(fsid, msg_type, mds_node):
    msg_info = {
        "cache_used": "mds.0.cache trim bytes_used=",
        "cache_trimmed": "mds.0.cache trim_lru trimmed",
        "caps_recalled": "mds.0.server recalled",
    }
    cmd = f'grep "{msg_info[msg_type]}" /var/log/ceph/{fsid}/*'
    out, _ = mds_node.exec_command(
        sudo=True,
        cmd=cmd,
    )
    out = out.strip()
    mds_dbg_msgs = out.split("\n")
    if msg_type == "cache_used":
        cache_used_pct_list = []
        for line in mds_dbg_msgs:
            cache_used_list = line.split()
            _, bytes_used = cache_used_list[5].split("=")
            _, limit = cache_used_list[6].split("=")
            bytes_val = re.findall(r"(\d+)(\w+)", bytes_used)[0][0]
            limit_val = re.findall(r"(\d+)(\w+)", limit)[0][0]
            cache_used_pct = math.floor(float(int(bytes_val) / int(limit_val)) * 100)
            cache_used_pct_list.append(cache_used_pct)
        cache_used_pct = max(cache_used_pct_list)
        return float(cache_used_pct)
    elif msg_type == "cache_trimmed":
        trimmed_items_list = []
        for line in mds_dbg_msgs:
            trim_msg_list = line.split()
            trimmed_items = int(trim_msg_list[6])
            trimmed_items_list.append(trimmed_items)
        trimmed_items = max(trimmed_items_list)
        return trimmed_items
    elif msg_type == "caps_recalled":
        caps_recalled_list = []
        for line in mds_dbg_msgs:
            recall_msg_list = line.split()
            caps_recalled = int(recall_msg_list[5])
            caps_recalled_list.append(caps_recalled)
        caps_recalled = max(caps_recalled_list)
        return caps_recalled


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


def run(ceph_cluster, **kw):
    """
    CEPH-83591709 - MDS Cache Trimming and Client Caps Recall when Cache is 95% full.

    Test Steps:
    Set mds_cache_memory_limit to 1GB(1070596096)
    Configure 2 MDS for FS Volume, 2 Active, 2 Standy -Replay, 1 Standyby
    Create 3 subvolumes and mount across two clients -  1 kernel, 2 fuse
    Enable MDS debug logs
    Create dataset on both subvolumes to use for read as below,
    python3 smallfile_cli.py --operation create --threads 10 --file-size 1 --files 10000 --top /mnt/sv1/smallfile_dir/
    python3 smallfile_cli.py --operation create --threads 10 --file-size 1 --files 10000 --top /mnt/sv2/smallfile_dir/
    python3 smallfile_cli.py --operation create --threads 10 --file-size 1 --files 10000 --top /mnt/sv3/smallfile_dir/
    Capture current MDS MEM usage after write IO.
    Start parallel read IO across two clients for subvolumes as below,
    python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i
      --threads 10 --file-size 1 --files 10000 --top /mnt/sv1/smallfile_dir/
    python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i
      --threads 10 --file-size 1 --files 10000 --top /mnt/sv2/smallfile_dir/
    python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i
      --threads 10 --file-size 1 --files 10000 --top /mnt/sv3/smallfile_dir/
    Run these read ops in background and monitor MEM usage by Active MDS, if mem usage is 95%, wait for cache trimming
    and caps recall by searching mds debug logs
    If mem usage not reached 95%, run additional io to increase mem usage as below,
    python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 1 --files 1000 --top
    /mnt/sv3/smallfile_dir2/
    Wait for Cache trimming and caps recall message as below,
    mds.0.cache trim_lru trimmed 103 items
    mds.0.server recalled 90000 client caps

    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        osp_cred = config.get("osp_cred")
        build = config.get("build", config.get("rhbuild"))

        if config.get("cloud-type") == "openstack":
            os_cred = osp_cred.get("globals").get("openstack-credentials")
            params = {}
            params["username"] = os_cred["username"]
            params["password"] = os_cred["password"]
            params["auth_url"] = os_cred["auth-url"]
            params["auth_version"] = os_cred["auth-version"]
            params["tenant_name"] = os_cred["tenant-name"]
            params["service_region"] = os_cred["service-region"]
            params["domain_name"] = os_cred["domain"]
            params["tenant_domain_id"] = os_cred["tenant-domain-id"]
            params["cloud_type"] = "openstack"
        elif config.get("cloud-type") == "ibmc":
            pass
        else:
            pass
        fs_name = "cephfs-mds"
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        client = clients[0]

        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        log.info("Save current MDS Cache memory limit")
        out, rc = client.exec_command(
            sudo=True,
            cmd="ceph config get mds mds_cache_memory_limit",
        )
        def_mds_mem = out.strip()
        crash_status_before = fs_util_v1.get_crash_ls_new(client)
        log.info(f"Crash status before Test: {crash_status_before}")

        log.info("Set MDS Cache memory limit to 600MB and verify")
        set_verify_mds_config(client, "629145600")
        log.info("Setup test configuration")
        setup_params = test_setup(fs_util_v1, ceph_cluster, client)
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
        for sv in sv_list:
            mnt_type = random.choice(["fuse", "kernel", "nfs"])
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
            mnt_client = random.choice(clients)
            mnt_params.update({"mnt_path": sv_path, "client": mnt_client})
            path, _ = fs_util_v1.mount_ceph(mnt_type, mnt_params)

            mnt_info.update({sv["subvol_name"]: {"path": path, "client": mnt_client}})

        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        for mds in parsed_data.get("mdsmap"):
            if mds.get("rank") == 0 and mds.get("state") == "active":
                active_mds = mds["name"]

        log.info(f"Active MDS : {active_mds}")
        for mds_nodes_iter in mds_nodes:
            if mds_nodes_iter.node.hostname in active_mds:
                active_mds_node = mds_nodes_iter
        daemons_value = {"mds": 10}
        fs_util_v1.enable_logs(client, daemons_value)
        log.info("Start MDS Load to increase MEM usage by 130%")
        fs_util_v1.mds_mem_cpu_load(
            client, 110, 50, active_mds, active_mds_node, sv_list, mnt_info
        )
        mds_mem_used_pct, _ = fs_util_v1.get_mds_cpu_mem_usage(
            client, active_mds, active_mds_node
        )
        if mds_mem_used_pct < 100:
            log.error(
                "Could not suceed in increasing MDS MEM usage by atleast 100% using IO"
            )
            return 1
        log.info(f"MDS MEM usage : MEM - {mds_mem_used_pct}")
        fsid = fs_util_v1.get_fsid(client)

        retry_cnt = 3
        while retry_cnt > 0:
            msg_type = "cache_used"
            mds_cache_used_pct = check_mds_dbg_log(fsid, msg_type, active_mds_node)
            log.info(f"mds_cache_used_pct:{mds_cache_used_pct}")
            if mds_cache_used_pct < 95:
                sv_iter = random.choice(sv_list)
                io_path = mnt_info[sv_iter["subvol_name"]]["path"]
                mnt_client = mnt_info[sv_iter["subvol_name"]]["client"]
                mds_cache_io(io_path, mnt_client)
                retry_cnt -= 1
            else:
                retry_cnt = 0
        msg_type = "cache_trimmed"
        trimmed_items = check_mds_dbg_log(fsid, msg_type, active_mds_node)
        msg_type = "caps_recalled"
        caps_recalled = check_mds_dbg_log(fsid, msg_type, active_mds_node)
        log.info(
            f"Cache items trimmed : {trimmed_items},Caps Recalled : {caps_recalled}"
        )
        if trimmed_items == 0 or caps_recalled == 0:
            log.error(
                "Unexpected : Either Cache items trimmed or Caps Recalled is zero"
            )
            return 1

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        daemons = ["mds"]
        fs_util_v1.disable_logs(client, daemons)
        log.info(f"Setting MDS cache mem limit back to {def_mds_mem}")
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph config set mds mds_cache_memory_limit {def_mds_mem}",
        )
        crash_status_after = fs_util_v1.get_crash_ls_new(client)
        log.info(f"Crash status after Test: {crash_status_after}")
        if wait_for_healthy_ceph(client, fs_util_v1, 300) == 0:
            assert False, "Cluster health is not OK even after waiting for 300secs"
        log.info("Cluster Health is OK")
        for sv in mnt_info:
            mnt_pt = mnt_info[sv]["path"]
            client_tmp = mnt_info[sv]["client"]
            out, rc = client_tmp.exec_command(
                sudo=True,
                cmd=f"umount -l {mnt_pt}",
            )
        for sv in sv_list:
            subvol_name = sv["subvol_name"]
            fs_util_v1.remove_subvolume(
                client,
                fs_name,
                subvol_name,
                validate=True,
                group_name=sv.get("group_name", None),
            )
            if sv.get("group_name"):
                group_name = sv["group_name"]
        fs_util_v1.remove_subvolumegroup(client, fs_name, group_name, validate=True)
        fs_util_v1.remove_fs(client, fs_name)
