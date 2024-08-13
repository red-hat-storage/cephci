import json
import random
import string
import time
import traceback
from datetime import datetime, timedelta
from threading import Thread

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
    default_fs = "cephfs"
    fs_details = fs_util.get_fs_info(client)
    mds_nodes = ceph_cluster.get_ceph_objects("mds")
    if not fs_details:
        fs_util.create_fs(client, default_fs)

    log.info(
        "Configure/Verify 5 MDS for FS Volume, 2 Active, 2 Standy-Replay, 1 Standyby"
    )
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

    log.info(f"Configure 2 standby-replay MDS in {default_fs}")
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {default_fs} allow_standby_replay true",
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
    CEPH-83591710 - MDS Failover to Standy-Replay MDS when CPU and MEM usage by MDS is 70%

    Test Steps:
    Set mds_cache_memory_limit to 2GB(1073741824)
    Configure 5 MDS for FS Volume, 2 Active, 2 Standy -Replay, 1 Standyby
    Create 2 subvolumes and mount across two clients -  1 kernel, 1 fuse
    Create dataset on both subvolumes to use for read as below,
    python3 smallfile_cli.py --operation create --threads 10 --file-size 1 --files 10000 --top /mnt/sv1/smallfile_dir/
    python3 smallfile_cli.py --operation create --threads 10 --file-size 1 --files 10000 --top /mnt/sv2/smallfile_dir/
    Capture current CPU and MEM usage after write IO.
    Start parallel read IO across two clients for subvolumes as below,
    for i in  read rename symlink stat chmod ls-l ; do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i
      --threads 10 --file-size 1 --files 10000 --top /mnt/sv1/smallfile_dir/;done
    for i in  read rename symlink stat chmod ls-l ;do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i
      --threads 10 --file-size 1 --files 10000 --top /mnt/sv2/smallfile_dir/;done
    Run these read ops in background and monitor CPU and MEM usage by Active MDS, when usage percentage crosses 70%,
    Perform MDS failover.Verify that Standy-by Replay daemon successfully comes to Active state and Ceph cluster remains
    Healthy. Wait for IO to complete. Verify mount points are accessible

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
        fs_name = "cephfs"
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

        log.info("Set MDS Cache memory limit to 900MB and verify")
        set_verify_mds_config(client, "943718400")
        log.info("Setup test configuration")
        setup_params = test_setup(fs_util_v1, ceph_cluster, client)
        sv_list = setup_params["sv_list"]
        mnt_params = {
            "client": clients[1],
            "fs_util": fs_util_v1,
            "fs_name": setup_params["default_fs"],
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
            cmd = f"ceph fs subvolume getpath cephfs {sv['subvol_name']}"
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
        out, rc = client.exec_command(cmd=f"ceph fs status {fs_name}", client_exec=True)
        out_list = out.split("\n")
        for out_iter in out_list:
            if "0-s" in out_iter:
                standby_replay_mds = out_iter.split()[2]
        log.info(
            f"Active MDS : {active_mds}, Standy-by Replay MDS:{standby_replay_mds}"
        )
        for mds_nodes_iter in mds_nodes:
            if mds_nodes_iter.node.hostname in active_mds:
                active_mds_node = mds_nodes_iter
        log.info("Start MDS Load to increased MEM and CPU usage by 70%")
        if (
            fs_util_v1.mds_mem_cpu_load(
                client, 70, 70, active_mds, active_mds_node, sv_list, mnt_info
            )
            == 1
        ):
            log.error("Failed to load MDS to increase MEM and CPU usage by 70%")
            return 1
        log.info(
            "Perform Active MDS failover, and Verify Standby-Replay daemon takesover"
        )
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        out, rc = client.exec_command(
            cmd=f"ceph mds fail {active_mds}", client_exec=True
        )
        log.info(out)

        if wait_for_healthy_ceph(client, fs_util_v1, 300) == 0:
            return 1
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        for mds in parsed_data.get("mdsmap"):
            if mds.get("rank") == 0 and mds.get("state") == "active":
                if standby_replay_mds in mds["name"]:
                    log.info(
                        f"Expected:Standby Replay MDS {standby_replay_mds} has takenover failed MDS {active_mds}"
                    )
                else:
                    log.error(
                        f"failed MDS {active_mds} is takenover by unexpected MDS {mds['name']}"
                    )
                    return 1
        log.info("Verify IO continues after MDS failover")
        write_procs = []
        io_args = {"run_time": 1}
        for sv in mnt_info:
            mnt_pt = mnt_info[sv]["path"]
            client_tmp = mnt_info[sv]["client"]
            p = Thread(
                target=fs_util_v1.run_ios_V1,
                args=(client_tmp, mnt_pt),
                kwargs=io_args,
            )
            p.start()
            write_procs.append(p)
        for p in write_procs:
            p.join()
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
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
                setup_params["default_fs"],
                subvol_name,
                validate=True,
                group_name=sv.get("group_name", None),
            )
            if sv.get("group_name"):
                group_name = sv["group_name"]
        fs_util_v1.remove_subvolumegroup(
            client, setup_params["default_fs"], group_name, validate=True
        )
