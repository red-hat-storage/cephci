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
from tests.cephfs.snapshot_clone.cg_snap_utils import CG_Snap_Utils
from utility.log import Log

log = Log(__name__)


def test_setup(fs_util, ceph_cluster, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if doesn't exist")
    default_fs = "cephfs"

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
        "Create subvolumegroup, Create subvolume in subvolumegroup and default grou"
    )
    subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
    fs_util.create_subvolumegroup(client, **subvolumegroup)
    sv_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_def",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_def)

    sv_non_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_non_def",
        "group_name": "subvolgroup_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_non_def)
    sv_list = [sv_def, sv_non_def]
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


def run_io_local(fs_util, mnt_info, sv_list):
    """
    This methods runs IOs in parallel on given mountpoints
    mnt_info : dict type input, with below format,
    {
    sv_name:{'path' : path,'client':client}
    }
    """
    write_procs = []
    rand_str = "".join(random.choice(string.digits) for i in range(3))
    for sv in sv_list:
        sv_name = sv["subvol_name"]
        mnt_path = mnt_info[sv_name]["path"]
        mnt_client = mnt_info[sv_name]["client"]
        io_tools = ["smallfile"]
        io_path = f"{mnt_path}/run_ios_{rand_str}"
        mnt_client.exec_command(sudo=True, cmd=f"mkdir -p {io_path}")
        p = Thread(target=fs_util.run_ios, args=(mnt_client, io_path, io_tools))
        p.start()
        write_procs.append(p)
    for p in write_procs:
        p.join()


def client_caps_quiesce_test(client, subvol_path, fs_name, qs_set, cg_snap_util):
    """
    This method is for Test1: Client caps validation during CG quiesce
    """
    out, _ = client.exec_command(sudo=True, cmd="ceph tell mds.0 client ls --f json")
    client_ls = json.loads(out)
    for i in range(0, len(client_ls)):
        log.info(client_ls[i])
        if client_ls[i]["client_metadata"].get("root"):
            sv_path = client_ls[i]["client_metadata"]["root"]
            if sv_path in subvol_path and sv_path != "/":
                caps_before_quiesce = client_ls[i]["num_caps"]
                client_id = client_ls[i]["id"]
                break
    log.info(f"CG quiesce on members:{qs_set}")
    rand_str = "".join(random.choice(string.digits) for i in range(3))
    qs_id_val = f"caps_test_{rand_str}"
    cg_snap_util.cg_quiesce(
        client, qs_set, qs_id=qs_id_val, fs_name=fs_name, timeout=600, expiration=600
    )
    time.sleep(5)
    retry_cnt = 12
    while retry_cnt > 0:
        out, _ = client.exec_command(
            sudo=True, cmd="ceph tell mds.0 client ls --f json"
        )
        client_ls = json.loads(out)
        for i in range(0, len(client_ls)):
            if client_id == client_ls[i]["id"]:
                caps_after_quiesce = client_ls[i]["num_caps"]
        log.info(
            f"Client Caps Before quiesce : {caps_before_quiesce},Clients Caps After quiesce : {caps_after_quiesce}"
        )

        if caps_after_quiesce >= caps_before_quiesce:
            retry_cnt -= 1
            time.sleep(5)
        else:
            retry_cnt = 0
    cg_snap_util.cg_quiesce_release(client, qs_id_val, fs_name=fs_name, if_await=True)
    if caps_after_quiesce >= caps_before_quiesce:
        log.error("Clients caps are not revoked during CG quiesce")
        return 1
    return 0


def client_caps_mds_failover_test(client, fs_name, fs_util, subvol_path):
    """
    This method is for Test2: Client caps validation during MDS failover
    """
    log.info("Capture Caps info before mds failover")
    out, _ = client.exec_command(sudo=True, cmd="ceph tell mds.0 client ls --f json")
    client_ls = json.loads(out)
    for i in range(0, len(client_ls)):
        log.info(client_ls[i])
        if client_ls[i]["client_metadata"].get("root"):
            sv_path = client_ls[i]["client_metadata"]["root"]
            if sv_path in subvol_path:
                caps_before_mds_fail = client_ls[i]["num_caps"]
                client_id = client_ls[i]["id"]
                break
    log.info(f"Perform MDS failover")
    mds_failover(fs_util, client, fs_name)
    log.info("Capture Caps info after mds failover")
    out, _ = client.exec_command(sudo=True, cmd="ceph tell mds.0 client ls --f json")
    client_ls = json.loads(out)
    for i in range(0, len(client_ls)):
        if client_id == client_ls[i]["id"]:
            caps_after_mds_fail = client_ls[i]["num_caps"]
    log.info(
        f"Client Caps Before MDS fail:{caps_before_mds_fail},Clients Caps After MDS fail:{caps_after_mds_fail}"
    )
    if caps_after_mds_fail >= caps_before_mds_fail:
        log.error("Clients caps are not updated during MDS failover")
        return 1
    return 0


def client_caps_evict_test(client, subvol_path, mnt_client, mnt_pt, fs_name, fs_util):
    """
    This is method for Test3: Client caps validation when client evicted
    """
    out, _ = client.exec_command(sudo=True, cmd="ceph tell mds.0 client ls --f json")
    client_ls = json.loads(out)

    for i in range(0, len(client_ls)):
        log.info(client_ls[i])
        if client_ls[i]["client_metadata"].get("root"):
            sv_path = client_ls[i]["client_metadata"]["root"]
            if sv_path in subvol_path:
                caps_before_evict = client_ls[i]["num_caps"]
                client_id = client_ls[i]["id"]
                log.info(f"Evicting Client with ID : {client_id}")
                cmd = f"ceph tell mds.0 client evict id={client_id}"
                client.exec_command(sudo=True, cmd=cmd)
                time.sleep(2)
                break
    log.info(f"Unmount and Remount {subvol_path} as Client session was evicted")
    mnt_client.exec_command(sudo=True, cmd=f"umount -l {mnt_pt}")
    mnt_params = {
        "mnt_path": subvol_path,
        "client": mnt_client,
        "fs_util": fs_util,
        "fs_name": fs_name,
    }
    mnt_params.update({"export_created": 0})
    fs_util.mount_ceph("fuse", mnt_params)

    out, _ = client.exec_command(sudo=True, cmd="ceph tell mds.0 client ls --f json")
    client_ls = json.loads(out)
    for i in range(0, len(client_ls)):
        if client_ls[i]["client_metadata"].get("root"):
            sv_path = client_ls[i]["client_metadata"]["root"]
            if sv_path in subvol_path:
                caps_after_evict = client_ls[i]["num_caps"]
    log.info(
        f"Client Caps Before Evict : {caps_before_evict},Clients Caps After Evict : {caps_after_evict}"
    )
    if caps_after_evict >= caps_before_evict:
        log.error("Clients caps are not revoked during Client eviction")
        return 1
    return 0


def mds_failover(fs_util, client, fs_name):
    """
    Helper routine to perform Rank 0 MDS failover of given FS
    """
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {fs_name} --format json"
    )
    output = json.loads(out)
    output = json.loads(out)
    active_mds_list = [
        mds["name"]
        for mds in output["mdsmap"]
        if (mds["state"] == "active") and (mds["rank"] == 0)
    ]
    active_mds = active_mds_list[0]
    log.info(f"FS status before MDS failover:{output}")
    out, rc = client.exec_command(cmd=f"ceph mds fail {active_mds}", client_exec=True)
    log.info(out)
    time.sleep(5)
    log.info("Waiting for atleast 2 active MDS after failing Rank 0 MDS")
    if wait_for_two_active_mds(client, fs_name):
        log.error("Wait for 2 active MDS failed")
        return 1
    if wait_for_healthy_ceph(client, fs_util, 300) == 0:
        return 1
    return 0


def wait_for_two_active_mds(client, fs_name, max_wait_time=180, retry_interval=10):
    """
    Wait until two active MDS (Metadata Servers) are found or the maximum wait time is reached.
    Args:
        max_wait_time (int): Maximum wait time in seconds (default: 180 seconds).
        retry_interval (int): Interval between retry attempts in seconds (default: 5 seconds).
    Returns:
        int: 0 if two active MDS are found within the specified time, 1 if not.
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        out, rc = client.exec_command(
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
            return 0  # Two active MDS found
        else:
            time.sleep(retry_interval)  # Retry after the specified interval

    return 1


def wait_for_healthy_ceph(client1, fs_util, wait_time_secs):
    """
    Wait for ceph to be healthy until given time
    """
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
    CEPH-83597462 - Verify Clients caps update during evict, IO quiesce and MDS failover.

    Test Steps:
    1. Validate Client Caps updated when CG quiesce is run on subvolumes
    2. Validate Client Caps are updated after MDS failover.
        3. Check if client caps are updated when client session is evicted

    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        cg_snap_util = CG_Snap_Utils(ceph_cluster)
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

        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        client = clients[0]

        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)

        crash_status_before = fs_util_v1.get_crash_ls_new(client)
        log.info(f"Crash status before Test: {crash_status_before}")

        log.info("Setup test configuration")
        setup_params = test_setup(fs_util_v1, ceph_cluster, client)
        fs_name = setup_params["default_fs"]
        sv_list = setup_params["sv_list"]
        qs_set = []
        for sv in sv_list:
            if sv.get("group_name"):
                qs_member = f"{sv['group_name']}/{sv['subvol_name']}"
            else:
                qs_member = f"{sv['subvol_name']}"
            qs_set.append(qs_member)
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
        sv_path_dict = {}
        sv_name_list = []
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
            sv_path_dict.update({sv["subvol_name"]: sv_path})
            sv_name_list.append(sv["subvol_name"])
            mnt_client = random.choice(clients)
            mnt_params.update({"mnt_path": sv_path, "client": mnt_client})
            path, _ = fs_util_v1.mount_ceph(mnt_type, mnt_params)

            mnt_info.update({sv["subvol_name"]: {"path": path, "client": mnt_client}})

        test_fail = 0

        run_io_local(fs_util_v1, mnt_info, sv_list)

        log.info(
            "Test1:Validate Client Caps recalled when CG quiesce is run on subvolumes"
        )

        sv_name = random.choice(sv_name_list)
        subvol_path = sv_path_dict[sv_name]
        if (
            client_caps_quiesce_test(client, subvol_path, fs_name, qs_set, cg_snap_util)
            == 1
        ):
            test_fail += 1

        run_io_local(fs_util_v1, mnt_info, sv_list)

        log.info("Test2:Check if client caps are recalled when MDS failover happens")
        sv_name = random.choice(sv_name_list)
        subvol_path = sv_path_dict[sv_name]
        if client_caps_mds_failover_test(client, fs_name, fs_util_v1, subvol_path) == 1:
            test_fail += 1

        run_io_local(fs_util_v1, mnt_info, sv_list)

        log.info(
            "Test3:Check if client caps are recalled when client session is evicted"
        )
        sv_name = random.choice(sv_name_list)
        subvol_path = sv_path_dict[sv_name]
        mnt_pt = mnt_info[sv_name]["path"]
        mnt_client = mnt_info[sv_name]["client"]
        if (
            client_caps_evict_test(
                client, subvol_path, mnt_client, mnt_pt, fs_name, fs_util_v1
            )
            == 1
        ):
            test_fail += 1

        if test_fail > 0:
            log.error("FAIL:Client Caps Validation Test Failed")
            return 1
        log.error("PASS:Client Caps Validation Test Passed")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")

        crash_status_after = fs_util_v1.get_crash_ls_new(client)
        log.info(f"Crash status after Test: {crash_status_after}")
        if wait_for_healthy_ceph(client, fs_util_v1, 300) == 0:
            assert False, "Cluster health is not OK even after waiting for 300secs"
        log.info("Cluster Health is OK")
        for sv in mnt_info:
            mnt_pt = mnt_info[sv]["path"]
            client_tmp = mnt_info[sv]["client"]
            try:
                out, rc = client_tmp.exec_command(
                    sudo=True,
                    cmd=f"umount -l {mnt_pt}",
                )
            except BaseException as ex:
                log.info(ex)
                if "not mounted" not in str(ex):
                    log.error("Unmount failed with unexpected reason")
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
