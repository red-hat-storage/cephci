import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

"""
Test steps for 83573489
1. Fill up the cluster with 60% data
2. Prepare the clients with different mounting options
3. Set max mds to 2
4. Set standby count to 2
5. Start IO on all the clients
6. Kill one of the active MDS
7. Start scrubbing
8. check if it shows any issue while scrubbing
"""


def run(ceph_cluster, **kw):
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        clean_up = False
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        # Check if there is file system created
        client1 = clients[0]
        client2 = clients[1]
        # count number of file systems
        rc, ec = client1.exec_command(sudo=True, cmd="ceph fs ls --format json-pretty")
        result = json.loads(rc)
        # set pool delete true
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        retry_remove_volume = retry(CommandFailed, tries=3, delay=60)(fs_util.remove_fs)
        for fs in result:
            fs_name = fs["name"]
            # delete the file systems
            retry_remove_volume(client1, fs_name)
            time.sleep(60)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        print(len(mds_nodes))
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        # get last 4 nodes
        hosts = mds_names[-4:]
        mds_hosts = " ".join(hosts) + " "
        client1.exec_command(
            sudo=True, cmd=f'ceph fs volume create cephfs --placement="4 {mds_hosts}"'
        )
        # getting all the MDS info to fail one of the MDS
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 2")
        # # set standby count to 2
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs standby_count_wanted 2")
        fs_util.wait_for_mds_process(client1, "cephfs")
        wait_for_two_active_mds(client1, fs_name="cephfs")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        mon_node_ips = fs_util.get_mon_node_ips()
        default_fs = "cephfs"
        for mount_dir in [fuse_mounting_dir_1, fuse_mounting_dir_2]:
            fs_util.fuse_mount(
                [client1],
                mount_dir,
                extra_params=f" --client_fs {default_fs}",
            )
        for mount_dir in [kernel_mounting_dir_1, kernel_mounting_dir_2]:
            fs_util.kernel_mount(
                [client2],
                mount_dir,
                ",".join(mon_node_ips),
                extra_params=f",fs={default_fs}",
            )
        clean_up = True
        cephfs = {
            "fill_data": 60,
            "io_tool": "smallfile",
            "mount": "fuse",
            "filesystem": "cephfs",
            "mount_dir": "/mnt/mycephfs1",
        }
        # fill up to 60% of the cluster
        fs_io(client=clients[0], fs_config=cephfs, fs_util=fs_util)
        result, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )
        result_json = json.loads(result)
        active_mds_before = []
        for elem in result_json["mdsmap"]:
            if elem["state"] == "active":
                active_mds_before.append(elem["name"])
        log.info(f"Active mds before mds fail {active_mds_before}")
        time.sleep(10)
        with parallel() as p:
            p.spawn(fs_util.run_ios, client1, fuse_mounting_dir_1)
            p.spawn(fs_util.run_ios, client1, fuse_mounting_dir_2)
            p.spawn(fs_util.run_ios, client2, kernel_mounting_dir_1)
            p.spawn(fs_util.run_ios, client2, kernel_mounting_dir_2)
        # kill active MDS
        client1.exec_command(sudo=True, cmd=f"ceph mds fail {active_mds_before[-1]}")
        time.sleep(30)
        rc, ec = client1.exec_command(
            sudo=True,
            cmd="ceph tell mds.cephfs:0 scrub start / recursive -f json-pretty",
        )
        result = json.loads(rc)
        log.info(result)
        if result["return_code"] != 0:
            log.error("Error while scrubbing")
            return 1
        retry_health = retry(CommandFailed, tries=4, delay=30)(
            fs_util.get_ceph_health_status
        )
        retry_health(client1)
        # check if there is damaged metadata from ceph -s
        rc, ec = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(rc)
        if "damaged" in rc:
            log.error("Damaged metadata found")
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # cleaning up the mounts
        if clean_up:
            fs_util.client_clean_up(
                "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_1
            )
            fs_util.client_clean_up(
                "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_2
            )
            fs_util.client_clean_up(
                "umount", kernel_clients=[client2], mounting_dir=kernel_mounting_dir_1
            )
            fs_util.client_clean_up(
                "umount", kernel_clients=[client2], mounting_dir=kernel_mounting_dir_2
            )
        fs_util.remove_fs(client1, vol_name="cephfs")
        time.sleep(60)

        # create 2 file system
        hosts_cephfs = mds_names[-5:-2]
        mds_hosts = " ".join(hosts_cephfs) + " "
        fs_util.create_fs(client1, vol_name="cephfs", placement=f"3 {mds_hosts}")
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 2")
        wait_for_two_active_mds(client1, fs_name="cephfs")
        hosts_ec = mds_names[-2:]
        mds_hosts_ec = " ".join(hosts_ec) + " "
        fs_util.create_fs(client1, vol_name="cephfs-ec", placement=f"3 {mds_hosts_ec}")
        fs_util.wait_for_mds_process(client1, "cephfs-ec")


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
