import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83573264 - [Cephfs]Taking the cephfs down with down flag

    Test Steps :
    1. We need atleast one client node to execute this test case
    2. creats fs volume create cephfs if the volume is not there
    3. write data on to the fs volume
    4. Mark Fs Down flag to ture
    5. MDS node should be in stopped state
    6. Mark Fs Down flag to false
    7. MDS node should be in active state
    8. Set joinable falg to false
    9. Fail all the MDS and see if Cephfs is down
    10.set joinable flag to true and verify ceohfs is UP


    Clean Up:
    1. Del all the snapshots created
    2. Del Subvolumes
    3. Del SubvolumeGroups
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        if erasure:
            log.info("Suite has been triggered in Erasure mode")
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs-down-flag"
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        retry_mount = retry(CommandFailed, tries=3, delay=30)(fs_util.kernel_mount)
        retry_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))
        retry_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}",
            long_running=True,
        )
        result, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )
        mds_name_list = []
        result_json = json.loads(result)
        active_mds_before = []
        for elem in result_json["mdsmap"]:
            if elem["state"] == "active":
                mds_name_list.append(elem["rank"])
                active_mds_before.append(elem["name"])
        log.info(f"Active mds before mds fail {active_mds_before}")
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} down true")
        retry_mds_status = retry(CommandFailed, tries=3, delay=30)(
            fs_util.get_mds_status
        )
        retry_mds_status(
            clients[0],
            len(mds_name_list),
            vol_name=default_fs,
            expected_status="standby",
            validate_mds=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} down false")
        retry_mds_status(clients[0], len(mds_name_list), vol_name=default_fs)
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} joinable false")
        with parallel() as p:
            for mds in mds_name_list:
                p.spawn(clients[0].exec_command, sudo=True, cmd=f"ceph mds fail {mds}")
        retry_mds_status(
            clients[0],
            len(mds_name_list),
            vol_name=default_fs,
            expected_status="standby",
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} joinable true")
        retry_mds_status(
            clients[0],
            len(mds_name_list),
            vol_name=default_fs,
            expected_status="active",
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Clean Up in progess")
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(clients[0], vol_name=default_fs, validate=False)
