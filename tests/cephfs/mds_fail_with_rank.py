import json
import secrets
import string
import traceback
from datetime import datetime, timedelta

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log
from utility.retry import retry

log = Log(__name__)
global stop_flag


def start_io_time(fs_util, client1, mounting_dir, timeout=300):
    global stop_flag
    stop_flag = False
    iter = 0
    if timeout:
        stop = datetime.now() + timedelta(seconds=timeout)
    else:
        stop = 0

    while not stop_flag:
        if stop and datetime.now() > stop:
            log.info("Timed out *************************")
            break
        client1.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}/run_ios_{iter}")
        fs_util.run_ios(
            client1, f"{mounting_dir}/run_ios_{iter}", io_tools=["smallfile"]
        )
        iter = iter + 1


def run(ceph_cluster, **kw):
    """
    CEPH-83573429 - [Cephfs] Fail mds daemon by it's rank

    Test Steps:
    1. Mount Fuse and Kernel mounts
    2. Run IOs and perform mds fail on all the active mds
    3. validate mds nodes with previous and current
    Args:
        ceph_cluster:
        **kw:

    Returns:

    """
    try:

        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            fs_util_v1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))

        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(clients[0], fs_name)

        if not fs_details:
            fs_util_v1.create_fs(clients[0], fs_name)
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        kernel_mount_dir = "/mnt/kernel_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs_name}",
        )
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )

        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        global stop_flag
        with parallel() as p:
            p.spawn(
                start_io_time,
                fs_util_v1,
                clients[0],
                fuse_mount_dir,
                timeout=0,
            )
            p.spawn(
                start_io_time,
                fs_util_v1,
                clients[0],
                kernel_mount_dir,
            )
            result, rc = clients[0].exec_command(
                sudo=True, cmd=f"ceph fs status {fs_name} --format json-pretty"
            )
            mds_name_list = []
            result_json = json.loads(result)
            active_mds_before = []
            for elem in result_json["mdsmap"]:
                if elem["state"] == "active":
                    mds_name_list.append(elem["rank"])
                    active_mds_before.append(elem["name"])
            log.info(f"Active mds before mds fail {active_mds_before}")
            with parallel() as p:
                for mds in mds_name_list:
                    p.spawn(
                        clients[0].exec_command, sudo=True, cmd=f"ceph mds fail {mds}"
                    )
            retry_mds_status = retry(CommandFailed, tries=3, delay=30)(
                fs_util_v1.get_mds_status
            )
            active_mds_after = retry_mds_status(
                clients[0], len(mds_name_list), vol_name=fs_name
            )
            log.info("Setting stop flag")
            stop_flag = True
            if active_mds_before == active_mds_after:
                log.error(
                    f"Active mds not changed before : {active_mds_before} After : {active_mds_after}"
                )
                return 1
            log.info(
                f"Active mds changed before : {active_mds_before} After : {active_mds_after}"
            )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mount_dir
        )
        fs_util_v1.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mount_dir,
        )
