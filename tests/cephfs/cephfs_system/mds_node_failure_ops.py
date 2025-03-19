import secrets
import string
import traceback
from datetime import datetime, timedelta
from time import sleep

from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)

global stop_flag
stop_flag = False


def start_io_time(fs_util, client1, mounting_dir, timeout=300):
    global stop_flag
    iter = 0
    if timeout:
        stop = datetime.now() + timedelta(seconds=timeout)
    else:
        stop = 0
    while True:
        if stop and datetime.now() > stop:
            log.info("Timed out *************************")
            break
        client1.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}/run_ios_{iter}")
        fs_util.run_ios(
            client1, f"{mounting_dir}/run_ios_{iter}", io_tools=["smallfile"]
        )
        iter = iter + 1
        if stop_flag:
            log.info("Exited as stop flag is set to True")
            break


def run(ceph_cluster, **kw):
    """
    CEPH-11256 - MDS failure/recovery with single and multiple services and client IOs
    - MDS daemon restart
    - MDS node reboot
    - MDS daemon kill
    - MDS daemon stop and start
    - MDS network disconnect

    Test Steps:
    1. Mount Fuse and Kernel mounts
    2. Run IOs and perform mds power off parallel
    3. Do this on all the mds operations in serial fashion
    Args:
        ceph_cluster:
        **kw:

    Returns:

    """
    try:
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        mds_nodes = ceph_cluster.get_ceph_objects("mds")

        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        osp_cred = config.get("osp_cred")
        num_of_osds = config.get("num_of_osds")
        build = config.get("build", config.get("rhbuild"))
        print(osp_cred)

        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(clients[0], fs_name)

        if not fs_details:
            fs_util_v1.create_fs(clients[0], fs_name)
        host_list = [mdsnode.node.hostname for mdsnode in mds_nodes]
        hosts = " ".join(host_list)
        mds_nodes = fs_util_v1.get_mds_nodes(clients[0])
        log.info(f"Operations will be done on this nodes:{mds_nodes}")
        clients[0].exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
        )
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
            global stop_flag
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

            for mds in mds_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.reboot_node(ceph_node=mds)
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info("Rebooted all the nodes and cluster is Healthy")
            for mds in mds_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.deamon_op(mds, rf"mds\.{fs_name}\.", "restart")
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info("Restarted all the mon services and cluster is Healthy")

            for mds in mds_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.pid_kill(mds, "mds")
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info("killed all the mon services and cluster is Healthy")
            for mds in mds_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                deamon_name = fs_util_v1.deamon_name(mds, "mds")

                try:
                    fs_util_v1.deamon_op(mds, rf"mds\.{fs_name}\.", "stop")
                    sleep(60)
                    fs_util_v1.deamon_op(
                        mds, rf"mds\.{fs_name}\.", "start", service_name=deamon_name
                    )
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())
                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info(
                "mds services have been stopped and started and cluster is Healthy"
            )
            for mds in mds_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.network_disconnect(mds)
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info(
                "mds services have been stopped and started and cluster is Healthy"
            )
            log.info("Setting stop flag")
            stop_flag = True
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        stop_flag = True
        return 1
    finally:
        log.info("Cleaning up the system")
        stop_flag = True
        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mount_dir
        )
        fs_util_v1.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mount_dir,
        )
