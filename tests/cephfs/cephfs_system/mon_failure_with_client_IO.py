import json
import secrets
import string
import traceback
from datetime import datetime, timedelta
from time import sleep

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


global stop_flag


def start_io_time(fs_util, client1, mounting_dir, timeout=600):
    global stop_flag
    stop_flag = False
    iter = 0
    if timeout:
        stop = datetime.now() + timedelta(seconds=timeout)
    else:
        stop = 0
    while True:
        if stop and datetime.now() > stop:
            log.info("Timed out *************************")
            break
        mount_type = "fuse" if "fuse" in mounting_dir else "kernel"
        client1.exec_command(
            sudo=True,
            cmd=f"mkdir -p {mounting_dir}/{mount_type}_{__name__}_run_ios_{iter}",
        )
        fs_util.run_ios(
            client1,
            f"{mounting_dir}/{mount_type}_{__name__}_run_ios_{iter}",
            io_tools=["smallfile"],
        )
        iter = iter + 1
        if stop_flag:
            break


# Retry wrapper for retry_check_ceph_healthy
@retry(CommandFailed, tries=3, delay=60)
def retry_check_ceph_healthy(client, num_osds, num_mons, build, mon_container, timeout):
    """Wrapper that raises CommandFailed if cluster is not healthy"""
    result = check_ceph_healthly(
        client, num_osds, num_mons, build, mon_container, timeout
    )
    if result != 0:
        raise CommandFailed("Cluster is not healthy")
    return result


def run(ceph_cluster, **kw):
    """
    CEPH-11261 - MON node power failure, with client IO

    Test Steps:
    1. Mount Fuse and Kernel mounts
    2. Run IOs and perform mon power off parallel
    3. Do this on all the mon nodes in serial fashion
    Args:
        ceph_cluster:
        **kw:

    Returns:

    """
    try:
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        fs_util = FsUtils(ceph_cluster)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        mon_nodes = ceph_cluster.get_ceph_objects("mon")
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
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(clients[0], fs_name)

        if not fs_details:
            fs_util_v1.create_fs(clients[0], fs_name)
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
        num_of_osds = int(fs_util.get_osd_count(clients[0]))
        log.info("Number of OSD from cluster: {}".format(num_of_osds))
        out, _ = clients[0].exec_command(sudo=True, cmd="ceph -s -f json")
        cluster_status = json.loads(out)
        all_mons = cluster_status["monmap"]["num_mons"]

        with parallel() as p:
            p.spawn(
                start_io_time,
                fs_util_v1,
                clients[0],
                fuse_mount_dir,
                timeout=1200,
            )
            p.spawn(
                start_io_time,
                fs_util_v1,
                clients[0],
                kernel_mount_dir,
            )
            global stop_flag
            for mon in mon_nodes:
                cluster_health_beforeIO = retry_check_ceph_healthy(
                    clients[0],
                    num_of_osds,
                    all_mons,
                    build,
                    None,
                    300,
                )
                try:
                    mon.node.vm_node.shutdown(wait=True)
                    sleep(10)
                    mon.node.vm_node.power_on()
                except Exception as e:
                    log.error(e)
                    log.error(traceback.format_exc())
                    log.error("Failed While doing power off operation")
                    stop_flag = True
                cluster_health_afterIO = retry_check_ceph_healthy(
                    clients[0],
                    num_of_osds,
                    all_mons,
                    build,
                    None,
                    300,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info("Setting stop flag")
            stop_flag = True
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
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
