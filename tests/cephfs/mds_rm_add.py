import secrets
import string
import traceback
from datetime import datetime, timedelta
from time import sleep

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


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


def wait_for_cmd(client1, process_name, count, timeout=180, interval=5):
    end_time = datetime.now() + timedelta(seconds=timeout)
    log.info("Wait for the process to start or stop")
    while end_time > datetime.now():
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph orch ps | grep {process_name} | wc -l ",
            check_ec=False,
        )
        out2, rc = client1.exec_command(
            sudo=True, cmd=f"ceph orch ps | grep {process_name} ", check_ec=False
        )
        print(out2)
        output = out.rstrip()
        log.info(f"Current mds count = {output}")
        log.info(f"Expected mds count = {count}")
        if output == count:
            return 0
        sleep(interval)
    raise CommandFailed


def mds_rm_add(fs_util, mdss, client, fs_name="cephfs"):
    try:
        global stop_flag
        log.info("removing mds")
        mds_hosts = []
        for mds_host in mdss:
            mds_hosts.append(mds_host.node.hostname)
        mdss_hosts = " ".join([str(elem) for elem in mds_hosts[:-1]])
        initial_mds_count = str(len(mds_hosts))
        initial_mds_count.rstrip()
        count = str(int(initial_mds_count) - 1)
        cmd = f"ceph orch  --verbose apply mds {fs_name} --placement='{mdss_hosts}'"
        client.exec_command(sudo=True, cmd=cmd)
        wait_for_cmd(client, f"mds.{fs_name}", count)
        mdss_hosts = " ".join([str(elem) for elem in mds_hosts])
        cmd = f"ceph orch  --verbose apply mds {fs_name} --placement='{mdss_hosts}'"
        client.exec_command(sudo=True, cmd=cmd)
        wait_for_cmd(client, f"mds.{fs_name}", initial_mds_count)
        stop_flag = True

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        raise CommandFailed


def run(ceph_cluster, get_fs_info=None, **kw):
    try:
        """
        CEPH-11259 - MDS node/service add and removal test, with client
        Pre-requisites :
        1. Create cephfs volume

        Test Case Flow:
        1. Start IOs
        2. remove 1 msd
        3. IO's shouldn't fail
        4. add 1 msd
        5. IO's shouldn't fail

        Cleanup:
        1. Remove all the data in cephfs
        2. Remove all the mounts
        """
        tc = "CEPH-11259"
        log.info("Running cephfs %s test case" % (tc))
        client = ceph_cluster.get_ceph_objects("client")
        mdss = ceph_cluster.get_ceph_objects("mds")
        client1 = client[0]
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)

        mount_dir = "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        kernel_mount_dir = f"/mnt/kernel_{mount_dir}"
        fuse_mount_dir = f"/mnt/fuse_{mount_dir}"
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs_util.kernel_mount(
            [client1],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs_name}",
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        with parallel() as p:
            p.spawn(mds_rm_add, fs_util, mdss, client1, fs_name=fs_name)
            p.spawn(
                start_io_time,
                fs_util,
                client1,
                kernel_mount_dir + "/kernel_1",
                timeout=0,
            )
            p.spawn(
                start_io_time,
                fs_util,
                client1,
                fuse_mount_dir + "/fuse_1",
                timeout=0,
            )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mount_dir}/*")
        client1.exec_command(sudo=True, cmd=f"umount {kernel_mount_dir}")
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mount_dir}/")
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}/")
