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
        log.info(f"Current mon count = {output}")
        log.info(f"Expected mon count = {count}")
        if output == count:
            return 0
        sleep(interval)
    raise CommandFailed


def mon_rm_add(fs_util, mons, client):
    try:
        global stop_flag
        log.info("removing mon")
        mon_hosts = []
        for mon_host in mons:
            mon_hosts.append(mon_host.node.hostname)
        mons_hosts = " ".join([str(elem) for elem in mon_hosts[:-1]])
        initial_mon_count = str(len(mon_hosts))
        initial_mon_count.rstrip()
        count = str(int(initial_mon_count) - 1)
        cmd = f"ceph orch  --verbose apply mon cephfs --placement='{mons_hosts}'"
        client.exec_command(sudo=True, cmd=cmd)
        wait_for_cmd(client, "mon", count)
        mons_hosts = " ".join([str(elem) for elem in mon_hosts])
        cmd = f"ceph orch  --verbose apply mon cephfs --placement='{mons_hosts}'"
        client.exec_command(sudo=True, cmd=cmd)
        wait_for_cmd(client, "mon", initial_mon_count)
        stop_flag = True

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        raise CommandFailed


def run(ceph_cluster, get_fs_info=None, **kw):
    try:
        """
        CEPH-11345 - MON node/service add and removal test, with client IO
        Pre-requisites :
        1. Create cephfs volume

        Test Case Flow:
        1. Start IOs
        2. remove 1 mds
        3. IO's shouldn't fail
        4. add 1 mds
        5. IO's shouldn't fail

        Cleanup:
        1. Remove all the data in cephfs
        2. Remove all the mounts
        """
        tc = "CEPH-11259"
        log.info("Running cephfs %s test case" % (tc))

        fs_util = FsUtils(ceph_cluster)
        client = ceph_cluster.get_ceph_objects("client")
        mons = ceph_cluster.get_ceph_objects("mon")
        client1 = client[0]
        mount_dir = "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        kernel_mount_dir = f"/mnt/kernel_{mount_dir}"
        fuse_mount_dir = f"/mnt/fuse_{mount_dir}"
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs_util.kernel_mount(
            [client1], kernel_mount_dir, mon_node_ip, new_client_hostname="admin"
        )
        fs_util.fuse_mount([client1], fuse_mount_dir, new_client_hostname="admin")
        with parallel() as p:
            p.spawn(mon_rm_add, fs_util, mons, client1)
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
