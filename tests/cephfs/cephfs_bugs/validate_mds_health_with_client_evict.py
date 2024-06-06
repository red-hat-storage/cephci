import datetime
import json
import re
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
         CEPH-83591136 - Verify Ceph Health Warning for Slow MDS Requests After Cluster Installation.

        Steps:

        1. Create File system with 1 active and 1 standby MDS
        2. mount the file system
        3. Create a file and keep writing data from different Threads
        4. Evict the client and unblock the client in loop client should evicted and joined back
        5. Do step 3 and step 4 in parallel for 100 times

        Cleanup:
        1. unmount the FS
        2. Remove FS

        """
        tc = "CEPH-83591136"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_mds_health"
        fs_util.create_fs(client1, fs_name)
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 1")
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            extra_params=f" --client_fs {fs_name}",
        )

        client1.upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_bugs/multithreaded_file_writer.py",
            dst="/root/multithreaded_file_writer.py",
        )
        with parallel() as p:
            p.spawn(
                client1.exec_command,
                sudo=True,
                cmd=f"python3 /root/multithreaded_file_writer.py {fuse_mount_dir};",
                check_ec=False,
                long_running=True,
            )
            time.sleep(10)
            p.spawn(get_ceph_health_detail, client1)
            for i in range(1, 100):
                log.info(f"Iteration : {i}")
                evict_unblock_client(fs_util, client1, fs_name, fuse_mount_dir)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False)
        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 0"
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(client1, fs_name)


def evict_unblock_client(fs_util, client1, fs_name, fuse_mount_dir):
    mds_ls = fs_util.get_active_mdss(client1, fs_name=fs_name)
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=60)
    log.info("Wait for the mount to appear")
    while end_time > datetime.datetime.now():
        out, rc = client1.exec_command(
            cmd=f"ceph tell mds.{mds_ls[0]} client ls --format json", sudo=True
        )
        client_details = json.loads(out)
        log.info(client_details)
        if client_details:
            break
        time.sleep(20)
    if not client_details:
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            extra_params=f" --client_fs {fs_name}",
        )
        return
    client_id = client_details[0]["id"]
    client_inst = client_details[0]["inst"]
    client1.exec_command(
        cmd=f"ceph tell mds.{mds_ls[0]} client evict {client_id}", sudo=True
    )
    time.sleep(20)
    out, rc = client1.exec_command(cmd="ceph osd blocklist ls", client_exec=True)
    match = re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}/\d+\b", out)
    if match:
        if match.group(0) in client_inst:
            client1.exec_command(
                cmd=f"ceph osd blocklist rm {match.group(0)}", sudo=True
            )


def get_ceph_health_detail(client1, interval=10, timeout=600):
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    log.info("Wait for the mount to appear")
    while end_time > datetime.datetime.now():
        out, rc = client1.exec_command(sudo=True, cmd="ceph health detail")
        log.info(out)
        if "slow requests" in out:
            raise CommandFailed("MDSs report slow requests")
        time.sleep(interval)
