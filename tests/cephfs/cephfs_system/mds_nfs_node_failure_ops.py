import json
import secrets
import string
import traceback
from datetime import datetime, timedelta
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run_io_commands(client, io_commands):
    for command in io_commands:
        client.exec_command(sudo=True, cmd=command, long_running=True)


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
        fs_util.run_ios(client1, f"{mounting_dir}/run_ios_{iter}/", io_tools=["dd"])
        client1.exec_command(sudo=True, cmd=f"rm -rf {mounting_dir}/run_ios_{iter}")
        iter = iter + 1


@retry(CommandFailed, tries=3, delay=60)
def check_nfs_ls(client, nfs_cluster):
    try:
        out, rc = client.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
        output = json.loads(out)
    except JSONDecodeError:
        output = json.dumps([out])
    if nfs_cluster in output:
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")


def run(ceph_cluster, **kw):
    """
    CEPH-11311 - NFS service Test:Kill MDS process while NFS I/O is runningNFS service restart TestNFS node
        reboot testNFS node power failure test
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Create nfs cluster with same name
       ceph nfs cluster create <nfs_name> <nfs_server>
    2. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    3. Mount nfs mount with cephfs export
       mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
    4. Run some IO's
    5. parallely run network disconnect, pid, deamon restart, reboot node and deamon stop and start
    Clean-up:
    1. Remove data in cephfs
    2. Remove cephfs nfs export
    3. Remove all nfs mounts
    """
    try:
        tc = "CEPH-11311"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        nfs_nodes = ceph_cluster.get_ceph_objects("nfs")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster create {nfs_name} {nfs_servers[0].node.hostname},{nfs_servers[1].node.hostname}",
        )
        if not wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            raise CommandFailed("Cluster has not been created")
        check_nfs_ls(client1, nfs_name)
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(clients[0], fs_name)

        if not fs_details:
            fs_util.create_fs(clients[0], fs_name)

        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_name} path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )
        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_name, nfs_mounting_dir
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        commands = [
            f"mkdir {nfs_mounting_dir}/dir1 {nfs_mounting_dir}/dir2",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir1",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir1",
        ]

        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)

        with parallel() as p:
            p.spawn(
                start_io_time,
                fs_util,
                clients[0],
                nfs_mounting_dir,
                timeout=0,
            )
            global stop_flag
            for mds in mds_nodes:
                fs_util.pid_kill(mds, "mds")

            for nfs in nfs_nodes:
                fs_util.deamon_op(nfs, "nfs", "restart")
                log.info("deamon restarted successfully")

            for nfs in nfs_nodes:
                fs_util.pid_kill(nfs, "nfs")
                log.info("demon killed successfully")

            for nfs in nfs_nodes:
                fs_util.reboot_node(ceph_node=nfs)
                log.info("node rebooted successfully")

            for nfs in nfs_nodes:
                fs_util.network_disconnect(nfs)
                log.info("node network disconnected successfully")
            stop_flag = True
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        commands = [
            f"rm -rf {nfs_mounting_dir}/*",
            f"umount {nfs_mounting_dir}",
            f"ceph nfs export delete {nfs_name} {nfs_export_name}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/", check_ec=False
        )
