# datetime module supplies classes to work with date and time
import datetime
import json
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def check_clean_pgs(client, timeout=300):
    """
    Verify all pgs are active+clean
    :param client: client node
    :param timeout: timeout for checking pg state
    """
    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()
    out, rc = client[0].exec_command(sudo=True, cmd="ceph pg stat --format json")
    output = json.loads(out)
    total_pgs = output["pg_summary"]["num_pgs"]
    log.info(f"total pgs = {total_pgs}")
    while datetime.datetime.now() - starttime <= timeout:
        out, rc = client[0].exec_command(sudo=True, cmd="ceph pg stat --format json")
        out = json.loads(out)
        output = out["pg_summary"]["num_pg_by_state"]
        active_clean_pgs = ""
        for item in output:
            if item["name"] == "active+clean":
                active_clean_pgs = item["num"]
                log.info(f"active+clean pgs = {active_clean_pgs}")
        if active_clean_pgs == total_pgs:
            log.info("All pgs are active+clean")
            return 0
    log.error("pgs are not active+clean")
    return 1


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Disable pg_autoscale_mode for cephfs pools if set
       ceph osd pool set cephfs_data pg_autoscale_mode off
       ceph osd pool set cephfs_metadata pg_autoscale_mode off
    3. Configure 2 clients with Fuse client and another 1 client with kernel client

    Test operation:
    1. Run IO's on both clients
    2. Verify there are no "heartbeat_map" timeout issue in logs
    3. Fill up cluster upto 20%
    4. Change cephfs data and metadata pool pg_num and pgp_num to existing size "-1" with client IO running
    5. Wait for cluster to come to active + clean state , while running IO's
    6. Write some more data to the cluster
    7. Change cephfs data and metadata pool pg_num and pgp_num to existing size "+1" with client IO running
    8. Wait for cluster to come to active + clean state , while running IO's

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    3. Reset pg_autoscale_mode for cephfs pools to on
    """
    try:
        tc = "CEPH-83574596"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        rhbuild = config.get("rhbuild")
        build = config.get("build", config.get("rhbuild"))
        mdss = ceph_cluster.get_ceph_objects("mds")

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        mount_points = []
        kernel_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.kernel_mount(
            clients, kernel_mount_dir, mon_node_ip, new_client_hostname="admin"
        )
        fuse_mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(clients, fuse_mount_dir, new_client_hostname="admin")
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        if "4." in rhbuild:
            data_pool = "cephfs_data"
            metadata_pool = "cephfs_metadata"
        else:
            data_pool = "cephfs.cephfs.data"
            metadata_pool = "cephfs.cephfs.meta"
        commands = [
            f"ceph osd pool set {data_pool} pg_autoscale_mode off",
            f"ceph osd pool set {metadata_pool} pg_autoscale_mode off",
        ]
        for command in commands:
            clients[0].exec_command(sudo=True, cmd=command, long_running=True)
        data_pool_pg_num, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph osd pool get {data_pool} pg_num | awk '{{print $2}}'"
        )
        metadata_pool_pg_num, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph osd pool get {metadata_pool} pg_num | awk '{{print $2}}'",
        )
        for num in range(1, 7):
            log.info("Creating Directories")
            out, rc = clients[0].exec_command(
                sudo=True, cmd="mkdir %s/%s%d" % (kernel_mount_dir, "dir", num)
            )
        log.info("Running IO's to get cluster upto 20% capacity")
        no_of_files = "{1..10}"
        commands = [
            f'for n in {no_of_files}; do dd if=/dev/urandom of={kernel_mount_dir}/dir1/file$( printf %03d "$n" )'
            f" bs=1M count=1000; done",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --top {kernel_mount_dir}/dir2",
        ]
        for command in commands:
            clients[0].exec_command(sudo=True, cmd=command, long_running=True)
        log.info("Checking for heartbeat map timeout issue")
        rc = fs_util.heartbeat_map(mdss[0])
        if rc == 1:
            log.error("heartbeat map timeout issue found")
            return 1
        log.info(
            "Changing cephfs data and metadata pool pg_num and pgp_num to existing size '-1' with client IO running"
        )
        no_of_files = "{1..1000}"
        data_pool_pg_num = str(int(data_pool_pg_num) - 1)
        metadata_pool_pg_num = str(int(metadata_pool_pg_num) - 1)
        commands = [
            f'for n in {no_of_files}; do dd if=/dev/urandom of={kernel_mount_dir}/dir3/file$( printf %03d "$n" )'
            f" bs=1M count=10; done",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --top {kernel_mount_dir}/dir4",
            f"ceph osd pool set {data_pool} pg_num {data_pool_pg_num}",
            f"ceph osd pool set {data_pool} pgp_num {data_pool_pg_num}",
            f"ceph osd pool set {metadata_pool} pg_num {metadata_pool_pg_num}",
            f"ceph osd pool set {metadata_pool} pgp_num {metadata_pool_pg_num}",
        ]
        with parallel() as p:
            for num in range(0, 6):
                p.spawn(clients[0].exec_command, sudo=True, cmd=commands[num])
                time.sleep(1)
        log.info("Verifying pgs are in active+clean state")
        rc = check_clean_pgs(clients)
        if rc == 1:
            return 1
        log.info(
            "Change cephfs data and metadata pool pg_num and pgp_num to existing size '+1' with client IO running"
        )
        data_pool_pg_num = str(int(data_pool_pg_num) + 1)
        metadata_pool_pg_num = str(int(metadata_pool_pg_num) + 1)
        commands = [
            f'for n in {no_of_files}; do     dd if=/dev/urandom of={kernel_mount_dir}/dir5/file$( printf %03d "$n" )'
            f" bs=1M count=10; done",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --top {kernel_mount_dir}/dir6",
            f"ceph osd pool set {data_pool} pg_num {data_pool_pg_num}",
            f"ceph osd pool set {data_pool} pgp_num {data_pool_pg_num}",
            f"ceph osd pool set {metadata_pool} pg_num {metadata_pool_pg_num}",
            f"ceph osd pool set {metadata_pool} pgp_num {metadata_pool_pg_num}",
        ]
        with parallel() as p:
            for num in range(0, 6):
                p.spawn(clients[0].exec_command, sudo=True, cmd=commands[num])
                time.sleep(1)
        log.info("Verifying pgs are in active+clean state")
        rc = check_clean_pgs(clients)
        if rc == 1:
            return 1
        return 0
    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        out, rc = clients[0].exec_command(sudo=True, cmd=f"rm -rf {mount_points[1]}/*")
        for mount_point in mount_points:
            clients[0].exec_command(sudo=True, cmd=f"umount {mount_point}")
            if "4." in rhbuild:
                commands = [
                    f"ceph osd pool set {data_pool} pg_autoscale_mode warn",
                    f"ceph osd pool set {metadata_pool} pg_autoscale_mode warn",
                ]
                for command in commands:
                    clients[0].exec_command(sudo=True, cmd=command, long_running=True)
            else:
                commands = [
                    f"ceph osd pool set {data_pool} pg_autoscale_mode on",
                    f"ceph osd pool set {metadata_pool} pg_autoscale_mode on",
                ]
                for command in commands:
                    clients[0].exec_command(sudo=True, cmd=command, long_running=True)
        for mount_point in mount_points:
            clients[0].exec_command(sudo=True, cmd=f"rm -rf {mount_point}")
