import json
import random
import string
import traceback
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573870 - Create 2 Filesystem with default values on different MDS daemons
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 2 file systems with placement arguments
    2. validate the mds came on the specified placements
    3. mount both the file systems and using fuse mount
    4. Run IOs on the FS
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))

        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_list = ["cephfs_df_fs_1", "cephfs_df_fs_2"]
        client1 = clients[0]
        host_list = [
            client1.node.hostname.replace("node7", "node2"),
            client1.node.hostname.replace("node7", "node3"),
        ]
        hosts = " ".join(host_list)

        host_list_1 = [
            client1.node.hostname.replace("node7", "node2"),
            client1.node.hostname.replace("node7", "node4"),
        ]
        hosts_1 = " ".join(host_list_1)
        fs_host_list = [host_list, host_list_1]
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume create {fs_list[0]} --placement='2 {hosts}'",
            check_ec=False,
        )
        fs_util.wait_for_mds_process(client1, fs_list[0])
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume create {fs_list[1]} --placement='2 {hosts_1}'",
            check_ec=False,
        )
        fs_util.wait_for_mds_process(client1, fs_list[1])
        for fs, host_ls in zip(fs_list, fs_host_list):
            validate_services_placements(client1, f"mds.{fs}", host_ls)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        for fs, mount in zip(fs_list, [fuse_mounting_dir_1, fuse_mounting_dir_2]):
            fs_util.fuse_mount([client1], mount, extra_params=f"--client_fs {fs}")

        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{fuse_mounting_dir_1}",
            long_running=True,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{fuse_mounting_dir_2}",
            long_running=True,
        )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_2
        )
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        for fs in fs_list:
            fs_util.remove_fs(client1, fs)
            fs_util.wait_for_mds_process(client1, fs, ispresent=False)


@retry(CommandFailed, tries=3, delay=60)
def validate_services_placements(client, service_name, placement_list):
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph orch ls --service_name={service_name} --format json"
    )
    try:
        service_ls = json.loads(out)
    except JSONDecodeError:
        raise CommandFailed("No services are UP")
    log.info(service_ls)
    if service_ls[0]["status"]["running"] != service_ls[0]["status"]["size"]:
        raise CommandFailed(f"All {service_name} are Not UP")
    if service_ls[0]["placement"]["hosts"] != placement_list:
        raise CommandFailed(
            f"Services did not come up on the hosts: {placement_list} which "
            f"has been specifed but came up on {service_ls[0]['placement']['hosts']}"
        )
