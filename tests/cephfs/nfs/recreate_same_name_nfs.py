import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574015 - verify if nfs cluster can be deleted. and recreate with the same name works.
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Delete nfs cluster
       ceph nfs cluster delete <nfs_name>
    2. Create nfs cluster with same name
       ceph nfs cluster create <nfs_name> <nfs_server>
    3. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    4. Mount nfs mount with cephfs export
       mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
    5. Run some IO's
    Clean-up:
    1. Remove data in cephfs
    2. Remove cephfs nfs export
    3. Remove all nfs mounts
    """
    try:
        tc = "CEPH-83574028"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls")
        output = out.split()
        if nfs_name in output:
            log.info("ceph nfs cluster is present")
        else:
            raise CommandFailed("ceph nfs cluster is absent")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster delete {nfs_name}"
        )
        if not wait_for_process(client=client1, process_name=nfs_name, ispresent=False):
            raise CommandFailed("Cluster has not been deleted")
        out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls")
        output = out.split()
        if nfs_name not in output:
            log.info("ceph nfs cluster deleted successfully")
        else:
            raise CommandFailed("Failed to delete nfs cluster")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if not wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            raise CommandFailed("Cluster has not been created")
        out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls")
        output = out.split()
        if nfs_name in output:
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs"
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
            f"mkdir {nfs_mounting_dir}/dir1 {nfs_mounting_dir}/dir2"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir1",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir1",
            f"for n in {{1..20}}; do     dd if=/dev/urandom of={nfs_mounting_dir}/dir2"
            f"/file$(printf %03d "
            "$n"
            ") bs=500k count=1000; done",
            f"dd if={nfs_mounting_dir}/dir2/file1 of={nfs_mounting_dir}/dir2/copy_file1 bs=500k count=1000; done",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
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
