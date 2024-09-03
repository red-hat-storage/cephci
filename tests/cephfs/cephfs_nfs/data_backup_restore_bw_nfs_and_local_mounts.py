import json
import random
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
    CEPH-11314 - Backup and restore data using existing NFS backup tools
        Pre-requisites:
    1. Create cephfs volume
       create fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Create cephfs nfs export with a valid path
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Verify path of cephf s nfs export
       ceph nfs export get <nfs_name> <nfs_export_name>
    3. Mount nfs export.
    4. Backup the data from the nfs mount to local and remote client.
    5. Restore the data back to NFS mount from local and remote client

    Clean-up:
    1. Remove cephfs nfs export
    2. Remove NFS Cluster
    """
    try:
        tc = "CEPH-11314"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.setup_ssh_root_keys(clients)
        client1 = clients[0]
        client2 = clients[1]
        client1_ip = clients[0].node.ip_address
        client2_ip = clients[1].node.ip_address
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
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
        out, rc = client1.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")
        if nfs_export_name not in out:
            raise CommandFailed("Failed to create nfs export")
        log.info("ceph nfs export created successfully")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}"
        )
        output = json.loads(out)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir_c1 = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir_c1}")
        nfs_mounting_dir_c2 = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client2.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir_c2}")
        command = f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir_c1}"
        output, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        command = f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir_c2}"
        output, err = client2.exec_command(sudo=True, cmd=command, check_ec=False)
        local_dir = f"/mnt/local_{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {local_dir}")
        remote_dir = f"/mnt/remote_{mounting_dir}_1/"
        client2.exec_command(sudo=True, cmd=f"mkdir -p {remote_dir}")

        io_tool = ["dd"]
        io_file_name = "file_nfs"
        fs_util.run_ios(clients[0], nfs_mounting_dir_c1, io_tool, f"{io_file_name}1")
        fs_util.run_ios(clients[0], nfs_mounting_dir_c1, io_tool, f"{io_file_name}2")
        nfspath1 = f"{nfs_mounting_dir_c1}{clients[0].node.hostname}_dd_file_nfs1"
        nfspath2 = f"{nfs_mounting_dir_c1}{clients[0].node.hostname}_dd_file_nfs2"

        log.info("Backup the data from NFS mounts to a local dir.")
        commands = [
            f"rsync {nfspath1} {local_dir}",
            f"scp {nfspath2} {local_dir}",
        ]
        for cmd in commands:
            clients[0].exec_command(sudo=True, cmd=cmd)
        log.info(" Backup the date from NFS mount to remote dir")
        commands = [
            f"rsync {nfspath1} {client2_ip}:{remote_dir}",
            f"scp {nfspath2} {client2_ip}:{remote_dir}",
        ]
        for cmd in commands:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Confirm if data is moved from NFS mount to Local path")
        commands = [
            f"diff -qr {nfspath1} {local_dir}*nfs1",
            f"diff -qr {nfspath2} {local_dir}*nfs2",
        ]
        for cmd in commands:
            clients[0].exec_command(sudo=True, cmd=cmd)
        log.info("Confirm if data is moved from NFS mount to Remote path")
        nfspath1_c2 = f"{nfs_mounting_dir_c2}{clients[0].node.hostname}_dd_file_nfs1"
        nfspath2_c2 = f"{nfs_mounting_dir_c2}{clients[0].node.hostname}_dd_file_nfs2"
        commands = [
            f"diff -qr {nfspath1_c2} {remote_dir}*nfs1",
            f"diff -qr {nfspath2_c2} {remote_dir}*nfs2",
        ]
        for cmd in commands:
            clients[1].exec_command(sudo=True, cmd=cmd)

        log.info("Delete the data from NFS mounts")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir_c1}*")

        log.info("Restore the data from Local dir to NFS mounts")
        cmd = f"mv {local_dir}*nfs1 {nfs_mounting_dir_c1}"
        clients[0].exec_command(sudo=True, cmd=cmd)
        log.info("Restore the data from Remote dir to NFS mounts")
        cmd = f"scp {remote_dir}*nfs2 {client1_ip}:{nfs_mounting_dir_c1}"
        clients[1].exec_command(sudo=True, cmd=cmd)

        log.info("Confirm if data is moved from Remote path to NFS mount")
        cmd = f"stat {nfs_mounting_dir_c1}*"
        clients[0].exec_command(sudo=True, cmd=cmd)
        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir_c1}", check_ec=False
        )
        client2.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir_c2}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir_c1}", check_ec=False
        )
        client2.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir_c2}", check_ec=False
        )
        client1.exec_command(sudo=True, cmd=f"rm -rf {local_dir}", check_ec=False)
        client2.exec_command(sudo=True, cmd=f"rm -rf {remote_dir}", check_ec=False)
        log.info("Removing the Export")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
        log.info("Removing NFS Cluster")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster rm {nfs_name}",
            check_ec=False,
        )
