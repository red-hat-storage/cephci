import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574027 - Ensure creation of Subvolgroups, subvolumes works on NFS exports and run IO from nfs clients
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>

    Test operation:
    1. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Crete 2 cephfs subvolume group
    3. Create cephfs subvolume in cephfs subvolume group
    4. Create cephfs subvolume in deafault cephfs subvolume group
    5. Mount nfs mount with cephfs export
       "mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
    7. Verify subvolume groups & subvolumes are created
    6. Run IOs on both cephfs subvolumegroups & subvolumes

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    3. Delete cephfs nfs export
    """
    try:
        tc = "CEPH-83574027"
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
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs"
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
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
        subvolumegroup_list = [
            {
                "vol_name": fs_name,
                "group_name": "subvolgroup_1",
            },
            {
                "vol_name": fs_name,
                "group_name": "subvolgroup_2",
            },
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)
        subvolume_list = [
            {
                "vol_name": fs_name,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368706371",
            },
            {
                "vol_name": fs_name,
                "subvol_name": "subvol_2",
                "size": "5368706371",
            },
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)
        commands = [
            f"mkdir -p {nfs_mounting_dir}",
            f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        out, rc = client1.exec_command(sudo=True, cmd=f"ls {nfs_mounting_dir}/volumes/")
        if "subvolgroup_1" not in out:
            raise CommandFailed("Subvolume group 1 creation failed")
        if "subvolgroup_2" not in out:
            raise CommandFailed("Subvolume group 2 creation failed")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ls {nfs_mounting_dir}/volumes/subvolgroup_1"
        )
        if "subvol_1" not in out:
            raise CommandFailed("Subvolume creation in subvolume group failed")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ls {nfs_mounting_dir}/volumes/_nogroup"
        )
        if "subvol_2" not in out:
            raise CommandFailed("Subvolume creation in default subvolume group failed")
        commands = [
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/volumes/subvolgroup_1/subvol_1",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/volumes/subvolgroup_1/subvol_1",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 8 "
            f"--files 2000 --files-per-dir 5 --dirs-per-dir 5 --top {nfs_mounting_dir}/volumes/_nogroup/subvol_2/",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 8 "
            f"--files 2000 --files-per-dir 5 --dirs-per-dir 5 --top {nfs_mounting_dir}/volumes/_nogroup/subvol_2/",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 5 --file-size 16 "
            f"--files 4000 --files-per-dir 20 --dirs-per-dir 4 --top {nfs_mounting_dir}/volumes/subvolgroup_2",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 5 --file-size 16 "
            f"--files 4000 --files-per-dir 20 --dirs-per-dir 4 --top {nfs_mounting_dir}/volumes/subvolgroup_2",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/*")
        client1.exec_command(sudo=True, cmd=f"umount {nfs_mounting_dir}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
