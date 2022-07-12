import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574024 - Ensure Snapshot and cloning works on nfs exports
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>

    Test operation:
    1. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Crete cephfs subvolume group
    3. Create cephfs subvolume in cephfs subvolume group
    4. Create cephfs subvolume in deafault cephfs subvolume group
    5. Mount nfs mount with cephfs export
       "mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
    6. Run IOs on both cephfs subvolumes
    7. Create snapshots of both cephfs subvolumes
    8. Create clone of both cephfs subvolumes from snapshots
    9. Verify data is consistent across subvolumes, snapshots & clones

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    3. Delete cephfs nfs export
    """
    try:
        tc = "CEPH-83574024"
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
        subvolumegroup = {
            "vol_name": fs_name,
            "group_name": "subvolume_group1",
        }
        fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume_list = [
            {
                "vol_name": fs_name,
                "subvol_name": "subvolume1",
                "group_name": "subvolume_group1",
            },
            {
                "vol_name": fs_name,
                "subvol_name": "subvolume2",
            },
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(client1, **subvolume)
        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_name, nfs_mounting_dir
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} subvolume1 --group_name subvolume_group1",
        )
        subvolume1_path = out.rstrip()
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {fs_name} subvolume2"
        )
        subvolume2_path = out.rstrip()
        commands = [
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --file-size 4 "
            f"--files 1000 --top {nfs_mounting_dir}{subvolume1_path}",
            f"for n in {{1..20}}; do     dd if=/dev/urandom of={nfs_mounting_dir}{subvolume2_path}"
            f"/file$(printf %03d "
            "$n"
            ") bs=500k count=1000; done",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        commands = [
            f"ceph fs subvolume snapshot create {fs_name} subvolume1 snap1 --group_name subvolume_group1",
            f"ceph fs subvolume snapshot create {fs_name} subvolume2 snap2",
        ]
        for command in commands:
            out, err = client1.exec_command(sudo=True, cmd=command)
        clone_status_1 = {
            "vol_name": fs_name,
            "subvol_name": "subvolume1",
            "snap_name": "snap1",
            "target_subvol_name": "clone1",
            "group_name": "subvolume_group1",
            "target_group_name": "subvolume_group1",
        }
        fs_util.create_clone(client1, **clone_status_1)
        fs_util.validate_clone_state(client1, clone_status_1, timeout=6000)
        clone_status_2 = {
            "vol_name": fs_name,
            "subvol_name": "subvolume2",
            "snap_name": "snap2",
            "target_subvol_name": "clone2",
        }
        fs_util.create_clone(client1, **clone_status_2)
        fs_util.validate_clone_state(client1, clone_status_2, timeout=6000)
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} clone1 --group_name subvolume_group1",
        )
        clone1_path = out.rstrip()
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {fs_name} clone2"
        )
        clone2_path = out.rstrip()
        commands = [
            f"diff -r {nfs_mounting_dir}{subvolume1_path} {nfs_mounting_dir}{subvolume1_path}/.snap/_snap1*",
            f"diff -r {nfs_mounting_dir}{subvolume2_path} {nfs_mounting_dir}{subvolume2_path}/.snap/_snap2*",
            f"diff -r {nfs_mounting_dir}{subvolume1_path} {nfs_mounting_dir}{clone1_path}",
            f"diff -r {nfs_mounting_dir}{subvolume2_path} {nfs_mounting_dir}{clone2_path}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume snapshot rm {fs_name} subvolume1 snap1 --group_name subvolume_group1",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume snapshot rm {fs_name} subvolume2 snap2",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/*")
        client1.exec_command(sudo=True, cmd=f"umount {nfs_mounting_dir}")
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/", check_ec=False
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
