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
    CEPH-11310 - Mount same volume on fuse,kernel and nfs and runIOs
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
    4. mount subvolume on all the mounts nfs,fuse and kernel
    5. Run IOs on all the mounts
    6. Validate data on all the mounts

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    3. Delete cephfs nfs export
    """
    try:
        tc = "CEPH-11310"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ips = fs_util.get_mon_node_ips()
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        fs_name = "cephfs"
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
        subvolumegroup = {
            "vol_name": fs_name,
            "group_name": "subvolume_group1",
        }
        fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": fs_name,
            "subvol_name": "subvolume1",
            "group_name": "subvolume_group1",
        }

        fs_util.create_subvolume(client1, **subvolume)
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} subvolume1 subvolume_group1",
        )
        export_path = f"{subvol_path}"
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

        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_name, nfs_mounting_dir
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()}",
        )
        client1.exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mounting_dir_1}/kernel_dir"
        )
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}/fuse_dir")
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}/nfs_dir")
        mount_points = [
            f"{kernel_mounting_dir_1}/kernel_dir",
            f"{fuse_mounting_dir_1}/fuse_dir",
            f"{nfs_mounting_dir}/nfs_dir",
        ]
        for mount in mount_points:
            clients[0].exec_command(sudo=True, cmd=f"rm -rf {mount}/*")
            fs_util.run_ios(clients[0], mount)
        log.info("Test completed successfully")
        client1.exec_command(
            sudo=True, cmd=f"diff -qr {nfs_mounting_dir} {kernel_mounting_dir_1}"
        )
        client1.exec_command(
            sudo=True, cmd=f"diff -qr {fuse_mounting_dir_1} {kernel_mounting_dir_1}"
        )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
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
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[client1],
            mounting_dir=kernel_mounting_dir_1,
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_1
        )
