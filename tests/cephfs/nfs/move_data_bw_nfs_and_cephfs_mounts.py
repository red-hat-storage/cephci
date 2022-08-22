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
    CEPH-11309 - Export and import data between NFS and CephFS
        Pre-requisites:
    1. Create cephfs volume
       create fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Create cephfs nfs export with a valid path
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Verify path of cephfs nfs export
       ceph nfs export get <nfs_name> <nfs_export_name>
    3. Mount nfs export.
    4. Mount the exports from cephfs-fuse and cephfs-kernel also.
    5. Move data b/w cephfs mounts and nfs exports and vice versa.

    Clean-up:
    1. Remove cephfs nfs export
    2. Remove NFS Cluster
    """
    try:
        tc = "CEPH-11309"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        default_fs = "cephfs"
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
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}"
        output, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if build.startswith("5"):
            kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_1,
                ",".join(mon_node_ips),
                extra_params=f",fs={default_fs}",
            )

            fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
                extra_params=f" --client_fs {default_fs}",
            )
        else:
            kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_1,
                ",".join(mon_node_ips),
            )

            fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
            )

        run_ios(
            clients[0],
            kernel_mounting_dir_1,
            file_name="dd_file_kernel",
            bs="100M",
            count=20,
        )
        run_ios(
            clients[0],
            fuse_mounting_dir_1,
            file_name="dd_file_fuse",
            bs="100M",
            count=20,
        )
        run_ios(
            clients[0], nfs_mounting_dir, file_name="dd_file_nfs", bs="100M", count=20
        )

        log.info("Migrate data b/w NFS mounts and CephFS mounts(kernel and fuse)")
        kernelpath = f"{kernel_mounting_dir_1}{clients[0].node.hostname}dd_file_kernel"
        fusepath = f"{fuse_mounting_dir_1}{clients[0].node.hostname}dd_file_fuse"
        nfspath = f"{nfs_mounting_dir}{clients[0].node.hostname}dd_file_nfs"
        mv_bw_mounts = [
            f"cp {kernelpath} {nfs_mounting_dir}_dd_file_kernel",
            f"cp {fusepath} {nfs_mounting_dir}_dd_file_fuse",
            f"cp {nfspath} {kernel_mounting_dir_1}_dd_file_nfs1",
            f"cp {nfspath} {fuse_mounting_dir_1}_dd_file_nfs2",
        ]
        for cmd in mv_bw_mounts:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Confirm if data b/w mounts are migrated")
        verify_data_movement = [
            f"diff -qr {kernelpath} {nfs_mounting_dir}_dd_file_kernel",
            f"diff -qr {fusepath} {nfs_mounting_dir}_dd_file_fuse",
            f"diff -qr {nfspath} {kernel_mounting_dir_1}_dd_file_nfs1",
            f"diff -qr {nfspath} {fuse_mounting_dir_1}_dd_file_nfs2",
        ]
        for cmd in verify_data_movement:
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
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"umount -l {kernel_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"umount -l {fuse_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}", check_ec=False
        )
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


def run_ios(client, mounting_dir, file_name, bs, count):
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}{file_name} bs={bs} "
        f"count={count}",
    )
