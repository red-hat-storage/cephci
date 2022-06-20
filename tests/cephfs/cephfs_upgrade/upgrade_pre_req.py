import json
import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Create multiple file systems(include EC-pool).
    Collect the information of MDS(like which is active mds node and standby mds nodes details)
    Collect the status of the cluster and version
    Create subvolumegroups, subvolumes
    Mount using fuse and kernel mount different subvolumes and also cephfs root folder on to different locations.
    Create a sample file, folder and get the stat details of it.
    Create NFS cluster and mount it. → 5.0 feature
    Taking snapshots and clones of the volume (Scheduled snapshots, retain snapshots → 5.0 feature).
    Client authorize and subvolume authorize feature
    Dir pinning and set quota to the subvolume
    Run IOs on all the mount points from a different client machine.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = "cephfs"
        if build.startswith("4"):
            # create EC pool
            list_cmds = [
                "ceph fs flag set enable_multiple true",
                "ceph osd pool create cephfs-data-ec 64 erasure",
                "ceph osd pool create cephfs-metadata 64",
                "ceph osd pool set cephfs-data-ec allow_ec_overwrites true",
                "ceph fs new cephfs-ec cephfs-metadata cephfs-data-ec --force",
            ]
            if fs_util.get_fs_info(clients[0], "cephfs_new"):
                default_fs = "cephfs_new"
                list_cmds.append("ceph fs volume create cephfs")
            for cmd in list_cmds:
                clients[0].exec_command(sudo=True, cmd=cmd)
        upgrade_config = None
        vol_list = [default_fs, "cephfs-ec"]
        with open(
            "/home/amk/Desktop/upgrade_cephfs/cephci/tests/cephfs/cephfs_upgrade/config.json",
            "r",
        ) as f:
            upgrade_config = json.load(f)
        svg_list = [
            f"{upgrade_config.get('subvolume_group_prefix','upgrade_svg')}_{svg}"
            for svg in range(0, upgrade_config.get("subvolume_group_count", 3))
        ]
        subvolumegroup_list = [
            {"vol_name": v, "group_name": svg} for v in vol_list for svg in svg_list
        ]
        log.info(subvolumegroup_list)
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": v,
                "group_name": svg,
                "subvol_name": f"{upgrade_config.get('subvolume_prefix','upgrade_sv')}_{sv}",
            }
            for v in vol_list
            for svg in svg_list
            for sv in range(0, upgrade_config.get("subvolume_count", 3))
        ]

        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        mount_points = {"kernel_mounts": [], "fuse_mounts": [], "nfs_mounts": []}
        for sv in subvolume_list[: len(subvolume_list) // 2]:
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {sv['vol_name']} {sv['subvol_name']} {sv['group_name']}",
            )
            mon_node_ips = fs_util.get_mon_node_ips()
            kernel_mounting_dir_1 = (
                f"/mnt/cephfs_kernel{mounting_dir}_{sv['vol_name']}_{sv['group_name']}_"
                f"{sv['subvol_name']}/"
            )
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_1,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path.read().decode().strip()}",
            )
            mount_points["kernel_mounts"].append(kernel_mounting_dir_1)
        for sv in subvolume_list[len(subvolume_list) // 2 :]:
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {sv['vol_name']} {sv['subvol_name']} {sv['group_name']}",
            )
            fuse_mounting_dir_1 = (
                f"/mnt/cephfs_fuse{mounting_dir}_{sv['vol_name']}_{sv['group_name']}_"
                f"{sv['subvol_name']}/"
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
                extra_params=f"-r {subvol_path.read().decode().strip()} --client_fs {sv['vol_name']}",
            )
            mount_points["fuse_mounts"].append(fuse_mounting_dir_1)
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        out, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if fs_util.wait_for_nfs_process(
            client=clients[0], process_name=nfs_name, ispresent=True
        ):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs"
        nfs_mounting_dir = "/mnt/nfs/"
        dir_name = "dir"
        if build.startswith("5"):
            clients[0].exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_name} path={export_path}",
            )
        else:
            clients[0].exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )
            # Verify ceph nfs export is created
            out, rc = clients[0].exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )
            output = out.read().decode()
            output.split()
            if nfs_export_name in output:
                log.info("ceph nfs export created successfully")
            else:
                raise CommandFailed("Failed to create nfs export")
            # Mount ceph nfs exports
            clients[0].exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            assert fs_util.wait_for_cmd_to_succeed(
                clients[0],
                cmd=f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}",
            )
            clients[0].exec_command(
                sudo=True,
                cmd=f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}",
            )
            out, rc = clients[0].exec_command(cmd="mount")
            mount_output = out.read().decode()
            mount_output = mount_output.split()
            log.info("Checking if nfs mount is is passed of failed:")
            assert nfs_mounting_dir.rstrip("/") in mount_output
            log.info("Creating Directory")
            out, rc = clients[0].exec_command(
                sudo=True, cmd=f"mkdir {nfs_mounting_dir}{dir_name}"
            )
        # with parallel() as p:
        for i in mount_points["kernel_mounts"] + mount_points["fuse_mounts"]:
            fs_util.run_ios(clients[0], i)

        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
