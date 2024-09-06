import datetime
import json
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574009 - On a A/A NFS-HA cluster, ensure all existing snapshots are listed within the nfs export
    created on top of subvolume.
    Steps:
    1. Create the NFS cluster with the --ingress flag:
        Syntax
        ceph nfs cluster create CLUSTER-ID [PLACEMENT] [--port PORT_NUMBER] [--ingress --virtual-ip IP_ADDRESS]
    2. Validate the Service is UP
    3. Create subgroup and 2 subvolumes within
    4. Create 2 Exports one for each subvolume and perform NFS mount on subvolumes.
    5. Run IOs on both subvolumes for test duration
    6. On both subvolumes, Create 5 snapshots at minutely interval.
    7. Verify all 5 snapshots are listed in mount points on both volumes. Run Read IO on latest snapshot dir.
    8. Get active NFS node. While IO in-progress, run HA failover by rebooting active nfs node.
    9. Verify failover to new node succeeds. Verify snapshots on both subvolumes are accessible after failover.
    10.Run write IO on subvolumes. Create 2 new snapshots and verify.Perform Read IO on latest snapshot dir.
    11. Cleanup
    """
    try:
        tc = "CEPH-83574009"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        nfs_name = "cephfs-nfs"
        virtual_ip = "10.8.128.100"
        subnet = "21"

        log.info("Checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        log.info("Creating NFS HA cluster with --ingress flag")

        client1.exec_command(
            sudo=True,
            cmd=f'ceph nfs cluster create {nfs_name} "2 {nfs_servers[0].node.hostname} '
            f'{nfs_servers[1].node.hostname} {nfs_servers[2].node.hostname}" '
            f"--ingress --virtual-ip {virtual_ip}/{subnet}",
        )

        log.info("Validate that the HA services have started")
        fs_util.validate_services(client1, f"nfs.{nfs_name}")
        fs_util.validate_services(client1, f"ingress.nfs.{nfs_name}")

        log.info("Create subgroup and 2 subvolumes")
        subvolumegroup = {
            "vol_name": fs_name,
            "group_name": "subvolume_group1",
        }
        fs_util.create_subvolumegroup(client1, **subvolumegroup)

        subvol_list = []
        nfs_mount_list = []
        export_name_list = []
        snap_list = []
        for i in range(2):
            subvol_name = "subvolume_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            subvolume = {
                "vol_name": fs_name,
                "subvol_name": subvol_name,
                "group_name": "subvolume_group1",
            }
            fs_util.create_subvolume(client1, **subvolume)
            subvol_list.append(subvol_name)
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} {subvol_name} subvolume_group1",
            )
            export_path = f"{subvol_path}"
            nfs_export_name = "/export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            log.info(f"Export create for subvolume {subvol_name}")
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )
            export_name_list.append(nfs_export_name)
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}"
            )
            json.loads(out)

            if nfs_export_name not in out:
                raise CommandFailed("Failed to create nfs export")

        log.info("Perform NFS mount of subvolumes")
        for nfs_export_name in export_name_list:
            nfs_mounting_dir = "/mnt/nfs_" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(3)
            )
            client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            command = f"mount -t nfs -o port=2049 {virtual_ip}:{nfs_export_name} {nfs_mounting_dir}"
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
            nfs_mount_list.append(nfs_mounting_dir)

        log.info("Run IOs on both subvolumes")
        dir_name = "dd_io_dir"
        for mount in nfs_mount_list:
            client1.exec_command(sudo=True, cmd=f"mkdir -p {mount}/{dir_name}")

        log.info("Create snapshots on subvolumes at regular intervals during IO")
        snap_list = [f"nfs_snap_{i}" for i in range(5)]
        with parallel() as p:
            p.spawn(write_io_wrapper, client1, nfs_mount_list, dir_name, 2)
            p.spawn(create_snapshot_wrapper, client1, fs_util, subvol_list, snap_list)

        log.info("Verify all snapshots are created")
        for subvol in subvol_list:
            for snap_name in snap_list:
                snap_info_cmd = (
                    f"ceph fs subvolume snapshot info {fs_name} {subvol} {snap_name}"
                )
                snap_info_cmd = f"{snap_info_cmd} --group_name subvolume_group1"
                cmd_out, cmd_rc = client1.exec_command(
                    sudo=True, cmd=snap_info_cmd, check_ec=True
                )
                log.info(cmd_out)

        log.info("Verify all 5 snapshots are listed in mount points on both volumes")
        validate_snap_nfs(client1, nfs_mount_list, snap_list)

        log.info("Run Read IO from snapshot dir on both subvolumes")

        for mount in nfs_mount_list:
            snap_name = get_snap_name(client1, f"{mount}/.snap", snap_list[-1])
            io_path = f"{mount}/.snap/{snap_name}/{dir_name}"
            read_io(client1, io_path)

        log.info("Run HA failover")
        active_nfs_before = fs_util.get_active_nfs(nfs_servers, virtual_ip)
        backend_server = fs_util.get_active_nfs_server(client1, nfs_name, ceph_cluster)
        log.info(backend_server)

        log.info(f"Active NFS server before reboot:{active_nfs_before.node.hostname}")

        log.info("Active backend servers before reboot:")
        for server in backend_server:
            log.info(f"{server.node.hostname}")

        log.info("Rebooting NFS nodes sequentially")
        dir_name = "dd_io_dir_failover"
        for mount in nfs_mount_list:
            client1.exec_command(sudo=True, cmd=f"mkdir -p {mount}/{dir_name}")
        with parallel() as p:
            p.spawn(write_io_wrapper, client1, nfs_mount_list, dir_name, 6)
            for i in backend_server:
                fs_util.reboot_node(i, timeout=300)
                time.sleep(120)

        log.info("Verify snapshots are accessible on both subvolumes")
        validate_snap_nfs(client1, nfs_mount_list, snap_list)

        active_nfs_after = fs_util.get_active_nfs(nfs_servers, virtual_ip)
        backend_server = fs_util.get_active_nfs_server(client1, nfs_name, ceph_cluster)
        log.info(backend_server)

        log.info(f"Active NFS server after reboot:{active_nfs_after.node.hostname}")
        log.info("Active backend servers after reboot:")
        for server in backend_server:
            log.info(f"{server.node.hostname}")

        log.info("Run IOs on both subvolumes")
        dir_name = "dd_io_dir_after"
        for mount in nfs_mount_list:
            client1.exec_command(sudo=True, cmd=f"mkdir -p {mount}/{dir_name}")

        snap_list_add = [f"nfs_snap_{i}" for i in range(5, 7)]
        with parallel() as p:
            p.spawn(write_io_wrapper, client1, nfs_mount_list, dir_name, 1)
            p.spawn(
                create_snapshot_wrapper, client1, fs_util, subvol_list, snap_list_add
            )

        # Wait for sometime for new snaps to be listed in .snap dir
        time.sleep(60)
        snap_list.extend(snap_list_add)

        log.info("Verify 2 new snapshots are listed in mount points on both volumes")
        validate_snap_nfs(client1, nfs_mount_list, snap_list_add)

        log.info("Run Read IO from snapshot dir on both subvolumes")

        for mount in nfs_mount_list:
            snap_name = get_snap_name(client1, f"{mount}/.snap", snap_list[-1])
            io_path = f"{mount}/.snap/{snap_name}/{dir_name}"
            read_io(client1, io_path)

        log.info(f"Test {tc} passed")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        log.info("Remove snapshots")
        for subvol in subvol_list:
            if snap_list:
                for snap_name in snap_list:
                    snapshot = {
                        "vol_name": fs_name,
                        "subvol_name": subvol,
                        "snap_name": snap_name,
                        "group_name": "subvolume_group1",
                    }
                    fs_util.remove_snapshot(client1, **snapshot)

        log.info("Unmount NFS export")
        for mount in nfs_mount_list:
            client1.exec_command(sudo=True, cmd=f"umount -l {mount}", check_ec=False)
        log.info("Removing the Export")
        for nfs_export_name in export_name_list:
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
        log.info("Removing Subvolumes and Subvolumegroup")
        for subvol in subvol_list:
            subvolume = {
                "vol_name": fs_name,
                "subvol_name": subvol,
                "group_name": "subvolume_group1",
            }
            fs_util.remove_subvolume(client1, **subvolume)
        fs_util.remove_subvolumegroup(client1, **subvolumegroup)


def validate_snap_nfs(client, mount_list, snap_list):
    for mount in mount_list:
        cmd_out, cmd_rc = client.exec_command(sudo=True, cmd=f"ls {mount}/.snap")
        log.info(cmd_out)
        for snap_name in snap_list:
            if snap_name not in cmd_out:
                cmd_list = [
                    "ceph orch ps --daemon_type=nfs",
                    "ceph nfs cluster info cephfs-nfs",
                ]
                for cmd in cmd_list:
                    client.exec_command(sudo=True, cmd=cmd)
                log.error(f"Failed to view snapshot {snap_name} in NFS mount {mount}")


def create_snapshot_wrapper(client1, fs_util, subvol_list, snap_list):
    with parallel() as p:
        for subvol in subvol_list:
            p.spawn(create_snapshot, client1, fs_util, subvol, snap_list)


def create_snapshot(client1, fs_util, subvol, snap_list):
    for snap_name in snap_list:
        snapshot = {
            "vol_name": "cephfs",
            "subvol_name": subvol,
            "snap_name": snap_name,
            "group_name": "subvolume_group1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        time.sleep(20)


def write_io_wrapper(client1, nfs_mount_list, dir_name, run_time):
    with parallel() as p:
        for mount in nfs_mount_list:
            p.spawn(write_io, client1, mount, dir_name, run_time)


def write_io(client1, mount_dir, dir_name, run_time):
    end_time = datetime.datetime.now() + datetime.timedelta(minutes=run_time)
    i = 0
    while datetime.datetime.now() < end_time:
        log.info(f"Iteration : {i}")
        client1.exec_command(
            sudo=True, cmd=f"touch {mount_dir}/{dir_name}/test_{i}.txt"
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/random of={mount_dir}/{dir_name}/test_{i}.txt bs=10M "
            "count=100",
        )
        log.info(out)
        i += 1


def read_io(client1, io_path):
    out, rc = client1.exec_command(sudo=True, cmd=f"ls {io_path}")
    test_files = out.split()
    for test_file in test_files:
        try:
            client1.exec_command(sudo=True, cmd=f"dd if={io_path}/{test_file}")
        except Exception as e:
            log.info(e)


def get_snap_name(client, path, name):
    out, rc = client.exec_command(sudo=True, cmd=f"ls {path}")
    snaps = out.split()
    for snap in snaps:
        if name in snap:
            return snap
    return 0
